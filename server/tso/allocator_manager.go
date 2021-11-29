// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/member"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	GlobalDCLocation            = "global"
	checkStep                   = 1 * time.Minute
	patrolStep                  = 1 * time.Second
	defaultAllocatorLeaderLease = 3
	leaderTickInterval          = 50 * time.Millisecond
	localTSOSuffixEtcdPrefix    = "local-tso-suffix"
)

var (
	// PriorityCheck exported is only for test.
	PriorityCheck = 1 * time.Minute
)

// AllocatorGroupFilter is used to select AllocatorGroup.
type AllocatorGroupFilter func(ag *allocatorGroup) bool

type allocatorGroup struct {
	dcLocation string
	// ctx is built with cancel from a parent context when set up which can be different
	// in order to receive Done() signal correctly.
	// cancel would be call when allocatorGroup is deleted to stop background loop.
	ctx    context.Context
	cancel context.CancelFunc
	// For the Global TSO Allocator, leadership is a PD leader's
	// leadership, and for the Local TSO Allocator, leadership
	// is a DC-level certificate to allow an allocator to generate
	// TSO for local transactions in its DC.
	leadership *election.Leadership
	allocator  Allocator
}

// DCLocationInfo is used to record some dc-location related info,
// such as suffix sign and server IDs in this dc-location.
type DCLocationInfo struct {
	// dc-location/global (string) -> Member IDs
	ServerIDs []uint64
	// dc-location (string) -> Suffix sign. It is collected and maintained by the PD leader.
	Suffix int32
}

func (info *DCLocationInfo) clone() DCLocationInfo {
	copiedInfo := DCLocationInfo{
		Suffix: info.Suffix,
	}
	// Make a deep copy here for the slice
	copiedInfo.ServerIDs = make([]uint64, len(info.ServerIDs))
	copy(copiedInfo.ServerIDs, info.ServerIDs)
	return copiedInfo
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	enableLocalTSO bool
	mu             struct {
		sync.RWMutex
		// There are two kinds of TSO Allocators:
		//   1. Global TSO Allocator, as a global single point to allocate
		//      TSO for global transactions, such as cross-region cases.
		//   2. Local TSO Allocator, servers for DC-level transactions.
		// dc-location/global (string) -> TSO Allocator
		allocatorGroups    map[string]*allocatorGroup
		clusterDCLocations map[string]*DCLocationInfo
		// The max suffix sign we have so far, it will be used to calculate
		// the number of suffix bits we need in the TSO logical part.
		maxSuffix int32
	}
	wg sync.WaitGroup
	// for election use
	member *member.Member
	// TSO config
	rootPath               string
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	maxResetTSGap          func() time.Duration
	securityConfig         *grpcutil.TLSConfig
	// for gRPC use
	localAllocatorConn struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
	}
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(
	m *member.Member,
	rootPath string,
	cfg *config.Config,
	maxResetTSGap func() time.Duration,
) *AllocatorManager {
	allocatorManager := &AllocatorManager{
		enableLocalTSO:         cfg.EnableLocalTSO,
		member:                 m,
		rootPath:               rootPath,
		saveInterval:           cfg.TSOSaveInterval.Duration,
		updatePhysicalInterval: cfg.TSOUpdatePhysicalInterval.Duration,
		maxResetTSGap:          maxResetTSGap,
		securityConfig:         &cfg.Security.TLSConfig,
	}
	allocatorManager.mu.allocatorGroups = make(map[string]*allocatorGroup)
	allocatorManager.mu.clusterDCLocations = make(map[string]*DCLocationInfo)
	allocatorManager.localAllocatorConn.clientConns = make(map[string]*grpc.ClientConn)
	return allocatorManager
}

// SetLocalTSOConfig receives the zone label of this PD server and write it into etcd as dc-location
// to make the whole cluster know the DC-level topology for later Local TSO Allocator campaign.
func (am *AllocatorManager) SetLocalTSOConfig(dcLocation string) error {
	serverName := am.member.Member().Name
	serverID := am.member.ID()
	if err := am.checkDCLocationUpperLimit(dcLocation); err != nil {
		log.Error("check dc-location upper limit failed",
			zap.Int("upper-limit", int(math.Pow(2, MaxSuffixBits))-1),
			zap.String("dc-location", dcLocation),
			zap.String("server-name", serverName),
			zap.Uint64("server-id", serverID),
			errs.ZapError(err))
		return err
	}
	// The key-value pair in etcd will be like: serverID -> dcLocation
	dcLocationKey := am.member.GetDCLocationPath(serverID)
	resp, err := kv.
		NewSlowLogTxn(am.member.Client()).
		Then(clientv3.OpPut(dcLocationKey, dcLocation)).
		Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		log.Warn("write dc-location configuration into etcd failed",
			zap.String("dc-location", dcLocation),
			zap.String("server-name", serverName),
			zap.Uint64("server-id", serverID))
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	log.Info("write dc-location configuration into etcd",
		zap.String("dc-location", dcLocation),
		zap.String("server-name", serverName),
		zap.Uint64("server-id", serverID))
	go am.ClusterDCLocationChecker()
	return nil
}

func (am *AllocatorManager) checkDCLocationUpperLimit(dcLocation string) error {
	clusterDCLocations, err := am.GetClusterDCLocationsFromEtcd()
	if err != nil {
		return err
	}
	// It's ok to add a new PD to the old dc-location.
	if _, ok := clusterDCLocations[dcLocation]; ok {
		return nil
	}
	// Check whether the dc-location number meets the upper limit 2**(LogicalBits-1)-1,
	// which includes 1 global and 2**(LogicalBits-1) local
	if len(clusterDCLocations) == int(math.Pow(2, MaxSuffixBits))-1 {
		return errs.ErrSetLocalTSOConfig.FastGenByArgs("the number of dc-location meets the upper limit")
	}
	return nil
}

// GetClusterDCLocationsFromEtcd fetches dcLocation topology from etcd
func (am *AllocatorManager) GetClusterDCLocationsFromEtcd() (clusterDCLocations map[string][]uint64, err error) {
	resp, err := etcdutil.EtcdKVGet(
		am.member.Client(),
		am.member.GetDCLocationPathPrefix(),
		clientv3.WithPrefix())
	if err != nil {
		return clusterDCLocations, err
	}
	clusterDCLocations = make(map[string][]uint64)
	for _, kv := range resp.Kvs {
		// The key will contain the member ID and the value is its dcLocation
		serverPath := strings.Split(string(kv.Key), "/")
		// Get serverID from serverPath, e.g, /pd/dc-location/1232143243253 -> 1232143243253
		serverID, err := strconv.ParseUint(serverPath[len(serverPath)-1], 10, 64)
		dcLocation := string(kv.Value)
		if err != nil {
			log.Warn("get server id and dcLocation from etcd failed, invalid server id",
				zap.Any("splitted-serverPath", serverPath),
				zap.String("dc-location", dcLocation),
				errs.ZapError(err))
			continue
		}
		clusterDCLocations[dcLocation] = append(clusterDCLocations[dcLocation], serverID)
	}
	return clusterDCLocations, nil
}

// GetDCLocationInfo returns a copy of DCLocationInfo of the given dc-location,
func (am *AllocatorManager) GetDCLocationInfo(dcLocation string) (DCLocationInfo, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	infoPtr, ok := am.mu.clusterDCLocations[dcLocation]
	if !ok {
		return DCLocationInfo{}, false
	}
	return infoPtr.clone(), true
}

// GetClusterDCLocations returns all dc-locations of a cluster with a copy of map,
// which satisfies dcLocation -> DCLocationInfo.
func (am *AllocatorManager) GetClusterDCLocations() map[string]DCLocationInfo {
	am.mu.RLock()
	defer am.mu.RUnlock()
	dcLocationMap := make(map[string]DCLocationInfo)
	for dcLocation, info := range am.mu.clusterDCLocations {
		dcLocationMap[dcLocation] = info.clone()
	}
	return dcLocationMap
}

// GetClusterDCLocationsNumber returns the number of cluster dc-locations.
func (am *AllocatorManager) GetClusterDCLocationsNumber() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.mu.clusterDCLocations)
}

// compareAndSetMaxSuffix sets the max suffix sign if suffix is greater than am.mu.maxSuffix.
func (am *AllocatorManager) compareAndSetMaxSuffix(suffix int32) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if suffix > am.mu.maxSuffix {
		am.mu.maxSuffix = suffix
	}
}

// GetSuffixBits calculates the bits of suffix sign
// by the max number of suffix so far,
// which will be used in the TSO logical part.
func (am *AllocatorManager) GetSuffixBits() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return CalSuffixBits(am.mu.maxSuffix)
}

// CalSuffixBits calculates the bits of suffix by the max suffix sign.
func CalSuffixBits(maxSuffix int32) int {
	// maxSuffix + 1 because we have the Global TSO holds 0 as the suffix sign
	return int(math.Ceil(math.Log2(float64(maxSuffix + 1))))
}

// SetUpAllocator is used to set up an allocator, which will initialize the allocator and put it into allocator daemon.
// One TSO Allocator should only be set once, and may be initialized and reset multiple times depending on the election.
func (am *AllocatorManager) SetUpAllocator(parentCtx context.Context, dcLocation string, leadership *election.Leadership) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if am.updatePhysicalInterval != config.DefaultTSOUpdatePhysicalInterval {
		log.Warn("tso update physical interval is non-default",
			zap.Duration("update-physical-interval", am.updatePhysicalInterval))
	}
	if _, exist := am.mu.allocatorGroups[dcLocation]; exist {
		return
	}
	var allocator Allocator
	if dcLocation == GlobalDCLocation {
		allocator = NewGlobalTSOAllocator(am, leadership)
	} else {
		allocator = NewLocalTSOAllocator(am, leadership, dcLocation)
	}
	// Create a new allocatorGroup
	ctx, cancel := context.WithCancel(parentCtx)
	am.mu.allocatorGroups[dcLocation] = &allocatorGroup{
		dcLocation: dcLocation,
		ctx:        ctx,
		cancel:     cancel,
		leadership: leadership,
		allocator:  allocator,
	}
	// Because the Global TSO Allocator only depends on PD leader's leadership,
	// so we can directly return here. The election and initialization process
	// will happen in server.campaignLeader().
	if dcLocation == GlobalDCLocation {
		return
	}
	// Start election of the Local TSO Allocator here
	localTSOAllocator, _ := allocator.(*LocalTSOAllocator)
	go am.allocatorLeaderLoop(parentCtx, localTSOAllocator)
}

func (am *AllocatorManager) getAllocatorPath(dcLocation string) string {
	// For backward compatibility, the global timestamp's store path will still use the old one
	if dcLocation == GlobalDCLocation {
		return am.rootPath
	}
	return path.Join(am.rootPath, dcLocation)
}

// similar logic with leaderLoop in server/server.go
func (am *AllocatorManager) allocatorLeaderLoop(ctx context.Context, allocator *LocalTSOAllocator) {
	defer log.Info("server is closed, return local tso allocator leader loop",
		zap.String("dc-location", allocator.GetDCLocation()),
		zap.String("local-tso-allocator-name", am.member.Member().Name))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check whether the Local TSO Allocator has the leader already
		allocatorLeader, rev, checkAgain := allocator.CheckAllocatorLeader()
		if checkAgain {
			continue
		}
		if allocatorLeader != nil {
			log.Info("start to watch allocator leader",
				zap.Stringer(fmt.Sprintf("%s-allocator-leader", allocator.GetDCLocation()), allocatorLeader),
				zap.String("local-tso-allocator-name", am.member.Member().Name))
			// WatchAllocatorLeader will keep looping and never return unless the Local TSO Allocator leader has changed.
			allocator.WatchAllocatorLeader(ctx, allocatorLeader, rev)
			log.Info("local tso allocator leader has changed, try to re-campaign a local tso allocator leader",
				zap.String("dc-location", allocator.GetDCLocation()))
		}

		// Check the next-leader key
		nextLeader, err := am.getNextLeaderID(allocator.GetDCLocation())
		if err != nil {
			log.Error("get next leader from etcd failed",
				zap.String("dc-location", allocator.GetDCLocation()),
				errs.ZapError(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		isNextLeader := false
		if nextLeader != 0 {
			if nextLeader != am.member.ID() {
				log.Info("skip campaigning of the local tso allocator leader and check later",
					zap.String("server-name", am.member.Member().Name),
					zap.Uint64("server-id", am.member.ID()),
					zap.Uint64("next-leader-id", nextLeader))
				time.Sleep(200 * time.Millisecond)
				continue
			}
			isNextLeader = true
		}

		// Make sure the leader is aware of this new dc-location in order to make the
		// Global TSO synchronization can cover up this dc-location.
		ok, dcLocationInfo, err := am.getDCLocationInfoFromLeader(ctx, allocator.GetDCLocation())
		if err != nil {
			log.Error("get dc-location info from pd leader failed",
				zap.String("dc-location", allocator.GetDCLocation()),
				errs.ZapError(err))
			// PD leader hasn't been elected out, wait for the campaign
			if !longSleep(ctx, time.Second) {
				return
			}
			continue
		}
		if !ok || dcLocationInfo.Suffix <= 0 || dcLocationInfo.MaxTs == nil {
			log.Warn("pd leader is not aware of dc-location during allocatorLeaderLoop, wait next round",
				zap.String("dc-location", allocator.GetDCLocation()),
				zap.Any("dc-location-info", dcLocationInfo),
				zap.String("wait-duration", checkStep.String()))
			// Because the checkStep is long, we use select here to check whether the ctx is done
			// to prevent the leak of goroutine.
			if !longSleep(ctx, checkStep) {
				return
			}
			continue
		}

		am.campaignAllocatorLeader(ctx, allocator, dcLocationInfo, isNextLeader)
	}
}

// longSleep is used to sleep the long wait duration while also watching the
// ctx.Done() to prevent the goroutine from leaking. This function returns
// true if the sleep is over, false if the ctx is done.
func longSleep(ctx context.Context, waitStep time.Duration) bool {
	waitTicker := time.NewTicker(waitStep)
	defer waitTicker.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-waitTicker.C:
		return true
	}
}

func (am *AllocatorManager) campaignAllocatorLeader(
	loopCtx context.Context,
	allocator *LocalTSOAllocator,
	dcLocationInfo *pdpb.GetDCLocationInfoResponse,
	isNextLeader bool,
) {
	log.Info("start to campaign local tso allocator leader",
		zap.String("dc-location", allocator.GetDCLocation()),
		zap.Any("dc-location-info", dcLocationInfo),
		zap.String("name", am.member.Member().Name))
	cmps := make([]clientv3.Cmp, 0)
	nextLeaderKey := am.nextLeaderKey(allocator.GetDCLocation())
	if !isNextLeader {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(nextLeaderKey), "=", 0))
	} else {
		nextLeaderValue := fmt.Sprintf("%v", am.member.ID())
		cmps = append(cmps, clientv3.Compare(clientv3.Value(nextLeaderKey), "=", nextLeaderValue))
	}
	failpoint.Inject("injectNextLeaderKey", func(val failpoint.Value) {
		if val.(bool) {
			// In order not to campaign leader too often in tests
			time.Sleep(5 * time.Second)
			cmps = []clientv3.Cmp{
				clientv3.Compare(clientv3.Value(nextLeaderKey), "=", "mockValue"),
			}
		}
	})
	if err := allocator.CampaignAllocatorLeader(defaultAllocatorLeaderLease, cmps...); err != nil {
		if err.Error() == errs.ErrEtcdTxnConflict.Error() {
			log.Info("failed to campaign local tso allocator leader due to txn conflict, another allocator may campaign successfully",
				zap.String("dc-location", allocator.GetDCLocation()),
				zap.Any("dc-location-info", dcLocationInfo),
				zap.String("name", am.member.Member().Name))
		} else {
			log.Error("failed to campaign local tso allocator leader due to etcd error",
				zap.String("dc-location", allocator.GetDCLocation()),
				zap.Any("dc-location-info", dcLocationInfo),
				zap.String("name", am.member.Member().Name),
				errs.ZapError(err))
		}
		return
	}

	// Start keepalive the Local TSO Allocator leadership and enable Local TSO service.
	ctx, cancel := context.WithCancel(loopCtx)
	defer cancel()
	defer am.ResetAllocatorGroup(allocator.GetDCLocation())
	// Maintain the Local TSO Allocator leader
	go allocator.KeepAllocatorLeader(ctx)
	log.Info("campaign local tso allocator leader ok",
		zap.String("dc-location", allocator.GetDCLocation()),
		zap.Any("dc-location-info", dcLocationInfo),
		zap.String("name", am.member.Member().Name))

	log.Info("initialize the local TSO allocator",
		zap.String("dc-location", allocator.GetDCLocation()),
		zap.Any("dc-location-info", dcLocationInfo),
		zap.String("name", am.member.Member().Name))
	if err := allocator.Initialize(int(dcLocationInfo.Suffix)); err != nil {
		log.Error("failed to initialize the local TSO allocator",
			zap.String("dc-location", allocator.GetDCLocation()),
			zap.Any("dc-location-info", dcLocationInfo),
			errs.ZapError(err))
		return
	}
	if dcLocationInfo.GetMaxTs().GetPhysical() != 0 {
		if err := allocator.WriteTSO(dcLocationInfo.GetMaxTs()); err != nil {
			log.Error("failed to write the max local TSO after member changed",
				zap.String("dc-location", allocator.GetDCLocation()),
				zap.Any("dc-location-info", dcLocationInfo),
				errs.ZapError(err))
			return
		}
	}
	am.compareAndSetMaxSuffix(dcLocationInfo.Suffix)
	allocator.EnableAllocatorLeader()
	// The next leader is me, delete it to finish campaigning
	am.deleteNextLeaderID(allocator.GetDCLocation())
	log.Info("local tso allocator leader is ready to serve",
		zap.String("dc-location", allocator.GetDCLocation()),
		zap.Any("dc-location-info", dcLocationInfo),
		zap.String("name", am.member.Member().Name))

	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !allocator.IsAllocatorLeader() {
				log.Info("no longer a local tso allocator leader because lease has expired, local tso allocator leader will step down",
					zap.String("dc-location", allocator.GetDCLocation()),
					zap.Any("dc-location-info", dcLocationInfo),
					zap.String("name", am.member.Member().Name))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed, reset the local tso allocator",
				zap.String("dc-location", allocator.GetDCLocation()),
				zap.Any("dc-location-info", dcLocationInfo),
				zap.String("name", am.member.Member().Name))
			return
		}
	}
}

// AllocatorDaemon is used to update every allocator's TSO and check whether we have
// any new local allocator that needs to be set up.
func (am *AllocatorManager) AllocatorDaemon(serverCtx context.Context) {
	// allocatorPatroller should only work when enableLocalTSO is true to
	// set up the new Local TSO Allocator in time.
	var patrolTicker = &time.Ticker{}
	if am.enableLocalTSO {
		patrolTicker = time.NewTicker(patrolStep)
		defer patrolTicker.Stop()
	}
	tsTicker := time.NewTicker(am.updatePhysicalInterval)
	defer tsTicker.Stop()
	checkerTicker := time.NewTicker(PriorityCheck)
	defer checkerTicker.Stop()

	for {
		select {
		case <-patrolTicker.C:
			// Inspect the cluster dc-location info and set up the new Local TSO Allocator in time.
			am.allocatorPatroller(serverCtx)
		case <-tsTicker.C:
			// Update the initialized TSO Allocator to advance TSO.
			am.allocatorUpdater()
		case <-checkerTicker.C:
			// Check and maintain the cluster's meta info about dc-location distribution.
			go am.ClusterDCLocationChecker()
			// We won't have any Local TSO Allocator set up in PD without enabling Local TSO.
			if am.enableLocalTSO {
				// Check the election priority of every Local TSO Allocator this PD is holding.
				go am.PriorityChecker()
			}
			// PS: ClusterDCLocationChecker and PriorityChecker are time consuming and low frequent to run,
			// we should run them concurrently to speed up the progress.
		case <-serverCtx.Done():
			return
		}
	}
}

// Update the Local TSO Allocator leaders TSO in memory concurrently.
func (am *AllocatorManager) allocatorUpdater() {
	// Filter out allocators without leadership and uninitialized
	allocatorGroups := am.getAllocatorGroups(FilterUninitialized(), FilterUnavailableLeadership())
	// Update each allocator concurrently
	for _, ag := range allocatorGroups {
		am.wg.Add(1)
		go am.updateAllocator(ag)
	}
	am.wg.Wait()
}

// updateAllocator is used to update the allocator in the group.
func (am *AllocatorManager) updateAllocator(ag *allocatorGroup) {
	defer am.wg.Done()
	select {
	case <-ag.ctx.Done():
		// Resetting the allocator will clear TSO in memory
		ag.allocator.Reset()
		return
	default:
	}
	if !ag.leadership.Check() {
		log.Info("allocator doesn't campaign leadership yet", zap.String("dc-location", ag.dcLocation))
		time.Sleep(200 * time.Millisecond)
		return
	}
	if err := ag.allocator.UpdateTSO(); err != nil {
		log.Warn("failed to update allocator's timestamp", zap.String("dc-location", ag.dcLocation), errs.ZapError(err))
		am.ResetAllocatorGroup(ag.dcLocation)
		return
	}
}

// Check if we have any new dc-location configured, if yes,
// then set up the corresponding local allocator.
func (am *AllocatorManager) allocatorPatroller(serverCtx context.Context) {
	// Collect all dc-locations
	dcLocations := am.GetClusterDCLocations()
	// Get all Local TSO Allocators
	allocatorGroups := am.getAllocatorGroups(FilterDCLocation(GlobalDCLocation))
	// Set up the new one
	for dcLocation := range dcLocations {
		if slice.NoneOf(allocatorGroups, func(i int) bool {
			return allocatorGroups[i].dcLocation == dcLocation
		}) {
			am.SetUpAllocator(serverCtx, dcLocation, election.NewLeadership(
				am.member.Client(),
				am.getAllocatorPath(dcLocation),
				fmt.Sprintf("%s local allocator leader election", dcLocation),
			))
		}
	}
	// Clean up the unused one
	for _, ag := range allocatorGroups {
		if _, exist := dcLocations[ag.dcLocation]; !exist {
			am.deleteAllocatorGroup(ag.dcLocation)
		}
	}
}

// ClusterDCLocationChecker collects all dc-locations of a cluster, computes some related info
// and stores them into the DCLocationInfo, then finally writes them into am.mu.clusterDCLocations.
func (am *AllocatorManager) ClusterDCLocationChecker() {
	// Wait for the PD leader to be elected out.
	if am.member.GetLeader() == nil {
		return
	}
	newClusterDCLocations, err := am.GetClusterDCLocationsFromEtcd()
	if err != nil {
		log.Error("get cluster dc-locations from etcd failed", errs.ZapError(err))
		return
	}
	am.mu.Lock()
	// Clean up the useless dc-locations
	for dcLocation := range am.mu.clusterDCLocations {
		if _, ok := newClusterDCLocations[dcLocation]; !ok {
			delete(am.mu.clusterDCLocations, dcLocation)
		}
	}
	// May be used to rollback the updating after
	newDCLocations := make([]string, 0)
	// Update the new dc-locations
	for dcLocation, serverIDs := range newClusterDCLocations {
		if _, ok := am.mu.clusterDCLocations[dcLocation]; !ok {
			am.mu.clusterDCLocations[dcLocation] = &DCLocationInfo{
				ServerIDs: serverIDs,
				Suffix:    -1,
			}
			newDCLocations = append(newDCLocations, dcLocation)
		}
	}
	// Only leader can write the TSO suffix to etcd in order to make it consistent in the cluster
	if am.member.IsLeader() {
		for dcLocation, info := range am.mu.clusterDCLocations {
			if info.Suffix > 0 {
				continue
			}
			suffix, err := am.getOrCreateLocalTSOSuffix(dcLocation)
			if err != nil {
				log.Warn("get or create the local tso suffix failed", zap.String("dc-location", dcLocation), errs.ZapError(err))
				continue
			}
			if suffix > am.mu.maxSuffix {
				am.mu.maxSuffix = suffix
			}
			am.mu.clusterDCLocations[dcLocation].Suffix = suffix
		}
	} else {
		// Follower should check and update the am.mu.maxSuffix
		maxSuffix, err := am.getMaxLocalTSOSuffix()
		if err != nil {
			log.Error("get the max local tso suffix from etcd failed", errs.ZapError(err))
			// Rollback the new dc-locations we update before
			for _, dcLocation := range newDCLocations {
				delete(am.mu.clusterDCLocations, dcLocation)
			}
		} else if maxSuffix > am.mu.maxSuffix {
			am.mu.maxSuffix = maxSuffix
		}
	}
	am.mu.Unlock()
}

// getOrCreateLocalTSOSuffix will check whether we have the Local TSO suffix written into etcd.
// If not, it will write a number into etcd according to the its joining order.
// If yes, it will just return the previous persisted one.
func (am *AllocatorManager) getOrCreateLocalTSOSuffix(dcLocation string) (int32, error) {
	// Try to get the suffix from etcd
	dcLocationSuffix, err := am.getDCLocationSuffixMapFromEtcd()
	if err != nil {
		return -1, nil
	}
	var maxSuffix int32
	for curDCLocation, suffix := range dcLocationSuffix {
		// If we already have the suffix persistted in etcd before,
		// just use it as the result directly.
		if curDCLocation == dcLocation {
			return suffix, nil
		}
		if suffix > maxSuffix {
			maxSuffix = suffix
		}
	}
	maxSuffix++
	localTSOSuffixKey := am.GetLocalTSOSuffixPath(dcLocation)
	// The Local TSO suffix is determined by the joining order of this dc-location.
	localTSOSuffixValue := strconv.FormatInt(int64(maxSuffix), 10)
	txnResp, err := kv.NewSlowLogTxn(am.member.Client()).
		If(clientv3.Compare(clientv3.CreateRevision(localTSOSuffixKey), "=", 0)).
		Then(clientv3.OpPut(localTSOSuffixKey, localTSOSuffixValue)).
		Commit()
	if err != nil {
		return -1, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !txnResp.Succeeded {
		log.Warn("write local tso suffix into etcd failed",
			zap.String("dc-location", dcLocation),
			zap.String("local-tso-surfix", localTSOSuffixValue),
			zap.String("server-name", am.member.Member().Name),
			zap.Uint64("server-id", am.member.ID()))
		return -1, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return maxSuffix, nil
}

func (am *AllocatorManager) getDCLocationSuffixMapFromEtcd() (map[string]int32, error) {
	resp, err := etcdutil.EtcdKVGet(
		am.member.Client(),
		am.GetLocalTSOSuffixPathPrefix(),
		clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	dcLocationSuffix := make(map[string]int32)
	for _, kv := range resp.Kvs {
		suffix, err := strconv.ParseInt(string(kv.Value), 10, 32)
		if err != nil {
			return nil, err
		}
		splittedKey := strings.Split(string(kv.Key), "/")
		dcLocation := splittedKey[len(splittedKey)-1]
		dcLocationSuffix[dcLocation] = int32(suffix)
	}
	return dcLocationSuffix, nil
}

func (am *AllocatorManager) getMaxLocalTSOSuffix() (int32, error) {
	// Try to get the suffix from etcd
	dcLocationSuffix, err := am.getDCLocationSuffixMapFromEtcd()
	if err != nil {
		return -1, err
	}
	var maxSuffix int32
	for _, suffix := range dcLocationSuffix {
		if suffix > maxSuffix {
			maxSuffix = suffix
		}
	}
	return maxSuffix, nil
}

// GetLocalTSOSuffixPathPrefix returns the etcd key prefix of the Local TSO suffix for the given dc-location.
func (am *AllocatorManager) GetLocalTSOSuffixPathPrefix() string {
	return path.Join(am.rootPath, localTSOSuffixEtcdPrefix)
}

// GetLocalTSOSuffixPath returns the etcd key of the Local TSO suffix for the given dc-location.
func (am *AllocatorManager) GetLocalTSOSuffixPath(dcLocation string) string {
	return path.Join(am.GetLocalTSOSuffixPathPrefix(), dcLocation)
}

// PriorityChecker is used to check the election priority of a Local TSO Allocator.
// In the normal case, if we want to elect a Local TSO Allocator for a certain DC,
// such as dc-1, we need to make sure the follow priority rules:
// 1. The PD server with dc-location="dc-1" needs to be elected as the allocator
// leader with the highest priority.
// 2. If all PD servers with dc-location="dc-1" are down, then the other PD servers
// of DC could be elected.
func (am *AllocatorManager) PriorityChecker() {
	serverID := am.member.ID()
	myServerDCLocation := am.getServerDCLocation(serverID)
	// Check all Local TSO Allocator followers to see if their priorities is higher than the leaders
	// Filter out allocators with leadership and initialized
	allocatorGroups := am.getAllocatorGroups(FilterDCLocation(GlobalDCLocation), FilterAvailableLeadership())
	for _, allocatorGroup := range allocatorGroups {
		localTSOAllocator, _ := allocatorGroup.allocator.(*LocalTSOAllocator)
		leaderServerID := localTSOAllocator.GetAllocatorLeader().GetMemberId()
		// No leader, maybe the leader is not been watched yet
		if leaderServerID == 0 {
			continue
		}
		leaderServerDCLocation := am.getServerDCLocation(leaderServerID)
		// For example, an allocator leader for dc-1 is elected by a server of dc-2, then the server of dc-1 will
		// find this allocator's dc-location isn't the same with server of dc-2 but is same with itself.
		if allocatorGroup.dcLocation != leaderServerDCLocation && allocatorGroup.dcLocation == myServerDCLocation {
			log.Info("try to move the local tso allocator",
				zap.Uint64("old-leader-id", leaderServerID),
				zap.String("old-dc-location", leaderServerDCLocation),
				zap.Uint64("next-leader-id", serverID),
				zap.String("next-dc-location", myServerDCLocation))
			if err := am.transferLocalAllocator(allocatorGroup.dcLocation, am.member.ID()); err != nil {
				log.Error("move the local tso allocator failed",
					zap.Uint64("old-leader-id", leaderServerID),
					zap.String("old-dc-location", leaderServerDCLocation),
					zap.Uint64("next-leader-id", serverID),
					zap.String("next-dc-location", myServerDCLocation),
					errs.ZapError(err))
				continue
			}
		}
	}
	// Check next leader and resign
	// Filter out allocators with leadership
	allocatorGroups = am.getAllocatorGroups(FilterDCLocation(GlobalDCLocation), FilterUnavailableLeadership())
	for _, allocatorGroup := range allocatorGroups {
		nextLeader, err := am.getNextLeaderID(allocatorGroup.dcLocation)
		if err != nil {
			log.Error("get next leader from etcd failed",
				zap.String("dc-location", allocatorGroup.dcLocation),
				errs.ZapError(err))
			continue
		}
		// nextLeader is not empty and isn't same with the server ID, resign the leader
		if nextLeader != 0 && nextLeader != serverID {
			log.Info("next leader key found, resign current leader", zap.Uint64("nextLeaderID", nextLeader))
			am.ResetAllocatorGroup(allocatorGroup.dcLocation)
		}
	}
}

// TransferAllocatorForDCLocation transfer local tso allocator to the target member for the given dcLocation
func (am *AllocatorManager) TransferAllocatorForDCLocation(dcLocation string, memberID uint64) error {
	if dcLocation == GlobalDCLocation {
		return fmt.Errorf("dc-location %v should be transferred by transfer leader", dcLocation)
	}
	dcLocationsInfo := am.GetClusterDCLocations()
	_, ok := dcLocationsInfo[dcLocation]
	if !ok {
		return fmt.Errorf("dc-location %v haven't been discovered yet", dcLocation)
	}
	allocator, err := am.GetAllocator(dcLocation)
	if err != nil {
		return err
	}
	localTSOAllocator, _ := allocator.(*LocalTSOAllocator)
	leaderServerID := localTSOAllocator.GetAllocatorLeader().GetMemberId()
	if leaderServerID == memberID {
		return nil
	}
	return am.transferLocalAllocator(dcLocation, memberID)
}

func (am *AllocatorManager) getServerDCLocation(serverID uint64) string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	for dcLocation, info := range am.mu.clusterDCLocations {
		if slice.AnyOf(info.ServerIDs, func(i int) bool { return info.ServerIDs[i] == serverID }) {
			return dcLocation
		}
	}
	return ""
}

func (am *AllocatorManager) getNextLeaderID(dcLocation string) (uint64, error) {
	nextLeaderKey := am.nextLeaderKey(dcLocation)
	nextLeaderValue, err := etcdutil.GetValue(am.member.Client(), nextLeaderKey)
	if err != nil {
		return 0, err
	}
	if len(nextLeaderValue) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(string(nextLeaderValue), 10, 64)
}

func (am *AllocatorManager) deleteNextLeaderID(dcLocation string) error {
	nextLeaderKey := am.nextLeaderKey(dcLocation)
	resp, err := kv.NewSlowLogTxn(am.member.Client()).
		Then(clientv3.OpDelete(nextLeaderKey)).
		Commit()
	if err != nil {
		return errs.ErrEtcdKVDelete.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// deleteAllocatorGroup should only be used to remove the unused Local TSO Allocator from an unused dc-location.
// If you want to clear or reset a TSO allocator, use (*AllocatorManager).ResetAllocatorGroup.
func (am *AllocatorManager) deleteAllocatorGroup(dcLocation string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		allocatorGroup.leadership.Reset()
		allocatorGroup.cancel()
		delete(am.mu.allocatorGroups, dcLocation)
	}
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleTSORequest(dcLocation string, count uint32) (pdpb.Timestamp, error) {
	if dcLocation == "" {
		dcLocation = GlobalDCLocation
	}
	allocatorGroup, exist := am.getAllocatorGroup(dcLocation)
	if !exist {
		err := errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found, generate timestamp failed", dcLocation))
		return pdpb.Timestamp{}, err
	}
	return allocatorGroup.allocator.GenerateTSO(count)
}

// ResetAllocatorGroup will reset the allocator's leadership and TSO initialized in memory.
// It usually should be called before re-triggering an Allocator leader campaign.
func (am *AllocatorManager) ResetAllocatorGroup(dcLocation string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
		if allocatorGroup.leadership.Check() {
			allocatorGroup.leadership.Reset()
		}
	}
}

func (am *AllocatorManager) getAllocatorGroups(filters ...AllocatorGroupFilter) []*allocatorGroup {
	am.mu.RLock()
	defer am.mu.RUnlock()
	var allocatorGroups []*allocatorGroup
	for _, ag := range am.mu.allocatorGroups {
		if ag == nil {
			continue
		}
		if slice.NoneOf(filters, func(i int) bool { return filters[i](ag) }) {
			allocatorGroups = append(allocatorGroups, ag)
		}
	}
	return allocatorGroups
}

func (am *AllocatorManager) getAllocatorGroup(dcLocation string) (*allocatorGroup, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]
	return allocatorGroup, exist
}

// GetAllocator get the allocator by dc-location.
func (am *AllocatorManager) GetAllocator(dcLocation string) (Allocator, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]
	if !exist {
		return nil, errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found", dcLocation))
	}
	return allocatorGroup.allocator, nil
}

// GetAllocators get all allocators with some filters.
func (am *AllocatorManager) GetAllocators(filters ...AllocatorGroupFilter) []Allocator {
	allocatorGroups := am.getAllocatorGroups(filters...)
	allocators := make([]Allocator, 0, len(allocatorGroups))
	for _, ag := range allocatorGroups {
		allocators = append(allocators, ag.allocator)
	}
	return allocators
}

// GetHoldingLocalAllocatorLeaders returns all Local TSO Allocator leaders this server holds.
func (am *AllocatorManager) GetHoldingLocalAllocatorLeaders() ([]*LocalTSOAllocator, error) {
	localAllocators := am.GetAllocators(
		FilterDCLocation(GlobalDCLocation),
		FilterUnavailableLeadership())
	localAllocatorLeaders := make([]*LocalTSOAllocator, 0, len(localAllocators))
	for _, localAllocator := range localAllocators {
		localAllocatorLeader, ok := localAllocator.(*LocalTSOAllocator)
		if !ok {
			return nil, errs.ErrGetLocalAllocator.FastGenByArgs("invalid local tso allocator found")
		}
		localAllocatorLeaders = append(localAllocatorLeaders, localAllocatorLeader)
	}
	return localAllocatorLeaders, nil
}

// GetLocalAllocatorLeaders returns all Local TSO Allocator leaders' member info.
func (am *AllocatorManager) GetLocalAllocatorLeaders() (map[string]*pdpb.Member, error) {
	localAllocators := am.GetAllocators(FilterDCLocation(GlobalDCLocation))
	localAllocatorLeaderMember := make(map[string]*pdpb.Member)
	for _, allocator := range localAllocators {
		localAllocator, ok := allocator.(*LocalTSOAllocator)
		if !ok {
			return nil, errs.ErrGetLocalAllocator.FastGenByArgs("invalid local tso allocator found")
		}
		localAllocatorLeaderMember[localAllocator.GetDCLocation()] = localAllocator.GetAllocatorLeader()
	}
	return localAllocatorLeaderMember, nil
}

func (am *AllocatorManager) getOrCreateGRPCConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, ok := am.getGRPCConn(addr)
	if ok {
		return conn, nil
	}
	tlsCfg, err := am.securityConfig.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cc, err := grpcutil.GetClientConn(ctxWithTimeout, addr, tlsCfg)
	if err != nil {
		return nil, err
	}
	am.setGRPCConn(cc, addr)
	conn, _ = am.getGRPCConn(addr)
	return conn, nil
}

func (am *AllocatorManager) getDCLocationInfoFromLeader(ctx context.Context, dcLocation string) (bool, *pdpb.GetDCLocationInfoResponse, error) {
	if am.member.IsLeader() {
		info, ok := am.GetDCLocationInfo(dcLocation)
		if !ok {
			return false, &pdpb.GetDCLocationInfoResponse{}, nil
		}
		dcLocationInfo := &pdpb.GetDCLocationInfoResponse{Suffix: info.Suffix}
		var err error
		if dcLocationInfo.MaxTs, err = am.GetMaxLocalTSO(ctx); err != nil {
			return false, &pdpb.GetDCLocationInfoResponse{}, err
		}
		return ok, dcLocationInfo, nil
	}

	leaderAddrs := am.member.GetLeader().GetClientUrls()
	if leaderAddrs == nil || len(leaderAddrs) < 1 {
		return false, &pdpb.GetDCLocationInfoResponse{}, fmt.Errorf("failed to get leader client url")
	}
	conn, err := am.getOrCreateGRPCConn(ctx, leaderAddrs[0])
	if err != nil {
		return false, &pdpb.GetDCLocationInfoResponse{}, err
	}
	getCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := pdpb.NewPDClient(conn).GetDCLocationInfo(getCtx, &pdpb.GetDCLocationInfoRequest{
		Header: &pdpb.RequestHeader{
			SenderId: am.member.Member().GetMemberId(),
		},
		DcLocation: dcLocation,
	})
	if err != nil {
		return false, &pdpb.GetDCLocationInfoResponse{}, err
	}
	return resp.GetSuffix() != 0, resp, nil
}

// GetMaxLocalTSO will sync with the current Local TSO Allocators among the cluster to get the
// max Local TSO.
func (am *AllocatorManager) GetMaxLocalTSO(ctx context.Context) (*pdpb.Timestamp, error) {
	// Sync the max local TSO from the other Local TSO Allocators who has been initialized
	clusterDCLocations := am.GetClusterDCLocations()
	for dcLocation := range clusterDCLocations {
		allocatorGroup, ok := am.getAllocatorGroup(dcLocation)
		if !(ok && allocatorGroup.leadership.Check()) {
			delete(clusterDCLocations, dcLocation)
		}
	}
	maxTSO := &pdpb.Timestamp{}
	if len(clusterDCLocations) == 0 {
		return maxTSO, nil
	}
	globalAllocator, err := am.GetAllocator(GlobalDCLocation)
	if err != nil {
		return nil, err
	}
	if err := globalAllocator.(*GlobalTSOAllocator).SyncMaxTS(ctx, clusterDCLocations, maxTSO, false); err != nil {
		return nil, err
	}
	return maxTSO, nil
}

func (am *AllocatorManager) getGRPCConn(addr string) (*grpc.ClientConn, bool) {
	am.localAllocatorConn.RLock()
	defer am.localAllocatorConn.RUnlock()
	conn, ok := am.localAllocatorConn.clientConns[addr]
	return conn, ok
}

func (am *AllocatorManager) setGRPCConn(newConn *grpc.ClientConn, addr string) {
	am.localAllocatorConn.Lock()
	defer am.localAllocatorConn.Unlock()
	if _, ok := am.localAllocatorConn.clientConns[addr]; ok {
		newConn.Close()
		log.Debug("use old connection", zap.String("target", newConn.Target()), zap.String("state", newConn.GetState().String()))
		return
	}
	am.localAllocatorConn.clientConns[addr] = newConn
}

func (am *AllocatorManager) transferLocalAllocator(dcLocation string, serverID uint64) error {
	nextLeaderKey := am.nextLeaderKey(dcLocation)
	// Grant a etcd lease with checkStep * 1.5
	nextLeaderLease := clientv3.NewLease(am.member.Client())
	ctx, cancel := context.WithTimeout(am.member.Client().Ctx(), etcdutil.DefaultRequestTimeout)
	leaseResp, err := nextLeaderLease.Grant(ctx, int64(checkStep.Seconds()*1.5))
	cancel()
	if err != nil {
		err = errs.ErrEtcdGrantLease.Wrap(err).GenWithStackByCause()
		log.Error("failed to grant the lease of the next leader key",
			zap.String("dc-location", dcLocation), zap.Uint64("serverID", serverID),
			errs.ZapError(err))
		return err
	}
	resp, err := kv.NewSlowLogTxn(am.member.Client()).
		If(clientv3.Compare(clientv3.CreateRevision(nextLeaderKey), "=", 0)).
		Then(clientv3.OpPut(nextLeaderKey, fmt.Sprint(serverID), clientv3.WithLease(leaseResp.ID))).
		Commit()
	if err != nil {
		err = errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
		log.Error("failed to write next leader key into etcd",
			zap.String("dc-location", dcLocation), zap.Uint64("serverID", serverID),
			errs.ZapError(err))
		return err
	}
	if !resp.Succeeded {
		log.Warn("write next leader id into etcd unsuccessfully", zap.String("dc-location", dcLocation))
		return errs.ErrEtcdTxnConflict.GenWithStack("write next leader id into etcd unsuccessfully")
	}
	return nil
}

func (am *AllocatorManager) nextLeaderKey(dcLocation string) string {
	return path.Join(am.rootPath, dcLocation, "next-leader")
}

// EnableLocalTSO returns the value of AllocatorManager.enableLocalTSO.
func (am *AllocatorManager) EnableLocalTSO() bool {
	return am.enableLocalTSO
}
