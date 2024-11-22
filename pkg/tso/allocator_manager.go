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
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	GlobalDCLocation            = "global"
	checkStep                   = time.Minute
	patrolStep                  = time.Second
	defaultAllocatorLeaderLease = 3
	localTSOAllocatorEtcdPrefix = "lta"
	localTSOSuffixEtcdPrefix    = "lts"
)

var (
	// PriorityCheck exported is only for test.
	PriorityCheck = time.Minute
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

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// IsLeaderElected returns true if the leader exists; otherwise false.
	IsLeaderElected() bool
	// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
	// otherwise returns a bool which indicates if it is needed to check later.
	CheckLeader() (leader member.ElectionLeader, checkAgain bool)
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// KeepLeader is used to keep the leader's leadership.
	KeepLeader(ctx context.Context)
	// CampaignLeader is used to campaign the leadership and make it become a leader in an election group.
	CampaignLeader(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetLeaderID returns current leader's member ID.
	GetLeaderID() uint64
	// GetLeaderPath returns the path of the leader.
	GetLeaderPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
	// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
	GetLastLeaderUpdatedTime() time.Time
	// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
	GetDCLocationPathPrefix() string
	// GetDCLocationPath returns the dc-location path of a member with the given member ID.
	GetDCLocationPath(id uint64) string
	// PreCheckLeader does some pre-check before checking whether it's the leader.
	PreCheckLeader() error
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	mu struct {
		syncutil.RWMutex
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
	// for the synchronization purpose of the allocator update checks
	wg sync.WaitGroup
	// for the synchronization purpose of the service loops
	svcLoopWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	// kgID is the keyspace group ID
	kgID uint32
	// member is for election use
	member ElectionMember
	// TSO config
	rootPath               string
	storage                endpoint.TSOStorage
	enableLocalTSO         bool
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	// leaderLease defines the time within which a TSO primary/leader must update its TTL
	// in etcd, otherwise etcd will expire the leader key and other servers can campaign
	// the primary/leader again. Etcd only supports seconds TTL, so here is second too.
	leaderLease    int64
	maxResetTSGap  func() time.Duration
	securityConfig *grpcutil.TLSConfig
	// for gRPC use
	localAllocatorConn struct {
		syncutil.RWMutex
		clientConns map[string]*grpc.ClientConn
	}
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(
	ctx context.Context,
	keyspaceGroupID uint32,
	member ElectionMember,
	rootPath string,
	storage endpoint.TSOStorage,
	cfg Config,
) *AllocatorManager {
	ctx, cancel := context.WithCancel(ctx)
	am := &AllocatorManager{
		ctx:                    ctx,
		cancel:                 cancel,
		kgID:                   keyspaceGroupID,
		member:                 member,
		rootPath:               rootPath,
		storage:                storage,
		enableLocalTSO:         cfg.IsLocalTSOEnabled(),
		saveInterval:           cfg.GetTSOSaveInterval(),
		updatePhysicalInterval: cfg.GetTSOUpdatePhysicalInterval(),
		leaderLease:            cfg.GetLeaderLease(),
		maxResetTSGap:          cfg.GetMaxResetTSGap,
		securityConfig:         cfg.GetTLSConfig(),
	}
	am.mu.allocatorGroups = make(map[string]*allocatorGroup)
	am.mu.clusterDCLocations = make(map[string]*DCLocationInfo)
	am.localAllocatorConn.clientConns = make(map[string]*grpc.ClientConn)

	// Set up the Global TSO Allocator here, it will be initialized once the member campaigns leader successfully.
	am.SetUpGlobalAllocator(am.ctx, am.member.GetLeadership())
	am.svcLoopWG.Add(1)
	go am.tsoAllocatorLoop()

	return am
}

// SetUpGlobalAllocator is used to set up the global allocator, which will initialize the allocator and put it into
// an allocator daemon. An TSO Allocator should only be set once, and may be initialized and reset multiple times
// depending on the election.
func (am *AllocatorManager) SetUpGlobalAllocator(ctx context.Context, leadership *election.Leadership) {
	am.mu.Lock()
	defer am.mu.Unlock()

	allocator := NewGlobalTSOAllocator(ctx, am)
	// Create a new allocatorGroup
	ctx, cancel := context.WithCancel(ctx)
	am.mu.allocatorGroups[GlobalDCLocation] = &allocatorGroup{
		dcLocation: GlobalDCLocation,
		ctx:        ctx,
		cancel:     cancel,
		leadership: leadership,
		allocator:  allocator,
	}
}

// getGroupID returns the keyspace group ID of the allocator manager.
func (am *AllocatorManager) getGroupID() uint32 {
	if am == nil {
		return 0
	}
	return am.kgID
}

// getGroupIDStr returns the keyspace group ID of the allocator manager in string format.
func (am *AllocatorManager) getGroupIDStr() string {
	if am == nil {
		return "0"
	}
	return strconv.FormatUint(uint64(am.kgID), 10)
}

// GetTimestampPath returns the timestamp path in etcd for the given DCLocation.
func (am *AllocatorManager) GetTimestampPath(dcLocation string) string {
	if am == nil {
		return ""
	}
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}

	am.mu.RLock()
	defer am.mu.RUnlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		return path.Join(am.rootPath, allocatorGroup.allocator.GetTimestampPath())
	}
	return ""
}

// tsoAllocatorLoop is used to run the TSO Allocator updating daemon.
func (am *AllocatorManager) tsoAllocatorLoop() {
	defer logutil.LogPanic()
	defer am.svcLoopWG.Done()

	am.AllocatorDaemon(am.ctx)
	log.Info("exit allocator loop", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
}

// close is used to shutdown TSO Allocator updating daemon.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (am *AllocatorManager) close() {
	log.Info("closing the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))

	if allocatorGroup, exist := am.getAllocatorGroup(GlobalDCLocation); exist {
		allocatorGroup.allocator.(*GlobalTSOAllocator).close()
	}

	for _, cc := range am.localAllocatorConn.clientConns {
		if err := cc.Close(); err != nil {
			log.Error("failed to close allocator manager grpc clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
	}

	am.cancel()
	am.svcLoopWG.Wait()

	log.Info("closed the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
}

// GetMember returns the ElectionMember of this AllocatorManager.
func (am *AllocatorManager) GetMember() ElectionMember {
	return am.member
}

// SetLocalTSOConfig receives the zone label of this PD server and write it into etcd as dc-location
// to make the whole cluster know the DC-level topology for later Local TSO Allocator campaign.
func (am *AllocatorManager) SetLocalTSOConfig(dcLocation string) error {
	serverName := am.member.Name()
	serverID := am.member.ID()
	if err := am.checkDCLocationUpperLimit(dcLocation); err != nil {
		log.Error("check dc-location upper limit failed",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID != 0),
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
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", dcLocation),
			zap.String("server-name", serverName),
			zap.Uint64("server-id", serverID))
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	log.Info("write dc-location configuration into etcd",
		logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
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
				logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
				zap.Any("split-serverPath", serverPath),
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

// CleanUpDCLocation cleans up certain server's DCLocationInfo
func (am *AllocatorManager) CleanUpDCLocation() error {
	serverID := am.member.ID()
	dcLocationKey := am.member.GetDCLocationPath(serverID)
	// remove dcLocationKey from etcd
	if resp, err := kv.
		NewSlowLogTxn(am.member.Client()).
		Then(clientv3.OpDelete(dcLocationKey)).
		Commit(); err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	} else if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	log.Info("delete the dc-location key previously written in etcd",
		logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
		zap.Uint64("server-id", serverID))
	go am.ClusterDCLocationChecker()
	return nil
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

func (am *AllocatorManager) getAllocatorPath(dcLocation string) string {
	// For backward compatibility, the global timestamp's store path will still use the old one
	if dcLocation == GlobalDCLocation {
		return am.rootPath
	}
	return path.Join(am.getLocalTSOAllocatorPath(), dcLocation)
}

// Add a prefix to the root path to prevent being conflicted
// with other system key paths such as leader, member, alloc_id, raft, etc.
func (am *AllocatorManager) getLocalTSOAllocatorPath() string {
	return path.Join(am.rootPath, localTSOAllocatorEtcdPrefix)
}

// AllocatorDaemon is used to update every allocator's TSO and check whether we have
// any new local allocator that needs to be set up.
func (am *AllocatorManager) AllocatorDaemon(ctx context.Context) {
	log.Info("entering into allocator daemon", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))

	tsTicker := time.NewTicker(am.updatePhysicalInterval)
	failpoint.Inject("fastUpdatePhysicalInterval", func() {
		tsTicker.Reset(time.Millisecond)
	})
	defer tsTicker.Stop()
	checkerTicker := time.NewTicker(PriorityCheck)
	defer checkerTicker.Stop()

	for {
		select {
		case <-tsTicker.C:
			// Update the initialized TSO Allocator to advance TSO.
			am.allocatorUpdater()
		case <-checkerTicker.C:
			// Check and maintain the cluster's meta info about dc-location distribution.
			go am.ClusterDCLocationChecker()
			// PS: ClusterDCLocationChecker are time consuming and low frequent to run,
			// we should run them concurrently to speed up the progress.
		case <-ctx.Done():
			log.Info("exit allocator daemon", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
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
	defer logutil.LogPanic()
	defer am.wg.Done()

	select {
	case <-ag.ctx.Done():
		// Resetting the allocator will clear TSO in memory
		ag.allocator.Reset()
		log.Info("exit the allocator update loop", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
		return
	default:
	}
	if !ag.leadership.Check() {
		log.Info("allocator doesn't campaign leadership yet",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", ag.dcLocation))
		time.Sleep(200 * time.Millisecond)
		return
	}
	if err := ag.allocator.UpdateTSO(); err != nil {
		log.Warn("failed to update allocator's timestamp",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", ag.dcLocation),
			zap.String("name", am.member.Name()),
			errs.ZapError(err))
		am.ResetAllocatorGroup(ag.dcLocation, false)
		return
	}
}

// ClusterDCLocationChecker collects all dc-locations of a cluster, computes some related info
// and stores them into the DCLocationInfo, then finally writes them into am.mu.clusterDCLocations.
func (am *AllocatorManager) ClusterDCLocationChecker() {
	defer logutil.LogPanic()
	// Wait for the group leader to be elected out.
	if !am.member.IsLeaderElected() {
		return
	}
	newClusterDCLocations, err := am.GetClusterDCLocationsFromEtcd()
	if err != nil {
		log.Error("get cluster dc-locations from etcd failed",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			errs.ZapError(err))
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
	if am.IsLeader() {
		for dcLocation, info := range am.mu.clusterDCLocations {
			if info.Suffix > 0 {
				continue
			}
			suffix, err := am.getOrCreateLocalTSOSuffix(dcLocation)
			if err != nil {
				log.Warn("get or create the local tso suffix failed",
					logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
					zap.String("dc-location", dcLocation),
					errs.ZapError(err))
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
			log.Error("get the max local tso suffix from etcd failed",
				logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
				errs.ZapError(err))
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
		// If we already have the suffix persisted in etcd before,
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
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", dcLocation),
			zap.String("local-tso-suffix", localTSOSuffixValue),
			zap.String("server-name", am.member.Name()),
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
		splitKey := strings.Split(string(kv.Key), "/")
		dcLocation := splitKey[len(splitKey)-1]
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

// HandleRequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleRequest(ctx context.Context, dcLocation string, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "AllocatorManager.HandleRequest").End()
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}
	allocatorGroup, exist := am.getAllocatorGroup(dcLocation)
	if !exist {
		err := errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found, generate timestamp failed", dcLocation))
		return pdpb.Timestamp{}, err
	}

	return allocatorGroup.allocator.GenerateTSO(ctx, count)
}

// ResetAllocatorGroup will reset the allocator's leadership and TSO initialized in memory.
// It usually should be called before re-triggering an Allocator leader campaign.
func (am *AllocatorManager) ResetAllocatorGroup(dcLocation string, skipResetLeader bool) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if allocatorGroup, exist := am.mu.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
		if !skipResetLeader && allocatorGroup.leadership.Check() {
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
	if len(dcLocation) == 0 {
		dcLocation = GlobalDCLocation
	}
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
		log.Debug("use old connection",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("target", newConn.Target()),
			zap.String("state", newConn.GetState().String()))
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
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", dcLocation),
			zap.Uint64("serverID", serverID),
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
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", dcLocation), zap.Uint64("serverID", serverID),
			errs.ZapError(err))
		return err
	}
	if !resp.Succeeded {
		log.Warn("write next leader id into etcd unsuccessfully",
			logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
			zap.String("dc-location", dcLocation))
		return errs.ErrEtcdTxnConflict.GenWithStack("write next leader id into etcd unsuccessfully")
	}
	return nil
}

func (am *AllocatorManager) nextLeaderKey(dcLocation string) string {
	return path.Join(am.getAllocatorPath(dcLocation), "next-leader")
}

// EnableLocalTSO returns the value of AllocatorManager.enableLocalTSO.
func (am *AllocatorManager) EnableLocalTSO() bool {
	return am.enableLocalTSO
}

// IsLeader returns whether the current member is the leader in the election group.
func (am *AllocatorManager) IsLeader() bool {
	if am == nil || am.member == nil || !am.member.IsLeader() {
		return false
	}
	return true
}

// GetLeaderAddr returns the address of leader in the election group.
func (am *AllocatorManager) GetLeaderAddr() string {
	if am == nil || am.member == nil {
		return ""
	}
	leaderAddrs := am.member.GetLeaderListenUrls()
	if len(leaderAddrs) < 1 {
		return ""
	}
	return leaderAddrs[0]
}

func (am *AllocatorManager) startGlobalAllocatorLoop() {
	globalTSOAllocator, ok := am.mu.allocatorGroups[GlobalDCLocation].allocator.(*GlobalTSOAllocator)
	if !ok {
		// it should never happen
		log.Error("failed to start global allocator loop, global allocator not found")
		return
	}
	globalTSOAllocator.wg.Add(1)
	go globalTSOAllocator.primaryElectionLoop()
}
