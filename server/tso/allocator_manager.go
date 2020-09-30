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
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/member"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	checkPriorityStep           = 1 * time.Minute
	checkAllocatorStep          = 1 * time.Second
	dcLocationConfigEtcdPrefix  = "dc-location"
	defaultAllocatorLeaderLease = 3
	leaderTickInterval          = 50 * time.Millisecond
)

// AllocatorGroupFilter is used to select AllocatorGroup.
type AllocatorGroupFilter func(ag *allocatorGroup) bool

type allocatorGroup struct {
	dcLocation string
	// Because an allocator may be set up with different context,
	// we need to store the parent context for each allocator in
	// order to receive the Done() signal correctly.
	parentCtx context.Context
	// For the Global TSO Allocator, leadership is a PD leader's
	// leadership, and for the Local TSO Allocator, leadership
	// is a DC-level certificate to allow an allocator to generate
	// TSO for local transactions in its DC.
	leadership *election.Leadership
	allocator  Allocator
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	sync.RWMutex
	wg sync.WaitGroup
	// There are two kinds of TSO Allocators:
	//   1. Global TSO Allocator, as a global single point to allocate
	//      TSO for global transactions, such as cross-region cases.
	//   2. Local TSO Allocator, servers for DC-level transactions.
	// dc-location/global (string) -> TSO Allocator
	allocatorGroups map[string]*allocatorGroup
	// for election use
	member *member.Member
	// TSO config
	rootPath      string
	saveInterval  time.Duration
	maxResetTSGap func() time.Duration
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(m *member.Member, rootPath string, saveInterval time.Duration, maxResetTSGap func() time.Duration) *AllocatorManager {
	allocatorManager := &AllocatorManager{
		allocatorGroups: make(map[string]*allocatorGroup),
		member:          m,
		rootPath:        rootPath,
		saveInterval:    saveInterval,
		maxResetTSGap:   maxResetTSGap,
	}
	return allocatorManager
}

// SetLocalTSOConfig receives a `LocalTSOConfig` and write it into etcd to make the whole
// cluster know the DC-level topology for later Local TSO Allocator campaign.
func (am *AllocatorManager) SetLocalTSOConfig(localTSOConfig config.LocalTSOConfig) error {
	serverName := am.member.Member().Name
	serverID := fmt.Sprint(am.member.ID())
	log.Info("write dc-location into etcd",
		zap.String("dc-location", localTSOConfig.DCLocation),
		zap.String("server-name", serverName),
		zap.String("server-id", serverID))
	if !localTSOConfig.EnableLocalTSO {
		log.Info("pd server doesn't enable local tso, skip writing dc-location into etcd",
			zap.String("server-name", serverName),
			zap.String("server-id", serverID))
		return nil
	}
	// The key-value pair in etcd will be like: serverID -> dcLocation
	dcLocationKey := path.Join(am.getLocalTSOConfigPath(), serverID)
	resp, err := kv.
		NewSlowLogTxn(am.member.Client()).
		Then(clientv3.OpPut(dcLocationKey, localTSOConfig.DCLocation)).
		Commit()
	if err != nil {
		return errs.ErrEtcdTxn.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		log.Warn("write dc-location into etcd failed",
			zap.String("dc-location", localTSOConfig.DCLocation),
			zap.String("server-name", serverName),
			zap.String("server-id", serverID))
		return errs.ErrEtcdTxn.FastGenByArgs()
	}
	return nil
}

// GetClusterDCLocations returns all dc-locations of a cluster and transform it into a map
// which satisfies dcLocation -> []serverID.
func (am *AllocatorManager) GetClusterDCLocations() (map[string][]uint64, error) {
	resp, err := etcdutil.EtcdKVGet(
		am.member.Client(),
		am.getLocalTSOConfigPath(),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		log.Error("get cluster dc-locations failed", errs.ZapError(err))
		return nil, err
	}
	dcLocationMap := make(map[string][]uint64)
	for _, kv := range resp.Kvs {
		// The key will contain the member ID and the value is its dcLocation
		serverPath := strings.Split(string(kv.Key), "/")
		dcLocation := string(kv.Value)
		// Get serverID from serverPath, e.g, /pd/dc-location/1232143243253 -> 1232143243253
		serverID, err := strconv.ParseUint(serverPath[len(serverPath)-1], 10, 64)
		if err != nil {
			log.Warn("get server id and dcLocation from etcd failed, invalid server id",
				zap.Any("splitted-serverPath", serverPath),
				zap.String("dc-location", dcLocation),
				errs.ZapError(err))
			continue
		}
		dcLocationMap[dcLocation] = append(dcLocationMap[dcLocation], serverID)
	}
	return dcLocationMap, nil
}

func (am *AllocatorManager) getLocalTSOConfigPath() string {
	return path.Join(am.rootPath, dcLocationConfigEtcdPrefix)
}

// SetUpAllocator is used to set up an allocator, which will initialize the allocator and put it into allocator daemon.
func (am *AllocatorManager) SetUpAllocator(parentCtx context.Context, dcLocation string, leadership *election.Leadership) error {
	am.Lock()
	defer am.Unlock()
	var allocator Allocator
	if dcLocation == config.GlobalDCLocation {
		allocator = NewGlobalTSOAllocator(leadership, am.getAllocatorPath(dcLocation), am.saveInterval, am.maxResetTSGap)
	} else {
		allocator = NewLocalTSOAllocator(am.member, leadership, dcLocation, am.saveInterval, am.maxResetTSGap)
	}
	// Update or create a new allocatorGroup
	am.allocatorGroups[dcLocation] = &allocatorGroup{
		dcLocation: dcLocation,
		parentCtx:  parentCtx,
		leadership: leadership,
		allocator:  allocator,
	}
	// Different kinds of allocators have different setup works to do
	switch dcLocation {
	// For Global TSO Allocator
	case config.GlobalDCLocation:
		// Because Global TSO Allocator only depends on PD leader's leadership,
		// so we can directly initialize it here.
		if err := am.allocatorGroups[dcLocation].allocator.Initialize(); err != nil {
			return err
		}
	// For Local TSO Allocator
	default:
		// Join in a Local TSO Allocator election
		localTSOAllocator, _ := allocator.(*LocalTSOAllocator)
		go am.allocatorLeaderLoop(parentCtx, localTSOAllocator)
	}
	return nil
}

func (am *AllocatorManager) getAllocatorPath(dcLocation string) string {
	// For backward compatibility, the global timestamp's store path will still use the old one
	if dcLocation == config.GlobalDCLocation {
		return am.rootPath
	}
	return path.Join(am.rootPath, dcLocation)
}

// similar logic with leaderLoop in server/server.go
func (am *AllocatorManager) allocatorLeaderLoop(ctx context.Context, allocator *LocalTSOAllocator) {
	for {
		select {
		case <-ctx.Done():
			log.Info("server is closed, return local tso allocator leader loop",
				zap.String("dc-location", allocator.dcLocation),
				zap.String("local-tso-allocator-name", am.member.Member().Name))
			return
		default:
		}

		allocatorLeader, rev, checkAgain := allocator.CheckAllocatorLeader()
		if checkAgain {
			continue
		}
		if allocatorLeader != nil {
			log.Info("start to watch allocator leader",
				zap.Stringer(fmt.Sprintf("%s-allocator-leader", allocator.dcLocation), allocatorLeader),
				zap.String("local-tso-allocator-name", am.member.Member().Name))
			// WatchAllocatorLeader will keep looping and never return unless the Local TSO Allocator leader has changed.
			allocator.WatchAllocatorLeader(ctx, allocatorLeader, rev)
			log.Info("local tso allocator leader has changed, try to re-campaign a local tso allocator leader",
				zap.String("dc-location", allocator.dcLocation))
		}
		// Check the next-leader key
		nextLeader, err := am.getNextLeaderID(allocator.dcLocation)
		if err != nil {
			log.Error("get next leader from etcd failed",
				zap.String("dc-location", allocator.dcLocation),
				errs.ZapError(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if nextLeader != 0 && nextLeader != am.member.ID() {
			log.Info("skip campaigning of the local tso allocator leader and check later",
				zap.String("server-name", am.member.Member().Name),
				zap.Uint64("server-id", am.member.ID()),
				zap.Uint64("next-leader-id", nextLeader))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		am.campaignAllocatorLeader(ctx, allocator)
	}
}

func (am *AllocatorManager) campaignAllocatorLeader(loopCtx context.Context, allocator *LocalTSOAllocator) {
	log.Info("start to campaign local tso allocator leader",
		zap.String("dc-location", allocator.dcLocation),
		zap.String("name", am.member.Member().Name))
	if err := allocator.CampaignAllocatorLeader(defaultAllocatorLeaderLease); err != nil {
		log.Error("failed to campaign local tso allocator leader", errs.ZapError(err))
		return
	}

	// Start keepalive the Local TSO Allocator leadership and enable Local TSO service.
	ctx, cancel := context.WithCancel(loopCtx)
	defer cancel()
	defer am.resetAllocatorGroup(allocator.dcLocation)
	// maintain the Local TSO Allocator leader
	go allocator.KeepAllocatorLeader(ctx)
	log.Info("campaign local tso allocator leader ok",
		zap.String("dc-location", allocator.dcLocation),
		zap.String("name", am.member.Member().Name))

	log.Info("initialize the local TSO allocator",
		zap.String("dc-location", allocator.dcLocation),
		zap.String("name", am.member.Member().Name))
	if err := allocator.Initialize(); err != nil {
		log.Error("failed to initialize the local TSO allocator", errs.ZapError(err))
		return
	}
	allocator.EnableAllocatorLeader()
	// The next leader is me, delete it to finish campaigning
	am.deleteNextLeaderID(allocator.dcLocation)
	log.Info("local tso allocator leader is ready to serve",
		zap.String("dc-location", allocator.dcLocation),
		zap.String("name", am.member.Member().Name))

	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !allocator.IsStillAllocatorLeader() {
				log.Info("no longer a local tso allocator leader because lease has expired, local tso allocator leader will step down",
					zap.String("dc-location", allocator.dcLocation),
					zap.String("name", am.member.Member().Name))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed, reset the local tso allocator",
				zap.String("dc-location", allocator.dcLocation),
				zap.String("name", am.member.Member().Name))
			return
		}
	}
}

// AllocatorDaemon is used to update every allocator's TSO and check whether we have
// any new local allocator that needs to be set up.
func (am *AllocatorManager) AllocatorDaemon(serverCtx context.Context) {
	tsTicker := time.NewTicker(UpdateTimestampStep)
	defer tsTicker.Stop()
	checkerTicker := time.NewTicker(checkAllocatorStep)
	defer checkerTicker.Stop()
	priorityTicker := time.NewTicker(checkPriorityStep)
	defer priorityTicker.Stop()

	for {
		select {
		case <-tsTicker.C:
			am.allocatorUpdater()
		case <-checkerTicker.C:
			am.allocatorPatroller(serverCtx)
		case <-priorityTicker.C:
			am.priorityChecker()
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
	case <-ag.parentCtx.Done():
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
		am.resetAllocatorGroup(ag.dcLocation)
		return
	}
}

// Check if we have any new dc-location configured, if yes,
// then set up the corresponding local allocator.
func (am *AllocatorManager) allocatorPatroller(serverCtx context.Context) {
	clusterDCLocations, err := am.GetClusterDCLocations()
	if err != nil {
		log.Error("check new allocators failed, can't get cluster dc-locations", errs.ZapError(err))
	}
	// Collect all dc-locations
	dcLocations := make([]string, 0, len(clusterDCLocations))
	for dcLocation := range clusterDCLocations {
		dcLocations = append(dcLocations, dcLocation)
	}
	// Get all Local TSO Allocators
	allocatorGroups := am.getAllocatorGroups(FilterDCLocation(config.GlobalDCLocation))
	// Set up the new one
	for _, dcLocation := range dcLocations {
		if slice.NoneOf(allocatorGroups, func(i int) bool {
			return allocatorGroups[i].dcLocation == dcLocation
		}) {
			if err := am.SetUpAllocator(serverCtx, dcLocation, election.NewLeadership(
				am.member.Client(),
				am.getAllocatorPath(dcLocation),
				fmt.Sprintf("%s local allocator leader election", dcLocation),
			)); err != nil {
				log.Error("check new allocators failed, can't set up a new local allocator", zap.String("dc-location", dcLocation), errs.ZapError(err))
				continue
			}
		}
	}
	// Clean up the unused one
	for _, ag := range allocatorGroups {
		if slice.NoneOf(dcLocations, func(i int) bool {
			return dcLocations[i] == ag.dcLocation
		}) {
			am.deleteAllocatorGroup(ag.dcLocation)
		}
	}
}

// priorityChecker is used to check the election priority of a Local TSO Allocator.
// In the normal case, if we want to elect a Local TSO Allocator for a certain DC,
// such as dc-1, we need to make sure the follow priority rules:
// 1. The PD server with dc-location="dc-1" needs to be elected as the allocator
// leader with the highest priority.
// 2. If all PD servers with dc-location="dc-1" are down, then the other PD servers
// of DC could be elected.
func (am *AllocatorManager) priorityChecker() {
	serverID := am.member.ID()
	myServerDCLocation, err := am.getServerDCLocation(serverID)
	if err != nil {
		log.Error("skip checking allocator priority, failed to get server's dc-location",
			zap.Uint64("server-id", serverID),
			errs.ZapError(err))
		return
	}
	// Check all Local TSO Allocator followers to see if their priorities is higher than the leaders
	// Filter out allocators with leadership and initialized
	allocatorGroups := am.getAllocatorGroups(FilterDCLocation(config.GlobalDCLocation), FilterAvailableLeadership())
	for _, allocatorGroup := range allocatorGroups {
		localTSOAllocator, _ := allocatorGroup.allocator.(*LocalTSOAllocator)
		leaderServerID := localTSOAllocator.GetAllocatorLeader().GetMemberId()
		// No leader, maybe the leader is not been watched yet
		if leaderServerID == 0 {
			continue
		}
		leaderServerDCLocation, err := am.getServerDCLocation(leaderServerID)
		if err != nil {
			log.Error("failed to get local tso allocator leader's dc-location",
				zap.Uint64("server-id", serverID),
				errs.ZapError(err))
			continue
		}
		// For example, an allocator leader for dc-1 is elected by a server of dc-2, then the server of dc-1 will
		// find this allocator's dc-location isn't the same with server of dc-2 but is same with itself.
		if allocatorGroup.dcLocation != leaderServerDCLocation && allocatorGroup.dcLocation == myServerDCLocation {
			log.Info("try to move the local tso allocator",
				zap.Uint64("old-leader-id", leaderServerID),
				zap.String("old-dc-location", leaderServerDCLocation),
				zap.Uint64("next-leader-id", serverID),
				zap.String("next-dc-location", myServerDCLocation))
			nextLeaderKey := path.Join(am.rootPath, allocatorGroup.dcLocation, "next-leader")
			// Grant a etcd lease with checkPriorityStep * 1.5
			nextLeaderLease := clientv3.NewLease(am.member.Client())
			ctx, cancel := context.WithTimeout(am.member.Client().Ctx(), etcdutil.DefaultRequestTimeout)
			leaseResp, err := nextLeaderLease.Grant(ctx, int64(checkPriorityStep.Seconds()*1.5))
			cancel()
			if err != nil {
				err = errs.ErrEtcdGrantLease.Wrap(err).GenWithStackByCause()
				log.Error("failed to grant the lease of the next leader id key", errs.ZapError(err))
				continue
			}
			resp, err := kv.NewSlowLogTxn(am.member.Client()).
				If(clientv3.Compare(clientv3.CreateRevision(nextLeaderKey), "=", 0)).
				Then(clientv3.OpPut(nextLeaderKey, fmt.Sprint(serverID), clientv3.WithLease(leaseResp.ID))).
				Commit()
			if err != nil {
				err = errs.ErrEtcdTxn.Wrap(err).GenWithStackByCause()
				log.Error("failed to write next leader id into etcd", errs.ZapError(err))
				continue
			}
			if !resp.Succeeded {
				log.Warn("write next leader id into etcd unsuccessfully")
			}
		}
	}
	// Check next leader and resign
	// Filter out allocators with leadership
	allocatorGroups = am.getAllocatorGroups(FilterDCLocation(config.GlobalDCLocation), FilterUnavailableLeadership())
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
			am.resetAllocatorGroup(allocatorGroup.dcLocation)
		}
	}
}

func (am *AllocatorManager) getServerDCLocation(serverID uint64) (string, error) {
	dcLocationMap, err := am.GetClusterDCLocations()
	if err != nil {
		return "", err
	}
	for dcLocation, serverIDs := range dcLocationMap {
		if slice.AnyOf(serverIDs, func(i int) bool { return serverIDs[i] == serverID }) {
			return dcLocation, nil
		}
	}
	return "", nil
}

func (am *AllocatorManager) getNextLeaderID(dcLocation string) (uint64, error) {
	nextLeaderKey := path.Join(am.rootPath, dcLocation, "next-leader")
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
	nextLeaderKey := path.Join(am.rootPath, dcLocation, "next-leader")
	resp, err := kv.NewSlowLogTxn(am.member.Client()).
		Then(clientv3.OpDelete(nextLeaderKey)).
		Commit()
	if err != nil {
		return errs.ErrEtcdKVDelete.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxn.FastGenByArgs()
	}
	return nil
}

func (am *AllocatorManager) deleteAllocatorGroup(dcLocation string) {
	am.Lock()
	defer am.Unlock()
	if allocatorGroup, exist := am.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		allocatorGroup.leadership.Reset()
	}
	delete(am.allocatorGroups, dcLocation)
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleTSORequest(dcLocation string, count uint32) (pdpb.Timestamp, error) {
	am.RLock()
	defer am.RUnlock()
	if len(dcLocation) == 0 {
		dcLocation = config.GlobalDCLocation
	}
	allocatorGroup, exist := am.allocatorGroups[dcLocation]
	if !exist {
		err := errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found, generate timestamp failed", dcLocation))
		return pdpb.Timestamp{}, err
	}
	return allocatorGroup.allocator.GenerateTSO(count)
}

func (am *AllocatorManager) resetAllocatorGroup(dcLocation string) {
	am.Lock()
	defer am.Unlock()
	if allocatorGroup, exist := am.allocatorGroups[dcLocation]; exist {
		allocatorGroup.allocator.Reset()
		allocatorGroup.leadership.Reset()
	}
}

func (am *AllocatorManager) getAllocatorGroups(filters ...AllocatorGroupFilter) []*allocatorGroup {
	am.RLock()
	defer am.RUnlock()
	allocatorGroups := make([]*allocatorGroup, 0)
	for _, ag := range am.allocatorGroups {
		if ag == nil {
			continue
		}
		if slice.NoneOf(filters, func(i int) bool { return filters[i](ag) }) {
			allocatorGroups = append(allocatorGroups, ag)
		}
	}
	return allocatorGroups
}

// GetAllocator get the allocator by dc-location.
func (am *AllocatorManager) GetAllocator(dcLocation string) (Allocator, error) {
	am.RLock()
	defer am.RUnlock()
	allocatorGroup, exist := am.allocatorGroups[dcLocation]
	if !exist {
		return nil, errs.ErrGetAllocator.FastGenByArgs(fmt.Sprintf("%s allocator not found", dcLocation))
	}
	return allocatorGroup.allocator, nil
}

// GetAllocators get all allocators with some filters.
func (am *AllocatorManager) GetAllocators(filters ...AllocatorGroupFilter) []Allocator {
	var allocators []Allocator
	for _, ag := range am.getAllocatorGroups(filters...) {
		allocators = append(allocators, ag.allocator)
	}
	return allocators
}

// GetLocalAllocatorLeaders returns all Local TSO Allocator leaders this server holds.
func (am *AllocatorManager) GetLocalAllocatorLeaders() ([]*LocalTSOAllocator, error) {
	localAllocators := am.GetAllocators(
		FilterDCLocation(config.GlobalDCLocation),
		FilterUnavailableLeadership(),
		FilterUninitialized())
	localAllocatorLeaders := make([]*LocalTSOAllocator, len(localAllocators))
	for _, localAllocator := range localAllocators {
		localAllocatorLeader, ok := localAllocator.(*LocalTSOAllocator)
		if !ok {
			return nil, errs.ErrGetLocalAllocator.FastGenByArgs("invalid local tso allocator found")
		}
		localAllocatorLeaders = append(localAllocatorLeaders, localAllocatorLeader)
	}
	return localAllocatorLeaders, nil
}
