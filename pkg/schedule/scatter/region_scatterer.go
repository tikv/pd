// Copyright 2017 TiKV Project Authors.
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

package scatter

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const regionScatterName = "region-scatter"

var (
	gcInterval = time.Minute
	gcTTL      = time.Minute * 3
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	scatterSkipEmptyRegionCounter   = scatterCounter.WithLabelValues("skip", "empty-region")
	scatterSkipNoRegionCounter      = scatterCounter.WithLabelValues("skip", "no-region")
	scatterSkipNoLeaderCounter      = scatterCounter.WithLabelValues("skip", "no-leader")
	scatterSkipHotRegionCounter     = scatterCounter.WithLabelValues("skip", "hot")
	scatterSkipNotReplicatedCounter = scatterCounter.WithLabelValues("skip", "not-replicated")
	scatterUnnecessaryCounter       = scatterCounter.WithLabelValues("unnecessary", "")
	scatterFailCounter              = scatterCounter.WithLabelValues("fail", "")
	scatterSuccessCounter           = scatterCounter.WithLabelValues("success", "")
)

const (
	maxSleepDuration     = time.Minute
	initialSleepDuration = 100 * time.Millisecond
	maxRetryLimit        = 30
)

type selectedStores struct {
	mu                syncutil.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

// Put plus count by storeID and group
func (s *selectedStores) Put(id uint64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id]++
	s.groupDistribution.Put(group, distribution)
}

// Get the count by storeID and group
func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedStores) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx               context.Context
	name              string
	cluster           sche.SharedCluster
	ordinaryEngine    engineContext
	specialEngines    sync.Map
	opController      *operator.Controller
	addSuspectRegions func(bool, ...uint64)
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster sche.SharedCluster, opController *operator.Controller, addSuspectRegions func(bool, ...uint64)) *RegionScatterer {
	return &RegionScatterer{
		ctx:               ctx,
		name:              regionScatterName,
		cluster:           cluster,
		opController:      opController,
		addSuspectRegions: addSuspectRegions,
		ordinaryEngine: newEngineContext(ctx, func() filter.Filter {
			return filter.NewEngineFilter(regionScatterName, filter.NotSpecialEngines)
		}),
	}
}

type filterFunc func() filter.Filter

type engineContext struct {
	filterFuncs    []filterFunc
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

func newEngineContext(ctx context.Context, filterFuncs ...filterFunc) engineContext {
	filterFuncs = append(filterFuncs, func() filter.Filter {
		return &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true, OperatorLevel: constant.High}
	})
	return engineContext{
		filterFuncs:    filterFuncs,
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
	}
}

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) (int, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, false)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int, skipStoreLimit bool) (int, map[uint64]error, error) {
	if len(regionsID) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	if len(regionsID) == 1 {
		region := r.cluster.GetRegion(regionsID[0])
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			return 0, nil, errs.ErrRegionNotFound
		}
	}
	failures := make(map[uint64]error, len(regionsID))
	regions := make([]*core.RegionInfo, 0, len(regionsID))
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			log.Warn("failed to find region during scatter", zap.Uint64("region-id", id))
			failures[id] = errors.New(fmt.Sprintf("failed to find region %v", id))
			continue
		}
		regions = append(regions, region)
	}
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, skipStoreLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// scatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) scatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int, skipStoreLimit bool) (int, error) {
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, errs.ErrEmptyRegion
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	opsCount := 0
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group, skipStoreLimit)
			failpoint.Inject("scatterFail", func() {
				if region.GetID() == 1 {
					err = errors.New("mock error")
				}
			})
			if err != nil {
				failures[region.GetID()] = err
				continue
			}
			delete(regions, region.GetID())
			opsCount++
			if op != nil {
				if ok := r.opController.AddOperator(op); !ok {
					// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
					failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
					continue
				}
				failpoint.Inject("scatterHbStreamsDrain", func() {
					_ = r.opController.GetHBStreams().Drain(1)
					r.opController.RemoveOperator(op, operator.AdminStop)
				})
			}
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return opsCount, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string, skipStoreLimit bool) (*operator.Operator, error) {
	if !filter.IsRegionReplicated(r.cluster, region) {
		r.addSuspectRegions(false, region.GetID())
		scatterSkipNotReplicatedCounter.Inc()
		log.Warn("region not replicated during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		scatterSkipNoLeaderCounter.Inc()
		log.Warn("region no leader during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	if r.cluster.IsRegionHot(region) {
		scatterSkipHotRegionCounter.Inc()
		log.Warn("region too hot during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegion(region, group, skipStoreLimit)
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string, skipStoreLimit bool) (*operator.Operator, error) {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	oldFit := r.cluster.GetRuleManager().FitRegion(r.cluster, region)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return nil, errs.ErrGetSourceStore.FastGenByArgs(fmt.Sprintf("store not found, peer: %v, region id: %d", peer, region.GetID()))
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			ordinaryPeers[peer.GetStoreId()] = peer
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]*metapb.Peer)
			}
			specialPeers[engine][peer.GetStoreId()] = peer
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))                  // StoreID -> Peer
	selectedStores := make(map[uint64]struct{}, len(region.GetPeers()))                   // selected StoreID set
	leaderCandidateStores := make([]uint64, 0, len(region.GetPeers()))                    // StoreID allowed to become Leader
	scatterWithSameEngine := func(peers map[uint64]*metapb.Peer, context engineContext) { // peers: StoreID -> Peer
		filterLen := len(context.filterFuncs) + 2
		filters := make([]filter.Filter, filterLen)
		for i, filterFunc := range context.filterFuncs {
			filters[i] = filterFunc()
		}
		filters[filterLen-2] = filter.NewExcludedFilter(r.name, nil, selectedStores)
		for _, peer := range peers {
			if _, ok := selectedStores[peer.GetStoreId()]; ok {
				if allowLeader(oldFit, peer) {
					leaderCandidateStores = append(leaderCandidateStores, peer.GetStoreId())
				}
				// It is both sourcePeer and targetPeer itself, no need to select.
				continue
			}
			sourceStore := r.cluster.GetStore(peer.GetStoreId())
			if sourceStore == nil {
				log.Error("failed to get the store", zap.Uint64("store-id", peer.GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
				continue
			}
			filters[filterLen-1] = filter.NewPlacementSafeguard(r.name, r.cluster.GetSharedConfig(), r.cluster.GetBasicCluster(), r.cluster.GetRuleManager(), region, sourceStore, oldFit)
			for {
				newPeer := r.selectNewPeer(context, group, peer, filters)
				targetPeers[newPeer.GetStoreId()] = newPeer
				selectedStores[newPeer.GetStoreId()] = struct{}{}
				// If the selected peer is a peer other than origin peer in this region,
				// it is considered that the selected peer select itself.
				// This origin peer re-selects.
				if _, ok := peers[newPeer.GetStoreId()]; !ok || peer.GetStoreId() == newPeer.GetStoreId() {
					selectedStores[peer.GetStoreId()] = struct{}{}
					if allowLeader(oldFit, peer) {
						leaderCandidateStores = append(leaderCandidateStores, newPeer.GetStoreId())
					}
					break
				}
			}
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores, maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader, leaderStorePickedCount := r.selectAvailableLeaderStore(group, region, leaderCandidateStores, r.ordinaryEngine)
	if targetLeader == 0 {
		scatterSkipNoLeaderCounter.Inc()
		return nil, errs.ErrGetTargetStore.FastGenByArgs(fmt.Sprintf("no target leader store found, region: %v", region))
	}

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines.Load(engine)
		if !ok {
			ctx = newEngineContext(r.ctx, func() filter.Filter {
				return filter.NewEngineFilter(r.name, placement.LabelConstraint{Key: core.EngineKey, Op: placement.In, Values: []string{engine}})
			})
			r.specialEngines.Store(engine, ctx)
		}
		scatterWithSameEngine(peers, ctx.(engineContext))
	}

	if isSameDistribution(region, targetPeers, targetLeader) {
		scatterUnnecessaryCounter.Inc()
		r.Put(targetPeers, targetLeader, group)
		return nil, nil
	}
	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader, skipStoreLimit)
	if err != nil {
		scatterFailCounter.Inc()
		for _, peer := range region.GetPeers() {
			targetPeers[peer.GetStoreId()] = peer
		}
		r.Put(targetPeers, region.GetLeader().GetStoreId(), group)
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil, errs.ErrCreateOperator.FastGenByArgs(fmt.Sprintf("failed to create scatter region operator for region %v", region.GetID()))
	}
	if op != nil {
		scatterSuccessCounter.Inc()
		r.Put(targetPeers, targetLeader, group)
		op.SetAdditionalInfo("group", group)
		op.SetAdditionalInfo("leader-picked-count", strconv.FormatUint(leaderStorePickedCount, 10))
		op.SetPriorityLevel(constant.High)
	}
	return op, nil
}

func allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}
	if peer.IsWitness {
		return false
	}
	peerFit := fit.GetRuleFit(peer.GetId())
	if peerFit == nil || peerFit.Rule == nil || peerFit.Rule.IsWitness {
		return false
	}
	switch peerFit.Rule.Role {
	case placement.Voter, placement.Leader:
		return true
	}
	return false
}

func isSameDistribution(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64) bool {
	peers := region.GetPeers()
	for _, peer := range peers {
		if _, ok := targetPeers[peer.GetStoreId()]; !ok {
			return false
		}
	}
	return region.GetLeader().GetStoreId() == targetLeader
}

// selectNewPeer return the new peer which pick the fewest picked count.
// it keeps the origin peer if the origin store's pick count is equal the fewest pick.
// it can be divided into three steps:
// 1. found the max pick count and the min pick count.
// 2. if max pick count equals min pick count, it means all store picked count are some, return the origin peer.
// 3. otherwise, select the store which pick count is the min pick count and pass all filter.
func (r *RegionScatterer) selectNewPeer(context engineContext, group string, peer *metapb.Peer, filters []filter.Filter) *metapb.Peer {
	stores := r.cluster.GetStores()
	maxStoreTotalCount := uint64(0)
	minStoreTotalCount := uint64(math.MaxUint64)
	for _, store := range stores {
		count := context.selectedPeer.Get(store.GetID(), group)
		if count > maxStoreTotalCount {
			maxStoreTotalCount = count
		}
		if count < minStoreTotalCount {
			minStoreTotalCount = count
		}
	}

	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	originStorePickedCount := uint64(math.MaxUint64)
	for _, store := range stores {
		storeCount := context.selectedPeer.Get(store.GetID(), group)
		if store.GetID() == peer.GetStoreId() {
			originStorePickedCount = storeCount
		}
		// If storeCount is equal to the maxStoreTotalCount, we should skip this store as candidate.
		// If the storeCount are all the same for the whole cluster(maxStoreTotalCount == minStoreTotalCount), any store
		// could be selected as candidate.
		if storeCount < maxStoreTotalCount || maxStoreTotalCount == minStoreTotalCount {
			if filter.Target(r.cluster.GetSharedConfig(), store, filters) {
				if storeCount < minCount {
					minCount = storeCount
					newPeer = &metapb.Peer{
						StoreId: store.GetID(),
						Role:    peer.GetRole(),
					}
				}
			}
		}
	}
	if originStorePickedCount <= minCount {
		return peer
	}
	if newPeer == nil {
		return peer
	}
	return newPeer
}

// selectAvailableLeaderStore select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level. Please use this func before scatter spacial engines.
func (r *RegionScatterer) selectAvailableLeaderStore(group string, region *core.RegionInfo,
	leaderCandidateStores []uint64, context engineContext) (leaderID uint64, leaderStorePickedCount uint64) {
	sourceStore := r.cluster.GetStore(region.GetLeader().GetStoreId())
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", region.GetLeader().GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
		return 0, 0
	}
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for _, storeID := range leaderCandidateStores {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	return id, minStoreGroupLeader
}

// Put put the final distribution in the context no matter the operator was created
func (r *RegionScatterer) Put(peers map[uint64]*metapb.Peer, leaderStoreID uint64, group string) {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			r.ordinaryEngine.selectedPeer.Put(storeID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", storeID),
				fmt.Sprintf("%v", false),
				core.EngineTiKV).Inc()
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			ctx, _ := r.specialEngines.Load(engine)
			ctx.(engineContext).selectedPeer.Put(storeID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", storeID),
				fmt.Sprintf("%v", false),
				engine).Inc()
		}
	}
	r.ordinaryEngine.selectedLeader.Put(leaderStoreID, group)
	scatterDistributionCounter.WithLabelValues(
		fmt.Sprintf("%v", leaderStoreID),
		fmt.Sprintf("%v", true),
		core.EngineTiKV).Inc()
}
