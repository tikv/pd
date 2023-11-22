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

package schedule

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

var gcInterval = time.Minute
var gcTTL = time.Minute * 3

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
	ctx            context.Context
	name           string
	cluster        Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
	opController   *OperatorController
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster Cluster, opController *OperatorController) *RegionScatterer {
	return &RegionScatterer{
		ctx:            ctx,
		name:           regionScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(ctx, filter.NewOrdinaryEngineFilter(regionScatterName)),
		specialEngines: make(map[string]engineContext),
		opController:   opController,
	}
}

type engineContext struct {
	filters        []filter.Filter
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

func newEngineContext(ctx context.Context, filters ...filter.Filter) engineContext {
	filters = append(filters, &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true})
	return engineContext{
		filters:        filters,
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
	}
}

const maxSleepDuration = 1 * time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) (int, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-region").Inc()
		return 0, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int) (int, map[uint64]error, error) {
	if len(regionsID) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-region").Inc()
		return 0, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regionsID))
	regions := make([]*core.RegionInfo, 0, len(regionsID))
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			scatterCounter.WithLabelValues("skip", "no-region").Inc()
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
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit)
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
func (r *RegionScatterer) scatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int) (int, error) {
	if len(regions) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-region").Inc()
		return 0, errors.New("empty region")
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	opsCount := 0
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group)
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
					r.opController.hbStreams.Drain(1)
					r.opController.RemoveOperator(op)
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
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string) (*operator.Operator, error) {
	if !IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		scatterCounter.WithLabelValues("skip", "not-replicated").Inc()
		log.Warn("region not replicated during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		scatterCounter.WithLabelValues("skip", "no-leader").Inc()
		log.Warn("region no leader during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	if r.cluster.IsRegionHot(region) {
		scatterCounter.WithLabelValues("skip", "hot").Inc()
		log.Warn("region too hot during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegion(region, group), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	ordinaryPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	oldFit := r.cluster.GetRuleManager().FitRegion(r.cluster, region)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return nil
		}
		if ordinaryFilter.Target(r.cluster.GetOpts(), store) {
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
		filterLen := len(context.filters) + 2
		filters := make([]filter.Filter, filterLen)
		copy(filters, context.filters)
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
			filters[filterLen-1] = filter.NewPlacementSafeguard(r.name, r.cluster.GetOpts(), r.cluster.GetBasicCluster(), r.cluster.GetRuleManager(), region, sourceStore)
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
		scatterCounter.WithLabelValues("no-leader", "").Inc()
		return nil
	}

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines[engine]
		if !ok {
			ctx = newEngineContext(r.ctx, filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = ctx
		}
		scatterWithSameEngine(peers, ctx)
	}

	if isSameDistribution(region, targetPeers, targetLeader) {
		scatterCounter.WithLabelValues("unnecessary", "").Inc()
		r.Put(targetPeers, targetLeader, group)
		return nil
	}
	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		scatterCounter.WithLabelValues("fail", "").Inc()
		for _, peer := range region.GetPeers() {
			targetPeers[peer.GetStoreId()] = peer
		}
		r.Put(targetPeers, region.GetLeader().GetStoreId(), group)
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil
	}
	if op != nil {
		scatterCounter.WithLabelValues("success", "").Inc()
		r.Put(targetPeers, targetLeader, group)
		op.AdditionalInfos["group"] = group
		op.AdditionalInfos["leader-picked-count"] = strconv.FormatUint(leaderStorePickedCount, 10)
		op.SetPriorityLevel(core.HighPriority)
	}
	return op
}

func allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}
	peerFit := fit.GetRuleFit(peer.GetId())
	if peerFit == nil || peerFit.Rule == nil {
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
// it can be diveded into three steps:
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
			if filter.Target(r.cluster.GetOpts(), store, filters) {
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
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if ordinaryFilter.Target(r.cluster.GetOpts(), store) {
			r.ordinaryEngine.selectedPeer.Put(storeID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", storeID),
				fmt.Sprintf("%v", false),
				core.EngineTiKV).Inc()
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			r.specialEngines[engine].selectedPeer.Put(storeID, group)
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
