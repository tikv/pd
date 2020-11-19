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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

var gcInterval = time.Minute
var gcTTL = time.Minute * 3

type selectedStores struct {
	mu sync.Mutex
	// If checkExist is true, after each putting operation, an entry with the key constructed by group and storeID would be put
	// into "stores" map. And the entry with the same key (storeID, group) couldn't be put before "stores" being reset
	checkExist bool

	stores            *cache.TTLString // value type: map[uint64]struct{}, group -> StoreID -> struct{}
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context, checkExist bool) *selectedStores {
	return &selectedStores{
		checkExist:        checkExist,
		stores:            cache.NewStringTTL(ctx, gcInterval, gcTTL),
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

func (s *selectedStores) getStore(group string) (map[uint64]struct{}, bool) {
	if result, ok := s.stores.Get(group); ok {
		return result.(map[uint64]struct{}), true
	}
	return nil, false
}

func (s *selectedStores) getGroupDistribution(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

func (s *selectedStores) getStoreOrDefault(group string) map[uint64]struct{} {
	if result, ok := s.getStore(group); ok {
		return result
	}
	return make(map[uint64]struct{})
}

func (s *selectedStores) getGroupDistributionOrDefault(group string) map[uint64]uint64 {
	if result, ok := s.getGroupDistribution(group); ok {
		return result
	}
	return make(map[uint64]uint64)
}

func (s *selectedStores) put(id uint64, group string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkExist {
		placed := s.getStoreOrDefault(group)
		if _, ok := placed[id]; ok {
			return false
		}
		placed[id] = struct{}{}
		s.stores.Put(group, placed)
	}
	distribution := s.getGroupDistributionOrDefault(group)
	distribution[id] = distribution[id] + 1
	s.groupDistribution.Put(group, distribution)
	return true
}

func (s *selectedStores) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkExist {
		return
	}
	s.stores.Clear()
}

func (s *selectedStores) get(id uint64, group string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getGroupDistribution(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

func (s *selectedStores) newFilters(scope, group string) []filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkExist {
		return nil
	}
	cloned := make(map[uint64]struct{})
	if groupPlaced, ok := s.getStore(group); ok {
		for id := range groupPlaced {
			cloned[id] = struct{}{}
		}
	}
	return []filter.Filter{filter.NewExcludedFilter(scope, nil, cloned)}
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx            context.Context
	name           string
	cluster        opt.Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster opt.Cluster) *RegionScatterer {
	return &RegionScatterer{
		ctx:            ctx,
		name:           regionScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(ctx, filter.NewOrdinaryEngineFilter(regionScatterName)),
		specialEngines: make(map[string]engineContext),
	}
}

type engineContext struct {
	filters        []filter.Filter
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

func newEngineContext(ctx context.Context, filters ...filter.Filter) engineContext {
	filters = append(filters, &filter.StoreStateFilter{ActionScope: regionScatterName})
	return engineContext{
		filters:        filters,
		selectedPeer:   newSelectedStores(ctx, true),
		selectedLeader: newSelectedStores(ctx, false),
	}
}

const maxSleepDuration = 1 * time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		return nil, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	ops, err := r.ScatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	if len(regionsID) < 1 {
		return nil, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regionsID))
	var regions []*core.RegionInfo
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
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
	ops, err := r.ScatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) ScatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int) ([]*operator.Operator, error) {
	if len(regions) < 1 {
		return nil, errors.New("empty region")
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	ops := make([]*operator.Operator, 0, len(regions))
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
			if op != nil {
				ops = append(ops, op)
			}
			delete(regions, region.GetID())
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return ops, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string) (*operator.Operator, error) {
	if !opt.IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	if r.cluster.IsRegionHot(region) {
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegion(region, group), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	var ordinaryPeers []*metapb.Peer
	specialPeers := make(map[string][]*metapb.Peer)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if ordinaryFilter.Target(r.cluster.GetOpts(), store) {
			ordinaryPeers = append(ordinaryPeers, peer)
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			specialPeers[engine] = append(specialPeers[engine], peer)
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer)

	scatterWithSameEngine := func(peers []*metapb.Peer, context engineContext) {
		stores := r.collectAvailableStores(group, region, context)
		for _, peer := range peers {
			if len(stores) == 0 {
				context.selectedPeer.reset()
				stores = r.collectAvailableStores(group, region, context)
			}
			if context.selectedPeer.put(peer.GetStoreId(), group) {
				delete(stores, peer.GetStoreId())
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			newPeer := r.selectPeerToReplace(group, stores, region, peer, context)
			if newPeer == nil {
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			// Remove it from stores and mark it as selected.
			delete(stores, newPeer.GetStoreId())
			context.selectedPeer.put(newPeer.GetStoreId(), group)
			targetPeers[newPeer.GetStoreId()] = newPeer
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores，maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader := r.selectAvailableLeaderStores(group, targetPeers, r.ordinaryEngine)

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines[engine]
		if !ok {
			ctx = newEngineContext(r.ctx, filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = ctx
		}
		scatterWithSameEngine(peers, ctx)
	}

	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectPeerToReplace(group string, stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer, context engineContext) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	storeID := oldPeer.GetStoreId()
	sourceStore := r.cluster.GetStore(storeID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID), errs.ZapError(errs.ErrGetSourceStore))
		return nil
	}
	scoreGuard := filter.NewPlacementSafeguard(r.name, r.cluster, region, sourceStore)

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if !scoreGuard.Target(r.cluster.GetOpts(), store) {
			continue
		}
		candidates = append(candidates, store)
	}

	if len(candidates) == 0 {
		return nil
	}

	minPeer := uint64(math.MaxUint64)
	var selectedCandidateID uint64
	for _, candidate := range candidates {
		count := context.selectedPeer.get(candidate.GetID(), group)
		if count < minPeer {
			minPeer = count
			selectedCandidateID = candidate.GetID()
		}
	}
	if selectedCandidateID < 1 {
		target := candidates[rand.Intn(len(candidates))]
		return &metapb.Peer{
			StoreId: target.GetID(),
			Role:    oldPeer.GetRole(),
		}
	}

	return &metapb.Peer{
		StoreId: selectedCandidateID,
		Role:    oldPeer.GetRole(),
	}
}

func (r *RegionScatterer) collectAvailableStores(group string, region *core.RegionInfo, context engineContext) map[uint64]*core.StoreInfo {
	filters := []filter.Filter{
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
		&filter.StoreStateFilter{ActionScope: r.name, MoveRegion: true},
	}
	filters = append(filters, context.filters...)
	filters = append(filters, context.selectedPeer.newFilters(r.name, group)...)

	stores := r.cluster.GetStores()
	targets := make(map[uint64]*core.StoreInfo, len(stores))
	for _, store := range stores {
		if filter.Target(r.cluster.GetOpts(), store, filters) && !store.IsBusy() {
			targets[store.GetID()] = store
		}
	}
	return targets
}

// selectAvailableLeaderStores select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level.
func (r *RegionScatterer) selectAvailableLeaderStores(group string, peers map[uint64]*metapb.Peer, context engineContext) uint64 {
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range peers {
		storeGroupLeaderCount := context.selectedLeader.get(storeID, group)
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	if id != 0 {
		context.selectedLeader.put(id, group)
	}
	return id
}
