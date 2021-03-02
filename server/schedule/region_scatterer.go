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
	"sort"
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
)

const regionScatterName = "region-scatter"

var gcInterval = time.Minute
var gcTTL = time.Minute * 3

type selectedStores struct {
	mu                sync.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

func (s *selectedStores) Put(id uint64, group string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getGroupDistribution(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id] = distribution[id] + 1
	s.groupDistribution.Put(group, distribution)
	return true
}

func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	return s.getGroupDistribution(group)
}

func (s *selectedStores) getGroupDistribution(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
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
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
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
		targetPeers = r.selectStores(group, peers, context)
		for storeID := range targetPeers {
			context.selectedPeer.Put(storeID, group)
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
		log.Debug("scatterRegion fail to create scatter region operator", errs.ZapError(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectStores(group string, peers []*metapb.Peer, context engineContext) map[uint64]*metapb.Peer {
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: r.name, MoveRegion: true},
	}
	filters = append(filters, context.filters...)
	stores := r.cluster.GetStores()
	l := make([]StoreCount, 0)
	candidates := make([]uint64, 0)
	for _, store := range stores {
		if filter.Target(r.cluster.GetOpts(), store, filters) && !store.IsBusy() {
			candidates = append(candidates, store.GetID())
		}
	}
	targetPeers := make(map[uint64]*metapb.Peer)
	if len(candidates) < len(peers) {
		for _, peer := range peers {
			targetPeers[peer.GetStoreId()] = peer
		}
		return targetPeers
	}
	for _, storeID := range candidates {
		l = append(l, StoreCount{
			count:   context.selectedPeer.Get(storeID, group),
			storeID: storeID,
		})
	}
	sort.Sort(StoreCountSlice(l))
	for i := 0; i < len(peers); i++ {
		storeID := l[i].storeID
		oldPeer := peers[i]
		newPeer := &metapb.Peer{
			StoreId: storeID,
			Role:    oldPeer.GetRole(),
		}
		targetPeers[storeID] = newPeer
	}
	return targetPeers
}

type StoreCount struct {
	count   uint64
	storeID uint64
}

type StoreCountSlice []StoreCount

func (a StoreCountSlice) Len() int {
	return len(a)
}
func (a StoreCountSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a StoreCountSlice) Less(i, j int) bool {
	return a[i].count < a[j].count
}

// selectAvailableLeaderStores select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level.
func (r *RegionScatterer) selectAvailableLeaderStores(group string, peers map[uint64]*metapb.Peer, context engineContext) uint64 {
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range peers {
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	if id != 0 {
		context.selectedLeader.Put(id, group)
	}
	return id
}
