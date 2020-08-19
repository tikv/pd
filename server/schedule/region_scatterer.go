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
	"math"
	"math/rand"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

type selectedLeaderStores struct {
	mu     sync.Mutex
	stores map[uint64]uint64 // storeID -> hintCount
}

func (s *selectedLeaderStores) put(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stores[id] = s.stores[id] + 1
}

func (s *selectedLeaderStores) get(id uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stores[id]
}

func newSelectedLeaderStores() *selectedLeaderStores {
	return &selectedLeaderStores{
		stores: make(map[uint64]uint64),
	}
}

type selectedStores struct {
	mu     sync.Mutex
	stores map[uint64]struct{}
}

func newSelectedStores() *selectedStores {
	return &selectedStores{
		stores: make(map[uint64]struct{}),
	}
}

func (s *selectedStores) put(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stores[id]; ok {
		return false
	}
	s.stores[id] = struct{}{}
	return true
}

func (s *selectedStores) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stores = make(map[uint64]struct{})
}

func (s *selectedStores) newFilter(scope string) filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[uint64]struct{})
	for id := range s.stores {
		cloned[id] = struct{}{}
	}
	return filter.NewExcludedFilter(scope, nil, cloned)
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	name           string
	cluster        opt.Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(cluster opt.Cluster) *RegionScatterer {
	return &RegionScatterer{
		name:           regionScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(filter.NewOrdinaryEngineFilter(regionScatterName)),
		specialEngines: make(map[string]engineContext),
	}
}

type engineContext struct {
	filters        []filter.Filter
	selected       *selectedStores
	selectedLeader *selectedLeaderStores
}

func newEngineContext(filters ...filter.Filter) engineContext {
	filters = append(filters, filter.StoreStateFilter{ActionScope: regionScatterName})
	return engineContext{
		filters:        filters,
		selected:       newSelectedStores(),
		selectedLeader: newSelectedLeaderStores(),
	}
}

// Scatter relocates the region.
func (r *RegionScatterer) Scatter(region *core.RegionInfo) (*operator.Operator, error) {
	if !opt.IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	return r.scatterRegion(region), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	var ordinaryPeers []*metapb.Peer
	specialPeers := make(map[string][]*metapb.Peer)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if ordinaryFilter.Target(r.cluster, store) {
			ordinaryPeers = append(ordinaryPeers, peer)
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			specialPeers[engine] = append(specialPeers[engine], peer)
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer)

	scatterWithSameEngine := func(peers []*metapb.Peer, context engineContext) {
		stores := r.collectAvailableStores(region, context)
		for _, peer := range peers {
			if len(stores) == 0 {
				context.selected.reset()
				stores = r.collectAvailableStores(region, context)
			}
			if context.selected.put(peer.GetStoreId()) {
				delete(stores, peer.GetStoreId())
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			newPeer := r.selectPeerToReplace(stores, region, peer)
			if newPeer == nil {
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			// Remove it from stores and mark it as selected.
			delete(stores, newPeer.GetStoreId())
			context.selected.put(newPeer.GetStoreId())
			targetPeers[newPeer.GetStoreId()] = newPeer
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores，maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader := r.searchLeastleaderStore(targetPeers, r.ordinaryEngine)

	for engine, peers := range specialPeers {
		context, ok := r.specialEngines[engine]
		if !ok {
			context = newEngineContext(filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = context
		}
		scatterWithSameEngine(peers, context)
	}

	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		log.Debug("fail to create scatter region operator", zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectPeerToReplace(stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	storeID := oldPeer.GetStoreId()
	sourceStore := r.cluster.GetStore(storeID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID))
		return nil
	}
	scoreGuard := filter.NewPlacementSafeguard(r.name, r.cluster, region, sourceStore)

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if !scoreGuard.Target(r.cluster, store) {
			continue
		}
		candidates = append(candidates, store)
	}

	if len(candidates) == 0 {
		return nil
	}

	target := candidates[rand.Intn(len(candidates))]
	return &metapb.Peer{
		StoreId: target.GetID(),
		Role:    oldPeer.GetRole(),
	}
}

func (r *RegionScatterer) collectAvailableStores(region *core.RegionInfo, context engineContext) map[uint64]*core.StoreInfo {
	filters := []filter.Filter{
		context.selected.newFilter(r.name),
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
		filter.StoreStateFilter{ActionScope: r.name, MoveRegion: true},
	}
	filters = append(filters, context.filters...)

	stores := r.cluster.GetStores()
	targets := make(map[uint64]*core.StoreInfo, len(stores))
	for _, store := range stores {
		if filter.Target(r.cluster, store, filters) && !store.IsBusy() {
			targets[store.GetID()] = store
		}
	}
	return targets
}

func (r *RegionScatterer) searchLeastleaderStore(peers map[uint64]*metapb.Peer, context engineContext) uint64 {
	m := uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range peers {
		count := context.selectedLeader.get(storeID)
		if m > count {
			m = count
			id = storeID
		}
	}
	if id != 0 {
		context.selectedLeader.put(id)
	}
	return id
}
