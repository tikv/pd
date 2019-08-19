// Copyright 2017 PingCAP, Inc.
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
	"math/rand"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

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

func (s *selectedStores) newFilter() filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[uint64]struct{})
	for id := range s.stores {
		cloned[id] = struct{}{}
	}
	return filter.NewExcludedFilter(nil, cloned)
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	cluster    Cluster
	classifier namespace.Classifier
	filters    []filter.Filter
	selected   *selectedStores
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(cluster Cluster, classifier namespace.Classifier) *RegionScatterer {
	return &RegionScatterer{
		cluster:    cluster,
		classifier: classifier,
		filters: []filter.Filter{
			filter.StoreStateFilter{},
		},
		selected: newSelectedStores(),
	}
}

// Scatter relocates the region.
func (r *RegionScatterer) Scatter(region *core.RegionInfo) (*operator.Operator, error) {
	if len(region.GetPeers()) != r.cluster.GetMaxReplicas() {
		return nil, errors.Errorf("the number replicas of region %d is not expected", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	return r.scatterRegion(region), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo) *operator.Operator {
	stores := r.collectAvailableStores(region)
	var (
		targetPeers   []*metapb.Peer
		replacedPeers []*metapb.Peer
	)
	for _, peer := range region.GetPeers() {
		if len(stores) == 0 {
			// Reset selected stores if we have no available stores.
			r.selected.reset()
			stores = r.collectAvailableStores(region)
		}

		if r.selected.put(peer.GetStoreId()) {
			delete(stores, peer.GetStoreId())
			targetPeers = append(targetPeers, peer)
			replacedPeers = append(replacedPeers, peer)
			continue
		}
		newPeer := r.selectPeerToReplace(stores, region, peer)
		if newPeer == nil {
			targetPeers = append(targetPeers, peer)
			replacedPeers = append(replacedPeers, peer)
			continue
		}
		// Remove it from stores and mark it as selected.
		delete(stores, newPeer.GetStoreId())
		r.selected.put(newPeer.GetStoreId())
		targetPeers = append(targetPeers, newPeer)
		replacedPeers = append(replacedPeers, peer)
	}
	op := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, replacedPeers, targetPeers)
	if op != nil {
		op.SetPriorityLevel(core.HighPriority)
	}
	return op
}

func (r *RegionScatterer) selectPeerToReplace(stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	regionStores := r.cluster.GetRegionStores(region)
	storeID := oldPeer.GetStoreId()
	sourceStore := r.cluster.GetStore(storeID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID))
	}
	scoreGuard := filter.NewDistinctScoreFilter(r.cluster.GetLocationLabels(), regionStores, sourceStore)

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if scoreGuard.Target(r.cluster, store) {
			continue
		}
		candidates = append(candidates, store)
	}

	if len(candidates) == 0 {
		return nil
	}

	target := candidates[rand.Intn(len(candidates))]
	newPeer, err := r.cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}
	return newPeer
}

func (r *RegionScatterer) collectAvailableStores(region *core.RegionInfo) map[uint64]*core.StoreInfo {
	namespace := r.classifier.GetRegionNamespace(region)
	filters := []filter.Filter{
		r.selected.newFilter(),
		filter.NewExcludedFilter(nil, region.GetStoreIds()),
		filter.NewNamespaceFilter(r.classifier, namespace),
	}
	filters = append(filters, r.filters...)

	stores := r.cluster.GetStores()
	targets := make(map[uint64]*core.StoreInfo, len(stores))
	for _, store := range stores {
		if !filter.Target(r.cluster, store, filters) && !store.GetIsBusy() {
			targets[store.GetID()] = store
		}
	}
	return targets
}
