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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pkg/errors"
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

func (s *selectedStores) newFilter() Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[uint64]struct{})
	for id := range s.stores {
		cloned[id] = struct{}{}
	}
	return NewExcludedFilter(nil, cloned)
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	cluster    Cluster
	classifier namespace.Classifier
	filters    []Filter
	selected   *selectedStores
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(cluster Cluster, classifier namespace.Classifier) *RegionScatterer {
	return &RegionScatterer{
		cluster:    cluster,
		classifier: classifier,
		filters:    []Filter{StoreStateFilter{}},
		selected:   newSelectedStores(),
	}
}

// Scatter relocates the region.
func (r *RegionScatterer) Scatter(region *core.RegionInfo) (*Operator, error) {
	if r.cluster.IsRegionHot(region.GetID()) {
		return nil, errors.Errorf("region %d is a hot region", region.GetID())
	}

	if len(region.GetPeers()) != r.cluster.GetMaxReplicas() {
		return nil, errors.Errorf("the number replicas of region %d is not expected", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	return r.scatterRegion(region), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo) *Operator {
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
	return r.createOperator(region, replacedPeers, targetPeers)
}

func (r *RegionScatterer) createOperator(origin *core.RegionInfo, replacedPeers, targetPeers []*metapb.Peer) *Operator {
	// Randomly pick a leader
	i := rand.Intn(len(targetPeers))
	targetLeaderPeer := targetPeers[i]
	originLeaderStoreID := origin.GetLeader().GetStoreId()

	originStoreIDs := origin.GetStoreIds()
	steps := make([]OperatorStep, 0, len(targetPeers)*3+1)
	// deferSteps will append to the end of the steps
	deferSteps := make([]OperatorStep, 0, 5)
	var kind OperatorKind
	sameLeader := targetLeaderPeer.GetStoreId() == originLeaderStoreID
	// No need to do anything
	if sameLeader {
		isSame := true
		for _, peer := range targetPeers {
			if _, ok := originStoreIDs[peer.GetStoreId()]; !ok {
				isSame = false
				break
			}
		}
		if isSame {
			return nil
		}
	}

	// Creates the first step
	if _, ok := originStoreIDs[targetLeaderPeer.GetStoreId()]; !ok {
		st := CreateAddPeerSteps(targetLeaderPeer.GetStoreId(), targetLeaderPeer.GetId(), r.cluster)
		steps = append(steps, st...)
		// Do not transfer leader to the newly added peer
		// Ref: https://github.com/tikv/tikv/issues/3819
		deferSteps = append(deferSteps, TransferLeader{FromStore: originLeaderStoreID, ToStore: targetLeaderPeer.GetStoreId()})
		deferSteps = append(deferSteps, RemovePeer{FromStore: replacedPeers[i].GetStoreId()})
		kind |= OpLeader
		kind |= OpRegion
	} else {
		if !sameLeader {
			steps = append(steps, TransferLeader{FromStore: originLeaderStoreID, ToStore: targetLeaderPeer.GetStoreId()})
			kind |= OpLeader
		}
	}

	// For the other steps
	for j, peer := range targetPeers {
		if peer.GetId() == targetLeaderPeer.GetId() {
			continue
		}
		if _, ok := originStoreIDs[peer.GetStoreId()]; ok {
			continue
		}
		if replacedPeers[j].GetStoreId() == originLeaderStoreID {
			st := CreateAddPeerSteps(peer.GetStoreId(), peer.GetId(), r.cluster)
			st = append(st, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
			deferSteps = append(deferSteps, st...)
			kind |= OpRegion | OpLeader
			continue
		}
		st := CreateAddPeerSteps(peer.GetStoreId(), peer.GetId(), r.cluster)
		steps = append(steps, st...)
		steps = append(steps, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
		kind |= OpRegion
	}

	steps = append(steps, deferSteps...)
	op := NewOperator("scatter-region", origin.GetID(), origin.GetRegionEpoch(), kind, steps...)
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectPeerToReplace(stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	regionStores := r.cluster.GetRegionStores(region)
	sourceStore := r.cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := NewDistinctScoreFilter(r.cluster.GetLocationLabels(), regionStores, sourceStore)

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if scoreGuard.FilterTarget(r.cluster, store) {
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
	filters := []Filter{
		r.selected.newFilter(),
		NewExcludedFilter(nil, region.GetStoreIds()),
		NewNamespaceFilter(r.classifier, namespace),
	}
	filters = append(filters, r.filters...)

	stores := r.cluster.GetStores()
	targets := make(map[uint64]*core.StoreInfo, len(stores))
	for _, store := range stores {
		if !FilterTarget(r.cluster, store, filters) && !store.GetIsBusy() {
			targets[store.GetID()] = store
		}
	}
	return targets
}
