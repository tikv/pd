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
	"reflect"
	"slices"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
)

// moveScatterPeer updates a request-owned clone after a completed selection.
// Address peers by ID so updating one store cannot accidentally move another
// peer in a chain of replacements. The standalone leader is a separate clone.
func moveScatterPeer(view *core.RegionInfo, peerID, storeID uint64) {
	view.GetPeer(peerID).StoreId = storeID
	if view.GetLeader().GetId() == peerID {
		view.GetLeader().StoreId = storeID
	}
}

type scatterStoreSet map[uint64]*core.StoreInfo

// GetStore returns the captured store information.
func (s scatterStoreSet) GetStore(id uint64) *core.StoreInfo { return s[id] }

// GetStores returns the captured stores.
func (s scatterStoreSet) GetStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(s))
	for _, store := range s {
		stores = append(stores, store)
	}
	return stores
}

// scatterPlacementValid checks the actual planned membership and leadership.
// Both fits use the same StoreInfo objects. Rules and labels are checked again
// before returning; this does not freeze metadata during operator execution.
func (r *RegionScatterer) scatterPlacementValid(region *core.RegionInfo, targets map[uint64]*metapb.Peer, leaderStore uint64) bool {
	if len(targets) != len(region.GetPeers()) {
		return false
	}
	type peerKind struct {
		role    metapb.PeerRole
		witness bool
	}
	kinds := make(map[peerKind]int)
	stores := make(scatterStoreSet, 2*len(targets))
	originalStores := make([]*core.StoreInfo, 0, len(targets))
	for _, peer := range region.GetPeers() {
		kinds[peerKind{peer.GetRole(), peer.GetIsWitness()}]++
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return false
		}
		stores[store.GetID()] = store
		originalStores = append(originalStores, store)
	}
	peers := make([]*metapb.Peer, 0, len(targets))
	targetStores := make([]*core.StoreInfo, 0, len(targets))
	var leader *metapb.Peer
	ids := make([]uint64, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		p := targets[id]
		if p == nil || id != p.GetStoreId() {
			return false
		}
		kind := peerKind{p.GetRole(), p.GetIsWitness()}
		kinds[kind]--
		if kinds[kind] < 0 {
			return false
		}
		store := stores[id]
		if store == nil {
			store = r.cluster.GetStore(id)
		}
		if store == nil {
			return false
		}
		stores[id] = store
		targetStores = append(targetStores, store)
		peer := *p
		// IDs are local to this fit; never consume persistent IDs for validation.
		peer.Id = uint64(len(peers) + 1)
		peers = append(peers, &peer)
		if id == leaderStore {
			leader = &peer
		}
	}
	if leader == nil || leader.GetRole() != metapb.PeerRole_Voter || leader.GetIsWitness() {
		return false
	}
	target := region.Clone(core.SetPeers(peers), core.WithLeader(leader))
	conf := r.cluster.GetSharedConfig()
	rulesEnabled, witnessAllowed := conf.IsPlacementRulesEnabled(), conf.IsWitnessAllowed()
	labels := slices.Clone(conf.GetLocationLabels())
	var rules []*placement.Rule
	if rulesEnabled {
		rm := r.cluster.GetRuleManager()
		before := rm.FitRegionWithoutCache(stores, region)
		after := rm.FitRegionWithoutCache(stores, target)
		rules = before.GetRules()
		if !reflect.DeepEqual(rules, after.GetRules()) || !before.IsSatisfied() || !after.IsSatisfied() {
			return false
		}
		for i, oldFit := range before.RuleFits {
			newFit := after.RuleFits[i]
			if !reflect.DeepEqual(oldFit.Rule, newFit.Rule) || newFit.IsolationScore < oldFit.IsolationScore {
				return false
			}
			rule := oldFit.Rule
			if statistics.IsRegionLabelIsolationSatisfied(oldFit.Stores, rule.LocationLabels, rule.IsolationLevel) &&
				!statistics.IsRegionLabelIsolationSatisfied(newFit.Stores, rule.LocationLabels, rule.IsolationLevel) {
				return false
			}
		}
	} else if scatterDistinctScore(labels, targetStores) < scatterDistinctScore(labels, originalStores) {
		return false
	}
	for id, store := range stores {
		current := r.cluster.GetStore(id)
		if current == nil || !reflect.DeepEqual(store.GetLabels(), current.GetLabels()) {
			return false
		}
	}
	if rulesEnabled != conf.IsPlacementRulesEnabled() || witnessAllowed != conf.IsWitnessAllowed() || !slices.Equal(labels, conf.GetLocationLabels()) {
		return false
	}
	return !rulesEnabled || reflect.DeepEqual(rules, r.cluster.GetRuleManager().GetRulesForApplyRegion(region))
}

func scatterDistinctScore(labels []string, stores []*core.StoreInfo) float64 {
	var score float64
	for i, store := range stores {
		score += core.DistinctScore(labels, stores[:i], store)
	}
	return score
}
