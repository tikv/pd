// Copyright 2026 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
)

type scatterStores map[uint64]*core.StoreInfo

// GetStore returns a store from the comparison snapshot.
func (s scatterStores) GetStore(id uint64) *core.StoreInfo { return s[id] }

// GetStores returns the stores in the comparison snapshot.
func (s scatterStores) GetStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(s))
	for _, store := range s {
		stores = append(stores, store)
	}
	return stores
}

// scatterIsolationChecker compares final layouts with one rules/labels snapshot.
// Candidate guards only cover the source RuleFit; final matching may reassign
// peers to other rules. The original fit is reused while trying leader choices.
func (r *RegionScatterer) scatterIsolationChecker(source, target *core.RegionInfo) func(uint64) bool {
	stores := make(scatterStores)
	for _, region := range []*core.RegionInfo{source, target} {
		for _, peer := range region.GetPeers() {
			id := peer.GetStoreId()
			if _, ok := stores[id]; ok {
				continue
			}
			store := r.cluster.GetStore(id)
			if store == nil {
				return func(uint64) bool { return false }
			}
			stores[id] = store
		}
	}
	conf := r.cluster.GetSharedConfig()
	if !conf.IsPlacementRulesEnabled() {
		labels := conf.GetLocationLabels()
		score := func(region *core.RegionInfo) float64 {
			peers := make([]*core.StoreInfo, 0, len(region.GetPeers()))
			for _, peer := range region.GetPeers() {
				peers = append(peers, stores[peer.GetStoreId()])
			}
			var total float64
			for i, store := range peers {
				total += core.DistinctScore(labels, peers[i+1:], store)
			}
			return total
		}
		allowed := len(source.GetPeers()) == len(target.GetPeers()) && score(target) >= score(source)
		return func(uint64) bool { return allowed }
	}
	rules := r.cluster.GetRuleManager().GetRulesForApplyRegion(source)
	witness := conf.IsWitnessAllowed()
	before := placement.FitRegionWithRules(stores, source, rules, witness)
	return func(leaderStore uint64) bool {
		leader := target.GetStorePeer(leaderStore)
		if leader == nil || !before.IsSatisfied() {
			return false
		}
		after := placement.FitRegionWithRules(stores, target.Clone(core.WithLeader(leader)), rules, witness)
		return after.IsSatisfied() && before.IsIsolationPreserved(after)
	}
}
