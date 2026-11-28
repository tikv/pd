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

package checker

import (
	"maps"
	"math"
	"slices"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// moveConflictingLearners makes room for target voters by moving TiKV learners
// created by SQL PLACEMENT POLICY. TiFlash learners are on separate engine
// stores and never conflict with TiKV voters. A swap reuses both peers.
func (c *AffinityChecker) moveConflictingLearners(region *core.RegionInfo, group *affinity.GroupState, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType) (bool, affinityPlacementStatus) {
	var conflicts []*metapb.Peer
	for _, learner := range region.GetLearners() {
		if roles[learner.GetStoreId()] != placement.Learner {
			conflicts = append(conflicts, learner)
		}
	}
	if len(conflicts) == 0 {
		return false, affinityPlacementReady
	}

	next := maps.Clone(roles)
	isolationStores := make(map[*placement.Rule][]*core.StoreInfo)
	for _, rf := range fit.RuleFits {
		for _, store := range rf.Stores {
			if next[store.GetID()] == placement.Learner {
				isolationStores[rf.Rule] = append(isolationStores[rf.Rule], store)
			}
		}
	}

	temporary := false
	for _, learner := range conflicts {
		rf := fit.GetRuleFit(learner.GetId())
		learnerStore := c.cluster.GetStore(learner.GetStoreId())
		if rf == nil || learner.GetIsWitness() || learnerStore == nil {
			return false, affinityPlacementRejected
		}
		rule := rf.Rule
		peers := isolationStores[rule]
		var isolation filter.Filter
		if len(rule.LocationLabels) > 0 && rule.IsolationLevel != "" && len(peers) > 0 {
			isolation = filter.NewIsolationFilter(c.Name(), rule.IsolationLevel, rule.LocationLabels, peers)
		}
		var storeID uint64
		var bestScore float64
		for _, store := range c.learnerSwapStores(region, rule, next) {
			if isolation != nil && !isolation.Target(c.cluster.GetCheckerConfig(), store).IsOK() {
				continue
			}
			score := core.DistinctScore(rule.LocationLabels, peers, store)
			if storeID == 0 || score > bestScore {
				storeID, bestScore = store.GetID(), score
			}
		}
		if storeID == 0 {
			var blocked bool
			storeID, blocked = c.selectLearnerStore(region, fit, rule, learnerStore, next, isolation)
			if storeID == 0 {
				if !blocked {
					return false, affinityPlacementRejected
				}
				// Check the remaining learners before deferring. A permanent
				// conflict still invalidates the target while this store is busy.
				temporary = true
				continue
			}
		}
		next[storeID] = placement.Learner
		isolationStores[rule] = append(peers, c.cluster.GetStore(storeID))
	}
	if temporary {
		return false, affinityPlacementTemporary
	}

	layout := regionWithRoles(region, group.LeaderStoreID, next)
	status := c.checkRegionPlacement(layout, c.ruleManager.FitRegionWithoutCache(c.cluster, layout))
	if status != affinityPlacementReady {
		return false, status
	}
	maps.Copy(roles, next)
	log.Debug("move learners off affinity voter stores",
		zap.Uint64("region-id", region.GetID()),
		zap.String("group-id", group.ID))
	return true, affinityPlacementReady
}

// Rank additions against the same engine and rule role as the placement checker.
// Hard isolation only compares peers fitted to the learner's own rule.
func (c *AffinityChecker) selectLearnerStore(region *core.RegionInfo, fit *placement.RegionFit, rule *placement.Rule,
	learnerStore *core.StoreInfo, roles map[uint64]placement.PeerRoleType, isolation filter.Filter) (uint64, bool) {
	used := make(map[uint64]struct{}, len(roles))
	var stores []*core.StoreInfo
	for id := range roles {
		used[id] = struct{}{}
		store := c.cluster.GetStore(id)
		if store == nil || store.GetLabelValue(core.EngineKey) != learnerStore.GetLabelValue(core.EngineKey) {
			continue
		}
		for _, r := range fit.GetRules() {
			if r.Role == rule.Role && placement.MatchLabelConstraints(store, r.LabelConstraints) {
				stores = append(stores, store)
				break
			}
		}
	}
	strategy := c.strategy(region, rule, false)
	strategy.isolationLevel = ""
	if isolation != nil {
		strategy.extraFilters = append(strategy.extraFilters, isolation)
	}
	return strategy.SelectStoreToAdd(stores, filter.NewExcludedFilter(c.Name(), nil, used))
}

// learnerSwapStores returns the voter stores of region that roles does not
// keep and that rule allows, in the order of the voters. The voter on such a
// store becomes the learner.
func (c *AffinityChecker) learnerSwapStores(region *core.RegionInfo, rule *placement.Rule, roles map[uint64]placement.PeerRoleType) []*core.StoreInfo {
	state := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	var stores []*core.StoreInfo
	for _, voter := range region.GetVoters() {
		if _, kept := roles[voter.GetStoreId()]; kept || voter.GetIsWitness() {
			continue
		}
		store := c.cluster.GetStore(voter.GetStoreId())
		if store != nil && placement.MatchLabelConstraints(store, rule.LabelConstraints) &&
			state.Target(c.cluster.GetCheckerConfig(), store).IsOK() {
			stores = append(stores, store)
		}
	}
	return stores
}

// regionWithRoles returns region with the peers of roles and the leader on
// leaderStoreID. Existing peers keep their IDs and witness flags.
func regionWithRoles(region *core.RegionInfo, leaderStoreID uint64, roles map[uint64]placement.PeerRoleType) *core.RegionInfo {
	peers := make([]*metapb.Peer, 0, len(roles))
	var leader *metapb.Peer
	for _, storeID := range slices.Sorted(maps.Keys(roles)) {
		// A new peer needs an ID that no peer of the Region has.
		peer := &metapb.Peer{Id: math.MaxUint64 - storeID, StoreId: storeID, Role: roles[storeID].MetaPeerRole()}
		if old := region.GetStorePeer(storeID); old != nil {
			peer.Id, peer.IsWitness = old.GetId(), old.GetIsWitness()
		}
		if storeID == leaderStoreID {
			leader = peer
		}
		peers = append(peers, peer)
	}
	return region.Clone(core.SetPeers(peers), core.WithLeader(leader))
}
