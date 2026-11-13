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

// moveConflictingLearners gives each learner of region that sits on a target
// voter store another store in roles, so that the affinity operator promotes
// that learner and the Region keeps the learners its rules require. A learner
// tries in turn each voter store that the target drops and its rule allows,
// which swaps the roles of two peers without copying data, before a store
// chosen as the rule checker adds a learner. The first layout that satisfies
// the rules at the best location is used. A witness does not move. It reports
// whether it moved a learner and the placement status. The move
// waits for a later check if the stores that fit a learner are only
// temporarily unavailable, or if a layout fails only because a better store is
// temporarily unavailable, so that the learner is not taken for one that
// cannot move. Rejection means this local search found no plan, so the caller
// can relearn a target; it does not prove that every learner layout is invalid.
func (c *AffinityChecker) moveConflictingLearners(region *core.RegionInfo, group *affinity.GroupState, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType) (bool, affinityPlacementStatus) {
	var conflicts []*metapb.Peer
	var stores []uint64
	for _, learner := range region.GetLearners() {
		if roles[learner.GetStoreId()] != placement.Learner {
			conflicts = append(conflicts, learner)
			stores = append(stores, learner.GetStoreId())
		}
	}
	if len(conflicts) == 0 {
		return false, affinityPlacementReady
	}
	// Hard isolation follows each fitted rule's learners, including moves made
	// earlier in the recursion, rather than the stores used for location ranking.
	isolationStores := make(map[*placement.Rule][]*core.StoreInfo)
	for _, rf := range fit.RuleFits {
		for _, store := range rf.Stores {
			if roles[store.GetID()] == placement.Learner {
				isolationStores[rf.Rule] = append(isolationStores[rf.Rule], store)
			}
		}
	}
	accept := func(target map[uint64]placement.PeerRoleType) affinityPlacementStatus {
		layout := regionWithRoles(region, group.LeaderStoreID, target)
		layoutFit := c.ruleManager.FitRegionWithoutCache(c.cluster, layout)
		return c.checkRegionFitAtBestLocation(layout, layoutFit)
	}
	target, temporary := c.placeLearners(region, fit, roles, isolationStores, conflicts, accept)
	if target != nil {
		maps.Copy(roles, target)
		log.Debug("move learners off affinity voter stores",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			zap.Uint64s("learner-stores", stores))
		return true, affinityPlacementReady
	}
	if temporary {
		return false, affinityPlacementTemporary
	}
	return false, affinityPlacementRejected
}

// placeLearners returns roles with another store for each learner of
// conflicts, the first such layout that accept takes, or nil. A
// learner takes in turn each voter store of the Region that the layout does
// not keep and its rule allows, followed by a store chosen as the rule checker
// adds a learner. This includes mixed swaps and moves and the all-move layout.
// It also reports whether a store or a complete layout was temporarily blocked.
func (c *AffinityChecker) placeLearners(region *core.RegionInfo, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType, isolationStores map[*placement.Rule][]*core.StoreInfo, conflicts []*metapb.Peer,
	accept func(map[uint64]placement.PeerRoleType) affinityPlacementStatus) (target map[uint64]placement.PeerRoleType, temporary bool) {
	if len(conflicts) == 0 {
		status := accept(roles)
		if status == affinityPlacementReady {
			return roles, false
		}
		return nil, status == affinityPlacementTemporary
	}
	learner := conflicts[0]
	rf := fit.GetRuleFit(learner.GetId())
	learnerStore := c.cluster.GetStore(learner.GetStoreId())
	if rf == nil || learner.GetIsWitness() || learnerStore == nil {
		return nil, false
	}
	storeIDs := c.learnerSwapStores(region, rf.Rule, roles)
	// A zero ID tries the rule checker's store after all swaps.
	for _, storeID := range append(storeIDs, 0) {
		if storeID == 0 {
			used := make(map[uint64]struct{}, len(roles))
			var stores []*core.StoreInfo
			for id := range roles {
				used[id] = struct{}{}
				store := c.cluster.GetStore(id)
				if store == nil || store.GetLabelValue(core.EngineKey) != learnerStore.GetLabelValue(core.EngineKey) {
					continue
				}
				for _, rule := range fit.GetRules() {
					if rule.Role == rf.Rule.Role && placement.MatchLabelConstraints(store, rule.LabelConstraints) {
						stores = append(stores, store)
						break
					}
				}
			}
			strategy := c.strategy(region, rf.Rule, false)
			strategy.isolationLevel = ""
			if len(rf.Rule.LocationLabels) > 0 && rf.Rule.IsolationLevel != "" && len(isolationStores[rf.Rule]) > 0 {
				strategy.extraFilters = append(strategy.extraFilters,
					filter.NewIsolationFilter(c.Name(), rf.Rule.IsolationLevel, rf.Rule.LocationLabels, isolationStores[rf.Rule]))
			}
			var blocked bool
			storeID, blocked = strategy.SelectStoreToAdd(stores, filter.NewExcludedFilter(c.Name(), nil, used))
			temporary = temporary || blocked
			if storeID == 0 {
				break
			}
		}
		next := maps.Clone(roles)
		next[storeID] = placement.Learner
		previous := isolationStores[rf.Rule]
		if store := c.cluster.GetStore(storeID); store != nil {
			isolationStores[rf.Rule] = append(previous, store)
		}
		var blocked bool
		target, blocked = c.placeLearners(region, fit, next, isolationStores, conflicts[1:], accept)
		isolationStores[rf.Rule] = previous
		temporary = temporary || blocked
		if target != nil {
			return target, temporary
		}
	}
	return target, temporary
}

// learnerSwapStores returns the voter stores of region that roles does not
// keep and that rule allows, in the order of the voters. The voter on such a
// store becomes the learner.
func (c *AffinityChecker) learnerSwapStores(region *core.RegionInfo, rule *placement.Rule, roles map[uint64]placement.PeerRoleType) []uint64 {
	state := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	var stores []uint64
	for _, voter := range region.GetVoters() {
		if _, kept := roles[voter.GetStoreId()]; kept || voter.GetIsWitness() {
			continue
		}
		store := c.cluster.GetStore(voter.GetStoreId())
		if store != nil && placement.MatchLabelConstraints(store, rule.LabelConstraints) &&
			state.Target(c.cluster.GetCheckerConfig(), store).IsOK() {
			stores = append(stores, store.GetID())
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
