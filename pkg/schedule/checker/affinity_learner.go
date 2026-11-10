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
	"github.com/tikv/pd/pkg/statistics"
)

// moveConflictingLearners gives each learner of region that sits on a target
// voter store another store in roles, so that the affinity operator promotes
// that learner and the Region keeps the learners its rules require. A learner
// tries in turn each voter store that the target drops and its rule allows,
// which swaps the roles of two peers without copying data, before a store
// chosen as the rule checker adds a learner. The first layout that satisfies
// the rules at the best location is used. A witness does not move. It reports
// whether it moved a learner and whether the operator can be built. The move
// waits for a later check if the stores that fit a learner are only
// temporarily unavailable, or if a layout fails only because a better store is
// temporarily unavailable, so that the learner is not taken for one that
// cannot move. If no store fits, the group expires and first-win relearns a
// target.
func (c *AffinityChecker) moveConflictingLearners(region *core.RegionInfo, group *affinity.GroupState,
	roles map[uint64]placement.PeerRoleType) (moved, ok bool) {
	var conflicts []*metapb.Peer
	var stores []uint64
	for _, learner := range region.GetLearners() {
		if roles[learner.GetStoreId()] != placement.Learner {
			conflicts = append(conflicts, learner)
			stores = append(stores, learner.GetStoreId())
		}
	}
	if len(conflicts) == 0 {
		return false, true
	}
	fit := c.ruleManager.FitRegion(c.cluster, region)
	temporary := false
	accept := func(target map[uint64]placement.PeerRoleType) bool {
		layout := regionWithRoles(region, group.LeaderStoreID, target)
		if c.isRegionPlacementRuleSatisfiedWithBestLocation(layout, false) {
			return true
		}
		temporary = temporary || c.waitsForBetterStore(layout)
		return false
	}
	for _, swap := range []bool{true, false} {
		target, swapped, blocked := c.placeLearners(region, fit, roles, conflicts, swap, accept)
		temporary = temporary || blocked
		if target != nil {
			maps.Copy(roles, target)
			log.Debug("move learners off affinity voter stores",
				zap.Uint64("region-id", region.GetID()),
				zap.String("group-id", group.ID),
				zap.Uint64s("learner-stores", stores),
				zap.Bool("swap", swapped))
			return true, true
		}
		// Without a swap, a second pass would place the learners the same way.
		if !swapped {
			break
		}
	}
	if temporary {
		return false, false
	}
	affinityCheckerLearnerPeerConflictCounter.Inc()
	// The learners must stay, so let first-win relearn a target from a healthy
	// Region instead of retrying an impossible peer move.
	c.affinityManager.ExpireAffinityGroup(group.ID)
	log.Debug("expire affinity group after learner-voter conflict",
		zap.Uint64("region-id", region.GetID()),
		zap.String("group-id", group.ID),
		zap.Uint64s("learner-stores", stores))
	return false, false
}

// waitsForBetterStore reports whether region fails the check of
// isRegionPlacementRuleSatisfiedWithBestLocation only because a better store
// for a peer is temporarily unavailable. Any other failure of that check is
// permanent for region.
func (c *AffinityChecker) waitsForBetterStore(region *core.RegionInfo) bool {
	fit := c.ruleManager.FitRegionWithoutCache(c.cluster, region)
	if fit == nil || !fit.IsSatisfied() {
		return false
	}
	blocked := false
	for _, rf := range fit.RuleFits {
		if len(rf.Rule.LocationLabels) == 0 {
			continue
		}
		isWitness := rf.Rule.IsWitness && isWitnessEnabled(c.cluster)
		_, newStoreID, filterByTempState := c.strategy(region, rf.Rule, isWitness).getBetterLocation(c.cluster, region, fit, rf)
		if newStoreID != 0 || !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rf.Rule.LocationLabels, rf.Rule.IsolationLevel) {
			return false
		}
		blocked = blocked || filterByTempState
	}
	return blocked
}

// placeLearners returns roles with another store for each learner of
// conflicts, the first such layout that accept takes, or nil. With swap, a
// learner takes in turn each voter store of the Region that the layout does
// not keep and its rule allows, and only a learner without such a store takes
// a store chosen as the rule checker adds a learner. It also reports whether a
// learner took a voter store and whether a store was temporarily unavailable.
func (c *AffinityChecker) placeLearners(region *core.RegionInfo, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType, conflicts []*metapb.Peer, swap bool,
	accept func(map[uint64]placement.PeerRoleType) bool) (target map[uint64]placement.PeerRoleType, swapped, temporary bool) {
	if len(conflicts) == 0 {
		if accept(roles) {
			return roles, false, false
		}
		return nil, false, false
	}
	learner := conflicts[0]
	rf := fit.GetRuleFit(learner.GetId())
	if rf == nil || learner.GetIsWitness() {
		return nil, false, false
	}
	var storeIDs []uint64
	if swap {
		storeIDs = c.learnerSwapStores(region, rf.Rule, roles)
		swapped = len(storeIDs) > 0
	}
	if !swapped {
		used := make(map[uint64]struct{}, len(roles))
		var stores []*core.StoreInfo
		for id := range roles {
			used[id] = struct{}{}
			if store := c.cluster.GetStore(id); store != nil && placement.MatchLabelConstraints(store, rf.Rule.LabelConstraints) {
				stores = append(stores, store)
			}
		}
		storeID, blocked := c.strategy(region, rf.Rule, false).SelectStoreToAdd(stores, filter.NewExcludedFilter(c.Name(), nil, used))
		if storeID == 0 {
			return nil, false, blocked
		}
		storeIDs = []uint64{storeID}
	}
	for _, storeID := range storeIDs {
		next := maps.Clone(roles)
		next[storeID] = placement.Learner
		var nextSwapped, blocked bool
		target, nextSwapped, blocked = c.placeLearners(region, fit, next, conflicts[1:], swap, accept)
		swapped, temporary = swapped || nextSwapped, temporary || blocked
		if target != nil {
			break
		}
	}
	return target, swapped, temporary
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
