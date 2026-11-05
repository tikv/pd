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
// takes a voter store that the target drops, which swaps the roles of two
// peers without copying data, or else a store chosen as the rule checker adds
// a learner. The resulting layout must satisfy the rules at the best location.
// A witness does not move. It reports whether the operator can be built. The
// move waits for a later check if the stores that fit a learner are only
// temporarily unavailable, or if a layout fails only because a better store is
// temporarily unavailable, so that the learner is not taken for one that
// cannot move. If no store fits, the group expires and first-win relearns a
// target.
func (c *AffinityChecker) moveConflictingLearners(region *core.RegionInfo, group *affinity.GroupState,
	roles map[uint64]placement.PeerRoleType) bool {
	var conflicts []*metapb.Peer
	var stores []uint64
	for _, learner := range region.GetLearners() {
		if roles[learner.GetStoreId()] != placement.Learner {
			conflicts = append(conflicts, learner)
			stores = append(stores, learner.GetStoreId())
		}
	}
	if len(conflicts) == 0 {
		return true
	}
	fit := c.ruleManager.FitRegion(c.cluster, region)
	temporary := false
	for _, swap := range []bool{true, false} {
		target, swapped, blocked := c.placeLearners(region, fit, roles, conflicts, swap)
		temporary = temporary || blocked
		if target != nil {
			layout := regionWithRoles(region, group.LeaderStoreID, target)
			if c.isRegionPlacementRuleSatisfiedWithBestLocation(layout, false) {
				maps.Copy(roles, target)
				affinityCheckerLearnerMoveCounter.Inc()
				log.Debug("move learners off affinity voter stores",
					zap.Uint64("region-id", region.GetID()),
					zap.String("group-id", group.ID),
					zap.Uint64s("learner-stores", stores),
					zap.Bool("swap", swapped))
				return true
			}
			temporary = temporary || c.waitsForBetterStore(layout)
		}
		// Without a swap, a second pass would place the learners the same way.
		if !swapped {
			break
		}
	}
	if temporary {
		return false
	}
	affinityCheckerLearnerPeerConflictCounter.Inc()
	// The learners must stay, so let first-win relearn a target from a healthy
	// Region instead of retrying an impossible peer move.
	c.affinityManager.ExpireAffinityGroup(group.ID)
	log.Debug("expire affinity group after learner-voter conflict",
		zap.Uint64("region-id", region.GetID()),
		zap.String("group-id", group.ID),
		zap.Uint64s("learner-stores", stores))
	return false
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
// conflicts, or nil and whether a store is temporarily unavailable. With swap,
// a learner first takes a voter store of the Region that roles does not keep.
func (c *AffinityChecker) placeLearners(region *core.RegionInfo, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType, conflicts []*metapb.Peer, swap bool) (target map[uint64]placement.PeerRoleType, swapped, temporary bool) {
	target = maps.Clone(roles)
	for _, learner := range conflicts {
		rf := fit.GetRuleFit(learner.GetId())
		if rf == nil || learner.GetIsWitness() {
			return nil, swapped, false
		}
		storeID := uint64(0)
		if swap {
			storeID = c.learnerSwapStore(region, rf.Rule, target)
			swapped = swapped || storeID != 0
		}
		if storeID == 0 {
			used := make(map[uint64]struct{}, len(target))
			for id := range target {
				used[id] = struct{}{}
			}
			stores := slices.DeleteFunc(getRuleFitStores(c.cluster, rf), func(store *core.StoreInfo) bool {
				return store.GetID() == learner.GetStoreId()
			})
			storeID, temporary = c.strategy(region, rf.Rule, false).SelectStoreToAdd(stores, filter.NewExcludedFilter(c.Name(), nil, used))
			if storeID == 0 {
				return nil, swapped, temporary
			}
		}
		target[storeID] = placement.Learner
	}
	return target, swapped, false
}

// learnerSwapStore returns a voter store of region that roles does not keep
// and that rule allows, or 0. The voter there becomes the learner.
func (c *AffinityChecker) learnerSwapStore(region *core.RegionInfo, rule *placement.Rule, roles map[uint64]placement.PeerRoleType) uint64 {
	state := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	for _, voter := range region.GetVoters() {
		if _, kept := roles[voter.GetStoreId()]; kept || voter.GetIsWitness() {
			continue
		}
		store := c.cluster.GetStore(voter.GetStoreId())
		if store != nil && placement.MatchLabelConstraints(store, rule.LabelConstraints) &&
			state.Target(c.cluster.GetCheckerConfig(), store).IsOK() {
			return store.GetID()
		}
	}
	return 0
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
