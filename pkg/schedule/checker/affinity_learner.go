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
	"cmp"
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
// created by SQL PLACEMENT POLICY. It tries to reuse dropped voters before
// adding peers and updates roles only when the full layout passes placement
// checks. A temporary result asks the caller to retry later. A rejection means
// the bounded search found no usable layout.
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
		return c.checkRegionPlacement(layout, c.ruleManager.FitRegionWithoutCache(c.cluster, layout))
	}
	remaining := 64
	target, temporary := c.placeLearners(region, fit, roles, isolationStores, conflicts, &remaining, accept)
	if target != nil {
		maps.Copy(roles, target)
		log.Debug("move learners off affinity voter stores",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID))
		return true, affinityPlacementReady
	}
	if !temporary {
		return false, affinityPlacementRejected
	}
	// A busy learner can prevent the search from reaching another learner
	// that cannot move. Check each learner on its own before waiting.
	for i := range conflicts {
		soloRemaining := 8
		target, blocked := c.placeLearners(region, fit, roles, isolationStores, conflicts[i:i+1], &soloRemaining,
			func(map[uint64]placement.PeerRoleType) affinityPlacementStatus { return affinityPlacementReady })
		if target == nil && !blocked && soloRemaining > 0 {
			return false, affinityPlacementRejected
		}
	}
	return false, affinityPlacementTemporary
}

// placeLearners tries each eligible swap in location score order, then up to
// one addition candidate per remaining conflict. It backtracks when later
// learners cannot move or accept rejects the complete layout, and reports if
// any attempted destination or layout was temporarily blocked.
func (c *AffinityChecker) placeLearners(region *core.RegionInfo, fit *placement.RegionFit,
	roles map[uint64]placement.PeerRoleType, isolationStores map[*placement.Rule][]*core.StoreInfo, conflicts []*metapb.Peer,
	remaining *int, accept func(map[uint64]placement.PeerRoleType) affinityPlacementStatus) (target map[uint64]placement.PeerRoleType, temporary bool) {
	if *remaining == 0 {
		return nil, false
	}
	*remaining--
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
	rule := rf.Rule
	peers := isolationStores[rule]
	var isolation filter.Filter
	if len(rule.LocationLabels) > 0 && rule.IsolationLevel != "" && len(peers) > 0 {
		isolation = filter.NewIsolationFilter(c.Name(), rule.IsolationLevel, rule.LocationLabels, peers)
	}
	tryStore := func(store *core.StoreInfo) map[uint64]placement.PeerRoleType {
		next := maps.Clone(roles)
		next[store.GetID()] = placement.Learner
		isolationStores[rule] = append(peers, store)
		target, blocked := c.placeLearners(region, fit, next, isolationStores, conflicts[1:], remaining, accept)
		isolationStores[rule] = peers
		temporary = temporary || blocked
		return target
	}
	swaps := c.learnerSwapStores(region, rule, roles)
	if len(rule.LocationLabels) > 0 && len(peers) > 0 {
		slices.SortStableFunc(swaps, func(a, b *core.StoreInfo) int {
			return cmp.Compare(core.DistinctScore(rule.LocationLabels, peers, b), core.DistinctScore(rule.LocationLabels, peers, a))
		})
	}
	for _, store := range swaps {
		if *remaining == 0 {
			return nil, temporary
		}
		if isolation != nil && !isolation.Target(c.cluster.GetCheckerConfig(), store).IsOK() {
			continue
		}
		if target := tryStore(store); target != nil {
			return target, temporary
		}
	}
	excluded := make(map[uint64]struct{}, len(roles))
	for id := range roles {
		excluded[id] = struct{}{}
	}
	for range conflicts {
		if *remaining == 0 {
			break
		}
		storeID, blocked := c.selectLearnerStore(region, fit, rule, learnerStore, roles, excluded, isolation)
		temporary = temporary || blocked
		if storeID == 0 {
			break
		}
		if store := c.cluster.GetStore(storeID); store != nil {
			if target := tryStore(store); target != nil {
				return target, temporary
			}
		}
		excluded[storeID] = struct{}{}
	}
	return nil, temporary
}

// Like getBetterLocation, selectLearnerStore scores location against peers on
// the same engine that match any rule of the same role. Hard isolation only
// compares peers fitted to the learner's own rule.
func (c *AffinityChecker) selectLearnerStore(region *core.RegionInfo, fit *placement.RegionFit, rule *placement.Rule,
	learnerStore *core.StoreInfo, roles map[uint64]placement.PeerRoleType, excluded map[uint64]struct{}, isolation filter.Filter) (uint64, bool) {
	var stores []*core.StoreInfo
	for id := range roles {
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
	return strategy.SelectStoreToAdd(stores, filter.NewExcludedFilter(c.Name(), nil, excluded))
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
