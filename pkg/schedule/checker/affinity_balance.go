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
	"context"
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func (c *AffinityChecker) balanceRegions(group *affinity.GroupState, ranges affinity.GroupKeyRanges) ([]*core.RegionInfo, bool) {
	if len(ranges.KeyRanges) == 0 || !group.AffinitySchedulingAllowed {
		return nil, false
	}
	regions, err := c.cluster.BatchScanRegions(keyutil.NewKeyRanges(ranges.KeyRanges), core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) == 0 {
		return nil, false
	}
	waiting := make(map[uint64]struct{})
	if c.opController != nil {
		for _, op := range c.opController.GetWaitingOperators() {
			waiting[op.RegionID()] = struct{}{}
		}
	}
	for _, region := range regions {
		if region.GetLeader() == nil || !filter.IsRegionHealthy(region) || len(region.GetWitnesses()) != 0 {
			return nil, false
		}
		current, aligned := c.affinityManager.GetRegionAffinityGroupState(region)
		if current == nil || current.ID != group.ID || !aligned {
			return nil, false
		}
		if c.opController != nil && c.opController.GetOperator(region.GetID()) != nil {
			return nil, false
		}
		if _, ok := waiting[region.GetID()]; ok {
			return nil, false
		}
	}
	return regions, true
}

func (c *AffinityChecker) validateBalanceAction(ctx context.Context, group *affinity.GroupState, ranges affinity.GroupKeyRanges, plan affinity.BalancePlan) (ready, legal bool) {
	regions, covered := c.balanceRegions(group, ranges)
	if !covered {
		return false, false
	}
	for _, region := range regions {
		if ctx.Err() != nil {
			return false, false
		}
		virtual := cloneRegionWithReplacePeerStores(region, plan.LeaderStoreID, plan.VoterStoreIDs...)
		if virtual == nil || !c.isRegionPlacementRuleSatisfiedWithBestLocation(virtual, false) {
			return true, false
		}
	}
	return true, true
}

// buildBalanceActions runs on the affinity worker, once per invalidated group.
// It preserves RuleManager as the source of placement legality and intersects
// complete single-step actions, including their allowed leaders.
func (c *AffinityChecker) buildBalanceActions(ctx context.Context, group *affinity.GroupState, ranges affinity.GroupKeyRanges) (affinity.BalanceActionSet, bool) {
	regions, ready := c.balanceRegions(group, ranges)
	if !ready {
		return affinity.BalanceActionSet{}, false
	}
	stores := c.cluster.GetStores()
	var result affinity.BalanceActionSet
	for i, region := range regions {
		if ctx.Err() != nil {
			return affinity.BalanceActionSet{}, false
		}
		fit := c.ruleManager.FitRegion(c.cluster, region)
		if fit == nil || !fit.IsSatisfied() || !c.isRegionPlacementRuleSatisfiedWithBestLocation(region, true) {
			return affinity.BalanceActionSet{}, false
		}
		actions := c.regionBalanceActions(ctx, region, fit, group.VoterStoreIDs, stores)
		if i == 0 {
			result = actions
		} else {
			intersectBalanceActions(&result, actions)
		}
		if size := region.GetApproximateSize(); size > 0 {
			result.MoveBytes += uint64(size) * 1024 * 1024
		}
	}
	return result, ctx.Err() == nil
}

func (c *AffinityChecker) regionBalanceActions(ctx context.Context, region *core.RegionInfo, fit *placement.RegionFit, voters []uint64, stores []*core.StoreInfo) affinity.BalanceActionSet {
	conf := c.cluster.GetSharedConfig()
	// Cache placement and persistent store constraints. Admission limits and
	// short lived throttling remain in the existing operator execution path.
	leaderState := &filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true, AllowTemporaryStates: true}
	moveState := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	leaderSource := c.cluster.GetStore(region.GetLeader().GetStoreId())
	actions := affinity.BalanceActionSet{VoterReplacements: make(map[uint64]map[uint64][]uint64)}
	if leaderSource == nil {
		return actions
	}
	leaderGuard := filter.NewPlacementLeaderSafeguardWithFit(c.Name(), conf, c.cluster.GetBasicCluster(), c.ruleManager, region, leaderSource, fit, false)
	for _, leader := range voters {
		store := c.cluster.GetStore(leader)
		if store != nil && leaderState.Target(conf, store).IsOK() && (leaderGuard == nil || leaderGuard.Target(conf, store).IsOK()) {
			virtual := cloneRegionWithReplacePeerStores(region, leader, voters...)
			if virtual != nil && c.ruleManager.FitRegionWithoutCache(c.cluster, virtual).IsSatisfied() {
				actions.LeaderTargets = append(actions.LeaderTargets, leader)
			}
		}
	}
	for _, from := range voters {
		source := c.cluster.GetStore(from)
		if source == nil {
			continue
		}
		guard := filter.NewPlacementSafeguard(c.Name(), conf, c.cluster.GetBasicCluster(), c.ruleManager, region, source, fit)
		targets := make(map[uint64][]uint64)
		for _, dst := range stores {
			if ctx.Err() != nil {
				return actions
			}
			to := dst.GetID()
			// Existing learners must be preserved, rather than overwritten by
			// a voter replacement.
			if region.GetStorePeer(to) != nil || !dst.IsUp() || !dst.IsTiKV() ||
				!moveState.Target(conf, dst).IsOK() || (guard != nil && !guard.Target(conf, dst).IsOK()) {
				continue
			}
			newVoters := slices.Clone(voters)
			newVoters[slices.Index(voters, from)] = to
			for _, leader := range newVoters {
				store := c.cluster.GetStore(leader)
				if store == nil || !leaderState.Target(conf, store).IsOK() {
					continue
				}
				virtual := cloneRegionWithReplacePeerStores(region, leader, newVoters...)
				if virtual == nil {
					continue
				}
				newFit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
				// The original layout is already at its best location. A
				// replacement must preserve every rule's isolation score;
				// do not enumerate stores again for each candidate.
				if newFit.IsSatisfied() && placementNotWorse(fit, newFit) {
					targets[to] = append(targets[to], leader)
				}
			}
		}
		if len(targets) > 0 {
			actions.VoterReplacements[from] = targets
		}
	}
	return actions
}

func placementNotWorse(oldFit, newFit *placement.RegionFit) bool {
	for _, oldRule := range oldFit.RuleFits {
		matched := false
		for _, newRule := range newFit.RuleFits {
			if oldRule.Rule.GroupID == newRule.Rule.GroupID && oldRule.Rule.ID == newRule.Rule.ID {
				matched = true
				if newRule.IsolationScore < oldRule.IsolationScore {
					return false
				}
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func intersectBalanceActions(dst *affinity.BalanceActionSet, src affinity.BalanceActionSet) {
	dst.LeaderTargets = slices.DeleteFunc(dst.LeaderTargets, func(store uint64) bool {
		return !slices.Contains(src.LeaderTargets, store)
	})
	for from, targets := range dst.VoterReplacements {
		for to, leaders := range targets {
			leaders = slices.DeleteFunc(leaders, func(leader uint64) bool {
				return !slices.Contains(src.VoterReplacements[from][to], leader)
			})
			if len(leaders) == 0 {
				delete(targets, to)
			} else {
				targets[to] = leaders
			}
		}
		if len(targets) == 0 {
			delete(dst.VoterReplacements, from)
		}
	}
}
