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
	"bytes"
	"context"
	"iter"
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceStatus uint8

const (
	// Store checks combine their results in increasing order of severity.
	balanceAllowed balanceStatus = iota
	balanceTemporarilyBlocked
	balanceRejected
	balanceUnready
)

// balanceRegions walks the known Regions of a group's ranges in key order.
func (c *AffinityChecker) balanceRegions(ctx context.Context, ranges affinity.GroupKeyRanges) iter.Seq[*core.RegionInfo] {
	return func(yield func(*core.RegionInfo) bool) {
		merged := keyutil.NewKeyRanges(ranges.KeyRanges)
		slices.SortFunc(merged.Ranges(), func(a, b *keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
		merged.Merge()
		var lastRegion uint64
		for _, keyRange := range merged.Ranges() {
			nextKey := slices.Clone(keyRange.StartKey)
			for {
				if ctx.Err() != nil {
					return
				}
				regions := c.cluster.ScanRegions(nextKey, keyRange.EndKey, 1)
				if len(regions) == 0 {
					break
				}
				region := regions[0]
				nextKey = slices.Clone(region.GetEndKey())
				// A Region can span consecutive ranges. ScanRegions releases
				// the tree lock before the consumer checks the Region.
				if id := region.GetID(); id != lastRegion {
					lastRegion = id
					if !yield(region) {
						return
					}
				}
				if len(nextKey) == 0 || len(keyRange.EndKey) > 0 && bytes.Compare(nextKey, keyRange.EndKey) >= 0 {
					break
				}
			}
		}
	}
}

// balanceRegionReady reports whether a Region of the group is healthy,
// schedulable and already in the group's target.
func (c *AffinityChecker) balanceRegionReady(group *affinity.GroupState, region *core.RegionInfo) bool {
	if region == nil || region.GetLeader() == nil || !filter.IsRegionHealthy(region) || len(region.GetWitnesses()) != 0 {
		return false
	}
	if labeler := c.cluster.GetRegionLabeler(); labeler != nil && labeler.ScheduleDisabled(region) {
		return false
	}
	current, isAffinity := c.affinityManager.GetRegionAffinityGroupState(region)
	return current != nil && current.ID == group.ID && isAffinity
}

// validateBalanceCurrentRegion checks that a Region is ready and at its best
// location, and returns its fit.
func (c *AffinityChecker) validateBalanceCurrentRegion(group *affinity.GroupState, region *core.RegionInfo) (*placement.RegionFit, balanceStatus) {
	if !c.balanceRegionReady(group, region) {
		return nil, balanceUnready
	}
	fit := c.ruleManager.FitRegion(c.cluster, region)
	if c.checkRegionPlacement(region, fit) != affinityPlacementReady {
		return nil, balanceUnready
	}
	return fit, balanceAllowed
}

// validateBalanceTarget checks a Region moved to the target: it must keep its
// learners, satisfy the rules without lower isolation and be at its best location.
func (c *AffinityChecker) validateBalanceTarget(target *affinity.Group, region *core.RegionInfo, currentFit *placement.RegionFit) balanceStatus {
	for _, learner := range region.GetLearners() {
		if slices.Contains(target.VoterStoreIDs, learner.GetStoreId()) {
			return balanceRejected
		}
	}
	virtual := cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
	if virtual == nil {
		return balanceRejected
	}
	fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
	if fit == nil || !fit.IsSatisfied() || !placementNotWorse(currentFit, fit) {
		return balanceRejected
	}
	switch c.checkRegionPlacement(virtual, fit) {
	case affinityPlacementReady:
		return balanceAllowed
	case affinityPlacementTemporary:
		return balanceTemporarilyBlocked
	default:
		return balanceRejected
	}
}

// balanceStoreAction is a change of a target on one store: the leader or a
// voter moves away from it (source) or onto it.
type balanceStoreAction struct {
	id             uint64
	leader, source bool
}

// balanceStoreActionChecker returns a balanceStoreActionStatus that remembers
// its results, for building the input of one plan.
func (c *AffinityChecker) balanceStoreActionChecker() func(balanceStoreAction) balanceStatus {
	results := make(map[balanceStoreAction]balanceStatus)
	return func(action balanceStoreAction) balanceStatus {
		if status, ok := results[action]; ok {
			return status
		}
		status := c.balanceStoreActionStatus(action.id, action.leader, action.source)
		results[action] = status
		return status
	}
}

// balanceTargetExecutionStatus checks the store actions that moving a group
// from its current target to target takes.
func (c *AffinityChecker) balanceTargetExecutionStatus(current, target *affinity.Group) balanceStatus {
	status := balanceAllowed
	check := func(id uint64, leader, source bool) {
		if status != balanceRejected {
			status = max(status, c.balanceStoreActionStatus(id, leader, source))
		}
	}
	if current.LeaderStoreID != target.LeaderStoreID {
		check(current.LeaderStoreID, true, true)
		check(target.LeaderStoreID, true, false)
	}
	for _, id := range current.VoterStoreIDs {
		if !slices.Contains(target.VoterStoreIDs, id) {
			check(id, false, true)
		}
	}
	for _, id := range target.VoterStoreIDs {
		if !slices.Contains(current.VoterStoreIDs, id) {
			check(id, false, false)
		}
	}
	return status
}

// balanceStoreActionStatus checks a store with the filters of leader transfer
// and Region move. A store that only temporary states block, such as busy or
// store limit, is temporarily blocked.
func (c *AffinityChecker) balanceStoreActionStatus(id uint64, leader, source bool) balanceStatus {
	return c.affinityStoreActionStatus(id, leader, source, true)
}

func (c *AffinityChecker) affinityStoreActionStatus(id uint64, leader, source, checkPeerLimit bool) balanceStatus {
	store := c.cluster.GetStore(id)
	if store == nil || !source && (!store.IsUp() || !store.IsTiKV()) {
		return balanceRejected
	}
	conf := c.cluster.GetSharedConfig()
	if !leader && !source && (!filter.NewSpecialUseFilter(c.Name()).Target(conf, store).IsOK() ||
		!filter.NewStorageThresholdFilter(c.Name()).Target(conf, store).IsOK()) {
		return balanceRejected
	}
	state := filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: leader, MoveRegion: !leader}
	check := func() bool {
		if source {
			return state.Source(conf, store).IsOK()
		}
		status := state.Target(conf, store)
		// Affinity moves reach operator admission even when AddPeer has no
		// tokens. The filter returns that reason before checking pressure.
		if !checkPeerLimit && status.StatusCode == plan.StatusStoreAddLimitThrottled {
			return uint64(store.GetSendingSnapCount()) <= conf.GetMaxSnapshotCount() &&
				uint64(store.GetReceivingSnapCount()) <= conf.GetMaxSnapshotCount() &&
				(conf.GetMaxPendingPeerCount() == 0 || store.GetPendingPeerCount() <= int(conf.GetMaxPendingPeerCount()))
		}
		return status.IsOK()
	}
	if check() {
		return balanceAllowed
	}
	state.AllowTemporaryStates = true
	if check() {
		return balanceTemporarilyBlocked
	}
	return balanceRejected
}

// balanceAllowed reports whether new balance targets may be written.
func (c *AffinityChecker) balanceAllowed() bool {
	return !c.IsPaused() && !c.cluster.IsSchedulingHalted() &&
		c.cluster.GetSharedConfig().IsPlacementRulesEnabled() && c.conf.GetAffinityScheduleLimit() > 0
}

// placementNotWorse reports whether every rule of oldFit is in newFit with an
// isolation score that is not lower.
func placementNotWorse(oldFit, newFit *placement.RegionFit) bool {
	for _, oldRule := range oldFit.RuleFits {
		i := slices.IndexFunc(newFit.RuleFits, func(newRule *placement.RuleFit) bool {
			return newRule.Rule.Key() == oldRule.Rule.Key()
		})
		if i < 0 || newFit.RuleFits[i].IsolationScore < oldRule.IsolationScore {
			return false
		}
	}
	return true
}
