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
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceStatus uint8

const (
	balanceAllowed balanceStatus = iota
	balanceRejected
	balanceTemporarilyBlocked
	balanceUnready
)

// balanceRegionScan walks the Regions of a group's ranges in key order.
type balanceRegionScan struct {
	ranges     []*keyutil.KeyRange
	rangeIndex int
	nextKey    []byte
	started    bool
	lastRegion uint64
}

func newBalanceRegionScan(ranges affinity.GroupKeyRanges) balanceRegionScan {
	merged := keyutil.NewKeyRanges(ranges.KeyRanges)
	slices.SortFunc(merged.Ranges(), func(a, b *keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	merged.Merge()
	return balanceRegionScan{ranges: merged.Ranges()}
}

// next returns the next Region. covered is false if no Region covers the next
// key, and done is true after the last Region.
func (p *balanceRegionScan) next(cluster sche.CheckerCluster) (region *core.RegionInfo, covered, done bool) {
	for p.rangeIndex < len(p.ranges) {
		keyRange := p.ranges[p.rangeIndex]
		if !p.started {
			p.nextKey, p.started = slices.Clone(keyRange.StartKey), true
		}
		regions := cluster.ScanRegions(p.nextKey, keyRange.EndKey, 1)
		if len(regions) == 0 || !balanceRegionCoversKey(regions[0], p.nextKey) {
			return nil, false, false
		}
		region = regions[0]
		p.nextKey = slices.Clone(region.GetEndKey())
		if len(p.nextKey) == 0 || len(keyRange.EndKey) > 0 && bytes.Compare(p.nextKey, keyRange.EndKey) >= 0 {
			p.rangeIndex++
			p.started = false
		}
		// A Region may cover the end of one range and the start of the next.
		if p.lastRegion == region.GetID() {
			continue
		}
		p.lastRegion = region.GetID()
		return region, true, false
	}
	return nil, true, true
}

func balanceRegionCoversKey(region *core.RegionInfo, key []byte) bool {
	return bytes.Compare(region.GetStartKey(), key) <= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(key, region.GetEndKey()) < 0)
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
	if c.regionPlacementBestLocation(region, fit) != bestLocationSatisfied {
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
	switch c.regionPlacementBestLocation(virtual, fit) {
	case bestLocationSatisfied:
		return balanceAllowed
	case bestLocationTemporarilyBlocked:
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
	var actions []balanceStoreAction
	if current.LeaderStoreID != target.LeaderStoreID {
		actions = append(actions, balanceStoreAction{current.LeaderStoreID, true, true}, balanceStoreAction{target.LeaderStoreID, true, false})
	}
	for _, id := range current.VoterStoreIDs {
		if !slices.Contains(target.VoterStoreIDs, id) {
			actions = append(actions, balanceStoreAction{id, false, true})
		}
	}
	for _, id := range target.VoterStoreIDs {
		if !slices.Contains(current.VoterStoreIDs, id) {
			actions = append(actions, balanceStoreAction{id, false, false})
		}
	}
	status := balanceAllowed
	for _, action := range actions {
		switch c.balanceStoreActionStatus(action.id, action.leader, action.source) {
		case balanceRejected:
			return balanceRejected
		case balanceTemporarilyBlocked:
			status = balanceTemporarilyBlocked
		}
	}
	return status
}

// balanceStoreActionStatus checks a store with the filters of leader transfer
// and Region move. A store that only temporary states block, such as busy or
// store limit, is temporarily blocked.
func (c *AffinityChecker) balanceStoreActionStatus(id uint64, leader, source bool) balanceStatus {
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
		return state.Target(conf, store).IsOK()
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
