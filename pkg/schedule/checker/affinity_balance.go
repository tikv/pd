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
	"encoding/json"
	"hash/fnv"
	"slices"
	"sort"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceStatus uint8

const (
	balanceAllowed balanceStatus = iota
	balanceRejected
	balanceTemporarilyBlocked
	balanceUnready
)

// This cursor belongs to one checker search. It stops only between complete
// Region checks; placement and store selection never carry resumable state.
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

func (p *balanceRegionScan) next(cluster sche.CheckerCluster) (region *core.RegionInfo, covered, done bool) {
	for p.rangeIndex < len(p.ranges) {
		keyRange := p.ranges[p.rangeIndex]
		if !p.started {
			p.nextKey, p.started = slices.Clone(keyRange.StartKey), true
		}
		regions := cluster.ScanRegions(p.nextKey, keyRange.EndKey, 1)
		if len(regions) == 0 || !balanceRegionCoversKeyRange(regions[0], p.nextKey, keyRange.EndKey) {
			return nil, false, false
		}
		region = regions[0]
		p.nextKey = slices.Clone(region.GetEndKey())
		if len(p.nextKey) == 0 || len(keyRange.EndKey) > 0 && bytes.Compare(p.nextKey, keyRange.EndKey) >= 0 {
			p.rangeIndex++
			p.started = false
		}
		if p.lastRegion == region.GetID() {
			continue
		}
		p.lastRegion = region.GetID()
		return region, true, false
	}
	return nil, true, true
}

func balanceRegionCoversKeyRange(region *core.RegionInfo, startKey, endKey []byte) bool {
	if region == nil || len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), startKey) <= 0 ||
		len(region.GetStartKey()) > 0 && bytes.Compare(region.GetStartKey(), startKey) > 0 {
		return false
	}
	return len(endKey) == 0 || len(region.GetStartKey()) == 0 || bytes.Compare(region.GetStartKey(), endKey) < 0
}

func (c *AffinityChecker) balanceRuleMetadata() map[string]string {
	rules, groups := c.ruleManager.GetRuleConfigForReconcile()
	metadata := make(map[string]string, len(rules)+len(groups))
	for _, rule := range rules {
		data, _ := json.Marshal(rule)
		metadata[keypath.RuleKeyPath(rule.StoreKey())] = string(data)
	}
	for _, group := range groups {
		data, _ := json.Marshal(group)
		metadata[keypath.RuleGroupIDPath(group.ID)] = string(data)
	}
	return metadata
}

// Only inputs intersecting the table affect its completed search or progress.
// Global rule/label generations are still sampled around this calculation and
// their durable tokens fence the final target transaction.
func (c *AffinityChecker) balanceInputFingerprint(snapshot *affinity.BalanceSnapshot) uint64 {
	var ranges []keyutil.KeyRange
	for _, group := range snapshot.Groups {
		ranges = append(ranges, snapshot.Ranges[group.ID].KeyRanges...)
	}
	// Merge the table's range union once. Its increasing end keys allow a
	// binary intersection check for each label range instead of a table scan.
	slices.SortFunc(ranges, func(a, b keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	merged := ranges[:0]
	for _, keyRange := range ranges {
		if len(merged) > 0 {
			last := &merged[len(merged)-1]
			if len(last.EndKey) == 0 || bytes.Compare(keyRange.StartKey, last.EndKey) <= 0 {
				if len(keyRange.EndKey) == 0 || len(last.EndKey) > 0 && bytes.Compare(keyRange.EndKey, last.EndKey) > 0 {
					last.EndKey = keyRange.EndKey
				}
				continue
			}
		}
		merged = append(merged, keyRange)
	}
	ranges = merged
	rules, groups := c.ruleManager.GetRuleConfigForRanges(ranges)
	inputs := make(map[string][]byte, len(rules)+len(groups))
	for _, rule := range rules {
		inputs["rule:"+rule.StoreKey()], _ = json.Marshal(rule)
	}
	for _, group := range groups {
		inputs["group:"+group.ID], _ = json.Marshal(group)
	}
	c.cluster.GetRegionLabeler().IterateLabelRules(func(rule *labeler.LabelRule) bool {
		for _, labelRange := range rule.GetKeyRanges() {
			index := sort.Search(len(ranges), func(i int) bool {
				return len(ranges[i].EndKey) == 0 || bytes.Compare(ranges[i].EndKey, labelRange.StartKey) > 0
			})
			if index < len(ranges) && (len(labelRange.EndKey) == 0 || bytes.Compare(ranges[index].StartKey, labelRange.EndKey) < 0) {
				inputs["label:"+rule.ID], _ = json.Marshal(rule)
				return true
			}
		}
		return true
	})
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	hash := fnv.New64a()
	for _, key := range keys {
		_, _ = hash.Write([]byte(key))
		_, _ = hash.Write(inputs[key])
	}
	return hash.Sum64()
}

func (c *AffinityChecker) waitingAffinityRegions() map[uint64]struct{} {
	waiting := make(map[uint64]struct{})
	if c.opController != nil {
		for _, op := range c.opController.GetWaitingOperators() {
			waiting[op.RegionID()] = struct{}{}
		}
	}
	return waiting
}

func (c *AffinityChecker) balanceRegionReady(group *affinity.GroupState, region *core.RegionInfo, waiting map[uint64]struct{}) bool {
	if region == nil || region.GetLeader() == nil || !filter.IsRegionHealthy(region) || len(region.GetWitnesses()) != 0 {
		return false
	}
	if labeler := c.cluster.GetRegionLabeler(); labeler != nil && labeler.ScheduleDisabled(region) {
		return false
	}
	current, aligned := c.affinityManager.GetRegionAffinityGroupState(region)
	if current == nil || current.ID != group.ID || !aligned {
		return false
	}
	if _, ok := waiting[region.GetID()]; ok {
		return false
	}
	if c.opController != nil {
		if c.opController.GetOperator(region.GetID()) != nil {
			return false
		}
	}
	return true
}

// validateBalanceRegion completes all checks for this Region synchronously.
// Current-layout failures stop the table until RuleChecker or operators repair
// it; a rejected candidate only advances the search to another target.
func (c *AffinityChecker) validateBalanceRegion(actual *affinity.GroupState, target *affinity.Group,
	region *core.RegionInfo, waiting map[uint64]struct{}) balanceStatus {
	if !c.balanceRegionReady(actual, region, waiting) {
		return balanceUnready
	}
	currentFit := c.ruleManager.FitRegion(c.cluster, region)
	if c.regionPlacementBestLocation(region, currentFit) != bestLocationSatisfied {
		return balanceUnready
	}
	if target == nil {
		return balanceAllowed
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
	case bestLocationRejected:
		return balanceRejected
	case bestLocationTemporarilyBlocked:
		return balanceTemporarilyBlocked
	default:
		return balanceAllowed
	}
}

func (c *AffinityChecker) balanceTargetExecutionStatus(actual, target *affinity.Group) balanceStatus {
	type storeAction struct {
		id             uint64
		leader, source bool
	}
	var actions []storeAction
	if actual.LeaderStoreID != target.LeaderStoreID {
		actions = append(actions, storeAction{actual.LeaderStoreID, true, true}, storeAction{target.LeaderStoreID, true, false})
	}
	for _, id := range actual.VoterStoreIDs {
		if !slices.Contains(target.VoterStoreIDs, id) {
			actions = append(actions, storeAction{id, false, true})
		}
	}
	for _, id := range target.VoterStoreIDs {
		if !slices.Contains(actual.VoterStoreIDs, id) {
			actions = append(actions, storeAction{id, false, false})
		}
	}
	status := balanceAllowed
	for _, action := range actions {
		result := c.balanceStoreActionStatus(action.id, action.leader, action.source)
		if result == balanceRejected {
			return result
		}
		if result == balanceTemporarilyBlocked {
			status = result
		}
	}
	return status
}

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

func (c *AffinityChecker) balanceSchedulingAllowed() bool {
	if c.cluster == nil || c.conf == nil || c.affinityManager == nil || c.IsPaused() || c.cluster.IsSchedulingHalted() ||
		!c.cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		return false
	}
	limit := c.conf.GetAffinityScheduleLimit()
	return limit > 0 && (c.opController == nil || c.opController.OperatorCount(operator.OpAffinity) < limit)
}

func approximateBalanceRegionBytes(region *core.RegionInfo) uint64 {
	if size := region.GetApproximateSize(); size > 0 {
		return uint64(size) * 1024 * 1024
	}
	return 0
}

func placementNotWorse(oldFit, newFit *placement.RegionFit) bool {
	if oldFit == nil || newFit == nil {
		return false
	}
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
