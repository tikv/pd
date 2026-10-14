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
	"encoding/json"
	"hash/fnv"
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceRegionRef struct {
	id       uint64
	version  uint64
	confVer  uint64
	leaderID uint64
}

type balanceVoterCandidate struct {
	from   uint64
	to     uint64
	leader uint64
}

type balanceRegionScanCursor struct {
	ranges     []keyutil.KeyRange
	rangeIndex int
	nextKey    []byte
	started    bool
	lastRegion uint64
}

type balanceRegionScanResult struct {
	region  *core.RegionInfo
	found   bool
	done    bool
	scanned bool
	covered bool
}

type balanceActionProgress struct {
	mode                       affinity.BalanceActionMode
	actualGroupID              string
	actualLeader               uint64
	actualVoters               []uint64
	targetLeader               uint64
	targetVoters               []uint64
	storeIDs                   []uint64
	regionScan                 balanceRegionScanCursor
	currentRegion              balanceRegionRef
	hasCurrentRegion           bool
	candidateIntersectionReady bool
	candidateIndex             int
	pendingCandidate           bool
	pendingCandidateValidated  bool
	pendingLeader              uint64
	pendingVoter               balanceVoterCandidate
	leaderTargets              []uint64
	nextLeaderTarget           []uint64
	voters                     []balanceVoterCandidate
	nextVoters                 []balanceVoterCandidate
	voterFromIndex             int
	voterToIndex               int
	voterLeaderIndex           int
	voterToChecked             bool
	baselineRegion             *core.RegionInfo
	baselineFit                *placement.RegionFit
	actualPlacementChecked     bool
	pageRegionBytes            map[uint64]uint64
	moveBytes                  uint64
	temporarilyBlocked         bool
	actions                    affinity.BalanceActionSet
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

// balanceRuleFingerprint hashes only placement inputs that can apply to the
// supplied affinity ranges. RuleManager's global revision is still checked at
// submission time, while this scoped value keeps unrelated tables from
// rebuilding their action indexes.
func (c *AffinityChecker) balanceRuleFingerprint(ranges []affinity.GroupKeyRanges) uint64 {
	rules := make(map[string][]byte)
	groups := make(map[string][]byte)
	for _, groupRanges := range ranges {
		for _, keyRange := range groupRanges.KeyRanges {
			for _, rule := range c.ruleManager.GetRulesForApplyRange(keyRange.StartKey, keyRange.EndKey) {
				data, _ := json.Marshal(rule)
				rules[rule.StoreKey()] = data
				if group := c.ruleManager.GetRuleGroup(rule.GroupID); group != nil {
					groupData, _ := json.Marshal(group)
					groups[rule.GroupID] = groupData
				}
			}
		}
	}
	h := fnv.New64a()
	keys := make([]string, 0, len(groups)+len(rules))
	for key := range groups {
		keys = append(keys, "group:"+key)
	}
	for key := range rules {
		keys = append(keys, "rule:"+key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		_, _ = h.Write([]byte(key))
		if len(key) > len("group:") && key[:len("group:")] == "group:" {
			_, _ = h.Write(groups[key[len("group:"):]])
		} else {
			_, _ = h.Write(rules[key[len("rule:"):]])
		}
	}
	return h.Sum64()
}

func newBalanceRegionScanCursor(ranges affinity.GroupKeyRanges) balanceRegionScanCursor {
	keyRanges := keyutil.NewKeyRanges(ranges.KeyRanges)
	keyRanges.Merge()
	merged := keyRanges.Ranges()
	cursor := balanceRegionScanCursor{ranges: make([]keyutil.KeyRange, 0, len(merged))}
	for _, keyRange := range merged {
		cursor.ranges = append(cursor.ranges, keyutil.KeyRange{
			StartKey: slices.Clone(keyRange.StartKey),
			EndKey:   slices.Clone(keyRange.EndKey),
		})
	}
	return cursor
}

// next scans at most one region. scanned counts ScanRegions calls; covered is
// false when a range is not covered. Duplicate regions are skipped one scan
// at a time so the caller can keep the work budget accurate.
func (p *balanceRegionScanCursor) next(cluster sche.CheckerCluster) balanceRegionScanResult {
	for p.rangeIndex < len(p.ranges) {
		keyRange := p.ranges[p.rangeIndex]
		if !p.started {
			p.nextKey = slices.Clone(keyRange.StartKey)
			p.started = true
		}
		if len(keyRange.EndKey) > 0 && bytes.Compare(p.nextKey, keyRange.EndKey) >= 0 {
			p.rangeIndex++
			p.started = false
			continue
		}

		regions := cluster.ScanRegions(p.nextKey, keyRange.EndKey, 1)
		if len(regions) == 0 || !balanceRegionCoversKeyRange(regions[0], p.nextKey, keyRange.EndKey) {
			return balanceRegionScanResult{scanned: true}
		}
		region := regions[0]
		p.nextKey = slices.Clone(region.GetEndKey())
		if len(region.GetEndKey()) == 0 || (len(keyRange.EndKey) > 0 && bytes.Compare(region.GetEndKey(), keyRange.EndKey) >= 0) {
			p.rangeIndex++
			p.started = false
		}
		if region.GetID() == p.lastRegion {
			return balanceRegionScanResult{scanned: true, covered: true}
		}
		p.lastRegion = region.GetID()
		return balanceRegionScanResult{region: region, found: true, scanned: true, covered: true}
	}
	return balanceRegionScanResult{done: true, covered: true}
}

func (p *balanceRegionScanCursor) done() bool {
	return p.rangeIndex >= len(p.ranges)
}

func balanceRegionCoversKeyRange(region *core.RegionInfo, startKey, endKey []byte) bool {
	if region == nil || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), startKey) <= 0) ||
		(len(region.GetStartKey()) > 0 && bytes.Compare(region.GetStartKey(), startKey) > 0) {
		return false
	}
	return len(endKey) == 0 || len(region.GetStartKey()) == 0 || bytes.Compare(region.GetStartKey(), endKey) < 0
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
	if c.opController != nil {
		if c.opController.GetOperator(region.GetID()) != nil {
			return false
		}
		if _, ok := waiting[region.GetID()]; ok {
			return false
		}
	}
	return true
}

func (c *AffinityChecker) validateBalanceAction(ctx context.Context, group *affinity.GroupState, ranges affinity.GroupKeyRanges, plan affinity.BalancePlan) (ready, legal bool) {
	if group == nil {
		return false, false
	}
	target := group.Group
	target.LeaderStoreID, target.VoterStoreIDs = plan.LeaderStoreID, plan.VoterStoreIDs
	page := c.buildBalanceActionsForTarget(ctx, group, &target, ranges, nil, affinity.BalanceValidateTarget, nil, 0)
	return page.Ready, page.Ready && page.Complete && !page.InvalidTarget
}

func (c *AffinityChecker) balanceTargetExecutionStatus(group *affinity.Group, plan affinity.BalancePlan) balanceStoreStatus {
	if plan.LeaderStoreID != group.LeaderStoreID {
		for _, status := range []balanceStoreStatus{c.leaderSourceStatus(group.LeaderStoreID), c.leaderTargetStatus(plan.LeaderStoreID)} {
			if status != balanceStoreAllowed {
				return status
			}
		}
	}
	for _, from := range group.VoterStoreIDs {
		if !slices.Contains(plan.VoterStoreIDs, from) {
			store := c.cluster.GetStore(from)
			if store == nil {
				return balanceStoreRejected
			}
			if status := c.voterSourceStatus(store); status != balanceStoreAllowed {
				return status
			}
		}
	}
	for _, to := range plan.VoterStoreIDs {
		if !slices.Contains(group.VoterStoreIDs, to) {
			store := c.cluster.GetStore(to)
			if store == nil || !store.IsUp() || !store.IsTiKV() {
				return balanceStoreRejected
			}
			if status := c.voterTargetAllowed(store); status != balanceStoreAllowed {
				return status
			}
		}
	}
	return balanceStoreAllowed
}

func (c *AffinityChecker) buildBalanceActionsProgress(ctx context.Context, group *affinity.GroupState, ranges affinity.GroupKeyRanges,
	storeIDs []uint64, mode affinity.BalanceActionMode, cursor any, budget int) affinity.BalanceActionBuildResult {
	if group == nil {
		return affinity.BalanceActionBuildResult{BlockTable: true}
	}
	return c.buildBalanceActionsForTarget(ctx, group, &group.Group, ranges, storeIDs, mode, cursor, budget)
}

func (c *AffinityChecker) buildVirtualBalanceActionsProgress(ctx context.Context, actual *affinity.GroupState, ranges affinity.GroupKeyRanges,
	target *affinity.Group, storeIDs []uint64, mode affinity.BalanceActionMode, cursor any, budget int) affinity.BalanceActionBuildResult {
	if actual == nil || target == nil {
		return affinity.BalanceActionBuildResult{BlockTable: true}
	}
	return c.buildBalanceActionsForTarget(ctx, actual, target, ranges, storeIDs, mode, cursor, budget)
}

func (c *AffinityChecker) buildBalanceActionsForTarget(ctx context.Context, actual *affinity.GroupState, target *affinity.Group,
	ranges affinity.GroupKeyRanges, storeIDs []uint64, mode affinity.BalanceActionMode, cursor any, budget int) affinity.BalanceActionBuildResult {
	if ctx.Err() != nil || !c.balanceSchedulingAllowed() {
		return affinity.BalanceActionBuildResult{BlockTable: true}
	}
	if mode != affinity.BalanceBuildLeaders && mode != affinity.BalanceBuildVoters && mode != affinity.BalanceCheckConvergence && mode != affinity.BalanceValidateTarget {
		return affinity.BalanceActionBuildResult{BlockTable: true}
	}
	if mode == affinity.BalanceValidateTarget {
		if len(target.VoterStoreIDs) != len(actual.VoterStoreIDs) || target.LeaderStoreID == 0 || !slices.Contains(target.VoterStoreIDs, target.LeaderStoreID) {
			return affinity.BalanceActionBuildResult{Ready: true, Complete: true, InvalidTarget: true}
		}
		seen := make(map[uint64]struct{}, len(target.VoterStoreIDs))
		for _, storeID := range target.VoterStoreIDs {
			if _, duplicate := seen[storeID]; storeID == 0 || duplicate {
				return affinity.BalanceActionBuildResult{Ready: true, Complete: true, InvalidTarget: true}
			}
			seen[storeID] = struct{}{}
		}
		if result := c.balanceValidationExecutionResult(actual, target, 0); !result.Ready || result.InvalidTarget {
			return result
		}
	}
	if !actual.AffinitySchedulingAllowed || actual.Phase != affinity.PhaseStable || len(ranges.KeyRanges) == 0 || len(target.VoterStoreIDs) != len(actual.VoterStoreIDs) ||
		target.LeaderStoreID == 0 || !slices.Contains(target.VoterStoreIDs, target.LeaderStoreID) {
		return affinity.BalanceActionBuildResult{}
	}

	progress, ok := cursor.(*balanceActionProgress)
	if !ok || !progress.matches(actual, target, ranges, storeIDs, mode) {
		progress = &balanceActionProgress{
			mode:          mode,
			actualGroupID: actual.ID,
			actualLeader:  actual.LeaderStoreID,
			actualVoters:  slices.Clone(actual.VoterStoreIDs),
			targetLeader:  target.LeaderStoreID,
			targetVoters:  slices.Clone(target.VoterStoreIDs),
			storeIDs:      slices.Clone(storeIDs),
			regionScan:    newBalanceRegionScanCursor(ranges),
			actions:       affinity.BalanceActionSet{VoterReplacements: make(map[uint64]map[uint64][]uint64)},
		}
	}

	checks := 0
	waiting := c.waitingAffinityRegions()
	for {
		if ctx.Err() != nil {
			return blockedBalanceBuild(checks)
		}
		if !progress.hasCurrentRegion {
			if progress.regionScan.done() {
				return c.completeBalanceTargetBuild(actual, target, progress, checks)
			}
			if budget > 0 && checks >= budget {
				return pendingBalanceBuild(progress, checks)
			}
			scanResult := progress.regionScan.next(c.cluster)
			if scanResult.scanned {
				checks++
			}
			if scanResult.done {
				return c.completeBalanceTargetBuild(actual, target, progress, checks)
			}
			if !scanResult.covered {
				return affinity.BalanceActionBuildResult{Checks: checks}
			}
			if !scanResult.found {
				continue
			}
			region := scanResult.region
			if !c.balanceRegionReady(actual, region, waiting) || region.GetLeader() == nil {
				return affinity.BalanceActionBuildResult{Checks: checks}
			}
			progress.currentRegion = balanceRegionRef{
				id: region.GetID(), version: region.GetRegionEpoch().GetVersion(), confVer: region.GetRegionEpoch().GetConfVer(),
				leaderID: region.GetLeader().GetStoreId(),
			}
			progress.hasCurrentRegion = true
		}
		ref := progress.currentRegion
		region := c.cluster.GetRegion(ref.id)
		if !c.balanceRegionMatchesRef(region, ref) || !c.balanceRegionReady(actual, region, waiting) {
			return affinity.BalanceActionBuildResult{Checks: checks}
		}
		if mode == affinity.BalanceCheckConvergence {
			progress.hasCurrentRegion = false
			continue
		}
		if mode == affinity.BalanceValidateTarget && !progress.actualPlacementChecked {
			if budget > 0 && checks >= budget {
				return pendingBalanceBuild(progress, checks)
			}
			checks++
			c.affinityManager.ObserveBalanceRegion(region)
			if c.regionPlacementBestLocation(region, c.ruleManager.FitRegion(c.cluster, region)) != bestLocationSatisfied {
				return blockedBalanceBuild(checks)
			}
			progress.actualPlacementChecked = true
		}
		if progress.baselineFit == nil {
			if budget > 0 && checks >= budget {
				return pendingBalanceBuild(progress, checks)
			}
			baseline := region
			cachedFit := target.LeaderStoreID == actual.LeaderStoreID && slices.Equal(target.VoterStoreIDs, actual.VoterStoreIDs)
			if !cachedFit {
				baseline = cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
				if baseline == nil {
					if mode == affinity.BalanceValidateTarget {
						return affinity.BalanceActionBuildResult{Ready: true, Complete: true, InvalidTarget: true, Checks: checks}
					}
					return affinity.BalanceActionBuildResult{Checks: checks}
				}
			}
			checks++
			if cachedFit {
				progress.baselineFit = c.ruleManager.FitRegion(c.cluster, baseline)
			} else {
				progress.baselineFit = c.ruleManager.FitRegionWithoutCache(c.cluster, baseline)
			}
			if mode == affinity.BalanceValidateTarget {
				status := c.regionPlacementBestLocation(baseline, progress.baselineFit)
				if status == bestLocationRejected {
					return affinity.BalanceActionBuildResult{Ready: true, Complete: true, InvalidTarget: true, Checks: checks}
				}
				if status == bestLocationTemporarilyBlocked {
					return blockedBalanceBuild(checks)
				}
			} else if progress.baselineFit == nil || !progress.baselineFit.IsSatisfied() {
				return blockedBalanceBuild(checks)
			} else if c.regionPlacementBestLocation(baseline, progress.baselineFit) != bestLocationSatisfied {
				return blockedBalanceBuild(checks)
			}
			progress.baselineRegion = baseline
		}
		if mode == affinity.BalanceValidateTarget {
			progress.hasCurrentRegion = false
			progress.baselineFit = nil
			progress.baselineRegion = nil
			progress.actualPlacementChecked = false
			continue
		}

		if mode == affinity.BalanceBuildLeaders {
			if progress.candidateIntersectionReady && progress.candidateIndex >= len(progress.leaderTargets) {
				finishBalanceBuildRegion(progress, region)
				continue
			}
			if !progress.pendingCandidate {
				if !progress.candidateIntersectionReady {
					if budget > 0 && checks >= budget {
						return pendingBalanceBuild(progress, checks)
					}
					leaderID, hasCandidate := nextBalanceLeaderTarget(progress, target)
					if !hasCandidate {
						progress.candidateIntersectionReady = true
						finishBalanceBuildRegion(progress, region)
						continue
					}
					checks++
					if leaderID != progress.baselineRegion.GetLeader().GetStoreId() {
						status := c.leaderTargetStatus(leaderID)
						if status == balanceStoreTemporarilyBlocked {
							progress.temporarilyBlocked = true
							progress.candidateIndex++
							continue
						}
						if status == balanceStoreRejected {
							progress.candidateIndex++
							continue
						}
					}
					progress.pendingLeader = leaderID
					progress.pendingCandidate = true
				}
				if progress.candidateIntersectionReady {
					progress.pendingLeader = progress.leaderTargets[progress.candidateIndex]
					progress.pendingCandidate = true
				}
			}
			leaderID := progress.pendingLeader
			if !progress.pendingCandidateValidated {
				if budget > 0 && checks >= budget {
					return pendingBalanceBuild(progress, checks)
				}
				checks++
				if leaderID != progress.baselineRegion.GetLeader().GetStoreId() {
					if status := c.leaderTargetStatus(leaderID); status == balanceStoreTemporarilyBlocked {
						progress.temporarilyBlocked = true
						discardBalanceCandidate(progress)
						continue
					} else if status == balanceStoreRejected {
						progress.pendingCandidate = false
						progress.candidateIndex++
						continue
					}
					if status := c.leaderSourceStatus(progress.baselineRegion.GetLeader().GetStoreId()); status == balanceStoreTemporarilyBlocked {
						progress.temporarilyBlocked = true
						discardBalanceCandidate(progress)
						continue
					} else if status == balanceStoreRejected {
						progress.pendingCandidate = false
						progress.candidateIndex++
						continue
					}
				}
				progress.pendingCandidateValidated = true
			}
			if budget > 0 && checks >= budget {
				return pendingBalanceBuild(progress, checks)
			}
			virtual := cloneRegionWithReplacePeerStores(progress.baselineRegion, leaderID, target.VoterStoreIDs...)
			if virtual == nil {
				progress.pendingCandidate = false
				progress.pendingCandidateValidated = false
				progress.candidateIndex++
				continue
			}
			checks++
			progress.pendingCandidate = false
			progress.pendingCandidateValidated = false
			progress.candidateIndex++
			fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
			if fit != nil && fit.IsSatisfied() && placementNotWorse(progress.baselineFit, fit) {
				status := c.regionPlacementBestLocation(virtual, fit)
				if status == bestLocationTemporarilyBlocked {
					progress.temporarilyBlocked = true
					continue
				}
				if status == bestLocationSatisfied {
					progress.nextLeaderTarget = append(progress.nextLeaderTarget, leaderID)
					if progress.regionScan.done() {
						progress.actions.LeaderTargets = append(progress.actions.LeaderTargets, leaderID)
					}
				}
			}
		} else {
			if !progress.candidateIntersectionReady && !progress.pendingCandidate {
				if budget > 0 && checks >= budget {
					return pendingBalanceBuild(progress, checks)
				}
				candidate, candidateState := c.nextBalanceVoterCandidate(progress, target)
				checks++
				if candidateState == balanceCandidateBlocked {
					progress.temporarilyBlocked = true
					continue
				}
				if candidateState == balanceCandidateDone {
					progress.candidateIntersectionReady = true
					finishBalanceBuildRegion(progress, region)
					continue
				}
				if candidateState != balanceCandidateReady {
					continue
				}
				progress.pendingVoter = candidate
				progress.pendingCandidate = true
			}
			if progress.candidateIntersectionReady && !progress.pendingCandidate {
				if progress.candidateIndex >= len(progress.voters) {
					finishBalanceBuildRegion(progress, region)
					continue
				}
				progress.pendingVoter = progress.voters[progress.candidateIndex]
				progress.pendingCandidate = true
			}
			candidate := progress.pendingVoter
			if !progress.pendingCandidateValidated {
				if budget > 0 && checks >= budget {
					return pendingBalanceBuild(progress, checks)
				}
				checks++
				blocked, allowed := c.balanceVoterCandidateAllowed(progress.baselineRegion, candidate)
				if blocked {
					progress.temporarilyBlocked = true
					discardBalanceCandidate(progress)
					continue
				}
				if !allowed {
					progress.pendingCandidate = false
					if progress.candidateIntersectionReady {
						progress.candidateIndex++
					}
					continue
				}
				progress.pendingCandidateValidated = true
			}
			if budget > 0 && checks >= budget {
				return pendingBalanceBuild(progress, checks)
			}
			fromIndex := slices.Index(target.VoterStoreIDs, candidate.from)
			if fromIndex < 0 {
				return blockedBalanceBuild(checks)
			}
			voters := slices.Clone(target.VoterStoreIDs)
			voters[fromIndex] = candidate.to
			virtual := cloneRegionWithReplacePeerStores(progress.baselineRegion, candidate.leader, voters...)
			progress.pendingCandidate = false
			progress.pendingCandidateValidated = false
			if progress.candidateIntersectionReady {
				progress.candidateIndex++
			}
			if virtual == nil {
				continue
			}
			checks++
			fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
			if fit != nil && fit.IsSatisfied() && placementNotWorse(progress.baselineFit, fit) {
				status := c.regionPlacementBestLocation(virtual, fit)
				if status == bestLocationTemporarilyBlocked {
					progress.temporarilyBlocked = true
					continue
				}
				if status == bestLocationSatisfied {
					progress.nextVoters = append(progress.nextVoters, candidate)
					if progress.regionScan.done() {
						addBalanceVoterAction(&progress.actions, candidate)
					}
				}
			}
		}
	}
}

func (c *AffinityChecker) balanceValidationExecutionResult(actual *affinity.GroupState, target *affinity.Group, checks int) affinity.BalanceActionBuildResult {
	if !c.balanceSchedulingAllowed() {
		return blockedBalanceBuild(checks)
	}
	status := c.balanceTargetExecutionStatus(&actual.Group, affinity.BalancePlan{LeaderStoreID: target.LeaderStoreID, VoterStoreIDs: target.VoterStoreIDs})
	return affinity.BalanceActionBuildResult{
		Ready: status != balanceStoreTemporarilyBlocked, Complete: true, InvalidTarget: status == balanceStoreRejected, Checks: checks,
	}
}

func (c *AffinityChecker) completeBalanceTargetBuild(actual *affinity.GroupState, target *affinity.Group, progress *balanceActionProgress, checks int) affinity.BalanceActionBuildResult {
	if progress.mode == affinity.BalanceValidateTarget {
		return c.balanceValidationExecutionResult(actual, target, checks)
	}
	return completeBalanceBuild(progress, checks)
}

func (p *balanceActionProgress) matches(actual *affinity.GroupState, target *affinity.Group, ranges affinity.GroupKeyRanges, storeIDs []uint64, mode affinity.BalanceActionMode) bool {
	return p.mode == mode && p.actualGroupID == actual.ID && p.actualLeader == actual.LeaderStoreID &&
		slices.Equal(p.actualVoters, actual.VoterStoreIDs) && p.targetLeader == target.LeaderStoreID &&
		slices.Equal(p.targetVoters, target.VoterStoreIDs) && slices.Equal(p.storeIDs, storeIDs) && balanceRangesEqual(p.regionScan.ranges, ranges)
}

func balanceRangesEqual(left []keyutil.KeyRange, right affinity.GroupKeyRanges) bool {
	ranges := newBalanceRegionScanCursor(right).ranges
	if len(left) != len(ranges) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i].StartKey, ranges[i].StartKey) || !bytes.Equal(left[i].EndKey, ranges[i].EndKey) {
			return false
		}
	}
	return true
}

func pendingBalanceBuild(progress *balanceActionProgress, checks int) affinity.BalanceActionBuildResult {
	return affinity.BalanceActionBuildResult{Actions: affinity.BalanceActionSet{VoterReplacements: make(map[uint64]map[uint64][]uint64)},
		Cursor: progress, Ready: true, Checks: checks, RegionBytes: takeBalanceRegionBytes(progress), TemporaryBlocked: progress.temporarilyBlocked}
}

func takeBalanceRegionBytes(progress *balanceActionProgress) map[uint64]uint64 {
	page := progress.pageRegionBytes
	progress.pageRegionBytes = nil
	return page
}

func discardBalanceCandidate(progress *balanceActionProgress) {
	progress.pendingCandidate = false
	progress.pendingCandidateValidated = false
	if progress.mode == affinity.BalanceBuildLeaders || progress.candidateIntersectionReady {
		progress.candidateIndex++
	}
}

func blockedBalanceBuild(checks int) affinity.BalanceActionBuildResult {
	return affinity.BalanceActionBuildResult{BlockTable: true, Checks: checks}
}

func finishBalanceBuildRegion(progress *balanceActionProgress, region *core.RegionInfo) {
	if progress.pageRegionBytes == nil {
		progress.pageRegionBytes = make(map[uint64]uint64)
	}
	size := approximateBalanceRegionBytes(region)
	progress.pageRegionBytes[region.GetID()] = size
	progress.moveBytes += size
	if progress.mode == affinity.BalanceBuildLeaders {
		progress.leaderTargets = progress.nextLeaderTarget
		progress.nextLeaderTarget = nil
	} else {
		progress.voters = progress.nextVoters
		progress.nextVoters = nil
	}
	progress.hasCurrentRegion = false
	progress.currentRegion = balanceRegionRef{}
	progress.candidateIndex = 0
	progress.pendingCandidate = false
	progress.baselineFit = nil
	progress.baselineRegion = nil
}

func nextBalanceLeaderTarget(progress *balanceActionProgress, target *affinity.Group) (uint64, bool) {
	if !progress.candidateIntersectionReady {
		if progress.candidateIndex >= len(target.VoterStoreIDs) {
			return 0, false
		}
		return target.VoterStoreIDs[progress.candidateIndex], true
	}
	if progress.candidateIndex >= len(progress.leaderTargets) {
		return 0, false
	}
	return progress.leaderTargets[progress.candidateIndex], true
}

func completeBalanceBuild(progress *balanceActionProgress, checks int) affinity.BalanceActionBuildResult {
	if progress.actions.VoterReplacements == nil {
		progress.actions.VoterReplacements = make(map[uint64]map[uint64][]uint64)
	}
	progress.actions.MoveBytes = progress.moveBytes
	return affinity.BalanceActionBuildResult{
		Actions: progress.actions, Complete: true, Ready: true, Checks: checks, RegionBytes: takeBalanceRegionBytes(progress), TemporaryBlocked: progress.temporarilyBlocked,
	}
}

func addBalanceVoterAction(actions *affinity.BalanceActionSet, candidate balanceVoterCandidate) {
	targets := actions.VoterReplacements[candidate.from]
	if targets == nil {
		targets = make(map[uint64][]uint64)
		actions.VoterReplacements[candidate.from] = targets
	}
	targets[candidate.to] = append(targets[candidate.to], candidate.leader)
}

type balanceCandidateState uint8

const (
	balanceCandidatePending balanceCandidateState = iota
	balanceCandidateReady
	balanceCandidateBlocked
	balanceCandidateDone
)

func (c *AffinityChecker) nextBalanceVoterCandidate(progress *balanceActionProgress, target *affinity.Group) (balanceVoterCandidate, balanceCandidateState) {
	if progress.voterFromIndex >= len(target.VoterStoreIDs) {
		return balanceVoterCandidate{}, balanceCandidateDone
	}
	from := target.VoterStoreIDs[progress.voterFromIndex]
	if progress.voterToIndex >= len(progress.storeIDs) {
		progress.voterFromIndex++
		progress.voterToIndex = 0
		progress.voterLeaderIndex = 0
		progress.voterToChecked = false
		return balanceVoterCandidate{}, balanceCandidatePending
	}
	to := progress.storeIDs[progress.voterToIndex]
	if !progress.voterToChecked {
		progress.voterToChecked = true
		store := c.cluster.GetStore(to)
		status := balanceStoreRejected
		if store != nil && store.IsUp() && store.IsTiKV() && !slices.Contains(target.VoterStoreIDs, to) {
			status = c.voterTargetAllowed(store)
		}
		if c.cluster.GetStore(from) == nil || status == balanceStoreRejected {
			progress.voterToIndex++
			progress.voterLeaderIndex = 0
			progress.voterToChecked = false
		} else if status == balanceStoreTemporarilyBlocked {
			progress.voterToIndex++
			progress.voterLeaderIndex = 0
			progress.voterToChecked = false
			return balanceVoterCandidate{}, balanceCandidateBlocked
		}
		return balanceVoterCandidate{}, balanceCandidatePending
	}
	if progress.voterLeaderIndex >= len(target.VoterStoreIDs) {
		progress.voterToIndex++
		progress.voterLeaderIndex = 0
		progress.voterToChecked = false
		return balanceVoterCandidate{}, balanceCandidatePending
	}
	leader := target.VoterStoreIDs[progress.voterLeaderIndex]
	if progress.voterLeaderIndex == progress.voterFromIndex {
		leader = to
	}
	progress.voterLeaderIndex++
	if progress.voterLeaderIndex >= len(target.VoterStoreIDs) {
		progress.voterToIndex++
		progress.voterLeaderIndex = 0
		progress.voterToChecked = false
	}
	leaderStore := c.cluster.GetStore(leader)
	if leaderStore == nil || !leaderStore.IsUp() || !leaderStore.IsTiKV() {
		return balanceVoterCandidate{}, balanceCandidatePending
	}
	status := balanceStoreAllowed
	if leader != target.LeaderStoreID {
		status = c.leaderTargetStatus(leader)
	}
	if status == balanceStoreTemporarilyBlocked {
		return balanceVoterCandidate{}, balanceCandidateBlocked
	}
	if status == balanceStoreRejected {
		return balanceVoterCandidate{}, balanceCandidatePending
	}
	return balanceVoterCandidate{from: from, to: to, leader: leader}, balanceCandidateReady
}

func (c *AffinityChecker) balanceVoterCandidateAllowed(region *core.RegionInfo, candidate balanceVoterCandidate) (blocked, allowed bool) {
	leaderStore := c.cluster.GetStore(candidate.leader)
	targetStore := c.cluster.GetStore(candidate.to)
	source := c.cluster.GetStore(candidate.from)
	if leaderStore == nil || !leaderStore.IsUp() || !leaderStore.IsTiKV() {
		return false, false
	}
	if candidate.leader != region.GetLeader().GetStoreId() {
		if status := c.leaderTargetStatus(candidate.leader); status == balanceStoreTemporarilyBlocked {
			return true, false
		} else if status == balanceStoreRejected {
			return false, false
		}
	}
	if targetStore == nil || !targetStore.IsUp() || !targetStore.IsTiKV() {
		return false, false
	}
	if status := c.voterTargetAllowed(targetStore); status == balanceStoreTemporarilyBlocked {
		return true, false
	} else if status == balanceStoreRejected {
		return false, false
	}
	if region.GetStorePeer(candidate.to) != nil || source == nil {
		return false, false
	}
	if status := c.voterSourceStatus(source); status == balanceStoreTemporarilyBlocked {
		return true, false
	} else if status == balanceStoreRejected {
		return false, false
	}
	if candidate.leader != region.GetLeader().GetStoreId() {
		if status := c.leaderSourceStatus(region.GetLeader().GetStoreId()); status == balanceStoreTemporarilyBlocked {
			return true, false
		} else if status == balanceStoreRejected {
			return false, false
		}
	}
	return false, true
}

type balanceStoreStatus uint8

const (
	balanceStoreAllowed balanceStoreStatus = iota
	balanceStoreRejected
	balanceStoreTemporarilyBlocked
)

func (c *AffinityChecker) voterTargetAllowed(store *core.StoreInfo) balanceStoreStatus {
	conf := c.cluster.GetSharedConfig()
	strict := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true}
	temporary := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	if !filter.NewSpecialUseFilter(c.Name()).Target(conf, store).IsOK() || !filter.NewStorageThresholdFilter(c.Name()).Target(conf, store).IsOK() {
		return balanceStoreRejected
	}
	if strict.Target(conf, store).IsOK() {
		return balanceStoreAllowed
	}
	if temporary.Target(conf, store).IsOK() {
		return balanceStoreTemporarilyBlocked
	}
	return balanceStoreRejected
}

func (c *AffinityChecker) leaderTargetStatus(storeID uint64) balanceStoreStatus {
	store := c.cluster.GetStore(storeID)
	if store == nil || !store.IsUp() || !store.IsTiKV() {
		return balanceStoreRejected
	}
	strict := &filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true}
	temporary := &filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true, AllowTemporaryStates: true}
	if strict.Target(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreAllowed
	}
	if temporary.Target(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreTemporarilyBlocked
	}
	return balanceStoreRejected
}

func (c *AffinityChecker) leaderSourceStatus(storeID uint64) balanceStoreStatus {
	store := c.cluster.GetStore(storeID)
	if store == nil {
		return balanceStoreRejected
	}
	strict := &filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true}
	temporary := &filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true, AllowTemporaryStates: true}
	if strict.Source(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreAllowed
	}
	if temporary.Source(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreTemporarilyBlocked
	}
	return balanceStoreRejected
}

func (c *AffinityChecker) voterSourceStatus(store *core.StoreInfo) balanceStoreStatus {
	strict := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true}
	temporary := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	if strict.Source(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreAllowed
	}
	if temporary.Source(c.cluster.GetSharedConfig(), store).IsOK() {
		return balanceStoreTemporarilyBlocked
	}
	return balanceStoreRejected
}

func (c *AffinityChecker) balanceStoreIDs() []uint64 {
	ids := make([]uint64, 0)
	for _, store := range c.cluster.GetStores() {
		if store != nil && store.IsUp() && store.IsTiKV() {
			ids = append(ids, store.GetID())
		}
	}
	slices.Sort(ids)
	return ids
}

func (c *AffinityChecker) balanceSchedulingAllowed() bool {
	if c.cluster == nil || c.conf == nil || c.affinityManager == nil || c.IsPaused() || c.cluster.IsSchedulingHalted() ||
		!c.cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		return false
	}
	limit := c.conf.GetAffinityScheduleLimit()
	if limit == 0 {
		return false
	}
	return c.opController == nil || c.opController.OperatorCount(operator.OpAffinity) < limit
}

func (*AffinityChecker) balanceRegionMatchesRef(region *core.RegionInfo, ref balanceRegionRef) bool {
	return region != nil && region.GetLeader() != nil && region.GetRegionEpoch().GetVersion() == ref.version &&
		region.GetRegionEpoch().GetConfVer() == ref.confVer && region.GetLeader().GetStoreId() == ref.leaderID
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
