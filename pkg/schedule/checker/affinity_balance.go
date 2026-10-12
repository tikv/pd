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
	"encoding/json"
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
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

type balanceActionProgress struct {
	mode             affinity.BalanceActionMode
	actualGroupID    string
	actualLeader     uint64
	actualVoters     []uint64
	targetLeader     uint64
	targetVoters     []uint64
	regions          []balanceRegionRef
	regionIndex      int
	candidateIndex   int
	leaderTargets    []uint64
	nextLeaderTarget []uint64
	voters           []balanceVoterCandidate
	nextVoters       []balanceVoterCandidate
	baselineRegion   *core.RegionInfo
	baselineFit      *placement.RegionFit
	regionBytes      map[uint64]uint64
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

func (c *AffinityChecker) balanceRegions(group *affinity.GroupState, ranges affinity.GroupKeyRanges) ([]*core.RegionInfo, bool) {
	if group == nil || len(ranges.KeyRanges) == 0 || !group.AffinitySchedulingAllowed {
		return nil, false
	}
	regions, err := c.cluster.BatchScanRegions(keyutil.NewKeyRanges(ranges.KeyRanges), core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) == 0 {
		return nil, false
	}
	waiting := c.waitingAffinityRegions()
	for _, region := range regions {
		if !c.balanceRegionReady(group, region, waiting) {
			return nil, false
		}
	}
	return regions, true
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
	if !c.balanceSchedulingAllowed() || ctx.Err() != nil {
		return false, false
	}
	regions, covered := c.balanceRegions(group, ranges)
	if !covered {
		return false, false
	}
	for _, region := range regions {
		// This also invalidates a cached plan when the checker has not yet seen
		// the newest heartbeat for this Region.
		c.affinityManager.ObserveBalanceRegion(region)
	}
	if !c.balanceSchedulingAllowed() || ctx.Err() != nil {
		return false, false
	}
	if len(plan.VoterStoreIDs) != len(group.VoterStoreIDs) || plan.LeaderStoreID == 0 || !slices.Contains(plan.VoterStoreIDs, plan.LeaderStoreID) {
		return true, false
	}
	seen := make(map[uint64]struct{}, len(plan.VoterStoreIDs))
	for _, storeID := range plan.VoterStoreIDs {
		if storeID == 0 {
			return true, false
		}
		if _, ok := seen[storeID]; ok {
			return true, false
		}
		seen[storeID] = struct{}{}
	}
	return true, true
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
	if mode != affinity.BalanceBuildLeaders && mode != affinity.BalanceBuildVoters {
		return affinity.BalanceActionBuildResult{BlockTable: true}
	}
	if !actual.AffinitySchedulingAllowed || actual.Phase != affinity.PhaseStable || len(target.VoterStoreIDs) != len(actual.VoterStoreIDs) ||
		target.LeaderStoreID == 0 || !slices.Contains(target.VoterStoreIDs, target.LeaderStoreID) {
		return affinity.BalanceActionBuildResult{}
	}

	progress, ok := cursor.(*balanceActionProgress)
	if !ok || !progress.matches(actual, target, mode) {
		regions, ready := c.balanceRegions(actual, ranges)
		if !ready {
			return affinity.BalanceActionBuildResult{}
		}
		progress = &balanceActionProgress{
			mode:          mode,
			actualGroupID: actual.ID,
			actualLeader:  actual.LeaderStoreID,
			actualVoters:  slices.Clone(actual.VoterStoreIDs),
			targetLeader:  target.LeaderStoreID,
			targetVoters:  slices.Clone(target.VoterStoreIDs),
			regions:       make([]balanceRegionRef, 0, len(regions)),
			regionBytes:   make(map[uint64]uint64, len(regions)),
		}
		for _, region := range regions {
			progress.regions = append(progress.regions, balanceRegionRef{
				id: region.GetID(), version: region.GetRegionEpoch().GetVersion(), confVer: region.GetRegionEpoch().GetConfVer(),
				leaderID: region.GetLeader().GetStoreId(),
			})
		}
		if mode == affinity.BalanceBuildLeaders {
			progress.leaderTargets = slices.Clone(target.VoterStoreIDs)
		} else {
			progress.voters = c.balanceVoterCandidates(target, storeIDs)
		}
	}

	checks := 0
	waiting := c.waitingAffinityRegions()
	for progress.regionIndex < len(progress.regions) {
		if ctx.Err() != nil {
			return affinity.BalanceActionBuildResult{BlockTable: true, Checks: checks}
		}
		ref := progress.regions[progress.regionIndex]
		region := c.cluster.GetRegion(ref.id)
		if !c.balanceRegionMatchesRef(region, ref) || !c.balanceRegionReady(actual, region, waiting) {
			return affinity.BalanceActionBuildResult{Checks: checks}
		}
		if progress.baselineFit == nil {
			if budget > 0 && checks >= budget {
				return affinity.BalanceActionBuildResult{Cursor: progress, Ready: true, Checks: checks}
			}
			baseline := region
			cachedFit := target.LeaderStoreID == actual.LeaderStoreID && slices.Equal(target.VoterStoreIDs, actual.VoterStoreIDs)
			if !cachedFit {
				baseline = cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
				if baseline == nil {
					return affinity.BalanceActionBuildResult{Checks: checks}
				}
			}
			checks++
			if cachedFit {
				progress.baselineFit = c.ruleManager.FitRegion(c.cluster, baseline)
			} else {
				progress.baselineFit = c.ruleManager.FitRegionWithoutCache(c.cluster, baseline)
			}
			if progress.baselineFit == nil || !progress.baselineFit.IsSatisfied() {
				return affinity.BalanceActionBuildResult{Checks: checks}
			}
			progress.baselineRegion = baseline
		}

		if mode == affinity.BalanceBuildLeaders {
			for progress.candidateIndex < len(progress.leaderTargets) {
				leaderID := progress.leaderTargets[progress.candidateIndex]
				store := c.cluster.GetStore(leaderID)
				if store == nil || !store.IsUp() || !store.IsTiKV() || !c.leaderTargetAllowed(store) {
					progress.candidateIndex++
					continue
				}
				if leaderID != progress.baselineRegion.GetLeader().GetStoreId() && !c.leaderSourceAllowed(progress.baselineRegion.GetLeader().GetStoreId()) {
					progress.candidateIndex++
					continue
				}
				if budget > 0 && checks >= budget {
					return affinity.BalanceActionBuildResult{Cursor: progress, Ready: true, Checks: checks}
				}
				virtual := cloneRegionWithReplacePeerStores(progress.baselineRegion, leaderID, target.VoterStoreIDs...)
				progress.candidateIndex++
				if virtual == nil {
					continue
				}
				checks++
				fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
				if fit != nil && fit.IsSatisfied() && placementNotWorse(progress.baselineFit, fit) {
					progress.nextLeaderTarget = append(progress.nextLeaderTarget, leaderID)
				}
			}
		} else {
			for progress.candidateIndex < len(progress.voters) {
				candidate := progress.voters[progress.candidateIndex]
				leaderStore := c.cluster.GetStore(candidate.leader)
				source := c.cluster.GetStore(candidate.from)
				if leaderStore == nil || !leaderStore.IsUp() || !leaderStore.IsTiKV() || !c.leaderTargetAllowed(leaderStore) ||
					progress.baselineRegion.GetStorePeer(candidate.to) != nil || source == nil || !c.voterSourceAllowed(source) ||
					(candidate.leader != progress.baselineRegion.GetLeader().GetStoreId() && !c.leaderSourceAllowed(progress.baselineRegion.GetLeader().GetStoreId())) {
					progress.candidateIndex++
					continue
				}
				if budget > 0 && checks >= budget {
					return affinity.BalanceActionBuildResult{Cursor: progress, Ready: true, Checks: checks}
				}
				voters := slices.Clone(target.VoterStoreIDs)
				voters[slices.Index(voters, candidate.from)] = candidate.to
				virtual := cloneRegionWithReplacePeerStores(progress.baselineRegion, candidate.leader, voters...)
				progress.candidateIndex++
				if virtual == nil {
					continue
				}
				checks++
				fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
				if fit != nil && fit.IsSatisfied() && placementNotWorse(progress.baselineFit, fit) {
					progress.nextVoters = append(progress.nextVoters, candidate)
				}
			}
		}

		progress.regionBytes[region.GetID()] = approximateBalanceRegionBytes(region)
		if mode == affinity.BalanceBuildLeaders {
			progress.leaderTargets = progress.nextLeaderTarget
			progress.nextLeaderTarget = nil
		} else {
			progress.voters = progress.nextVoters
			progress.nextVoters = nil
		}
		progress.regionIndex++
		progress.candidateIndex = 0
		progress.baselineFit = nil
		progress.baselineRegion = nil
	}
	return completeBalanceBuild(progress, checks)
}

func (p *balanceActionProgress) matches(actual *affinity.GroupState, target *affinity.Group, mode affinity.BalanceActionMode) bool {
	return p.mode == mode && p.actualGroupID == actual.ID && p.actualLeader == actual.LeaderStoreID &&
		slices.Equal(p.actualVoters, actual.VoterStoreIDs) && p.targetLeader == target.LeaderStoreID &&
		slices.Equal(p.targetVoters, target.VoterStoreIDs)
}

func completeBalanceBuild(progress *balanceActionProgress, checks int) affinity.BalanceActionBuildResult {
	actions := affinity.BalanceActionSet{VoterReplacements: make(map[uint64]map[uint64][]uint64)}
	if progress.mode == affinity.BalanceBuildLeaders {
		actions.LeaderTargets = slices.Clone(progress.leaderTargets)
	} else {
		for _, candidate := range progress.voters {
			targets := actions.VoterReplacements[candidate.from]
			if targets == nil {
				targets = make(map[uint64][]uint64)
				actions.VoterReplacements[candidate.from] = targets
			}
			targets[candidate.to] = append(targets[candidate.to], candidate.leader)
		}
	}
	for _, size := range progress.regionBytes {
		actions.MoveBytes += size
	}
	return affinity.BalanceActionBuildResult{
		Actions: actions, Complete: true, Ready: true, Checks: checks, RegionBytes: progress.regionBytes,
	}
}

func (c *AffinityChecker) balanceVoterCandidates(target *affinity.Group, storeIDs []uint64) []balanceVoterCandidate {
	conf := c.cluster.GetSharedConfig()
	moveState := &filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true, AllowTemporaryStates: true}
	specialUse := filter.NewSpecialUseFilter(c.Name())
	storage := filter.NewStorageThresholdFilter(c.Name())
	stores := make(map[uint64]*core.StoreInfo, len(storeIDs))
	for _, store := range c.cluster.GetStores() {
		stores[store.GetID()] = store
	}
	candidates := make([]balanceVoterCandidate, 0)
	for i, from := range target.VoterStoreIDs {
		if c.cluster.GetStore(from) == nil {
			continue
		}
		for _, to := range storeIDs {
			store := stores[to]
			if store == nil || slices.Contains(target.VoterStoreIDs, to) || !store.IsUp() || !store.IsTiKV() ||
				!moveState.Target(conf, store).IsOK() || !specialUse.Target(conf, store).IsOK() || !storage.Target(conf, store).IsOK() {
				continue
			}
			voters := slices.Clone(target.VoterStoreIDs)
			voters[i] = to
			for _, leader := range voters {
				leaderStore := stores[leader]
				if leaderStore == nil || !leaderStore.IsUp() || !leaderStore.IsTiKV() || !c.leaderTargetAllowed(leaderStore) {
					continue
				}
				candidates = append(candidates, balanceVoterCandidate{from: from, to: to, leader: leader})
			}
		}
	}
	return candidates
}

func (c *AffinityChecker) leaderTargetAllowed(store *core.StoreInfo) bool {
	return (&filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true, AllowTemporaryStates: true}).
		Target(c.cluster.GetSharedConfig(), store).IsOK()
}

func (c *AffinityChecker) leaderSourceAllowed(storeID uint64) bool {
	store := c.cluster.GetStore(storeID)
	return store != nil && (&filter.StoreStateFilter{ActionScope: c.Name(), TransferLeader: true, AllowTemporaryStates: true}).
		Source(c.cluster.GetSharedConfig(), store).IsOK()
}

func (c *AffinityChecker) voterSourceAllowed(store *core.StoreInfo) bool {
	return (&filter.StoreStateFilter{ActionScope: c.Name(), MoveRegion: true}).
		Source(c.cluster.GetSharedConfig(), store).IsOK()
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
