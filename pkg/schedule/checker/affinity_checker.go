// Copyright 2025 TiKV Project Authors.
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
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const recentMergeTTL = time.Minute

// AffinityChecker groups regions with affinity labels together by affinity group.
// It ensures regions adhere to affinity group constraints by creating operators.
type AffinityChecker struct {
	PauseController
	cluster          sche.CheckerCluster
	ruleManager      *placement.RuleManager
	affinityManager  *affinity.Manager
	conf             config.CheckerConfigProvider
	recentMergeCache *cache.TTLUint64
	startTime        time.Time
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *AffinityChecker {
	return &AffinityChecker{
		cluster:          cluster,
		ruleManager:      cluster.GetRuleManager(),
		affinityManager:  cluster.GetAffinityManager(),
		conf:             conf,
		recentMergeCache: cache.NewIDTTL(ctx, gcInterval, recentMergeTTL),
		startTime:        time.Now(),
	}
}

// GetType return AffinityChecker's type.
func (*AffinityChecker) GetType() types.CheckerSchedulerType {
	return types.AffinityChecker
}

// Name returns AffinityChecker's name.
func (*AffinityChecker) Name() string {
	return types.AffinityChecker.String()
}

// Check verifies a region's replicas according to affinity group constraints, creating an Operator if needed.
func (c *AffinityChecker) Check(region *core.RegionInfo) []*operator.Operator {
	affinityCheckerCounter.Inc()

	if c.IsPaused() {
		affinityCheckerPausedCounter.Inc()
		return nil
	}
	if !c.cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		affinityCheckerPlacementRulesDisabledCounter.Inc()
		return nil
	}

	// Check region state
	if region.GetLeader() == nil {
		affinityCheckerRegionNoLeaderCounter.Inc()
		return nil
	}
	if !filter.IsRegionHealthy(region) {
		affinityCheckerUnhealthyRegionCounter.Inc()
		return nil
	}
	if !c.isRegionPlacementRuleSatisfiedWithBestLocation(region, true /* isRealRegion */) {
		affinityCheckerAbnormalReplicaCounter.Inc()
		return nil
	}

	// Get the affinity group for this region
	group, isAffinity := c.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	if group == nil {
		// Region doesn't belong to any affinity group
		return nil
	}

	// In some cases, the Group must be updated via ObserveAvailableRegion and then fetched again.
	needRefetch := false
	if group.AffinitySchedulingAllowed {
		// If the Group is affinity scheduling allowed and the Region is not in the affinity state,
		// we expect to schedule the Region to match the Group’s peers.
		// Before doing so, check whether the Group’s peers are replicated.
		// If not, the Group information should be expired (e.g. due to Placement Rules changes),
		// so expire the group first, then provide the available Region information and fetch the Group state again.
		if !isAffinity {
			targetRegion := cloneRegionWithReplacePeerStores(region, group.LeaderStoreID, group.VoterStoreIDs...)
			if targetRegion == nil || !c.isRegionPlacementRuleSatisfiedWithBestLocation(targetRegion, false /* isRealRegion */) {
				c.affinityManager.ExpireAffinityGroup(group.ID)
				group = c.affinityManager.GetAffinityGroupState(group.ID)
				needRefetch = true
			}
		}
	} else {
		// If the Group is not affinity scheduling allowed, provide the available Region information and fetch group state again.
		needRefetch = true
	}

	// Recheck after refetching.
	if needRefetch {
		c.affinityManager.ObserveAvailableRegion(region, group)
		group, isAffinity = c.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	}

	// A Region may no longer exist in the RegionTree due to a merge.
	// In this case, clear the cache in affinity manager for that Region and skip processing it.
	if c.cluster.GetRegion(region.GetID()) == nil {
		c.affinityManager.InvalidCache(region.GetID())
		return nil
	}

	if group == nil || !group.AffinitySchedulingAllowed {
		affinityCheckerGroupSchedulingDisabledCounter.Inc()
		return nil
	}

	// For a Region already in affinity, try to merge it with neighboring affinity Regions.
	if isAffinity {
		return c.mergeCheck(region, group)
	}

	// Create operator to adjust region according to affinity group
	op := c.createAffinityOperator(region, group)
	if op != nil {
		affinityCheckerNewOpCounter.Inc()
		return []*operator.Operator{op}
	}

	return nil
}

// createAffinityOperator creates an operator to adjust region replicas according to affinity group constraints.
// It creates a "combo" operator that directly specifies the final leader and voter positions,
// moving the region to match the expected configuration in one operation.
//
// This function preserves:
// - Existing learner peers (e.g., TiFlash peers)
//
// Note: This function assumes that regions passed to it already comply with placement rules.
// The affinity checker focuses solely on adjusting voter peers according to affinity group configuration.
func (c *AffinityChecker) createAffinityOperator(region *core.RegionInfo, group *affinity.GroupState) *operator.Operator {
	// Build expected voter stores set
	expectedVoterStores := make(map[uint64]bool)
	for _, storeID := range group.VoterStoreIDs {
		expectedVoterStores[storeID] = true
	}

	// Build current voter stores set
	currentVoterStores := make(map[uint64]bool)
	for _, peer := range region.GetVoters() {
		currentVoterStores[peer.GetStoreId()] = true
	}

	// Check if region already matches the expected configuration
	currentLeaderStoreID := region.GetLeader().GetStoreId()
	if currentLeaderStoreID == group.LeaderStoreID && len(currentVoterStores) == len(expectedVoterStores) {
		allMatch := true
		for storeID := range expectedVoterStores {
			if !currentVoterStores[storeID] {
				allMatch = false
				break
			}
		}
		if allMatch {
			// No adjustment needed
			return nil
		}
	}

	// Build roles map for the target configuration
	// This includes voters (from affinity group) and existing learners
	roles := make(map[uint64]placement.PeerRoleType)

	// Add voters from affinity group
	for _, storeID := range group.VoterStoreIDs {
		if storeID == group.LeaderStoreID {
			roles[storeID] = placement.Leader
		} else {
			roles[storeID] = placement.Voter
		}
	}

	// Preserve all existing learner peers
	// Since regions that violate placement rules won't reach this function,
	// we can safely assume all existing learners should be preserved
	for _, learner := range region.GetLearners() {
		storeID := learner.GetStoreId()
		// Only add learner if it's not already in the voters list
		// (in case there's a conflict, voters take precedence)
		if existingRole, exists := roles[storeID]; !exists {
			roles[storeID] = placement.Learner
		} else if existingRole != placement.Learner {
			affinityCheckerLearnerPeerConflictCounter.Inc()
			log.Debug("learner peer conflicts with affinity voter, voter takes precedence",
				zap.Uint64("region-id", region.GetID()),
				zap.String("group-id", group.ID),
				zap.Uint64("store-id", storeID),
				zap.String("conflicting-role", string(existingRole)))
			return nil
		}
	}

	// Skip building if target leader store currently disallows leader in (e.g., evict-leader / reject-leader).
	if targetLeader := c.cluster.GetStore(group.LeaderStoreID); targetLeader != nil {
		if !targetLeader.AllowLeaderTransferIn() || c.conf.CheckLabelProperty(config.RejectLeader, targetLeader.GetLabels()) {
			return nil
		}
	} else {
		return nil
	}

	// Determine operator kind based on whether leader needs to change
	kind := operator.OpAffinity
	if currentLeaderStoreID != group.LeaderStoreID {
		kind |= operator.OpLeader
	}

	// Create operator with the target roles configuration
	op, err := operator.CreateMoveRegionOperator("affinity-move-region", c.cluster, region, kind, roles)
	if err != nil {
		log.Warn("create affinity move region operator failed",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			errs.ZapError(err))
		affinityCheckerCreateOpFailedCounter.Inc()
		return nil
	}

	return op
}

// mergeCheck verifies if a region can be merged with its adjacent regions within the same affinity group.
// It follows similar logic to merge_checker but with affinity-specific constraints:
// - Does NOT skip recently split regions
// - Does NOT skip hot spots regions
// - Only merges regions within the same affinity group
// - Skips regions that are recently merged
func (c *AffinityChecker) mergeCheck(region *core.RegionInfo, group *affinity.GroupState) []*operator.Operator {
	// Skip merge during startup TTL period
	if time.Since(c.startTime) < recentMergeTTL {
		affinityMergeCheckerSkipStartupCounter.Inc()
		return nil
	}

	// Check if region is in cache
	if c.recentMergeCache.Exists(region.GetID()) {
		affinityMergeCheckerSkipCachedCounter.Inc()
		return nil
	}

	maxAffinityMergeRegionSize := c.conf.GetMaxAffinityMergeRegionSize()

	if maxAffinityMergeRegionSize == 0 {
		affinityMergeCheckerDisabledCounter.Inc()
		return nil
	}
	if c.conf.GetMaxMergeRegionSize() == 0 || c.conf.GetMergeScheduleLimit() == 0 {
		affinityMergeCheckerGlobalDisabledCounter.Inc()
		return nil
	}
	affinityMergeCheckerCounter.Inc()

	// Region is not small enough
	maxSize := int64(maxAffinityMergeRegionSize)
	maxKeys := maxSize * config.RegionSizeToKeysRatio
	if !region.NeedMerge(maxSize, maxKeys) {
		affinityMergeCheckerNoNeedCounter.Inc()
		return nil
	}

	// Get adjacent regions
	prev, next := c.cluster.GetAdjacentRegions(region)
	var target *core.RegionInfo
	if c.checkAffinityMergeTarget(region, next, group) {
		target = next
	}

	// Check prev region (allow merging from both sides)
	if !c.conf.IsOneWayMergeEnabled() && c.checkAffinityMergeTarget(region, prev, group) { // allow a region can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		affinityMergeCheckerNoTargetCounter.Inc()
		return nil
	}

	// Check if target region is in cache
	if c.recentMergeCache.Exists(target.GetID()) {
		affinityMergeCheckerSkipCachedCounter.Inc()
		return nil
	}

	if region.GetApproximateSize()+target.GetApproximateSize() > maxSize ||
		region.GetApproximateKeys()+target.GetApproximateKeys() > maxKeys {
		affinityMergeCheckerTargetTooBigCounter.Inc()
		return nil
	}

	log.Debug("try to merge affinity region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())),
		zap.String("affinity-group", group.ID))

	ops, err := operator.CreateMergeRegionOperator("affinity-merge-region", c.cluster, region, target, operator.OpAffinity)
	if err != nil {
		log.Warn("create affinity merge region operator failed", errs.ZapError(err))
		return nil
	}

	affinityMergeCheckerNewOpCounter.Inc()
	return ops
}

// checkAffinityMergeTarget checks if an adjacent region is a valid merge target.
// It ensures both regions belong to the same affinity group and satisfy merge conditions.
func (c *AffinityChecker) checkAffinityMergeTarget(region, adjacent *core.RegionInfo, group *affinity.GroupState) bool {
	if adjacent == nil {
		affinityMergeCheckerAdjNotExistCounter.Inc()
		return false
	}

	// Check if adjacent region belongs to the same affinity group
	adjacentGroup, isAffinity := c.affinityManager.GetRegionAffinityGroupState(adjacent)
	if adjacentGroup == nil || adjacentGroup.ID != group.ID {
		// Adjacent region is not in the same affinity group
		affinityMergeCheckerAdjDifferentGroupCounter.Inc()
		return false
	}

	if !isAffinity {
		affinityMergeCheckerAdjNotAffinityCounter.Inc()
		return false
	}

	// Check if regions can be merged according to merge rules
	if !c.allowAffinityMerge(region, adjacent) {
		affinityMergeCheckerAdjDisallowMergeCounter.Inc()
		return false
	}

	if !checkPeerStore(c.cluster, region, adjacent) {
		affinityMergeCheckerAdjAbnormalPeerStoreCounter.Inc()
		return false
	}

	// Check if adjacent region is healthy
	if !filter.IsRegionHealthy(adjacent) {
		affinityMergeCheckerAdjUnhealthyCounter.Inc()
		return false
	}

	if !c.isRegionPlacementRuleSatisfiedWithBestLocation(adjacent, true /* isRealRegion */) {
		affinityMergeCheckerAdjAbnormalReplicaCounter.Inc()
		return false
	}

	return true
}

// allowAffinityMerge checks if two regions can be merged according to merge rules.
// This is based on checker.AllowMerge but adapted for affinity regions.
func (c *AffinityChecker) allowAffinityMerge(region, adjacent *core.RegionInfo) bool {
	var start, end []byte
	if bytes.Equal(region.GetEndKey(), adjacent.GetStartKey()) && len(region.GetEndKey()) != 0 {
		start, end = region.GetStartKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), region.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, end = adjacent.GetStartKey(), region.GetEndKey()
	} else {
		return false
	}

	// Check placement rules
	if c.cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		if len(c.cluster.GetRuleManager().GetSplitKeys(start, end)) > 0 {
			return false
		}
	}

	// Check region labeler
	l := c.cluster.GetRegionLabeler()
	if l != nil {
		if len(l.GetSplitKeys(start, end)) > 0 {
			return false
		}
		// Note: For affinity regions, we allow merge even if merge_option=deny is set
	}

	return true
}

// Presence flags for tracking store locations
const (
	inSource = 1 << 0 // 0b01: if only in source
	inTarget = 1 << 1 // 0b10: if only in target
	// inBoth = inSource | inTarget = 0b11: store exists in both
)

// cloneRegionWithReplacePeerStores clones the Region and updates its voters and leader to the target stores.
// If the number of voters does not match or the leader is not among the target voters,
// it returns nil to indicate failure. Source and target placement must not contain duplicate store IDs respectively.
func cloneRegionWithReplacePeerStores(region *core.RegionInfo, leaderStoreID uint64, voterStoreIDs ...uint64) *core.RegionInfo {
	if region == nil {
		return nil
	}

	sourceVoters := region.GetVoters()

	if len(sourceVoters) != len(voterStoreIDs) || !slices.Contains(voterStoreIDs, leaderStoreID) {
		return nil
	}

	// presences records whether a storeID exists in the source and/or target
	presences := make(map[uint64]int, len(sourceVoters)+len(voterStoreIDs))
	for _, voter := range sourceVoters {
		presences[voter.GetStoreId()] |= inSource
	}
	for _, voterStoreID := range voterStoreIDs {
		presences[voterStoreID] |= inTarget
	}

	storesToRemove := make([]uint64, 0, len(sourceVoters))
	storesToAdd := make([]uint64, 0, len(voterStoreIDs))
	for storeID, presence := range presences {
		switch presence {
		case inSource:
			storesToRemove = append(storesToRemove, storeID)
		case inTarget:
			storesToAdd = append(storesToAdd, storeID)
		}
	}

	options := make([]core.RegionCreateOption, 0, len(storesToRemove)+1)
	for i, removeStoreID := range storesToRemove {
		options = append(options, core.WithReplacePeerStore(removeStoreID, storesToAdd[i]))
	}
	options = append(options, core.WithReplaceLeaderStore(leaderStoreID))

	return region.Clone(options...)
}

// RecordOpSuccess is called when an operator completes successfully.
// It caches merged regions to prevent immediate re-merging.
//
// Merge completes quickly (e.g., 21ms) but TiKV needs time to update approximate_size via split-check.
// During this gap, PD may use stale size data to schedule more merges. This 1-minute cache prevents that.
func (c *AffinityChecker) RecordOpSuccess(op *operator.Operator) {
	// if schedule limit is 0, disable schedule, so we don't need to cache it.
	if c.conf.GetAffinityScheduleLimit() == 0 {
		return
	}
	// Process both merge and affinity merge operators
	if !op.HasRelatedMergeRegion() {
		return
	}
	relatedID := op.GetRelatedMergeRegion()
	if relatedID == 0 {
		return
	}
	c.recentMergeCache.PutWithTTL(op.RegionID(), nil, recentMergeTTL)
	c.recentMergeCache.PutWithTTL(relatedID, nil, recentMergeTTL)
}

func (c *AffinityChecker) isRegionPlacementRuleSatisfiedWithBestLocation(region *core.RegionInfo, isRealRegion bool) bool {
	// Get the RegionFit for the given Region. If the Region is not a real existing Region but a virtual target state,
	// use FitRegionWithoutCache to bypass the cache.
	var fit *placement.RegionFit
	if isRealRegion {
		fit = c.ruleManager.FitRegion(c.cluster, region)
	} else {
		fit = c.ruleManager.FitRegionWithoutCache(c.cluster, region)
	}

	// Check region is satisfied
	if fit == nil || !fit.IsSatisfied() {
		return false
	}

	// Check whether all peers covered by the rules are at the best isolation level.
	// This logic is based on `RuleChecker.fixBetterLocation`.
	for _, rf := range fit.RuleFits {
		if len(rf.Rule.LocationLabels) == 0 {
			continue
		}
		isWitness := rf.Rule.IsWitness && isWitnessEnabled(c.cluster)
		// If the peer to be moved is a witness, since no snapshot is needed, we also reuse the fast failover logic.
		strategy := c.strategy(region, rf.Rule, isWitness)
		_, newStoreID, filterByTempState := strategy.getBetterLocation(c.cluster, region, fit, rf)
		// filterByTempState being true means a better placement exists but is temporarily unschedulable.
		// This is also considered not satisfied.
		if newStoreID != 0 || filterByTempState {
			return false
		}
		// If the isolation level does not meet the requirement, it is also considered not to be at the best location.
		if !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rf.Rule.LocationLabels, rf.Rule.IsolationLevel) {
			return false
		}
	}

	return true
}

func (c *AffinityChecker) strategy(region *core.RegionInfo, rule *placement.Rule, fastFailover bool) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.Name(),
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		region:         region,
		extraFilters:   []filter.Filter{filter.NewLabelConstraintFilter(c.Name(), rule.LabelConstraints)},
		fastFailover:   fastFailover,
	}
}
