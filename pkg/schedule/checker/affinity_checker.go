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
	"math"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
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
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const recentMergeTTL = time.Minute

type affinityPlacementStatus uint8

const (
	affinityPlacementReady affinityPlacementStatus = iota
	affinityPlacementTemporary
	affinityPlacementRejected
)

// AffinityChecker groups regions with affinity labels together by affinity group.
// It ensures regions adhere to affinity group constraints by creating operators.
type AffinityChecker struct {
	PauseController
	cluster          sche.CheckerCluster
	ruleManager      *placement.RuleManager
	affinityManager  *affinity.Manager
	conf             config.CheckerConfigProvider
	recentMergeCache *cache.TTLUint64
	splitMu          syncutil.Mutex
	splitAttempts    map[uint64]affinitySplitAttempt
	maxSplitAttempts int
	startTime        time.Time
	balancer         *affinityBalancer
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *AffinityChecker {
	c := &AffinityChecker{
		cluster:          cluster,
		ruleManager:      cluster.GetRuleManager(),
		affinityManager:  cluster.GetAffinityManager(),
		conf:             conf,
		recentMergeCache: cache.NewIDTTL(ctx, gcInterval, recentMergeTTL),
		splitAttempts:    make(map[uint64]affinitySplitAttempt),
		maxSplitAttempts: affinitySplitMaxAttempts,
		startTime:        time.Now(),
	}
	if c.affinityManager != nil {
		c.balancer = newAffinityBalancer(c)
	}
	return c
}

// RunBalancer balances the targets of partition tables until ctx is done. The
// coordinator runs it with its other background jobs, so that stopping the
// coordinator waits for it.
func (c *AffinityChecker) RunBalancer(ctx context.Context) {
	if c.balancer != nil {
		c.balancer.run(ctx)
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

func (c *AffinityChecker) hasAffinityGroups() bool {
	return c.affinityManager != nil && c.affinityManager.GetAffinityGroupCount() > 0
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

	if !c.hasAffinityGroups() {
		return nil
	}

	// Check the affinity group before the heavier best-location gate. Most regions
	// are not affinity regions.
	group, isAffinity := c.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	if group == nil {
		// Region doesn't belong to any affinity group.
		return nil
	}

	fit := c.ruleManager.FitRegion(c.cluster, region)
	if c.checkRegionPlacement(region, fit, false).status != affinityPlacementReady {
		affinityCheckerAbnormalReplicaCounter.Inc()
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
			if targetRegion != nil {
				// Affinity moves preserve witness stores and cannot make a witness the leader.
				for _, peer := range region.GetVoters() {
					if peer.GetIsWitness() && (!slices.Contains(group.VoterStoreIDs, peer.GetStoreId()) || peer.GetStoreId() == group.LeaderStoreID) {
						if c.witnessTargetAllowed(region, group, fit, targetRegion.Clone()) {
							return nil
						}
						targetRegion = nil
						break
					}
				}
			}
			hasConflictingLearner := slices.ContainsFunc(region.GetLearners(), func(peer *metapb.Peer) bool {
				return slices.Contains(group.VoterStoreIDs, peer.GetStoreId())
			})
			// Validate conflicting learners together with their final destinations.
			if targetRegion == nil || (hasConflictingLearner && c.targetVotersRejected(targetRegion)) || (!hasConflictingLearner && !c.isRegionPlacementRuleSatisfiedWithBestLocation(targetRegion, false /* isExistingRegion */)) {
				c.affinityManager.ExpireAffinityGroup(group.ID)
				group = c.affinityManager.GetAffinityGroupState(group.ID)
				needRefetch = true
			}
		} else if group.HasUnpersistedTarget() {
			c.affinityManager.ObserveAvailableRegion(region, group)
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

	// Once a group is stable, register its table with the balancer, which plans
	// the targets of the whole table in the background.
	if !group.BalancePolicy.IsFixed() && group.Phase == affinity.PhaseStable {
		c.balancer.enqueue(group.ID)
	}

	// For a Region already in affinity, try to merge it with neighboring affinity Regions.
	if isAffinity {
		// The move is done, so a later move of the Region tries to split it again.
		c.splitMu.Lock()
		delete(c.splitAttempts, region.GetID())
		c.splitMu.Unlock()
		return c.mergeCheck(region, group)
	}

	// Create operator to adjust region according to affinity group
	op, relearn := c.createAffinityOperator(region, group, fit)
	if relearn {
		affinityCheckerLearnerPeerConflictCounter.Inc()
		// Local learner placement is a heuristic. Relearn from this healthy
		// Region now instead of letting the next Region restore the old target.
		c.affinityManager.ExpireAffinityGroup(group.ID)
		fresh := c.affinityManager.GetAffinityGroupState(group.ID)
		c.affinityManager.ObserveAvailableRegion(region, fresh)
		log.Debug("relearn affinity group after learner-voter conflict",
			zap.Uint64("region-id", region.GetID()), zap.String("group-id", group.ID))
		return nil
	}
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
// - Existing learner peers (e.g., TiFlash peers); moveConflictingLearners moves one off a target voter store
//
// Note: This function assumes that regions passed to it already comply with placement rules.
// The affinity checker focuses solely on adjusting voter peers according to affinity group configuration.
// It requests relearning only when local learner placement fails without a temporary blocker.
func (c *AffinityChecker) createAffinityOperator(region *core.RegionInfo, group *affinity.GroupState, fit *placement.RegionFit) (*operator.Operator, bool) {
	currentLeaderStoreID := region.GetLeader().GetStoreId()
	if currentLeaderStoreID == group.LeaderStoreID && len(region.GetVoters()) == len(group.VoterStoreIDs) &&
		!slices.ContainsFunc(region.GetVoters(), func(peer *metapb.Peer) bool { return !slices.Contains(group.VoterStoreIDs, peer.GetStoreId()) }) {
		return nil, false
	}
	roles := affinityRoles(region, group)
	movedLearners, status := c.moveConflictingLearners(region, group, fit, roles, nil)
	if status != affinityPlacementReady {
		return nil, status == affinityPlacementRejected
	}

	// Skip building if target leader store currently disallows leader in (e.g., evict-leader / reject-leader).
	if targetLeader := c.cluster.GetStore(group.LeaderStoreID); targetLeader != nil {
		if !targetLeader.AllowLeaderTransferIn() || c.conf.CheckLabelProperty(config.RejectLeader, targetLeader.GetLabels()) {
			affinityCheckerTargetStoreRejectedCounter.Inc()
			log.Debug("affinity target stores cannot take the region now",
				zap.Uint64("region-id", region.GetID()),
				zap.String("group-id", group.ID),
				zap.Bool("temporary", false))
			return nil, false
		}
	} else {
		return nil, false
	}
	if status := c.affinityTargetStoreStatus(region, group); status != balanceAllowed {
		if status == balanceRejected {
			affinityCheckerTargetStoreRejectedCounter.Inc()
		} else {
			affinityCheckerTargetStoreBlockedCounter.Inc()
		}
		log.Debug("affinity target stores cannot take the region now",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			zap.Bool("temporary", status == balanceTemporarilyBlocked))
		return nil, false
	}

	if op, ready := c.prepareAffinityPeerMove(region, group, roles); !ready {
		return op, false
	}

	// Determine operator kind based on whether leader needs to change
	kind := operator.OpAffinity
	if currentLeaderStoreID != group.LeaderStoreID {
		kind |= operator.OpLeader
	}

	op, err := operator.CreateMoveRegionOperator("affinity-move-region", c.cluster, region, kind, roles)
	if err != nil {
		log.Warn("create affinity move region operator failed",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			errs.ZapError(err))
		affinityCheckerCreateOpFailedCounter.Inc()
		return nil, false
	}
	if movedLearners {
		affinityCheckerLearnerMoveCounter.Inc()
	}

	return op, false
}

// affinityTargetStoreStatus checks the stores that moving region to the target
// of group adds: a new leader with the filters of a leader transfer, and each
// new voter with those of a new peer, as the balance schedulers and the rule
// checker do. A busy store or a store limit blocks the move only for now; the
// availability check later degrades or expires a group whose target has a
// removing, down or low-space store.
func (c *AffinityChecker) affinityTargetStoreStatus(region *core.RegionInfo, group *affinity.GroupState) balanceStatus {
	status := balanceAllowed
	check := func(id uint64, leader bool) {
		switch c.balanceStoreActionStatus(id, leader, false) {
		case balanceRejected:
			status = balanceRejected
		case balanceTemporarilyBlocked:
			if status == balanceAllowed {
				status = balanceTemporarilyBlocked
			}
		}
	}
	if region.GetLeader().GetStoreId() != group.LeaderStoreID {
		check(group.LeaderStoreID, true)
	}
	for _, id := range group.VoterStoreIDs {
		if region.GetStorePeer(id) == nil {
			check(id, false)
		}
	}
	return status
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

	if !c.isRegionPlacementRuleSatisfiedWithBestLocation(adjacent, true /* isExistingRegion */) {
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

// cloneRegionWithReplacePeerStores clones the Region and updates its voters and leader to the target stores.
// If the number of voters does not match or the leader is not among the target voters,
// it returns nil to indicate failure. Source and target placement must not contain duplicate store IDs respectively.
func cloneRegionWithReplacePeerStores(region *core.RegionInfo, leaderStoreID uint64, voterStoreIDs ...uint64) *core.RegionInfo {
	if region == nil || len(region.GetVoters()) != len(voterStoreIDs) || !slices.Contains(voterStoreIDs, leaderStoreID) {
		return nil
	}

	peers := make([]*metapb.Peer, 0, len(region.GetPeers()))
	var leader *metapb.Peer
	for _, storeID := range voterStoreIDs {
		peer := region.GetStoreVoter(storeID)
		if peer == nil {
			// Placement-rule fitting identifies the leader by peer ID.
			peer = &metapb.Peer{Id: math.MaxUint64 - storeID, StoreId: storeID, Role: metapb.PeerRole_Voter}
		}
		peers = append(peers, peer)
		if storeID == leaderStoreID {
			leader = peer
		}
	}
	peers = append(peers, region.GetLearners()...)

	return region.Clone(core.SetPeers(peers), core.WithLeader(leader))
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

// isRegionPlacementRuleSatisfiedWithBestLocation is an affinity-specific
// scheduling gate. Besides requiring the region to satisfy placement rules, it
// also requires that affinity scheduling cannot find a strictly better location
// under the current topology and that the matched rule's isolation level is
// satisfied. This is intentionally stricter than filter.IsRegionReplicated and
// should not be used as a general replicated-state check.
func (c *AffinityChecker) isRegionPlacementRuleSatisfiedWithBestLocation(region *core.RegionInfo, isExistingRegion bool) bool {
	// Get the RegionFit for the given Region. If the Region is not an existing Region but a virtual target state,
	// use FitRegionWithoutCache to bypass the cache.
	var fit *placement.RegionFit
	if isExistingRegion {
		fit = c.ruleManager.FitRegion(c.cluster, region)
	} else {
		fit = c.ruleManager.FitRegionWithoutCache(c.cluster, region)
	}
	return c.checkRegionPlacement(region, fit, false).status == affinityPlacementReady
}

// bestLocationFeedback keeps a replacement scoped to the checked Region target.
type bestLocationFeedback struct {
	status       affinityPlacementStatus
	replacements []bestLocationReplacement
	scans        int
}

type bestLocationReplacement struct {
	old        uint64
	candidates []uint64
}

func (c *AffinityChecker) regionPlacementFeedback(region *core.RegionInfo, fit *placement.RegionFit) bestLocationFeedback {
	return c.checkRegionPlacement(region, fit, true)
}

// checkRegionPlacement always merges all rule decisions. Detailed repair hints
// are only needed by the balance planner; boolean callers reuse the same status.
func (c *AffinityChecker) checkRegionPlacement(region *core.RegionInfo, fit *placement.RegionFit, withReplacements bool) bestLocationFeedback {
	// Check region is satisfied
	if fit == nil || !fit.IsSatisfied() {
		return bestLocationFeedback{status: affinityPlacementRejected}
	}

	feedback := bestLocationFeedback{status: affinityPlacementReady}
	var preferred []uint64
	if withReplacements {
		preferred = make([]uint64, len(fit.RuleFits))
	}
	// Check whether all peers covered by the rules are at the best isolation level.
	// This logic is based on `RuleChecker.fixBetterLocation`.
	for i, rf := range fit.RuleFits {
		if len(rf.Rule.LocationLabels) == 0 {
			continue
		}
		isWitness := rf.Rule.IsWitness && isWitnessEnabled(c.cluster)
		// If the peer to be moved is a witness, since no snapshot is needed, we also reuse the fast failover logic.
		strategy := c.strategy(region, rf.Rule, isWitness)
		oldStoreID, candidates, filterByTempState := strategy.getBetterLocationCandidates(c.cluster, region, fit, rf)
		feedback.scans++
		// A source chosen by RegionScore can miss a same-domain peer shared
		// with another rule. Check the remaining sources only in that case.
		if candidates == nil && !filterByTempState && hasCoLocationRules(fit, rf) {
			for _, store := range getRuleFitStores(c.cluster, rf) {
				if store.GetID() == oldStoreID {
					continue
				}
				old, next, temporary := strategy.getBetterLocationCandidatesFromStores(c.cluster, region, fit, rf, []*core.StoreInfo{store})
				feedback.scans++
				filterByTempState = filterByTempState || temporary
				if next != nil {
					oldStoreID, candidates, filterByTempState = old, next, false
					break
				}
			}
		}
		if withReplacements {
			preferred[i] = oldStoreID
		}
		// Hard isolation is a definite rejection even when an improvement is busy.
		if !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rf.Rule.LocationLabels, rf.Rule.IsolationLevel) {
			feedback.status = affinityPlacementRejected
		}
		// filterByTempState being true means a better placement exists but is temporarily unschedulable.
		// This is also considered not satisfied.
		if filterByTempState {
			if feedback.status == affinityPlacementReady {
				feedback.status = affinityPlacementTemporary
			}
			// This rule can reach better isolation after temporary recovery.
			// Continue with other rules so their definite rejection still wins.
			continue
		}
		if candidates != nil {
			feedback.status = affinityPlacementRejected
			if withReplacements {
				ids := make([]uint64, 0, len(candidates.Stores))
				for _, store := range candidates.Stores {
					ids = append(ids, store.GetID())
				}
				feedback.replacements = append(feedback.replacements, bestLocationReplacement{old: oldStoreID, candidates: ids})
			}
		}
	}

	// A rejected Region may be repaired through another fitted rule, or another
	// source of that rule. Keep the existing decision; only add directed hints.
	if withReplacements && feedback.status == affinityPlacementRejected {
		for i, rf := range fit.RuleFits {
			if len(rf.Rule.LocationLabels) == 0 {
				continue
			}
			strategy := c.strategy(region, rf.Rule, rf.Rule.IsWitness && isWitnessEnabled(c.cluster))
			for _, store := range getRuleFitStores(c.cluster, rf) {
				if store.GetID() == preferred[i] {
					continue
				}
				old, candidates, _ := strategy.getBetterLocationCandidatesFromStores(c.cluster, region, fit, rf, []*core.StoreInfo{store})
				feedback.scans++
				if candidates == nil {
					continue
				}
				replacement := bestLocationReplacement{old: old, candidates: make([]uint64, 0, len(candidates.Stores))}
				for _, candidate := range candidates.Stores {
					replacement.candidates = append(replacement.candidates, candidate.GetID())
				}
				feedback.replacements = append(feedback.replacements, replacement)
			}
		}
	}

	return feedback
}

// targetVotersRejected checks hard voter constraints without learner interference.
func (c *AffinityChecker) targetVotersRejected(target *core.RegionInfo) bool {
	voters := target.Clone(core.SetPeers(target.GetVoters()))
	for _, rf := range c.ruleManager.FitRegionWithoutCache(c.cluster, voters).RuleFits {
		if rf.Rule.Role != placement.Learner && (!rf.IsSatisfied() || !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rf.Rule.LocationLabels, rf.Rule.IsolationLevel)) {
			return true
		}
	}
	return false
}

// witnessTargetAllowed probes at most 16 witness layouts on a private target.
func (c *AffinityChecker) witnessTargetAllowed(region *core.RegionInfo, group *affinity.GroupState, fit *placement.RegionFit, target *core.RegionInfo) bool {
	witnesses := len(region.GetVoters()) - len(region.GetNonWitnessVoters())
	var candidates []*metapb.Peer
	for _, peer := range target.GetVoters() {
		if peer.GetStoreId() == group.LeaderStoreID {
			peer.IsWitness = false
		} else if peer.GetIsWitness() {
			witnesses--
		} else {
			candidates = append(candidates, peer)
		}
	}
	roles, remaining := affinityRoles(region, group), 16
	var probe func(int, int) bool
	probe = func(start, count int) bool {
		if count == 0 {
			remaining--
			if c.targetVotersRejected(target) {
				return false
			}
			if slices.ContainsFunc(region.GetLearners(), func(peer *metapb.Peer) bool { return roles[peer.GetStoreId()] != placement.Learner }) {
				_, status := c.moveConflictingLearners(region, group, fit, roles, target)
				return status != affinityPlacementRejected
			}
			return c.checkRegionPlacement(target, c.ruleManager.FitRegionWithoutCache(c.cluster, target), false).status != affinityPlacementRejected
		}
		for i := start; i <= len(candidates)-count && remaining > 0; i++ {
			candidates[i].IsWitness = true
			allowed := probe(i+1, count-1)
			candidates[i].IsWitness = false
			if allowed {
				return true
			}
		}
		return false
	}
	return probe(0, witnesses)
}

func affinityRoles(region *core.RegionInfo, group *affinity.GroupState) map[uint64]placement.PeerRoleType {
	roles := make(map[uint64]placement.PeerRoleType, len(region.GetPeers()))
	for _, learner := range region.GetLearners() {
		roles[learner.GetStoreId()] = placement.Learner
	}
	for _, storeID := range group.VoterStoreIDs {
		roles[storeID] = placement.Voter
	}
	roles[group.LeaderStoreID] = placement.Leader
	return roles
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

// hasCoLocationRules limits the extra source scan to competing location rules.
func hasCoLocationRules(fit *placement.RegionFit, rf *placement.RuleFit) bool {
	if len(fit.RuleFits) <= 1 {
		return false
	}
	for _, other := range fit.RuleFits {
		if other == rf {
			continue
		}
		if other.Rule.Role == rf.Rule.Role || other.Rule.Role != placement.Learner && rf.Rule.Role != placement.Learner {
			return true
		}
	}
	return false
}
