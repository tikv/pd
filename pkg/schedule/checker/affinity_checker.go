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
	"container/list"
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
	balancer         *affinityBalancer

	// splitMu protects split attempts, reservations, and the FIFO wait queue.
	splitMu syncutil.Mutex
	// Attempts and reservations share the same limit.
	splitAttempts    map[uint64]affinitySplitAttempt
	splitGranted     map[uint64]time.Time // Reservation deadlines by Region ID.
	splitWaitQueue   *list.List           // Region IDs in admission order.
	splitWaitIndex   map[uint64]*list.Element
	maxSplitAttempts int
	// maxSplitWaitQueue limits queued Regions independently of attempts and reservations.
	maxSplitWaitQueue int
	nextSplitPruneAt  time.Time
	startTime         time.Time
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *AffinityChecker {
	c := &AffinityChecker{
		cluster:           cluster,
		ruleManager:       cluster.GetRuleManager(),
		affinityManager:   cluster.GetAffinityManager(),
		conf:              conf,
		recentMergeCache:  cache.NewIDTTL(ctx, gcInterval, recentMergeTTL),
		splitAttempts:     make(map[uint64]affinitySplitAttempt),
		splitGranted:      make(map[uint64]time.Time),
		splitWaitQueue:    list.New(),
		splitWaitIndex:    make(map[uint64]*list.Element),
		maxSplitAttempts:  affinitySplitMaxAttempts,
		maxSplitWaitQueue: affinitySplitMaxAttempts,
		startTime:         time.Now(),
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
	// A merge can invalidate the cache before this snapshot is cached or refetched.
	regionID := region.GetID()
	defer func() {
		if c.cluster.GetRegion(regionID) == nil {
			c.affinityManager.InvalidCache(regionID)
		}
	}()
	if c.cluster.GetRegion(regionID) == nil {
		return nil
	}

	fit := c.ruleManager.FitRegion(c.cluster, region)
	if c.checkRegionPlacement(region, fit) != affinityPlacementReady {
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
			witnessDisplaced := false
			for _, peer := range region.GetVoters() {
				if peer.GetIsWitness() {
					if !slices.Contains(group.VoterStoreIDs, peer.GetStoreId()) || peer.GetStoreId() == group.LeaderStoreID {
						witnessDisplaced = true
						break
					}
				}
			}
			hasConflictingLearner := slices.ContainsFunc(region.GetLearners(), func(peer *metapb.Peer) bool {
				return slices.Contains(group.VoterStoreIDs, peer.GetStoreId())
			})
			// Validate conflicting learners together with their final destinations.
			rejected := targetRegion == nil || witnessDisplaced
			if !rejected {
				if hasConflictingLearner {
					rejected = c.targetVotersRejected(targetRegion, fit)
				} else {
					targetFit := c.ruleManager.FitRegionWithoutCache(c.cluster, targetRegion)
					switch c.checkRegionPlacement(targetRegion, targetFit) {
					case affinityPlacementTemporary:
						return nil
					case affinityPlacementRejected:
						rejected = true
					}
				}
			}
			if rejected {
				if c.cluster.GetRegion(regionID) == nil {
					return nil
				}
				expired, ok := c.affinityManager.ExpireAffinityGroupIf(group)
				if !ok {
					return nil
				}
				group = expired
				needRefetch = true
			}
		}
	} else {
		// If the Group is not affinity scheduling allowed, provide the available Region information and fetch group state again.
		needRefetch = true
	}

	// Recheck after refetching.
	if needRefetch {
		if !c.canLearnAffinityTarget(region) {
			return nil
		}
		if c.cluster.GetRegion(regionID) == nil {
			return nil
		}
		c.affinityManager.ObserveAvailableRegion(region, group)
		group, isAffinity = c.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	}

	current := c.cluster.GetRegion(regionID)
	if current == nil {
		return nil
	}

	if group == nil || !group.AffinitySchedulingAllowed {
		affinityCheckerGroupSchedulingDisabledCounter.Inc()
		return nil
	}

	if needRefetch && !isAffinity {
		// Validate a concurrently updated target on the next check before scheduling.
		return nil
	}

	// Once a group is stable, register its table with the balancer, which plans
	// the targets of the whole table in the background.
	if !group.BalancePolicy.IsFixed() && group.Phase == affinity.PhaseStable {
		c.balancer.enqueue(group.ID)
	}

	// For a Region already in affinity, try to merge it with neighboring affinity Regions.
	if isAffinity {
		c.clearCompletedSplitAttempt(region, current)
		return c.mergeCheck(region, group)
	}

	// Create operator to adjust region according to affinity group
	op, relearn := c.createAffinityOperator(region, group, fit)
	if relearn != affinityKeepTarget {
		if c.cluster.GetRegion(regionID) == nil {
			return nil
		}
		if fresh, ok := c.affinityManager.ExpireAffinityGroupIf(group); ok {
			if relearn == affinityRelearnLearnerConflict {
				affinityCheckerLearnerPeerConflictCounter.Inc()
			}
			if c.canLearnAffinityTarget(region) && c.cluster.GetRegion(regionID) != nil {
				c.affinityManager.ObserveAvailableRegion(region, fresh)
			}
			log.Debug("expire unachievable affinity target",
				zap.Uint64("region-id", region.GetID()), zap.String("group-id", group.ID))
		}
		return nil
	}
	if op != nil {
		affinityCheckerNewOpCounter.Inc()
		return []*operator.Operator{op}
	}

	return nil
}

func (c *AffinityChecker) canLearnAffinityTarget(region *core.RegionInfo) bool {
	specialUse := filter.NewSpecialUseFilter(c.Name())
	for _, voter := range region.GetVoters() {
		store := c.cluster.GetStore(voter.GetStoreId())
		if store == nil || !specialUse.Target(c.conf, store).IsOK() {
			return false
		}
	}
	return true
}

type affinityRelearnReason uint8

const (
	affinityKeepTarget affinityRelearnReason = iota
	affinityRelearnLearnerConflict
	affinityRelearnSpecialUse
)

// createAffinityOperator creates an operator to adjust region replicas according to affinity group constraints.
// It creates a "combo" operator that directly specifies the final leader and voter positions,
// moving the region to match the expected configuration in one operation.
// Pre-move split protection applies to affinity-move-region and may return a
// standalone SCAN split operator first.
//
// This function preserves:
// - Existing learner peers (e.g., TiFlash peers); moveConflictingLearners moves one off a target voter store
//
// Note: This function assumes that regions passed to it already comply with placement rules.
// The affinity checker focuses solely on adjusting voter peers according to affinity group configuration.
// A permanently unachievable target can request relearning.
func (c *AffinityChecker) createAffinityOperator(region *core.RegionInfo, group *affinity.GroupState, fit *placement.RegionFit) (*operator.Operator, affinityRelearnReason) {
	currentLeaderStoreID := region.GetLeader().GetStoreId()
	if currentLeaderStoreID == group.LeaderStoreID && len(region.GetVoters()) == len(group.VoterStoreIDs) &&
		!slices.ContainsFunc(region.GetVoters(), func(peer *metapb.Peer) bool { return !slices.Contains(group.VoterStoreIDs, peer.GetStoreId()) }) {
		return nil, affinityKeepTarget
	}
	roles := affinityRoles(region, group)
	movedLearners, status := c.moveConflictingLearners(region, group, fit, roles)
	if status != affinityPlacementReady {
		if status == affinityPlacementRejected {
			return nil, affinityRelearnLearnerConflict
		}
		return nil, affinityKeepTarget
	}

	if status, specialUseOnly := c.affinityTargetStoreStatus(region, group); status != balanceAllowed {
		if status == balanceRejected {
			affinityCheckerTargetStoreRejectedCounter.Inc()
		} else {
			affinityCheckerTargetStoreBlockedCounter.Inc()
		}
		log.Debug("affinity target stores cannot take the region now",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			zap.Bool("temporary", status == balanceTemporarilyBlocked))
		if specialUseOnly && !group.BalancePolicy.IsFixed() {
			return nil, affinityRelearnSpecialUse
		}
		return nil, affinityKeepTarget
	}

	split, latest, ready := c.prepareAffinityPeerMove(region, group, roles)
	if !ready {
		return split, affinityKeepTarget
	}

	// Determine operator kind based on whether leader needs to change
	kind := operator.OpAffinity
	if latest.GetLeader().GetStoreId() != group.LeaderStoreID {
		kind |= operator.OpLeader
	}

	op, err := operator.CreateMoveRegionOperator("affinity-move-region", c.cluster, latest, kind, roles)
	if err != nil {
		log.Warn("create affinity move region operator failed",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID),
			errs.ZapError(err))
		affinityCheckerCreateOpFailedCounter.Inc()
		return nil, affinityKeepTarget
	}
	if movedLearners {
		affinityCheckerLearnerMoveCounter.Inc()
	}

	return op, affinityKeepTarget
}

// affinityTargetStoreStatus checks the stores that moving region to the target
// of group touches: both sides of a leader transfer and each new voter store.
// Peer rate limits are enforced at admission so rejected moves can be retried.
func (c *AffinityChecker) affinityTargetStoreStatus(region *core.RegionInfo, group *affinity.GroupState) (balanceStatus, bool) {
	status := balanceAllowed
	rejectedSpecialUse := false
	check := func(id uint64, leader, source bool) {
		actionStatus, specialUse := c.affinityStoreActionStatus(id, leader, source, false)
		status = max(status, actionStatus)
		rejectedSpecialUse = rejectedSpecialUse || specialUse
	}
	if source := region.GetLeader().GetStoreId(); source != group.LeaderStoreID {
		check(source, true, true)
		check(group.LeaderStoreID, true, false)
	} else {
		store := c.cluster.GetStore(source)
		if store == nil || !store.AllowLeaderTransferIn() || c.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels()) {
			return balanceRejected, false
		}
	}
	for _, id := range group.VoterStoreIDs {
		if region.GetStorePeer(id) == nil {
			check(id, false, false)
		}
	}
	if rejectedSpecialUse {
		return balanceRejected, status == balanceAllowed
	}
	return status, false
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

	// Check if the adjacent region has the same affinity target.
	adjacentGroup, isAffinity := c.affinityManager.GetRegionAffinityGroupState(adjacent)
	if adjacentGroup == nil || !adjacentGroup.MatchesTarget(group) {
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
	return c.checkRegionPlacement(region, fit) == affinityPlacementReady
}

// checkRegionPlacement checks the current layout using the same best-location
// search as RuleChecker. Hard isolation failures take precedence over temporary
// store blocks.
func (c *AffinityChecker) checkRegionPlacement(region *core.RegionInfo, fit *placement.RegionFit) affinityPlacementStatus {
	if fit == nil || !fit.IsSatisfied() {
		return affinityPlacementRejected
	}

	status := affinityPlacementReady
	for _, rf := range fit.RuleFits {
		rule := rf.Rule
		if !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rule.LocationLabels, rule.IsolationLevel) {
			return affinityPlacementRejected
		}
		if len(rule.LocationLabels) == 0 {
			continue
		}
		// Witnesses can use fast failover because they do not need snapshots.
		strategy := c.strategy(region, rule, rule.IsWitness && isWitnessEnabled(c.cluster))
		_, newStoreID, filterByTempState := strategy.getBetterLocation(c.cluster, region, fit, rf)
		if filterByTempState {
			status = affinityPlacementTemporary
		} else if newStoreID != 0 {
			return affinityPlacementRejected
		}
	}
	return status
}

// targetVotersRejected checks hard voter constraints without learner interference.
func (c *AffinityChecker) targetVotersRejected(target *core.RegionInfo, fit *placement.RegionFit) bool {
	voterRules := make([]*placement.Rule, 0, len(fit.GetRules()))
	for _, rule := range fit.GetRules() {
		if rule.Role != placement.Learner {
			voterRules = append(voterRules, rule)
		}
	}
	voterFit := c.ruleManager.FitRegionWithRules(c.cluster, target.Clone(core.SetPeers(target.GetVoters())), voterRules)
	if !voterFit.IsSatisfied() {
		return true
	}
	for _, rf := range voterFit.RuleFits {
		if !statistics.IsRegionLabelIsolationSatisfied(rf.Stores, rf.Rule.LocationLabels, rf.Rule.IsolationLevel) {
			return true
		}
	}
	return false
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
