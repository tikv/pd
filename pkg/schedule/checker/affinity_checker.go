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

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	// nolint:unused
	affinityLabel = "affinity"
)

// AffinityChecker groups regions with affinity labels together by affinity group.
// It ensures regions adhere to affinity group constraints by creating operators.
type AffinityChecker struct {
	PauseController
	cluster         sche.CheckerCluster
	affinityManager *affinity.Manager
	conf            config.CheckerConfigProvider
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(cluster sche.CheckerCluster, affinityManager *affinity.Manager, conf config.CheckerConfigProvider) *AffinityChecker {
	return &AffinityChecker{
		cluster:         cluster,
		affinityManager: affinityManager,
		conf:            conf,
	}
}

// GetType return AffinityChecker's type.
// nolint:unused
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

	// Check if region has a leader
	if region.GetLeader() == nil {
		affinityCheckerRegionNoLeaderCounter.Inc()
		return nil
	}

	// Get the affinity group for this region
	group := c.affinityManager.GetRegionAffinityGroup(region.GetID())
	if group == nil {
		// Region doesn't belong to any affinity group
		return nil
	}

	// Check if the group is in effect
	if !group.Effect {
		affinityCheckerGroupNotInEffectCounter.Inc()
		return nil
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
		if _, exists := roles[storeID]; !exists {
			roles[storeID] = placement.Learner
		}
	}

	// Build target peers map with same roles
	// TODO: Consider preserving witness flags in the future if needed
	peers := make(map[uint64]*metapb.Peer)
	for storeID, role := range roles {
		peers[storeID] = &metapb.Peer{
			StoreId: storeID,
			Role:    role.MetaPeerRole(),
		}
	}

	// Use Builder to create the operator with preserved learners
	builder := operator.NewBuilder(
		"affinity-move-region",
		c.cluster,
		region,
	).SetPeers(peers).SetExpectedRoles(roles)

	// Determine operator kind based on whether leader needs to change
	kind := operator.OpAffinity | operator.OpRegion
	if currentLeaderStoreID != group.LeaderStoreID {
		kind |= operator.OpLeader
	}

	op, err := builder.Build(kind)
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

// MergeCheck verifies if a region can be merged with its adjacent regions within the same affinity group.
// It follows similar logic to merge_checker but with affinity-specific constraints:
// - Does NOT skip recently split or recently started regions (as requested)
// - Only merges regions within the same affinity group
func (c *AffinityChecker) MergeCheck(region *core.RegionInfo) []*operator.Operator {
	affinityMergeCheckerCounter.Inc()

	if c.IsPaused() {
		affinityMergeCheckerPausedCounter.Inc()
		return nil
	}

	// Check if region has a leader
	if region.GetLeader() == nil {
		affinityMergeCheckerNoLeaderCounter.Inc()
		return nil
	}

	// Check if region belongs to an affinity group and is an affinity region
	group := c.affinityManager.GetRegionAffinityGroup(region.GetID())
	if group == nil {
		// Region doesn't belong to any affinity group
		affinityMergeCheckerNoAffinityGroupCounter.Inc()
		return nil
	}

	if !c.affinityManager.IsRegionAffinity(region) {
		affinityMergeCheckerNotAffinityRegionCounter.Inc()
		return nil
	}

	// Region is not small enough
	maxSize := int64(c.conf.GetMaxAffinityMergeRegionSize())
	maxKeys := maxSize * config.RegionSizeToKeysRatio
	if !region.NeedMerge(maxSize, maxKeys) {
		affinityMergeCheckerNoNeedCounter.Inc()
		return nil
	}

	if !filter.IsRegionHealthy(region) {
		affinityMergeCheckerUnhealthyRegionCounter.Inc()
		return nil
	}

	if !filter.IsRegionReplicated(c.cluster, region) {
		affinityMergeCheckerAbnormalReplicaCounter.Inc()
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

	if region.GetApproximateSize()+target.GetApproximateSize() > maxSize ||
		region.GetApproximateKeys()+target.GetApproximateKeys() > maxKeys {
		affinityMergeCheckerTargetTooBigCounter.Inc()
		return nil
	}

	log.Debug("try to merge affinity region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())),
		zap.String("affinity-group", group.ID))

	ops, err := operator.CreateMergeRegionOperator("affinity-merge-region", c.cluster, region, target, operator.OpMerge)
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
	adjacentGroup := c.affinityManager.GetRegionAffinityGroup(adjacent.GetID())
	if adjacentGroup == nil || adjacentGroup.ID != group.ID {
		// Adjacent region is not in the same affinity group
		affinityMergeCheckerAdjDifferentGroupCounter.Inc()
		return false
	}

	if !c.affinityManager.IsRegionAffinity(adjacent) {
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

	if !filter.IsRegionReplicated(c.cluster, adjacent) {
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
