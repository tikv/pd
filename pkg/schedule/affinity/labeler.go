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

package affinity

import (
	"bytes"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	// labelRuleIDPrefix is the prefix for affinity group label rules.
	labelRuleIDPrefix = "affinity_group/"
)

// GetLabelRuleID returns the label rule ID for an affinity group.
// This ensures consistent naming between label creation and deletion.
// Format: "affinity_group/{group_id}"
func GetLabelRuleID(groupID string) string {
	return labelRuleIDPrefix + groupID
}

// parseAffinityGroupIDFromLabelRule parses the affinity group ID from the label rule.
// It will return the group ID and a boolean indicating whether the label rule is an affinity label rule.
func parseAffinityGroupIDFromLabelRule(rule *labeler.LabelRule) (string, bool) {
	// Validate the ID matches the expected format "affinity_group/{group_id}".
	if rule == nil || !strings.HasPrefix(rule.ID, labelRuleIDPrefix) {
		return "", false
	}
	// Retrieve the group ID.
	groupID := strings.TrimPrefix(rule.ID, labelRuleIDPrefix)
	if groupID == "" {
		return "", false
	}
	// Double-check the group ID from the label rule.
	var groupIDFromLabel string
	for _, label := range rule.Labels {
		if label.Key == labelKey {
			groupIDFromLabel = label.Value
			break
		}
	}
	if groupID != groupIDFromLabel {
		return "", false
	}
	return groupID, true
}

// MakeLabelRule makes the label rule for the given GroupKeyRanges.
func MakeLabelRule(groupKeyRanges *GroupKeyRanges) *labeler.LabelRule {
	var labelData []any
	for _, kr := range groupKeyRanges.KeyRanges {
		labelData = append(labelData, map[string]any{
			"start_key": hex.EncodeToString(kr.StartKey),
			"end_key":   hex.EncodeToString(kr.EndKey),
		})
	}
	return &labeler.LabelRule{
		ID:       GetLabelRuleID(groupKeyRanges.GroupID),
		Labels:   []labeler.RegionLabel{{Key: labelKey, Value: groupKeyRanges.GroupID}},
		RuleType: labeler.KeyRange,
		Data:     labelData,
	}
}

// MakeLabelRuleFromRanges makes the label rule from GroupKeyRanges.
func MakeLabelRuleFromRanges(gkr GroupKeyRanges) *labeler.LabelRule {
	var labelData []any
	for _, kr := range gkr.KeyRanges {
		labelData = append(labelData, map[string]any{
			"start_key": hex.EncodeToString(kr.StartKey),
			"end_key":   hex.EncodeToString(kr.EndKey),
		})
	}
	return &labeler.LabelRule{
		ID:       GetLabelRuleID(gkr.GroupID),
		Labels:   []labeler.RegionLabel{{Key: labelKey, Value: gkr.GroupID}},
		RuleType: labeler.KeyRange,
		Data:     labelData,
	}
}

// CreateAffinityGroups adds multiple affinity groups to storage and creates corresponding label rules.
func (m *Manager) CreateAffinityGroups(changes []GroupKeyRanges) error {
	// Step 0: Validate all groups first (without lock)
	groups := make([]*Group, 0, len(changes))
	for _, change := range changes {
		group := &Group{
			ID:              change.GroupID,
			CreateTimestamp: uint64(time.Now().Unix()),
			LeaderStoreID:   0,
			VoterStoreIDs:   nil,
		}
		if err := m.AdjustGroup(group); err != nil {
			return err
		}
		groups = append(groups, group)
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	// Step 1: Check whether the Group exists.
	if err := m.noGroupsExist(groups); err != nil {
		return err
	}

	// Step 2: Convert and validate key ranges no overlaps
	if err := m.validateNoKeyRangeOverlap(changes); err != nil {
		return err
	}

	// Step 3: Create the change plan for the Label
	labelRules := make([]*labeler.LabelRule, len(changes))
	plan := m.regionLabeler.NewPlan()
	for i, change := range changes {
		if len(change.KeyRanges) > 0 {
			labelRule := MakeLabelRule(&change)
			if err := plan.SetLabelRule(labelRule); err != nil {
				log.Error("failed to create label rule",
					zap.String("failed-group-id", change.GroupID),
					zap.Int("total-groups", len(changes)),
					zap.Error(err))
				return err
			}
			labelRules[i] = labelRule
		}
	}

	// Step 4: Create the change plan for the Group
	saveOps := plan.CommitOps()
	for _, g := range groups {
		group := g
		saveOps = append(saveOps, func(txn kv.Txn) error {
			return m.storage.SaveAffinityGroup(txn, group.ID, group)
		})
	}

	// Step 5: Save the Group and Label information in storage.
	if err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, saveOps); err != nil {
		log.Error("failed to add affinity groups",
			zap.Int("total-groups", len(changes)),
			zap.Error(err))
		return err
	}

	// Step 6: Save the Group and Label information in memory.
	plan.Apply()

	for i, change := range changes {
		// Update key ranges cache for this group
		if len(change.KeyRanges) > 0 {
			m.keyRanges[change.GroupID] = GroupKeyRanges{
				KeyRanges: change.KeyRanges,
				GroupID:   change.GroupID,
			}
		} else {
			// No key ranges, remove from cache if exists
			delete(m.keyRanges, change.GroupID)
		}
		log.Info("affinity group added", zap.String("group", groups[i].String()))
	}

	m.createGroups(groups, labelRules)
	return nil
}

// DeleteAffinityGroups deletes multiple affinity groups in a single transaction.
// If force is false:
//   - Returns error if any group does not exist
//   - Returns error if any group has key ranges
//
// If force is true:
//   - Skips non-existent groups
//   - Deletes groups even if they have key ranges
func (m *Manager) DeleteAffinityGroups(groupIDs []string, force bool) error {
	if len(groupIDs) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("no group ids provided")
	}

	seen := make(map[string]struct{}, len(groupIDs))
	toDelete := make([]string, 0, len(groupIDs))

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	// Step 1: Check if group has key ranges when force is false
	for _, groupID := range groupIDs {
		if _, exists := seen[groupID]; exists {
			continue
		}
		seen[groupID] = struct{}{}
		// Check if group has key ranges when force is false
		if !force {
			if ranges, exists := m.keyRanges[groupID]; exists && len(ranges.KeyRanges) > 0 {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"affinity group " + groupID + " has key ranges, use force=true to delete")
			}
		}
		toDelete = append(toDelete, groupID)
	}

	// Step 2: Check if all Groups exist when force is false
	if !force {
		if err := m.allGroupsExist(toDelete); err != nil {
			return err
		}
	}

	// Step 3: Create the change plan for the Label
	plan := m.regionLabeler.NewPlan()
	for _, groupID := range toDelete {
		labelRuleID := GetLabelRuleID(groupID)
		if err := plan.DeleteLabelRule(labelRuleID); err != nil {
			log.Error("failed to delete label rule for affinity group",
				zap.String("group-id", groupID),
				zap.String("label-rule-id", labelRuleID),
				zap.Error(err))
			return err
		}
	}

	// Step 4: Create the change plan for the Group
	saveOps := plan.CommitOps()
	for _, id := range toDelete {
		groupID := id
		saveOps = append(saveOps, func(txn kv.Txn) error {
			return m.storage.DeleteAffinityGroup(txn, groupID)
		})
	}

	// Step 5: Save the Group and Label information in storage.
	if err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, saveOps); err != nil {
		log.Error("failed to delete affinity groups",
			zap.Int("total-groups", len(toDelete)),
			zap.Error(err))
		return err
	}

	// Step 6: Save the Group and Label information in memory.
	plan.Apply()

	for _, groupID := range toDelete {
		delete(m.keyRanges, groupID)
		log.Info("affinity group deleted", zap.String("group-id", groupID))
	}

	m.deleteGroups(toDelete)
	return nil
}

// UpdateAffinityGroupPeers updates the leader and voter stores of an affinity group and marks it available.
func (m *Manager) UpdateAffinityGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	// To make it easier to compare using slices.Equal.
	voterStoreIDs = slices.Clone(voterStoreIDs)
	slices.Sort(voterStoreIDs)

	return m.updateAffinityGroupPeersWithAffinityVer(groupID, 0, leaderStoreID, voterStoreIDs)
}

// updateAffinityGroupPeersWithAffinityVer updates the leader and voter stores of an affinity group and marks it available.
// If affinityVer is non-zero (0 indicates an admin operation and is enforced)
//   - Its equality will be checked.
//   - Group must not change voterStoreIDs while it is not in the expired state.
func (m *Manager) updateAffinityGroupPeersWithAffinityVer(groupID string, affinityVer uint64, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	// Step 0: Validate the correctness of leaderStoreID and voterStoreIDs.
	if leaderStoreID == 0 || len(voterStoreIDs) == 0 {
		return nil, errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided")
	}
	if err := m.AdjustGroup(&Group{
		ID:            groupID,
		LeaderStoreID: leaderStoreID,
		VoterStoreIDs: voterStoreIDs,
	}); err != nil {
		return nil, err
	}

	// Step 1: Check whether the Group exists and validate.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	group := m.GetAffinityGroupState(groupID)
	if group == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	// When affinityVer is non-zero, it indicates a non-admin operation and triggers additional checks.
	// Note: if the check fails, no changes are applied. The existing Group is returned as-is without error.
	if affinityVer != 0 {
		// If affinityVer is not equal, the update may come from stale statistics and will be ignored.
		if group.affinityVer != affinityVer {
			return group, nil
		}
		// Group must not change voterStoreIDs while it is not in the expired state.
		// RegularSchedulingEnabled == IsExpired
		// The VoterStoreIDs from the API and Observe are already sorted, so they can be compared directly
		if !group.RegularSchedulingEnabled && !slices.Equal(voterStoreIDs, group.VoterStoreIDs) {
			return group, nil
		}
	}

	// Step 2: Save the Group in storage.
	group.LeaderStoreID = leaderStoreID
	group.VoterStoreIDs = slices.Clone(voterStoreIDs)
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, groupID, &group.Group)
	}); err != nil {
		return nil, err
	}

	// Step 3: Save the information in memory.
	return m.updateGroupPeers(groupID, leaderStoreID, voterStoreIDs)
}

// UpdateAffinityGroupKeyRanges batch modifies key ranges for multiple affinity groups.
// Note: Validates add and remove operations separately, then applies both atomically in a single transaction.
// Currently migration (removing a range from group A and adding the same range to group B in one request)
// is not supported because add validation checks overlaps against existing ranges before removals are applied.
func (m *Manager) UpdateAffinityGroupKeyRanges(addOps, removeOps []GroupKeyRanges) error {
	toAdd := make(map[string]GroupKeyRanges, len(addOps))
	toRemove := make(map[string]GroupKeyRanges, len(removeOps))
	newAddedLabelRules := make(map[string]*labeler.LabelRule, len(addOps))
	newRemovedLabelRules := make(map[string]*labeler.LabelRule, len(removeOps))

	plan := m.regionLabeler.NewPlan()

	// Step 0: Validate that a Group is either fully added or fully removed.
	for _, op := range addOps {
		if len(op.KeyRanges) == 0 {
			continue
		}
		if _, exists := toAdd[op.GroupID]; exists {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(op.GroupID)
		}
		toAdd[op.GroupID] = GroupKeyRanges{}
	}
	for _, op := range removeOps {
		if len(op.KeyRanges) == 0 {
			continue
		}
		if _, exists := toRemove[op.GroupID]; exists {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(op.GroupID)
		}
		if _, exists := toAdd[op.GroupID]; exists {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(op.GroupID)
		}
		toRemove[op.GroupID] = GroupKeyRanges{}
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	// Step 1: Validate the added KeyRanges.
	var newRangesToValidate []GroupKeyRanges
	for _, op := range addOps {
		currentRanges, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}

		// Merge current ranges with new ranges to add
		toAdd[op.GroupID] = GroupKeyRanges{
			GroupID:   op.GroupID,
			KeyRanges: slices.Concat(currentRanges.KeyRanges, op.KeyRanges),
		}

		// Collect new ranges for overlap validation
		newRangesToValidate = append(newRangesToValidate, GroupKeyRanges{
			KeyRanges: op.KeyRanges,
			GroupID:   op.GroupID,
		})
	}
	// Validate no overlaps with newly added ranges
	if len(newRangesToValidate) > 0 {
		if err := m.validateNoKeyRangeOverlap(newRangesToValidate); err != nil {
			return err
		}
	}

	// Step 2: Validate the removed KeyRanges.
	for _, op := range removeOps {
		currentRanges, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}
		currentRanges, err = applyRemoveOps(currentRanges, op)
		if err != nil {
			return err
		}
		toRemove[op.GroupID] = currentRanges
	}

	// Step 3: Create the change plan for the Label
	for _, op := range addOps {
		// For addOps, directly SetLabelRule (Save will overwrite the old value).
		// No need to Delete first.
		labelRule := MakeLabelRuleFromRanges(toAdd[op.GroupID])
		if err := plan.SetLabelRule(labelRule); err != nil {
			log.Error("failed to create label rule",
				zap.String("failed-group-id", op.GroupID),
				zap.Int("total-groups", len(addOps)+len(removeOps)),
				zap.Error(err))
			return err
		}
		newAddedLabelRules[op.GroupID] = labelRule
	}
	for _, op := range removeOps {
		ranges := toRemove[op.GroupID]
		var labelRule *labeler.LabelRule

		if len(ranges.KeyRanges) > 0 {
			// If there are still ranges after removal, SetLabelRule (overwrite the old value).
			labelRule = MakeLabelRuleFromRanges(ranges)
			if err := plan.SetLabelRule(labelRule); err != nil {
				log.Error("failed to create label rule",
					zap.String("failed-group-id", op.GroupID),
					zap.Int("total-groups", len(addOps)+len(removeOps)),
					zap.Error(err))
				return err
			}
		} else {
			// If no ranges remain after removal, DeleteLabelRule.
			labelRuleID := GetLabelRuleID(op.GroupID)
			if err := plan.DeleteLabelRule(labelRuleID); err != nil {
				log.Error("failed to delete label rule for affinity group",
					zap.String("group-id", op.GroupID),
					zap.String("label-rule-id", labelRuleID),
					zap.Error(err))
				return err
			}
		}
		newRemovedLabelRules[op.GroupID] = labelRule
	}

	// Step 4: Save the Label information in storage.
	if err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, plan.CommitOps()); err != nil {
		log.Error("failed to update affinity group ranges",
			zap.Int("total-groups", len(addOps)+len(removeOps)),
			zap.Error(err))
		return err
	}

	// Step 5: Save the Group and Label information in memory.
	plan.Apply()

	for groupID, currentRanges := range toAdd {
		m.keyRanges[groupID] = currentRanges
	}
	for groupID, currentRanges := range toRemove {
		if len(currentRanges.KeyRanges) > 0 {
			m.keyRanges[groupID] = currentRanges
		} else {
			delete(m.keyRanges, groupID)
		}
	}

	m.updateGroupLabelRules(newAddedLabelRules, false)
	m.updateGroupLabelRules(newRemovedLabelRules, true)
	return nil
}

// getCurrentRanges retrieves the current key ranges for a group.
func (m *Manager) getCurrentRanges(groupID string) (GroupKeyRanges, error) {
	// Try cache first
	if gkr, ok := m.keyRanges[groupID]; ok {
		return gkr, nil
	}

	// Parse from label rule
	labelRule := m.regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
	if labelRule == nil {
		// Allow starting from empty when the group exists but has no ranges yet.
		m.RLock()
		_, exists := m.groups[groupID]
		m.RUnlock()
		if exists {
			return GroupKeyRanges{GroupID: groupID}, nil
		}
		return GroupKeyRanges{}, errors.Errorf("label rule not found for group %s", groupID)
	}

	dataSlice, ok := labelRule.Data.([]*labeler.KeyRangeRule)
	if !ok {
		return GroupKeyRanges{}, errors.Errorf("invalid label rule data type for group %s, got type %T", groupID, labelRule.Data)
	}

	return parseKeyRangesFromData(dataSlice, groupID)
}

// applyRemoveOps filters out ranges that match remove operations.
// Optimized with a map for O(n+m) complexity instead of O(n*m).
// currentRanges has been sorted by start key and not overlapping.
func applyRemoveOps(currentRanges GroupKeyRanges, removes GroupKeyRanges) (GroupKeyRanges, error) {
	if len(removes.KeyRanges) == 0 {
		return currentRanges, nil
	}

	// Build a set of ranges to remove for O(1) lookup
	// Use hex encoding to avoid key collisions
	removeSet := make(map[string]keyutil.KeyRange, len(removes.KeyRanges))
	for _, remove := range removes.KeyRanges {
		key := hex.EncodeToString(remove.StartKey)
		if _, exists := removeSet[key]; exists {
			return GroupKeyRanges{}, errs.ErrAffinityGroupExist.GenWithStackByArgs(removes.GroupID)
		}
		removeSet[key] = remove
	}

	var filtered []keyutil.KeyRange
	for _, current := range currentRanges.KeyRanges {
		key := hex.EncodeToString(current.StartKey)
		if remove, exists := removeSet[key]; exists {
			if !bytes.Equal(remove.EndKey, current.EndKey) {
				return GroupKeyRanges{}, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(removes.GroupID)
			}
			delete(removeSet, key)
		} else {
			filtered = append(filtered, current)
		}
	}

	if len(removeSet) > 0 {
		// There exists a Range that does not appear in currentRanges.
		return GroupKeyRanges{}, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(removes.GroupID)
	}

	return GroupKeyRanges{
		KeyRanges: filtered,
		GroupID:   currentRanges.GroupID,
	}, nil
}

// parseKeyRangesFromData parses key ranges from []*labeler.KeyRangeRule format (from label rule).
func parseKeyRangesFromData(data []*labeler.KeyRangeRule, groupID string) (GroupKeyRanges, error) {
	if len(data) == 0 {
		return GroupKeyRanges{GroupID: groupID}, nil
	}

	var keyRanges []keyutil.KeyRange
	for _, item := range data {
		if item == nil {
			continue
		}
		startKey, err := decodeHexKey(item.StartKeyHex, groupID, "start")
		if err != nil {
			return GroupKeyRanges{}, err
		}

		endKey, err := decodeHexKey(item.EndKeyHex, groupID, "end")
		if err != nil {
			return GroupKeyRanges{}, err
		}
		keyRanges = append(keyRanges, keyutil.KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		})
	}
	return GroupKeyRanges{
		KeyRanges: keyRanges,
		GroupID:   groupID,
	}, nil
}

// decodeHexKey decodes a hex string and returns an error if decoding fails.
func decodeHexKey(hexStr, groupID, keyType string) ([]byte, error) {
	if hexStr == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, errs.ErrInvalidKeyFormat.FastGenByArgs(keyType, hexStr, groupID)
	}
	return decoded, nil
}

// extractKeyRangesFromLabelRule extracts key ranges from a label rule data.
func extractKeyRangesFromLabelRule(rule *labeler.LabelRule) (GroupKeyRanges, error) {
	if rule == nil || rule.Data == nil {
		return GroupKeyRanges{}, nil
	}

	groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
	if !ok {
		return GroupKeyRanges{}, nil
	}

	dataSlice, ok := rule.Data.([]*labeler.KeyRangeRule)
	if !ok {
		return GroupKeyRanges{}, errs.ErrAffinityGroupContent.FastGenByArgs("invalid label rule data format")
	}

	return parseKeyRangesFromData(dataSlice, groupID)
}

// checkKeyRangesOverlap checks if two key ranges overlap.
// Returns true if [start1, end1) and [start2, end2) have any overlap.
func checkKeyRangesOverlap(start1, end1, start2, end2 []byte) bool {
	// If the start or end keys are the same, they overlap
	if bytes.Equal(start1, start2) || bytes.Equal(end1, end2) {
		return true
	}

	// Handle empty keys (representing infinity)
	if len(start1) == 0 && len(end1) == 0 {
		return true // Range covers everything
	}
	if len(start2) == 0 && len(end2) == 0 {
		return true // Range covers everything
	}

	// Check if ranges are disjoint
	// Range 1 ends before Range 2 starts: end1 <= start2
	if len(end1) > 0 && len(start2) > 0 && bytes.Compare(end1, start2) <= 0 {
		return false
	}

	// Range 2 ends before Range 1 starts: end2 <= start1
	if len(end2) > 0 && len(start1) > 0 && bytes.Compare(end2, start1) <= 0 {
		return false
	}

	return true
}

// flattenKeyRange is used to flatten all ranges for easier comparison
type flattenKeyRange struct {
	keyutil.KeyRange
	groupID string
}

// validateNoKeyRangeOverlap validates that the given key ranges do not overlap with existing ones.
// It should be called with the manager lock held.
// Uses in-memory keyRanges cache to avoid repeated labeler access and reduce lock contention.
func (m *Manager) validateNoKeyRangeOverlap(newRanges []GroupKeyRanges) error {
	var allNewRanges []flattenKeyRange
	for _, gkr := range newRanges {
		for _, kr := range gkr.KeyRanges {
			allNewRanges = append(allNewRanges, flattenKeyRange{
				KeyRange: kr,
				groupID:  gkr.GroupID,
			})
		}
	}

	// Step 1: Check for overlaps within the new ranges themselves
	// This is O(N²) where N is the total number of new ranges across all groups
	for i := range allNewRanges {
		for j := i + 1; j < len(allNewRanges); j++ {
			if checkKeyRangesOverlap(
				allNewRanges[i].StartKey, allNewRanges[i].EndKey,
				allNewRanges[j].StartKey, allNewRanges[j].EndKey,
			) {
				if allNewRanges[i].groupID == allNewRanges[j].groupID {
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"key ranges overlap within group " + allNewRanges[i].groupID)
				}
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"key range overlaps between groups: " +
						allNewRanges[i].groupID + " and " + allNewRanges[j].groupID)
			}
		}
	}

	// Step 2: Check new ranges against ALL existing ranges
	// This is O(N × G×R) where N is new ranges, G is existing groups, R is ranges per group
	// We must check against all existing groups, including those being updated,
	// to catch cases like:
	// - Adding ranges to group A and group B, where A's new range overlaps with B's existing range
	// - Adding duplicate range to the same group
	for _, newRange := range allNewRanges {
		for _, existingGKR := range m.keyRanges {
			for _, existingRange := range existingGKR.KeyRanges {
				if checkKeyRangesOverlap(
					newRange.StartKey, newRange.EndKey,
					existingRange.StartKey, existingRange.EndKey,
				) {
					if newRange.groupID == existingGKR.GroupID {
						return errs.ErrAffinityGroupContent.FastGenByArgs(
							"key range overlaps with existing ranges in group " + newRange.groupID)
					}
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"key range overlaps between groups: " +
							newRange.groupID + " and " + existingGKR.GroupID)
				}
			}
		}
	}

	return nil
}

// loadRegionLabel rebuilds the mapping between groups and label rules after restart.
// It should be called with the manager lock held.
func (m *Manager) loadRegionLabel() error {
	// Collect all key ranges from label rules and populate in-memory cache
	var allRanges []GroupKeyRanges

	m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
		if !ok {
			// Not an affinity label rule, skip
			return true
		}

		if _, exists := m.groups[groupID]; !exists {
			log.Debug("found label rule for unknown affinity group, skip rebuilding",
				zap.String("group-id", groupID),
				zap.String("rule-id", rule.ID))
			return true
		}

		gkr, err := extractKeyRangesFromLabelRule(rule)
		if err != nil {
			log.Warn("failed to extract key ranges from label rule during rebuild",
				zap.String("rule-id", rule.ID),
				zap.String("group-id", groupID),
				zap.Error(err))
			return true
		}

		if len(gkr.KeyRanges) > 0 {
			allRanges = append(allRanges, gkr)
			// Populate in-memory key ranges cache
			m.keyRanges[groupID] = gkr
		}

		// Associate the label rule with the group
		m.updateGroupLabelRuleLocked(groupID, rule, false)

		return true
	})

	// Validate that all key ranges are non-overlapping
	for i := range allRanges {
		// Check within the same group
		for idx1 := range allRanges[i].KeyRanges {
			for idx2 := idx1 + 1; idx2 < len(allRanges[i].KeyRanges); idx2++ {
				if checkKeyRangesOverlap(
					allRanges[i].KeyRanges[idx1].StartKey, allRanges[i].KeyRanges[idx1].EndKey,
					allRanges[i].KeyRanges[idx2].StartKey, allRanges[i].KeyRanges[idx2].EndKey,
				) {
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"found overlapping key ranges within group " + allRanges[i].GroupID + " during rebuild")
				}
			}
		}
		// Check between different groups
		for _, rangeI := range allRanges[i].KeyRanges {
			for j := i + 1; j < len(allRanges); j++ {
				for _, rangeJ := range allRanges[j].KeyRanges {
					if checkKeyRangesOverlap(
						rangeI.StartKey, rangeI.EndKey,
						rangeJ.StartKey, rangeJ.EndKey,
					) {
						return errs.ErrAffinityGroupContent.FastGenByArgs(
							"found overlapping key ranges during rebuild: group " +
								allRanges[i].GroupID + " overlaps with group " + allRanges[j].GroupID)
					}
				}
			}
		}
	}

	log.Info("rebuilt group-label mapping",
		zap.Int("total-groups", len(m.keyRanges)),
		zap.Int("total-ranges", func() int {
			total := 0
			for _, gkr := range allRanges {
				total += len(gkr.KeyRanges)
			}
			return total
		}()))

	return nil
}
