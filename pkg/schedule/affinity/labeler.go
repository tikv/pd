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
	"strings"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// keyRange represents a key range extracted from label rules.
type keyRange struct {
	GroupID  string
	StartKey []byte
	EndKey   []byte
}

// GroupRangeModification defines a range modification operation for a group.
type GroupRangeModification keyRange

// GroupWithRanges represents a group with its associated key ranges.
type GroupWithRanges struct {
	Group     *Group
	KeyRanges []keyutil.KeyRange
}

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

// SaveAffinityGroups adds multiple affinity groups to storage and creates corresponding label rules.
func (m *Manager) SaveAffinityGroups(groupsWithRanges []GroupWithRanges) error {
	if !m.IsInitialized() {
		return errs.ErrAffinityDisabled
	}

	// Validate all groups first (without lock)
	for _, gwr := range groupsWithRanges {
		if err := m.AdjustGroup(gwr.Group); err != nil {
			return err
		}
	}

	m.Lock()
	defer m.Unlock()

	// Step 0: Convert and validate key ranges no overlaps under write lock
	var allNewRanges []keyRange
	for _, gwr := range groupsWithRanges {
		for _, kr := range gwr.KeyRanges {
			allNewRanges = append(allNewRanges, keyRange{
				StartKey: kr.StartKey,
				EndKey:   kr.EndKey,
				GroupID:  gwr.Group.ID,
			})
		}
	}
	if err := m.validateNoKeyRangeOverlap(allNewRanges); err != nil {
		return err
	}

	// Step 1: Build batch operations for storage
	batch := make([]func(txn kv.Txn) error, 0, len(groupsWithRanges))
	for _, gwr := range groupsWithRanges {
		localGroup := gwr.Group
		batch = append(batch, func(txn kv.Txn) error {
			return m.storage.SaveAffinityGroup(txn, localGroup.ID, localGroup)
		})
	}
	err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch)
	if err != nil {
		return err
	}

	// Step 2: Create label rules for each group
	labelRules := make(map[string]*labeler.LabelRule)
	if m.regionLabeler != nil {
		for _, gwr := range groupsWithRanges {
			if len(gwr.KeyRanges) > 0 {
				// Convert byte slices to hex-encoded label rule data
				var labelData []any
				for _, kr := range gwr.KeyRanges {
					labelData = append(labelData, map[string]any{
						"start_key": hex.EncodeToString(kr.StartKey),
						"end_key":   hex.EncodeToString(kr.EndKey),
					})
				}

				labelRule := &labeler.LabelRule{
					ID:       GetLabelRuleID(gwr.Group.ID),
					Labels:   []labeler.RegionLabel{{Key: labelKey, Value: gwr.Group.ID}},
					RuleType: labeler.KeyRange,
					Data:     labelData,
				}
				if err := m.regionLabeler.SetLabelRule(labelRule); err != nil {
					log.Error("failed to create label rule",
						zap.String("failed-group-id", gwr.Group.ID),
						zap.Int("total-groups", len(groupsWithRanges)),
						zap.Error(err))
					// TODO: rollback newly created groups
					return err
				}
				labelRules[gwr.Group.ID] = labelRule
			}
		}
	}

	// Step 3: Update in-memory cache with label rule pointers and key ranges
	for _, gwr := range groupsWithRanges {
		labelRule := labelRules[gwr.Group.ID]
		m.updateGroupLabelRuleLocked(gwr.Group.ID, labelRule)
		// Update key ranges cache for this group
		if len(gwr.KeyRanges) > 0 {
			ranges := make([]keyRange, len(gwr.KeyRanges))
			for i, kr := range gwr.KeyRanges {
				ranges[i] = keyRange{
					StartKey: kr.StartKey,
					EndKey:   kr.EndKey,
					GroupID:  gwr.Group.ID,
				}
			}
			m.keyRanges[gwr.Group.ID] = ranges
		} else {
			// No key ranges, remove from cache if exists
			delete(m.keyRanges, gwr.Group.ID)
		}

		log.Info("affinity group added/updated", zap.String("group", gwr.Group.String()))
	}

	return nil
}

// DeleteAffinityGroup deletes an affinity group by ID and removes its label rule.
// If force is false and the group has key ranges, it returns an error.
func (m *Manager) DeleteAffinityGroup(id string, force bool) error {
	if !m.IsInitialized() {
		return errs.ErrAffinityDisabled
	}
	m.Lock()
	defer m.Unlock()

	// Check if group has key ranges when force is false
	if !force {
		if ranges, exists := m.keyRanges[id]; exists && len(ranges) > 0 {
			return errs.ErrAffinityGroupContent.FastGenByArgs(
				"affinity group has key ranges, use force=true to delete")
		}
	}

	// Step 1: Delete from storage
	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.DeleteAffinityGroup(txn, id)
	})
	if err != nil {
		return err
	}

	// Step 2: Delete the corresponding label rule
	if m.regionLabeler != nil {
		labelRuleID := GetLabelRuleID(id)
		if err := m.regionLabeler.DeleteLabelRule(labelRuleID); err != nil {
			log.Warn("failed to delete label rule for affinity group",
				zap.String("group-id", id),
				zap.String("label-rule-id", labelRuleID),
				zap.Error(err))
			// Don't return error here - the group is already deleted from storage
		}
	}

	// Step 3: Delete key ranges cache and regions map
	m.deleteGroupLocked(id)
	log.Info("affinity group deleted",
		zap.String("group-id", id),
		zap.Bool("force", force))
	return nil
}

// BatchModifyGroupRanges batch modifies key ranges for multiple affinity groups.
// Remove operations are executed before add operations to handle range migration scenarios.
func (m *Manager) BatchModifyGroupRanges(addOps, removeOps []GroupRangeModification) error {
	m.Lock()
	defer m.Unlock()

	if m.regionLabeler == nil {
		return errors.New("region labeler is not available")
	}

	// Group operations by GroupID
	type groupOps struct {
		adds    []GroupRangeModification
		removes []GroupRangeModification
	}
	opsByGroup := make(map[string]*groupOps)

	for _, op := range addOps {
		if opsByGroup[op.GroupID] == nil {
			opsByGroup[op.GroupID] = &groupOps{}
		}
		opsByGroup[op.GroupID].adds = append(opsByGroup[op.GroupID].adds, op)
	}
	for _, op := range removeOps {
		if opsByGroup[op.GroupID] == nil {
			opsByGroup[op.GroupID] = &groupOps{}
		}
		opsByGroup[op.GroupID].removes = append(opsByGroup[op.GroupID].removes, op)
	}

	// Process all groups: apply removes then adds, collect new ranges for validation
	var allNewRanges []keyRange
	updatedRanges := make(map[string][]keyRange)

	for groupID, ops := range opsByGroup {
		// Get current ranges for this group
		currentRanges, err := m.getCurrentRanges(groupID)
		if err != nil {
			return err
		}

		// Apply remove operations
		currentRanges = applyRemoveOps(currentRanges, ops.removes)

		// Apply add operations and collect new ranges
		for _, addOp := range ops.adds {
			newRange := keyRange{
				StartKey: addOp.StartKey,
				EndKey:   addOp.EndKey,
				GroupID:  groupID,
			}
			currentRanges = append(currentRanges, newRange)
			allNewRanges = append(allNewRanges, newRange)
		}

		updatedRanges[groupID] = currentRanges
	}

	// Validate no overlaps with newly added ranges
	if len(allNewRanges) > 0 {
		if err := m.validateNoKeyRangeOverlap(allNewRanges); err != nil {
			return err
		}
	}

	// Update label rules and cache for all affected groups
	for groupID, ranges := range updatedRanges {
		if err := m.updateGroupRanges(groupID, ranges); err != nil {
			return err
		}
	}

	return nil
}

// getCurrentRanges retrieves the current key ranges for a group.
func (m *Manager) getCurrentRanges(groupID string) ([]keyRange, error) {
	// Try cache first
	if ranges := m.keyRanges[groupID]; ranges != nil {
		return append([]keyRange(nil), ranges...), nil
	}

	// Parse from label rule
	labelRule := m.regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
	if labelRule == nil {
		return nil, errors.Errorf("label rule not found for group %s", groupID)
	}

	dataSlice, ok := labelRule.Data.([]*labeler.KeyRangeRule)
	if !ok {
		return nil, errors.Errorf("invalid label rule data type for group %s, got type %T", groupID, labelRule.Data)
	}

	return parseKeyRangesFromData(dataSlice, groupID)
}

// applyRemoveOps filters out ranges that match remove operations.
// Optimized with a map for O(n+m) complexity instead of O(n*m).
func applyRemoveOps(currentRanges []keyRange, removes []GroupRangeModification) []keyRange {
	if len(removes) == 0 {
		return currentRanges
	}

	// Build a set of ranges to remove for O(1) lookup
	// Use hex encoding to avoid key collisions
	removeSet := make(map[string]struct{}, len(removes))
	for _, r := range removes {
		key := hex.EncodeToString(r.StartKey) + "|" + hex.EncodeToString(r.EndKey)
		removeSet[key] = struct{}{}
	}

	var filtered []keyRange
	for _, current := range currentRanges {
		key := hex.EncodeToString(current.StartKey) + "|" + hex.EncodeToString(current.EndKey)
		if _, found := removeSet[key]; !found {
			filtered = append(filtered, current)
		}
	}
	return filtered
}

// updateGroupRanges updates the label rule and cache for a group's key ranges.
func (m *Manager) updateGroupRanges(groupID string, ranges []keyRange) error {
	labelRule := m.regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
	if labelRule == nil {
		return errors.Errorf("label rule not found for group %s", groupID)
	}

	// Allow clearing ranges by deleting the label rule and cache.
	if len(ranges) == 0 {
		if err := m.regionLabeler.DeleteLabelRule(labelRule.ID); err != nil {
			return err
		}
		delete(m.keyRanges, groupID)
		if groupInfo, ok := m.groups[groupID]; ok {
			m.affinityRegionCount -= groupInfo.AffinityRegionCount
			groupInfo.LabelRule = nil
			groupInfo.RangeCount = 0
			groupInfo.AffinityRegionCount = 0
			for regionID := range groupInfo.Regions {
				delete(m.regions, regionID)
			}
			groupInfo.Regions = make(map[uint64]regionCache)
		}
		return nil
	}

	var newData []any
	for _, kr := range ranges {
		newData = append(newData, map[string]any{
			"start_key": hex.EncodeToString(kr.StartKey),
			"end_key":   hex.EncodeToString(kr.EndKey),
		})
	}

	labelRule.Data = newData
	if err := m.regionLabeler.SetLabelRule(labelRule); err != nil {
		return err
	}

	// Update in-memory cache; keep RangeCount aligned with label rule.
	m.keyRanges[groupID] = ranges
	if groupInfo, ok := m.groups[groupID]; ok {
		groupInfo.RangeCount = len(ranges)
	}
	return nil
}

// BatchDeleteAffinityGroups deletes multiple affinity groups in a single transaction.
// If force is false:
//   - Returns error if any group does not exist
//   - Returns error if any group has key ranges
//
// If force is true:
//   - Skips non-existent groups
//   - Deletes groups even if they have key ranges
//
// TODO: use smaller lock
func (m *Manager) BatchDeleteAffinityGroups(ids []string, force bool) error {
	m.Lock()
	defer m.Unlock()

	if len(ids) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("no group ids provided")
	}

	seen := make(map[string]struct{}, len(ids))
	toDelete := make([]string, 0, len(ids))

	for _, id := range ids {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}

		if _, ok := m.groups[id]; ok {
			// Check if group has key ranges when force is false
			if !force {
				if ranges, exists := m.keyRanges[id]; exists && len(ranges) > 0 {
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"affinity group " + id + " has key ranges, use force=true to delete")
				}
			}
			toDelete = append(toDelete, id)
			continue
		}
		if !force {
			return errs.ErrAffinityGroupNotFound.GenWithStackByArgs(id)
		}
		// force: skip non-exist
	}

	if len(toDelete) == 0 {
		return nil
	}

	// Step 1: delete from storage atomically.
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, id := range toDelete {
			if err := m.storage.DeleteAffinityGroup(txn, id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Step 2: delete label rules.
	if m.regionLabeler != nil {
		for _, id := range toDelete {
			if err := m.regionLabeler.DeleteLabelRule(GetLabelRuleID(id)); err != nil {
				log.Warn("failed to delete label rule for affinity group",
					zap.String("group-id", id),
					zap.Error(err))
				// TODO: Don't return error here - the group is already deleted from storage
			}
		}
	}

	// Step 3: clean caches and in-memory states.
	for _, id := range toDelete {
		m.deleteGroupLocked(id)
	}

	return nil
}

// UpdateGroupPeers updates the leader and voter stores of an affinity group and marks it effective.
func (m *Manager) UpdateGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	// Basic validation outside the lock to avoid blocking other operations
	if err := m.AdjustGroup(&Group{
		ID:            groupID,
		LeaderStoreID: leaderStoreID,
		VoterStoreIDs: voterStoreIDs,
	}); err != nil {
		return nil, err
	}

	m.Lock()
	defer m.Unlock()

	groupInfo, ok := m.groups[groupID]
	if !ok {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	// Persist updated peer distribution
	group := &Group{
		ID:              groupInfo.ID,
		CreateTimestamp: groupInfo.CreateTimestamp,
		LeaderStoreID:   leaderStoreID,
		VoterStoreIDs:   append([]uint64{}, voterStoreIDs...),
	}
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, groupID, group)
	}); err != nil {
		return nil, err
	}

	// Apply to in-memory state.
	// TODO: We pass current groupInfo to updateGroupEffectLocked
	m.updateGroupEffectLocked(groupID, groupInfo.AffinityVer, leaderStoreID, voterStoreIDs)

	return newGroupState(groupInfo), nil
}

// parseKeyRangesFromData parses key ranges from []*labeler.KeyRangeRule format (from label rule).
func parseKeyRangesFromData(data []*labeler.KeyRangeRule, groupID string) ([]keyRange, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var ranges []keyRange
	for _, item := range data {
		if item == nil {
			continue
		}
		startKey, err := decodeHexKey(item.StartKeyHex, groupID, "start")
		if err != nil {
			return nil, err
		}

		endKey, err := decodeHexKey(item.EndKeyHex, groupID, "end")
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, keyRange{
			StartKey: startKey,
			EndKey:   endKey,
			GroupID:  groupID,
		})
	}
	return ranges, nil
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
func extractKeyRangesFromLabelRule(rule *labeler.LabelRule) ([]keyRange, error) {
	if rule == nil || rule.Data == nil {
		return nil, nil
	}

	groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
	if !ok {
		return nil, nil
	}

	dataSlice, ok := rule.Data.([]*labeler.KeyRangeRule)
	if !ok {
		return nil, errs.ErrAffinityGroupContent.FastGenByArgs("invalid label rule data format")
	}

	return parseKeyRangesFromData(dataSlice, groupID)
}

// checkKeyRangesOverlap checks if two key ranges overlap.
// Returns true if [start1, end1) and [start2, end2) have any overlap.
func checkKeyRangesOverlap(start1, end1, start2, end2 []byte) bool {
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

// validateNoKeyRangeOverlap validates that the given key ranges do not overlap with existing ones.
// It should be called with the manager lock held.
// Uses in-memory keyRanges cache to avoid repeated labeler access and reduce lock contention.
func (m *Manager) validateNoKeyRangeOverlap(newRanges []keyRange) error {
	// First, check for overlaps within the new ranges themselves
	for i := range newRanges {
		for j := i + 1; j < len(newRanges); j++ {
			if checkKeyRangesOverlap(
				newRanges[i].StartKey, newRanges[i].EndKey,
				newRanges[j].StartKey, newRanges[j].EndKey,
			) {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"key ranges overlap within the same request: group " +
						newRanges[i].GroupID + " and " + newRanges[j].GroupID)
			}
		}
	}

	// Then, check for overlaps with existing key ranges from in-memory cache
	// This avoids repeated labeler lock acquisition for better performance
	for _, newRange := range newRanges {
		for groupID, existingRanges := range m.keyRanges {
			// Skip if it's the same group (updating existing group)
			if newRange.GroupID == groupID {
				continue
			}

			for _, existingRange := range existingRanges {
				if checkKeyRangesOverlap(
					newRange.StartKey, newRange.EndKey,
					existingRange.StartKey, existingRange.EndKey,
				) {
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"key range overlaps with existing group: new group " +
							newRange.GroupID + " overlaps with group " + existingRange.GroupID)
				}
			}
		}
	}

	return nil
}

// loadRegionLabel rebuilds the mapping between groups and label rules after restart.
// It should be called with the manager lock held.
func (m *Manager) loadRegionLabel() error {
	if m.regionLabeler == nil {
		return nil
	}

	// Collect all key ranges from label rules and populate in-memory cache
	var allRanges []keyRange

	m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
		if !ok {
			// Not an affinity label rule, skip
			return true
		}

		ranges, err := extractKeyRangesFromLabelRule(rule)
		if err != nil {
			log.Warn("failed to extract key ranges from label rule during rebuild",
				zap.String("rule-id", rule.ID),
				zap.String("group-id", groupID),
				zap.Error(err))
			return true
		}

		if len(ranges) > 0 {
			allRanges = append(allRanges, ranges...)
			// Populate in-memory key ranges cache
			m.keyRanges[groupID] = ranges
		}

		// Associate the label rule with the group
		if _, ok = m.keyRanges[groupID]; !ok {
			log.Warn("found label rule for unknown affinity group",
				zap.String("group-id", groupID),
				zap.String("rule-id", rule.ID))
		} else {
			m.updateGroupLabelRuleLocked(groupID, rule)
		}

		return true
	})

	// Validate that all key ranges are non-overlapping
	for i := range allRanges {
		for j := i + 1; j < len(allRanges); j++ {
			if checkKeyRangesOverlap(
				allRanges[i].StartKey, allRanges[i].EndKey,
				allRanges[j].StartKey, allRanges[j].EndKey,
			) {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"found overlapping key ranges during rebuild: group " +
						allRanges[i].GroupID + " overlaps with group " + allRanges[j].GroupID)
			}
		}
	}

	log.Info("rebuilt group-label mapping",
		zap.Int("total-groups", len(m.keyRanges)),
		zap.Int("total-ranges", len(allRanges)))

	return nil
}
