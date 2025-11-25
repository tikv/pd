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

// MakeLabelRuleFromRanges MakeLabelRule makes the label rule.
func MakeLabelRuleFromRanges(groupID string, ranges []GroupKeyRange) *labeler.LabelRule {
	var labelData []any
	for _, kr := range ranges {
		labelData = append(labelData, map[string]any{
			"start_key": hex.EncodeToString(kr.StartKey),
			"end_key":   hex.EncodeToString(kr.EndKey),
		})
	}
	return &labeler.LabelRule{
		ID:       GetLabelRuleID(groupID),
		Labels:   []labeler.RegionLabel{{Key: labelKey, Value: groupID}},
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
	if err := m.groupsNotExist(groups); err != nil {
		return err
	}

	// Step 2: Convert and validate key ranges no overlaps
	var allNewRanges []GroupKeyRange
	for _, change := range changes {
		for _, kr := range change.KeyRanges {
			allNewRanges = append(allNewRanges, GroupKeyRange{
				KeyRange: kr,
				GroupID:  change.GroupID,
			})
		}
	}
	if err := m.validateNoKeyRangeOverlap(allNewRanges); err != nil {
		return err
	}

	// Step 3: Create the change plan for the Label
	labelRules := make([]*labeler.LabelRule, 0, len(changes))
	plan := m.regionLabeler.NewPlan()
	for _, change := range changes {
		if len(change.KeyRanges) > 0 {
			labelRule := MakeLabelRule(&change)
			if err := plan.SetLabelRule(labelRule); err != nil {
				log.Error("failed to create label rule",
					zap.String("failed-group-id", change.GroupID),
					zap.Int("total-groups", len(changes)),
					zap.Error(err))
				return err
			}
			labelRules = append(labelRules, labelRule)
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
			ranges := make([]GroupKeyRange, 0, len(change.KeyRanges))
			for _, kr := range change.KeyRanges {
				ranges = append(ranges, GroupKeyRange{
					KeyRange: kr,
					GroupID:  change.GroupID,
				})
			}
			m.keyRanges[change.GroupID] = ranges
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
			if ranges, exists := m.keyRanges[groupID]; exists && len(ranges) > 0 {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"affinity group " + groupID + " has key ranges, use force=true to delete")
			}
		}
		toDelete = append(toDelete, groupID)
	}

	// Step 2: Check if all Groups exist when force is false
	if !force {
		if err := m.groupsExistAll(toDelete); err != nil {
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

// UpdateAffinityGroupPeers updates the leader and voter stores of an affinity group and marks it effective.
func (m *Manager) UpdateAffinityGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	return m.updateAffinityGroupPeersWithAffinityVer(groupID, 0, leaderStoreID, voterStoreIDs)
}

// updateAffinityGroupPeersWithAffinityVer updates the leader and voter stores of an affinity group and marks it effective.
// If affinityVer is non-zero, its equality will be checked.
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

	// Step 1: Check whether the Group exists and validate affinityVer.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	group := m.GetAffinityGroupState(groupID)
	if group == nil || (affinityVer != 0 && group.affinityVer != affinityVer) {
		if affinityVer != 0 {
			// No error is generated for changes with a non-zero affinityVer.
			return nil, nil
		}
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	// Step 2: Save the Group in storage.
	group.LeaderStoreID = leaderStoreID
	group.VoterStoreIDs = append([]uint64{}, voterStoreIDs...)
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, groupID, &group.Group)
	}); err != nil {
		return nil, err
	}

	// Step 3: Save the information in memory.
	return m.updateAffinityGroupsPeer(groupID, leaderStoreID, voterStoreIDs)
}

// UpdateAffinityGroupKeyRanges batch modifies key ranges for multiple affinity groups.
// Remove operations are executed before add operations to handle range migration scenarios.
func (m *Manager) UpdateAffinityGroupKeyRanges(addOps, removeOps []GroupKeyRanges) error {
	toAdd := make(map[string][]GroupKeyRange, len(addOps))
	toRemove := make(map[string][]GroupKeyRange, len(removeOps))
	newAddedLabelRules := make(map[string]*labeler.LabelRule, len(addOps))
	newRemovedLabelRules := make(map[string]*labeler.LabelRule, len(removeOps))

	plan := m.regionLabeler.NewPlan()
	var allNewRanges []GroupKeyRange

	// Step 0: Validate that a Group is either fully added or fully removed.
	for _, op := range addOps {
		if len(op.KeyRanges) == 0 {
			continue
		}
		if _, exists := toAdd[op.GroupID]; exists {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(op.GroupID)
		}
		toAdd[op.GroupID] = nil
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
		toRemove[op.GroupID] = nil
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	// Step 1: Validate the added KeyRanges.
	for _, op := range addOps {
		currentRanges, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}
		// Set to add operations and collect new ranges
		for _, keyRange := range op.KeyRanges {
			groupKeyRange := GroupKeyRange{
				KeyRange: keyRange,
				GroupID:  op.GroupID,
			}
			currentRanges = append(currentRanges, groupKeyRange)
			allNewRanges = append(allNewRanges, groupKeyRange)
		}
		toAdd[op.GroupID] = currentRanges
	}
	// Validate no overlaps with newly added ranges
	if len(allNewRanges) > 0 {
		if err := m.validateNoKeyRangeOverlap(allNewRanges); err != nil {
			return err
		}
	}

	// Step 2: Validate the removed KeyRanges.
	for _, op := range removeOps {
		currentRanges, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}
		removeRanges := make([]GroupKeyRange, 0, len(op.KeyRanges))
		for _, keyRange := range op.KeyRanges {
			removeRanges = append(removeRanges, GroupKeyRange{
				KeyRange: keyRange,
				GroupID:  op.GroupID,
			})
		}
		currentRanges, err = applyRemoveOps(currentRanges, removeRanges)
		if err != nil {
			return err
		}
		toRemove[op.GroupID] = currentRanges
	}

	// Step 3: Create the change plan for the Label
	for _, op := range addOps {
		labelRuleID := GetLabelRuleID(op.GroupID)
		if err := plan.DeleteLabelRule(labelRuleID); err != nil {
			log.Error("failed to delete label rule for affinity group",
				zap.String("group-id", op.GroupID),
				zap.String("label-rule-id", labelRuleID),
				zap.Error(err))
			return err
		}
		labelRule := MakeLabelRuleFromRanges(op.GroupID, toAdd[op.GroupID])
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
		labelRuleID := GetLabelRuleID(op.GroupID)
		if err := plan.DeleteLabelRule(labelRuleID); err != nil {
			log.Error("failed to delete label rule for affinity group",
				zap.String("group-id", op.GroupID),
				zap.String("label-rule-id", labelRuleID),
				zap.Error(err))
			return err
		}
		ranges := toRemove[op.GroupID]
		var labelRule *labeler.LabelRule
		if len(ranges) > 0 {
			labelRule = MakeLabelRuleFromRanges(op.GroupID, ranges)
			if err := plan.SetLabelRule(labelRule); err != nil {
				log.Error("failed to create label rule",
					zap.String("failed-group-id", op.GroupID),
					zap.Int("total-groups", len(addOps)+len(removeOps)),
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
		if len(currentRanges) > 0 {
			m.keyRanges[groupID] = currentRanges
		} else {
			delete(m.keyRanges, groupID)
		}
	}

	m.updateGroupLabelRules(newAddedLabelRules)
	m.updateGroupLabelRules(newRemovedLabelRules)
	return nil
}

// SaveAffinityGroups adds multiple affinity groups to storage and creates corresponding label rules.
// TODO: Change to AddAffinityGroups.
func (m *Manager) SaveAffinityGroups(groupsWithRanges []GroupWithRanges) error {
	// Validate all groups first (without lock)
	for _, gwr := range groupsWithRanges {
		if err := m.AdjustGroup(gwr.Group); err != nil {
			return err
		}
	}

	m.Lock()
	defer m.Unlock()

	// Step 0: Convert and validate key ranges no overlaps under write lock
	var allNewRanges []GroupKeyRange
	for _, gwr := range groupsWithRanges {
		for _, kr := range gwr.KeyRanges {
			allNewRanges = append(allNewRanges, GroupKeyRange{
				KeyRange: kr,
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

	// Step 3: Update in-memory cache with label rule pointers and key ranges
	for _, gwr := range groupsWithRanges {
		labelRule := labelRules[gwr.Group.ID]
		m.updateGroupLabelRuleLocked(gwr.Group.ID, labelRule)
		// Update key ranges cache for this group
		if len(gwr.KeyRanges) > 0 {
			ranges := make([]GroupKeyRange, len(gwr.KeyRanges))
			for i, kr := range gwr.KeyRanges {
				ranges[i] = GroupKeyRange{
					KeyRange: kr,
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
// TODO: Change to DeleteAffinityGroups
func (m *Manager) DeleteAffinityGroup(id string, force bool) error {
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
	labelRuleID := GetLabelRuleID(id)
	if err = m.regionLabeler.DeleteLabelRule(labelRuleID); err != nil {
		log.Warn("failed to delete label rule for affinity group",
			zap.String("group-id", id),
			zap.String("label-rule-id", labelRuleID),
			zap.Error(err))
		// Don't return error here - the group is already deleted from storage
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
// TODO: change to UpdateAffinityGroupKeyRanges
func (m *Manager) BatchModifyGroupRanges(addOps, removeOps []GroupKeyRange) error {
	m.Lock()
	defer m.Unlock()
	// Group operations by GroupID
	type groupOps struct {
		adds    []GroupKeyRange
		removes []GroupKeyRange
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
	var allNewRanges []GroupKeyRange
	updatedRanges := make(map[string][]GroupKeyRange)

	for groupID, ops := range opsByGroup {
		// Get current ranges for this group
		currentRanges, err := m.getCurrentRanges(groupID)
		if err != nil {
			return err
		}

		// Apply remove operations
		// currentRanges = applyRemoveOps(currentRanges, ops.removes)

		// Apply add operations and collect new ranges
		for _, addOp := range ops.adds {
			currentRanges = append(currentRanges, addOp)
			allNewRanges = append(allNewRanges, addOp)
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
func (m *Manager) getCurrentRanges(groupID string) ([]GroupKeyRange, error) {
	// Try cache first
	if ranges := m.keyRanges[groupID]; ranges != nil {
		return append([]GroupKeyRange(nil), ranges...), nil
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
func applyRemoveOps(currentRanges []GroupKeyRange, removes []GroupKeyRange) ([]GroupKeyRange, error) {
	if len(removes) == 0 {
		return currentRanges, nil
	}

	// Build a set of ranges to remove for O(1) lookup
	// Use hex encoding to avoid key collisions
	removeSet := make(map[string]GroupKeyRange, len(removes))
	for _, remove := range removes {
		key := hex.EncodeToString(remove.StartKey)
		if _, exists := removeSet[key]; exists {
			return nil, errs.ErrAffinityGroupExist.GenWithStackByArgs(remove.GroupID)
		}
		removeSet[key] = remove
	}

	var filtered []GroupKeyRange
	for _, current := range currentRanges {
		key := hex.EncodeToString(current.StartKey)
		if remove, exists := removeSet[key]; exists {
			if !bytes.Equal(remove.EndKey, current.EndKey) {
				return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(remove.GroupID)
			}
			delete(removeSet, key)
		} else {
			filtered = append(filtered, current)
		}
	}

	for _, remove := range removeSet {
		// There exists a Range that does not appear in currentRanges.
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(remove.GroupID)
	}

	return filtered, nil
}

// updateGroupRanges updates the label rule and cache for a group's key ranges.
// TODO: remove it
func (m *Manager) updateGroupRanges(groupID string, ranges []GroupKeyRange) error {
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
// TODO: change to DeleteAffinityGroups
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
	for _, id := range toDelete {
		if err := m.regionLabeler.DeleteLabelRule(GetLabelRuleID(id)); err != nil {
			log.Warn("failed to delete label rule for affinity group",
				zap.String("group-id", id),
				zap.Error(err))
			// TODO: Don't return error here - the group is already deleted from storage
		}
	}

	// Step 3: clean caches and in-memory states.
	for _, id := range toDelete {
		m.deleteGroupLocked(id)
	}

	return nil
}

// UpdateGroupPeers updates the leader and voter stores of an affinity group and marks it effective.
// TODO: change to UpdateAffinityGroupPeers
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
	// m.updateGroupEffectLocked(groupID, groupInfo.AffinityVer, leaderStoreID, voterStoreIDs)

	return newGroupState(groupInfo), nil
}

// parseKeyRangesFromData parses key ranges from []*labeler.KeyRangeRule format (from label rule).
func parseKeyRangesFromData(data []*labeler.KeyRangeRule, groupID string) ([]GroupKeyRange, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var ranges []GroupKeyRange
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
		ranges = append(ranges, GroupKeyRange{
			KeyRange: keyutil.KeyRange{
				StartKey: startKey,
				EndKey:   endKey,
			},
			GroupID: groupID,
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
func extractKeyRangesFromLabelRule(rule *labeler.LabelRule) ([]GroupKeyRange, error) {
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
func (m *Manager) validateNoKeyRangeOverlap(newRanges []GroupKeyRange) error {
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
	// Collect all key ranges from label rules and populate in-memory cache
	var allRanges []GroupKeyRange

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
