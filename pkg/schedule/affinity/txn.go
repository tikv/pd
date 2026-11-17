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
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	// LabelRuleIDPrefix is the prefix for affinity group label rules.
	// This is exposed publicly to allow other packages (like watcher) to use it.
	LabelRuleIDPrefix = "affinity_group/"
)

// GetLabelRuleID returns the label rule ID for an affinity group.
// This ensures consistent naming between label creation and deletion.
// Format: "affinity_group/{group_id}"
func GetLabelRuleID(groupID string) string {
	return LabelRuleIDPrefix + groupID
}

// parseAffinityGroupIDFromLabelRule parses the affinity group ID from the label rule.
// It will return the group ID and a boolean indicating whether the label rule is an affinity label rule.
func parseAffinityGroupIDFromLabelRule(rule *labeler.LabelRule) (string, bool) {
	// Validate the ID matches the expected format "affinity_group/{group_id}".
	if rule == nil || !strings.HasPrefix(rule.ID, LabelRuleIDPrefix) {
		return "", false
	}
	// Retrieve the group ID.
	groupID := strings.TrimPrefix(rule.ID, LabelRuleIDPrefix)
	if groupID == "" {
		return "", false
	}
	// Double-check the group ID from the label rule.
	var groupIDFromLabel string
	for _, label := range rule.Labels {
		if label.Key == AffinityLabelKey {
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
func MakeLabelRule(groupKeyRanges GroupKeyRanges) *labeler.LabelRule {
	var labelData []any
	for _, kr := range groupKeyRanges.KeyRanges {
		labelData = append(labelData, map[string]any{
			"start_key": hex.EncodeToString(kr.StartKey),
			"end_key":   hex.EncodeToString(kr.EndKey),
		})
	}
	return &labeler.LabelRule{
		ID:       GetLabelRuleID(groupKeyRanges.GroupID),
		Labels:   []labeler.RegionLabel{{Key: AffinityLabelKey, Value: groupKeyRanges.GroupID}},
		RuleType: labeler.KeyRange,
		Data:     labelData,
	}
}

// CreateAffinityGroups adds multiple affinity groups to storage and creates corresponding label rules.
func (m *Manager) CreateAffinityGroups(changes []GroupKeyRanges) error {
	return m.createGroupsWithSkipExisting(changes, false)
}

// CreateAffinityGroupsIfNotExists adds multiple affinity groups, skipping those that already exist.
func (m *Manager) CreateAffinityGroupsIfNotExists(changes []GroupKeyRanges) error {
	return m.createGroupsWithSkipExisting(changes, true)
}

func (m *Manager) createGroupsWithSkipExisting(changes []GroupKeyRanges, skipExisting bool) error {
	// Step 0: Validate all groups first (without lock)
	groups := make([]*Group, 0, len(changes))
	seenIDs := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		if _, seen := seenIDs[change.GroupID]; seen {
			return errs.ErrAffinityGroupContent.FastGenByArgs("duplicate group " + change.GroupID)
		}
		seenIDs[change.GroupID] = struct{}{}
		if err := validateGroupKeyRanges(change); err != nil {
			return err
		}
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

	// Sources must already exist outside this request, independent of chunking.
	for _, change := range changes {
		if _, inRequest := seenIDs[change.InheritPolicyFrom]; change.InheritPolicyFrom != "" && inRequest {
			return errs.ErrAffinityGroupContent.FastGenByArgs("policy inheritance source must not be in the same request")
		}
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	filteredGroups := groups
	filteredChanges := changes
	if skipExisting {
		filteredGroups = make([]*Group, 0, len(groups))
		filteredChanges = make([]GroupKeyRanges, 0, len(changes))
		m.RLock()
		for i, group := range groups {
			if _, exists := m.groups[group.ID]; exists {
				continue
			}
			filteredGroups = append(filteredGroups, group)
			filteredChanges = append(filteredChanges, changes[i])
		}
		m.RUnlock()
		if len(filteredGroups) == 0 {
			return nil
		}
	} else {
		// Step 1: Check whether the Group exists.
		if err := m.noGroupsExist(groups); err != nil {
			return err
		}
	}

	// Step 2: Convert and validate key ranges no overlaps
	if err := m.validateNoOverlapLocked(filteredChanges); err != nil {
		return err
	}

	// Step 3: Create the change plan for the Label
	labelRules := make([]*labeler.LabelRule, len(filteredChanges))
	plan := m.regionLabeler.NewPlan()
	for i, change := range filteredChanges {
		if len(change.KeyRanges) > 0 {
			labelRule := MakeLabelRule(change)
			if err := plan.SetLabelRule(labelRule); err != nil {
				log.Error("failed to create label rule",
					zap.String("failed-group-id", change.GroupID),
					zap.Int("total-groups", len(filteredChanges)),
					zap.Error(err))
				return err
			}
			labelRules[i] = labelRule
		}
	}

	// Step 4: Create the change plan for the Group
	labelOps := plan.CommitOps()
	groupOps := make([][]func(kv.Txn) error, 0, len(filteredGroups))
	for i, group := range filteredGroups {
		source := filteredChanges[i].InheritPolicyFrom
		ops := make([]func(kv.Txn) error, 0, 2)
		if labelRules[i] != nil {
			ops = append(ops, labelOps[0])
			labelOps = labelOps[1:]
		}
		ops = append(ops, func(txn kv.Txn) error {
			if source != "" {
				value, err := m.storage.LoadAffinityGroupInTxn(txn, source)
				if err != nil {
					return err
				}
				if value != "" {
					var sourceGroup Group
					if err := json.Unmarshal([]byte(value), &sourceGroup); err != nil {
						return err
					}
					group.BalancePolicy = sourceGroup.BalancePolicy
				}
			}
			return m.storage.SaveAffinityGroup(txn, group.ID, group)
		})
		groupOps = append(groupOps, ops)
	}

	// Step 5: Save the Group and Label information in storage.
	committed, err := m.runGroupOpsInTxn(groupOps)
	if err != nil {
		log.Error("failed to add affinity groups",
			zap.Int("total-groups", len(filteredChanges)),
			zap.Int("committed-groups", committed),
			zap.Error(err))
		// Publish the groups that have committed and treat the failed
		// transaction as not committed. Only groups with ranges have a label
		// rule in the plan.
		committedRules := 0
		for _, rule := range labelRules[:committed] {
			if rule != nil {
				committedRules++
			}
		}
		plan = plan.Prefix(committedRules)
		filteredGroups, filteredChanges, labelRules = filteredGroups[:committed], filteredChanges[:committed], labelRules[:committed]
	}

	// Step 6: Save the Group and Label information in memory.
	plan.Apply()

	for i, change := range filteredChanges {
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
		log.Info("affinity group added", zap.String("group", filteredGroups[i].String()))
	}

	m.createGroups(filteredGroups, labelRules)
	return err
}

// DeleteAffinityGroups deletes multiple affinity groups and their label rules.
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
	for _, groupID := range groupIDs {
		if err := ValidateGroupID(groupID); err != nil {
			return err
		}
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
	labelOps := plan.CommitOps()
	groupOps := make([][]func(kv.Txn) error, 0, len(toDelete))
	for i, id := range toDelete {
		groupID := id
		groupOps = append(groupOps, []func(kv.Txn) error{
			labelOps[i],
			func(txn kv.Txn) error {
				return m.storage.DeleteAffinityGroup(txn, groupID)
			},
		})
	}

	// Step 5: Save the Group and Label information in storage.
	committed, err := m.runGroupOpsInTxn(groupOps)
	if err != nil {
		log.Error("failed to delete affinity groups",
			zap.Int("total-groups", len(toDelete)),
			zap.Int("committed-groups", committed),
			zap.Error(err))
		// Publish the deletions that have committed and treat the failed
		// transaction as not committed.
		plan, toDelete = plan.Prefix(committed), toDelete[:committed]
	}

	// Step 6: Save the Group and Label information in memory.
	plan.Apply()

	for _, groupID := range toDelete {
		delete(m.keyRanges, groupID)
		log.Info("affinity group deleted", zap.String("group-id", groupID))
	}

	m.deleteGroups(toDelete)
	return err
}

// runGroupOpsInTxn keeps each group's one or two writes in the same transaction
// and returns how many groups, from the first, have committed.
// Count the individual operations so a group without a label rule cannot make
// the next group's pair straddle the transaction limit.
func (m *Manager) runGroupOpsInTxn(groupOps [][]func(kv.Txn) error) (int, error) {
	batch := make([]func(kv.Txn) error, 0, etcdutil.MaxEtcdTxnOps)
	committed := 0
	for i, ops := range groupOps {
		if len(batch)+len(ops) > etcdutil.MaxEtcdTxnOps {
			if err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch); err != nil {
				return committed, err
			}
			committed, batch = i, batch[:0]
		}
		batch = append(batch, ops...)
	}
	if err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch); err != nil {
		return committed, err
	}
	return len(groupOps), nil
}

// UpdateAffinityGroupPeers updates the leader and voter stores of an affinity group and marks it available.
func (m *Manager) UpdateAffinityGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	return m.updateAffinityGroupPeersWithState(groupID, nil, leaderStoreID, voterStoreIDs)
}

// updateAffinityGroupPeersWithState updates the leader and voter stores and marks the group available.
// An observation must still match the runtime group and its version. Only an
// expired group can change voters. A nil observation indicates an admin update.
func (m *Manager) updateAffinityGroupPeersWithState(groupID string, observed *GroupState, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
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
	if err := m.checkHasUnavailableStore(leaderStoreID, voterStoreIDs); err != nil {
		return nil, err
	}

	// Step 1: Check whether the Group exists and validate.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	group := m.GetAffinityGroupState(groupID)
	if group == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	voterStoreIDs = slices.Clone(voterStoreIDs)
	slices.Sort(voterStoreIDs)
	// Note: if the check fails, no changes are applied. The existing Group is returned as-is without error.
	if observed != nil {
		if group.groupInfoPtr != observed.groupInfoPtr || group.affinityVer != observed.affinityVer || group.rangeConflict {
			return group, nil
		}
		// Group must not change voterStoreIDs while it is not in the expired status. Changing only leaderStoreID is allowed.
		// RegularSchedulingAllowed == IsExpired
		// The VoterStoreIDs are already sorted, so they can be compared directly
		if !group.RegularSchedulingAllowed && !slices.Equal(voterStoreIDs, group.VoterStoreIDs) {
			return group, nil
		}
		m.RLock()
		conditionsKnown := m.storeConditionsKnown
		m.RUnlock()
		if !conditionsKnown && !group.targetUnpersisted && group.LeaderStoreID != 0 && len(group.VoterStoreIDs) != 0 &&
			(m.targetWriter != nil || leaderStoreID != group.LeaderStoreID) {
			return group, nil
		}
		// Policy-only updates keep affinityVer, so check the current policy while
		// holding metaMutex. A degraded fixed group can only confirm its target.
		if group.BalancePolicy.IsFixed() && !group.RegularSchedulingAllowed && leaderStoreID != group.LeaderStoreID {
			return group, nil
		}
		expected := group.Group
		if group.targetUnpersisted {
			expected = withoutTarget(expected)
		}
		target := GroupTarget{Expected: expected, LeaderStoreID: leaderStoreID, VoterStoreIDs: voterStoreIDs}
		if m.targetWriter == nil {
			// PD has no affinity watcher. Even an unchanged observation must
			// check storage after a write whose committed result was unknown.
			if _, err := m.updateGroupTargetLocked(m.ctx, target); err != nil {
				return nil, err
			}
		} else {
			if group.AffinitySchedulingAllowed && leaderStoreID != group.LeaderStoreID {
				return group, nil
			}
			persistOnly := group.AffinitySchedulingAllowed && group.targetUnpersisted
			result := target.result()
			if !group.AffinitySchedulingAllowed && sameStoredGroup(&group.Group, result) {
				m.Lock()
				if info := m.groups[groupID]; info.AffinityVer == observed.affinityVer && !info.IsAvailable() {
					m.setTargetAvailabilityLocked(info)
					m.resetCountLocked(info)
				}
				m.Unlock()
			}
			if !sameStoredGroup(&target.Expected, result) {
				m.sendObservedTarget(observedTarget{
					target:       target,
					groupInfoPtr: observed.groupInfoPtr,
					affinityVer:  observed.affinityVer,
					persistOnly:  persistOnly,
				})
			}
		}
		return m.GetAffinityGroupState(groupID), nil
	}

	// Step 2: Save the Group in storage.
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		stored, err := m.loadAffinityGroupInTxn(txn, groupID)
		if err != nil {
			return err
		}
		group.Group = *stored
		group.LeaderStoreID = leaderStoreID
		group.VoterStoreIDs = slices.Clone(voterStoreIDs)
		return m.storage.SaveAffinityGroup(txn, groupID, &group.Group)
	}); err != nil {
		return nil, err
	}

	// Step 3: Save the information in memory.
	m.Lock()
	defer m.Unlock()
	changed := m.syncGroupTargetLocked(m.groups[groupID], &group.Group)
	groupInfo := m.groups[groupID]
	if m.storeConditionsKnown {
		m.setTargetAvailabilityLocked(groupInfo)
	}
	if !changed {
		m.resetCountLocked(groupInfo)
	}
	return newGroupState(groupInfo), nil
}

// UpdateAffinityGroupPolicy updates the balance policy of an affinity group.
// The policy must be auto or fixed. It preserves the runtime state unless the
// stored target differs from memory.
func (m *Manager) UpdateAffinityGroupPolicy(groupID string, policy BalancePolicy) (*GroupState, error) {
	states, err := m.UpdateAffinityGroupPolicies([]string{groupID}, policy)
	if err != nil {
		return nil, err
	}
	return states[0], nil
}

// UpdateAffinityGroupPolicies sets the balance policy of several affinity
// groups and returns their states. The policy must be auto or fixed. Every
// group must exist when the request starts; otherwise nothing is written. The
// groups are written in chunks that each fit in one etcd transaction, so a
// failed chunk leaves the chunks written before it in place. Writing the same
// policy again does not change the stored groups.
func (m *Manager) UpdateAffinityGroupPolicies(groupIDs []string, policy BalancePolicy) ([]*GroupState, error) {
	if err := validateRequestedBalancePolicy(policy); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(groupIDs))
	seen := make(map[string]struct{}, len(groupIDs))
	for _, id := range groupIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		if err := ValidateGroupID(id); err != nil {
			return nil, err
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, errs.ErrEmptyRequest.FastGenByArgs("no group ids provided")
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := m.allGroupsExist(ids); err != nil {
		return nil, err
	}
	states := make([]*GroupState, 0, len(ids))
	// Each group takes one comparison and at most one write in the transaction.
	for chunk := range slices.Chunk(ids, etcdutil.MaxEtcdTxnOps) {
		updated, err := m.saveGroupPoliciesLocked(chunk, policy)
		if err != nil {
			return nil, errors.Annotatef(err, "updated the balance policy of %d of %d affinity groups before the failure", len(states), len(ids))
		}
		states = append(states, updated...)
	}
	return states, nil
}

// validateRequestedBalancePolicy checks the policy of a request. An empty
// policy reads as auto in a stored group, but a request must name one, so that
// a body without the field does not reset the policy.
func validateRequestedBalancePolicy(policy BalancePolicy) error {
	if policy != BalancePolicyAuto && policy != BalancePolicyFixed {
		return errs.ErrAffinityGroupContent.FastGenByArgs(
			fmt.Sprintf("invalid balance policy %q, it must be %q or %q", policy, BalancePolicyAuto, BalancePolicyFixed))
	}
	return nil
}

// saveGroupPoliciesLocked writes policy to the stored groups in one
// transaction and synchronizes their runtime states. A group that
// already has the policy is not written. metaMutex must be held.
func (m *Manager) saveGroupPoliciesLocked(groupIDs []string, policy BalancePolicy) ([]*GroupState, error) {
	updated := make([]*Group, len(groupIDs))
	if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		keys := make([]string, len(groupIDs))
		for i, id := range groupIDs {
			keys[i] = keypath.AffinityGroupPath(id)
		}
		// Storage may be ahead of memory after a committed write loses its reply.
		if err := kv.Prefetch(txn, keys); err != nil {
			return err
		}
		for i, groupID := range groupIDs {
			group, err := m.loadAffinityGroupInTxn(txn, groupID)
			if err != nil {
				return err
			}
			updated[i] = group
			if group.BalancePolicy == policy {
				continue
			}
			group.BalancePolicy = policy
			if err := m.storage.SaveAffinityGroup(txn, groupID, group); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	m.Lock()
	defer m.Unlock()
	states := make([]*GroupState, len(updated))
	for i, group := range updated {
		m.syncGroupTargetLocked(m.groups[group.ID], group)
		states[i] = newGroupState(m.groups[group.ID])
	}
	return states, nil
}

func (m *Manager) loadAffinityGroupInTxn(txn kv.Txn, groupID string) (*Group, error) {
	value, err := m.storage.LoadAffinityGroupInTxn(txn, groupID)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	var group Group
	if err := json.Unmarshal([]byte(value), &group); err != nil {
		return nil, errors.WithStack(err)
	}
	group.VoterStoreIDs = sortedStoreIDs(group.VoterStoreIDs)
	return &group, nil
}

// UpdateAffinityGroupKeyRanges batch modifies key ranges for multiple affinity groups.
// Note: Validates add and remove operations separately, then writes both in one batch, which spans several
// transactions when it has more than MaxEtcdTxnOps operations.
// Currently migration (removing a range from group A and adding the same range to group B in one request)
// is not supported because add validation checks overlaps against existing ranges before removals are applied.
func (m *Manager) UpdateAffinityGroupKeyRanges(addOps, removeOps []GroupKeyRanges) error {
	// Validate the full batch before planning changes, including empty operations.
	for _, ops := range [][]GroupKeyRanges{addOps, removeOps} {
		for _, op := range ops {
			if err := validateGroupKeyRanges(op); err != nil {
				return err
			}
		}
	}
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
	}
	// Validate no overlaps with newly added ranges
	if len(addOps) > 0 {
		if err := m.validateNoOverlapLocked(addOps); err != nil {
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
		labelRule := MakeLabelRule(toAdd[op.GroupID])
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
			labelRule = MakeLabelRule(ranges)
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

	// Step 4: Save the Label information in storage. Each operation writes one
	// label rule.
	labelOps := plan.CommitOps()
	ruleOps := make([][]func(kv.Txn) error, len(labelOps))
	for i, op := range labelOps {
		ruleOps[i] = []func(kv.Txn) error{op}
	}
	committed, err := m.runGroupOpsInTxn(ruleOps)
	if err != nil {
		log.Error("failed to update affinity group ranges",
			zap.Int("total-groups", len(addOps)+len(removeOps)),
			zap.Int("committed-groups", committed),
			zap.Error(err))
		// Publish the rules that have committed and treat the failed
		// transaction as not committed. The rules of addOps come first.
		plan = plan.Prefix(committed)
		added := min(committed, len(addOps))
		toAdd, newAddedLabelRules = filterGroups(toAdd, addOps[:added]), filterGroups(newAddedLabelRules, addOps[:added])
		toRemove, newRemovedLabelRules = filterGroups(toRemove, removeOps[:committed-added]), filterGroups(newRemovedLabelRules, removeOps[:committed-added])
	}

	// Step 5: Save the Group and Label information in memory.
	plan.Apply()

	maps.Copy(m.keyRanges, toAdd)
	for groupID, currentRanges := range toRemove {
		if len(currentRanges.KeyRanges) > 0 {
			m.keyRanges[groupID] = currentRanges
		} else {
			delete(m.keyRanges, groupID)
		}
	}

	m.Lock()
	defer m.Unlock()
	m.updateGroupLabelRulesLocked(newAddedLabelRules, false)
	m.updateGroupLabelRulesLocked(newRemovedLabelRules, true)
	if m.hasRangeConflicts {
		m.reconcileRangeConflictsLocked(m.collectRangeConflictsLocked())
	}
	return err
}

// filterGroups returns the entries of m for the groups of ops.
func filterGroups[V any](m map[string]V, ops []GroupKeyRanges) map[string]V {
	filtered := make(map[string]V, len(ops))
	for _, op := range ops {
		if v, ok := m[op.GroupID]; ok {
			filtered[op.GroupID] = v
		}
	}
	return filtered
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

	return extractKeyRangesFromLabelRule(labelRule)
}

// applyRemoveOps filters out ranges that match remove operations.
// Optimized with a map for O(n+m) complexity instead of O(n*m).
func applyRemoveOps(currentRanges GroupKeyRanges, removes GroupKeyRanges) (GroupKeyRanges, error) {
	if len(removes.KeyRanges) == 0 {
		return currentRanges, nil
	}

	// Match both bounds because persisted overlapping ranges may share a start.
	removeSet := make(map[[2]string]struct{}, len(removes.KeyRanges))
	for _, remove := range removes.KeyRanges {
		key := [2]string{string(remove.StartKey), string(remove.EndKey)}
		if _, exists := removeSet[key]; exists {
			return GroupKeyRanges{}, errs.ErrAffinityGroupExist.GenWithStackByArgs(removes.GroupID)
		}
		removeSet[key] = struct{}{}
	}

	var filtered []keyutil.KeyRange
	for _, current := range currentRanges.KeyRanges {
		key := [2]string{string(current.StartKey), string(current.EndKey)}
		if _, exists := removeSet[key]; exists {
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
// This function expects the rule to have already been processed by checkAndAdjust(),
// which guarantees that rule.Data is of type []*labeler.KeyRangeRule.
func extractKeyRangesFromLabelRule(rule *labeler.LabelRule) (GroupKeyRanges, error) {
	if rule == nil || rule.Data == nil {
		return GroupKeyRanges{}, nil
	}

	groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
	if !ok {
		return GroupKeyRanges{}, nil
	}

	data, ok := rule.Data.([]*labeler.KeyRangeRule)
	if !ok {
		return GroupKeyRanges{}, errs.ErrAffinityGroupContent.FastGenByArgs(
			fmt.Sprintf("invalid label rule data type for group %s, got type %T", groupID, rule.Data))
	}

	return parseKeyRangesFromData(data, groupID)
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

// validateNoOverlapLocked validates that the given key ranges do not overlap with existing ones.
// It should be called with metaMutex held.
// Uses in-memory keyRanges cache to avoid repeated labeler access and reduce lock contention.
// Complexity: O(M log M) where M is the total number of ranges (new + existing).
func (m *Manager) validateNoOverlapLocked(newRanges []GroupKeyRanges) error {
	added := flattenRanges(newRanges)
	for i := range len(added) - 1 {
		if checkKeyRangesOverlap(added[i].StartKey, added[i].EndKey, added[i+1].StartKey, added[i+1].EndKey) {
			return overlapError(added[i], added[i+1])
		}
	}
	existing := flattenRanges(slices.Collect(maps.Values(m.keyRanges)))
	// Persisted conflicts must not block changes to unrelated ranges. Only
	// compare new ranges with old ones, not old ranges with each other.
	for i, j := 0, 0; i < len(added) && j < len(existing); {
		if checkKeyRangesOverlap(added[i].StartKey, added[i].EndKey, existing[j].StartKey, existing[j].EndKey) {
			return overlapError(added[i], existing[j])
		}
		if bytes.Compare(added[i].StartKey, existing[j].StartKey) < 0 {
			i++
		} else {
			j++
		}
	}
	return nil
}

func overlapError(a, b flattenKeyRange) error {
	if a.groupID == b.groupID {
		return errs.ErrAffinityGroupContent.FastGenByArgs("key ranges overlap within group " + a.groupID)
	}
	return errs.ErrAffinityGroupContent.FastGenByArgs("key range overlaps between groups: " + a.groupID + " and " + b.groupID)
}

func flattenRanges(ranges []GroupKeyRanges) []flattenKeyRange {
	var allRanges []flattenKeyRange
	for _, gkr := range ranges {
		for _, kr := range gkr.KeyRanges {
			allRanges = append(allRanges, flattenKeyRange{
				KeyRange: kr,
				groupID:  gkr.GroupID,
			})
		}
	}

	slices.SortFunc(allRanges, func(a, b flattenKeyRange) int {
		if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.groupID, b.groupID)
	})
	return allRanges
}

// collectRangeConflictsLocked requires metaMutex, but not the manager write lock.
func (m *Manager) collectRangeConflictsLocked() map[string]struct{} {
	failpoint.InjectCall("beforeRangeConflictScan")
	ranges := flattenRanges(slices.Collect(maps.Values(m.keyRanges)))
	conflicts := make(map[string]struct{})
	if len(ranges) > 0 {
		furthest := ranges[0]
		for _, current := range ranges[1:] {
			if checkKeyRangesOverlap(furthest.StartKey, furthest.EndKey, current.StartKey, current.EndKey) {
				conflicts[furthest.groupID] = struct{}{}
				conflicts[current.groupID] = struct{}{}
			}
			// Keep the furthest end, including infinity, so nested ranges do
			// not hide a conflict with a later range.
			if len(furthest.EndKey) > 0 && (len(current.EndKey) == 0 || bytes.Compare(current.EndKey, furthest.EndKey) > 0) {
				furthest = current
			}
		}
	}
	return conflicts
}

// reconcileRangeConflictsLocked preserves the raw ranges for administrative
// repairs, but excludes every conflicting group from affinity scheduling. It
// logs the groups that start or stop conflicting, at the error level if any
// group starts conflicting.
// Both metaMutex and the manager write lock must be held.
func (m *Manager) reconcileRangeConflictsLocked(conflicts map[string]struct{}) {
	m.rangeConflictsStale = false
	m.hasRangeConflicts = len(conflicts) > 0
	var conflicting, resolved []string
	for id, group := range m.groups {
		_, conflict := conflicts[id]
		if group.rangeConflict != conflict {
			group.rangeConflict = conflict
			m.clearGroupCacheLocked(id)
			if conflict {
				conflicting = append(conflicting, id)
			} else {
				resolved = append(resolved, id)
			}
		}
	}
	slices.Sort(conflicting)
	slices.Sort(resolved)
	if len(conflicting) > 0 {
		log.Error("overlapping affinity key ranges; delete or update conflicting groups to restore affinity scheduling",
			zap.Strings("newly-conflicting-group-ids", conflicting),
			zap.Strings("resolved-group-ids", resolved))
	} else if len(resolved) > 0 {
		log.Info("affinity key range conflicts resolved", zap.Strings("resolved-group-ids", resolved))
	}
}

// loadRegionLabel rebuilds the mapping between groups and label rules after restart.
// It should be called with the manager lock held.
func (m *Manager) loadRegionLabel() {
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

	m.reconcileRangeConflictsLocked(m.collectRangeConflictsLocked())

	log.Info("rebuilt group-label mapping",
		zap.Int("total-groups", len(m.keyRanges)),
		zap.Int("total-ranges", func() int {
			total := 0
			for _, gkr := range allRanges {
				total += len(gkr.KeyRanges)
			}
			return total
		}()))
}
