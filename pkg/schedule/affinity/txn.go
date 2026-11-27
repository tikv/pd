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
	"context"
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
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	// leaderStorage adds two leadership term comparisons to each transaction.
	maxAffinityTxnOps = etcdutil.MaxEtcdTxnOps - 2

	// maxLoggedLeftoverGroupIDs limits the group IDs that the warning about label
	// rules without groups lists.
	maxLoggedLeftoverGroupIDs = 10
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
	_, err := m.createGroupsWithSkipExisting(context.Background(), changes, false, false)
	return err
}

// CreateAffinityGroupsIfNotExists adds multiple affinity groups, skipping those that already exist.
func (m *Manager) CreateAffinityGroupsIfNotExists(changes []GroupKeyRanges) error {
	_, err := m.createGroupsWithSkipExisting(context.Background(), changes, true, false)
	return err
}

// CreateAffinityGroupsWithContext creates groups with key ranges and returns snapshots of
// the created and skipped groups. New groups must have at least one key range.
func (m *Manager) CreateAffinityGroupsWithContext(ctx context.Context, changes []GroupKeyRanges, skipExisting bool) (map[string]*GroupState, error) {
	return m.createGroupsWithSkipExisting(ctx, changes, skipExisting, true)
}

type groupCreation struct {
	group     *Group
	ranges    GroupKeyRanges
	rule      *labeler.LabelRule
	recovered bool
}

func (m *Manager) createGroupsWithSkipExisting(reqCtx context.Context, changes []GroupKeyRanges, skipExisting, requireRanges bool) (map[string]*GroupState, error) {
	seenIDs := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		if _, exists := seenIDs[change.GroupID]; exists {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("duplicate group " + change.GroupID)
		}
		seenIDs[change.GroupID] = struct{}{}
		if err := ValidateGroupID(change.GroupID); err != nil {
			return nil, err
		}
	}
	// Sources must be outside the original request, including groups skipped below.
	for _, change := range changes {
		if _, inRequest := seenIDs[change.InheritPolicyFrom]; inRequest && change.InheritPolicyFrom != "" {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("policy inheritance source must not be in the same request")
		}
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return nil, err
	}

	states := make(map[string]*GroupState, len(changes))
	newRanges := make([]GroupKeyRanges, 0, len(changes))
	for _, change := range changes {
		if state := m.GetAffinityGroupState(change.GroupID); state != nil {
			if !skipExisting {
				return nil, errs.ErrAffinityGroupExist.GenWithStackByArgs(change.GroupID)
			}
			for _, kr := range change.KeyRanges {
				if err := ValidateKeyRange(change.GroupID, kr.StartKey, kr.EndKey, false); err != nil {
					return nil, err
				}
			}
			if err := validateGroupKeyRanges(change); err != nil {
				return nil, err
			}
			states[state.ID] = state
			continue
		}
		if requireRanges {
			for _, kr := range change.KeyRanges {
				if err := ValidateKeyRange(change.GroupID, kr.StartKey, kr.EndKey, false); err != nil {
					return nil, err
				}
			}
		}
		if err := validateGroupKeyRanges(change); err != nil {
			return nil, err
		}
		// Reject invalid rules before any chunk can commit.
		if len(change.KeyRanges) > 0 {
			if err := m.regionLabeler.NewPlan().SetLabelRule(MakeLabelRule(change)); err != nil {
				return nil, err
			}
		}
		newRanges = append(newRanges, change)
	}
	if len(newRanges) == 0 {
		return states, nil
	}
	groupOps := make([]groupTxnOp, 0, len(newRanges))
	for _, change := range newRanges {
		op := groupTxnOp{
			writes:     1,
			readKeys:   []string{keypath.AffinityGroupPath(change.GroupID)},
			extraReads: 1, // The persisted group may have a label rule.
		}
		if len(change.KeyRanges) > 0 {
			op.writes++
		}
		if source := change.InheritPolicyFrom; source != "" {
			op.readKeys = append(op.readKeys, keypath.AffinityGroupPath(source))
		}
		groupOps = append(groupOps, op)
	}

	multipleChunks := groupTxnChunkSize(groupOps) < len(groupOps)
	if multipleChunks {
		// A group recovered in a later chunk can own a range requested in
		// an earlier chunk. Restore all persisted candidates before any writes.
		if err := m.reconcilePersistedGroupsLocked(reqCtx, newRanges, states); err != nil {
			return nil, err
		}
		remaining := 0
		for i, change := range newRanges {
			if states[change.GroupID] != nil {
				if !skipExisting {
					return nil, errs.ErrAffinityGroupExist.GenWithStackByArgs(change.GroupID)
				}
				continue
			}
			if requireRanges && len(change.KeyRanges) == 0 {
				return nil, errs.ErrAffinityGroupContent.FastGenByArgs("no key ranges provided for group " + change.GroupID)
			}
			newRanges[remaining], groupOps[remaining] = change, groupOps[i]
			remaining++
		}
		newRanges, groupOps = newRanges[:remaining], groupOps[:remaining]
		if remaining == 0 {
			return states, nil
		}
		if err := m.validateNoOverlapLocked(newRanges); err != nil {
			return nil, err
		}
	}

	creations := make([]groupCreation, len(newRanges))
	plan := m.regionLabeler.NewPlan()
	for start := 0; start < len(newRanges); {
		end := start + groupTxnChunkSize(groupOps[start:])
		chunk := creations[start:end]
		// Save the acknowledged plan before the callback adds to it. A failed
		// transaction can leave pending or invalid rules in its plan.
		acknowledgedPlan := plan.Prefix(len(plan.CommitOps()))
		var conflictID string
		err := m.runAdminTxn(reqCtx, func(txn kv.Txn) error {
			if err := kv.Prefetch(txn, groupTxnReadKeys(groupOps[start:end])); err != nil {
				return err
			}
			if err := m.restoreGroupCreations(txn, newRanges[start:end], chunk, plan); err != nil {
				return err
			}
			for i, create := range chunk {
				if create.recovered {
					newRanges[start+i] = create.ranges
					if conflictID == "" {
						conflictID = create.group.ID
					}
				}
			}
			if conflictID != "" && !skipExisting {
				return nil // Verify the reads before publishing the recovered groups.
			}
			if !multipleChunks && requireRanges {
				for i := range chunk {
					if !chunk[i].recovered && len(newRanges[start+i].KeyRanges) == 0 {
						return errs.ErrAffinityGroupContent.FastGenByArgs("no key ranges provided for group " + newRanges[start+i].GroupID)
					}
				}
			}
			if !multipleChunks || conflictID != "" {
				if err := m.validateNoOverlapLocked(newRanges[:end]); err != nil {
					return err
				}
			}

			firstWrite := len(plan.CommitOps())
			for i := range chunk {
				if !chunk[i].recovered {
					if err := m.stageGroupCreation(txn, newRanges[start+i], &chunk[i], plan); err != nil {
						return err
					}
				}
			}
			for _, write := range plan.CommitOps()[firstWrite:] {
				if err := write(txn); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			m.publishGroupCreationsLocked(acknowledgedPlan, creations[:start], states)
			return states, err
		}
		if conflictID != "" && !skipExisting {
			m.publishGroupCreationsLocked(plan, creations[:end], states)
			return nil, errs.ErrAffinityGroupExist.GenWithStackByArgs(conflictID)
		}
		start = end
	}
	m.publishGroupCreationsLocked(plan, creations, states)
	return states, nil
}

func (m *Manager) reconcilePersistedGroupsLocked(reqCtx context.Context, changes []GroupKeyRanges, states map[string]*GroupState) error {
	creations := make([]groupCreation, len(changes))
	plan := m.regionLabeler.NewPlan()
	for start := 0; start < len(changes); start += maxAffinityTxnOps / 2 {
		end := min(start+maxAffinityTxnOps/2, len(changes))
		keys := make([]string, 0, end-start)
		for _, change := range changes[start:end] {
			keys = append(keys, keypath.AffinityGroupPath(change.GroupID))
		}
		if err := m.runAdminTxn(reqCtx, func(txn kv.Txn) error {
			if err := kv.Prefetch(txn, keys); err != nil {
				return err
			}
			return m.restoreGroupCreations(txn, changes[start:end], creations[start:end], plan)
		}); err != nil {
			return err
		}
	}
	m.publishGroupCreationsLocked(plan, creations, states)
	return nil
}

// restoreGroupCreations reads only. Target group keys must already be prefetched.
func (m *Manager) restoreGroupCreations(txn kv.Txn, changes []GroupKeyRanges, creations []groupCreation, plan *labeler.Plan) error {
	var labelKeys []string
	for i, change := range changes {
		value, _, err := m.storage.LoadAffinityGroupInTxn(txn, change.GroupID)
		if err != nil {
			return err
		}
		if value == "" {
			continue
		}
		create := &creations[i]
		create.group = &Group{}
		if err := json.Unmarshal([]byte(value), create.group); err != nil {
			return errors.WithStack(err)
		}
		create.recovered = true
		create.ranges.GroupID = change.GroupID
		labelKeys = append(labelKeys, keypath.RegionLabelKeyPath(GetLabelRuleID(change.GroupID)))
	}
	if err := kv.Prefetch(txn, labelKeys); err != nil {
		return err
	}
	for i := range creations {
		create := &creations[i]
		if !create.recovered {
			continue
		}
		id := GetLabelRuleID(create.ranges.GroupID)
		value, _, err := kv.LoadWithRevision(txn, keypath.RegionLabelKeyPath(id))
		if err != nil {
			return err
		}
		if value == "" {
			if err := plan.DeleteLabelRule(id); err != nil {
				return err
			}
			continue
		}
		create.rule = &labeler.LabelRule{}
		if err := json.Unmarshal([]byte(value), create.rule); err != nil {
			return errors.WithStack(err)
		}
		if err := plan.SetLabelRule(create.rule); err != nil {
			return err
		}
		create.ranges, err = extractKeyRangesFromLabelRule(create.rule)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) stageGroupCreation(txn kv.Txn, change GroupKeyRanges, create *groupCreation, plan *labeler.Plan) error {
	create.group = &Group{ID: change.GroupID, CreateTimestamp: uint64(time.Now().Unix())}
	create.ranges = change
	if err := m.AdjustGroup(create.group); err != nil {
		return err
	}
	if source := change.InheritPolicyFrom; source != "" {
		value, _, err := m.storage.LoadAffinityGroupInTxn(txn, source)
		if err != nil {
			return err
		}
		if value != "" {
			var sourceGroup Group
			if err := json.Unmarshal([]byte(value), &sourceGroup); err != nil {
				return errors.WithStack(err)
			}
			create.group.BalancePolicy = sourceGroup.BalancePolicy
		}
	}
	if len(change.KeyRanges) > 0 {
		create.rule = MakeLabelRule(change)
		if err := plan.SetLabelRule(create.rule); err != nil {
			return err
		}
	}
	return m.storage.SaveAffinityGroup(txn, change.GroupID, create.group)
}

// publishGroupCreationsLocked publishes only records from acknowledged transactions.
func (m *Manager) publishGroupCreationsLocked(plan *labeler.Plan, creations []groupCreation, states map[string]*GroupState) {
	plan.Apply()
	for _, create := range creations {
		if create.group == nil {
			continue
		}
		id := create.group.ID
		if len(create.ranges.KeyRanges) > 0 {
			m.keyRanges[id] = GroupKeyRanges{GroupID: id, KeyRanges: create.ranges.KeyRanges}
		} else {
			delete(m.keyRanges, id)
		}
		log.Info("affinity group added", zap.String("group", create.group.String()))
	}
	m.Lock()
	defer m.Unlock()
	for _, create := range creations {
		if create.group != nil {
			id := create.group.ID
			m.initGroupLocked(create.group)
			m.updateGroupLabelRuleLocked(id, create.rule, false)
			states[id] = newGroupState(m.groups[id])
		}
	}
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
	return m.DeleteAffinityGroupsWithContext(context.Background(), groupIDs, force)
}

// DeleteAffinityGroupsWithContext deletes groups and checks cancellation between transactions.
func (m *Manager) DeleteAffinityGroupsWithContext(reqCtx context.Context, groupIDs []string, force bool) error {
	if len(groupIDs) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("no group ids provided")
	}
	for _, groupID := range groupIDs {
		if err := ValidateExistingGroupID(groupID); err != nil {
			return err
		}
	}

	seen := make(map[string]struct{}, len(groupIDs))
	toDelete := make([]string, 0, len(groupIDs))

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return err
	}

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
	groupOps := make([]groupTxnOp, 0, len(toDelete))
	for i, id := range toDelete {
		groupID := id
		groupOps = append(groupOps, groupTxnOp{writes: 2, apply: func(txn kv.Txn) error {
			if err := labelOps[i](txn); err != nil {
				return err
			}
			return m.storage.DeleteAffinityGroup(txn, groupID)
		}})
	}

	// Step 5: Save the Group and Label information in storage.
	committed, err := m.runGroupOpsInTxn(reqCtx, groupOps)
	if err != nil {
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

func combineCtxErr(reqCtx, managerCtx context.Context) error {
	if reqCtx != nil {
		if err := reqCtx.Err(); err != nil {
			return err
		}
	}
	return managerCtx.Err()
}

// runAdminTxn checks for cancellation only before starting a transaction. Once
// started, only the manager context can cancel it, so a request timeout cannot
// interrupt the commit and prevent publishing a successful write in memory.
func (m *Manager) runAdminTxn(reqCtx context.Context, apply func(kv.Txn) error) error {
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return err
	}
	return m.storage.RunInTxn(m.ctx, apply)
}

type groupTxnOp struct {
	writes     int
	readKeys   []string
	extraReads int
	apply      func(kv.Txn) error
}

func groupTxnChunkSize(ops []groupTxnOp) int {
	var writes, reads int
	for i, op := range ops {
		writes += op.writes
		reads += len(op.readKeys) + op.extraReads
		if writes > maxAffinityTxnOps || reads > maxAffinityTxnOps {
			return i
		}
	}
	return len(ops)
}

func groupTxnReadKeys(ops []groupTxnOp) []string {
	var keys []string
	for _, op := range ops {
		keys = append(keys, op.readKeys...)
	}
	return keys
}

// runGroupOpsInTxn keeps each group's writes and comparisons in one transaction.
// It returns the number of groups in the acknowledged prefix.
func (m *Manager) runGroupOpsInTxn(reqCtx context.Context, groupOps []groupTxnOp) (int, error) {
	for start := 0; start < len(groupOps); {
		end := start + groupTxnChunkSize(groupOps[start:])
		chunk := groupOps[start:end]
		if err := m.runAdminTxn(reqCtx, func(txn kv.Txn) error {
			if err := kv.Prefetch(txn, groupTxnReadKeys(chunk)); err != nil {
				return err
			}
			for _, op := range chunk {
				if err := op.apply(txn); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return start, err
		}
		start = end
	}
	return len(groupOps), nil
}

// UpdateAffinityGroupPeers updates the leader and voter stores of an affinity group
// and resets target availability using cached store conditions. Before the first
// store scan, it checks the candidate stores directly and marks the group
// available on success.
func (m *Manager) UpdateAffinityGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	return m.UpdateAffinityGroupPeersWithContext(context.Background(), groupID, leaderStoreID, voterStoreIDs)
}

// UpdateAffinityGroupPeersWithContext updates the group's target if the request is still active.
func (m *Manager) UpdateAffinityGroupPeersWithContext(reqCtx context.Context, groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	return m.updateGroupPeers(reqCtx, groupID, nil, leaderStoreID, voterStoreIDs)
}

// updateAffinityGroupPeersWithState updates the leader and voter stores of an affinity group
// and resets target availability as described by UpdateAffinityGroupPeers.
// A nil observed state indicates an admin update. An observation must match the runtime
// group instance, affinity version, and watched revision. Its ranges must not conflict,
// and voter stores can change only when regular scheduling is allowed.
func (m *Manager) updateAffinityGroupPeersWithState(groupID string, observed *GroupState, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	return m.updateGroupPeers(context.Background(), groupID, observed, leaderStoreID, voterStoreIDs)
}

func (m *Manager) updateGroupPeers(reqCtx context.Context, groupID string, observed *GroupState, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
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
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return nil, err
	}
	group := m.GetAffinityGroupState(groupID)
	if group == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	voterStoreIDs = sortedStoreIDs(voterStoreIDs)
	if observed != nil {
		// A recreated group can reuse the observed version.
		if group.groupInfoPtr != observed.groupInfoPtr || group.affinityVer != observed.affinityVer ||
			group.modRevision != observed.modRevision || group.rangeConflict {
			return group, nil
		}
		// Changing only the leader is allowed while regular scheduling is disabled.
		// The VoterStoreIDs are already sorted, so they can be compared directly.
		if !group.RegularSchedulingAllowed && !slices.Equal(voterStoreIDs, group.VoterStoreIDs) {
			return group, nil
		}
		// Policy updates preserve affinityVer, so use the current policy under metaMutex.
		if group.BalancePolicy.IsFixed() && !group.RegularSchedulingAllowed && leaderStoreID != group.LeaderStoreID {
			return group, nil
		}
		if group.AffinitySchedulingAllowed && leaderStoreID != group.LeaderStoreID {
			return group, nil
		}
		if m.targetWriter != nil {
			observation := observedTarget{
				target: GroupTarget{
					ExpectedModRevision: observed.modRevision,
					LeaderStoreID:       leaderStoreID,
					VoterStoreIDs:       voterStoreIDs,
				},
				groupInfoPtr: observed.groupInfoPtr,
				affinityVer:  observed.affinityVer,
				persistOnly:  group.AffinitySchedulingAllowed && group.targetUnpersisted,
			}
			m.RLock()
			current := m.groups[groupID]
			if !observation.matches(current) {
				m.RUnlock()
				return m.GetAffinityGroupState(groupID), nil
			}
			conditionsKnown := m.storeConditionsKnown
			observation.target.Expected = current.storedGroup()
			observation.target.Expected.VoterStoreIDs = slices.Clone(observation.target.Expected.VoterStoreIDs)
			m.RUnlock()
			if !conditionsKnown && !group.targetUnpersisted && group.LeaderStoreID != 0 && len(group.VoterStoreIDs) != 0 {
				return group, nil
			}
			result := observation.target.result()
			matchesStored := sameStoredGroup(&observation.target.Expected, result)
			if !group.AffinitySchedulingAllowed && (matchesStored || sameStoredGroup(&group.Group, result)) {
				m.recoverLocalObservedTarget(&observation, matchesStored)
			}
			if !matchesStored {
				m.sendObservedTarget(observation)
			}
			return m.GetAffinityGroupState(groupID), nil
		}
		// Recheck the persisted target and policy before a classic first-win write.
		target := GroupTarget{Expected: group.Group, LeaderStoreID: leaderStoreID, VoterStoreIDs: voterStoreIDs}
		if _, _, err := m.updateGroupTargetLocked(m.ctx, target, true); err != nil {
			return nil, err
		}
		return m.GetAffinityGroupState(groupID), nil
	}

	// Step 2: Save the Group in storage.
	if err := m.runAdminTxn(reqCtx, func(txn kv.Txn) error {
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
	m.targetWriteMutex.Lock()
	defer m.targetWriteMutex.Unlock()
	m.Lock()
	defer m.Unlock()
	groupInfo := m.groups[groupID]
	groupInfo.lastSentWritten = false
	delete(m.sendingTargets, groupID)
	changed := m.syncGroupTargetLocked(groupInfo, &group.Group)
	groupInfo = m.groups[groupID]
	if !sameGroupTarget(&groupInfo.Group, &group.Group) {
		m.applyGroupTargetLocked(groupInfo, &group.Group)
		changed = true
	}
	if m.storeConditionsKnown {
		m.setTargetAvailabilityLocked(groupInfo)
	} else {
		groupInfo.SetAvailability(groupAvailable)
	}
	if !changed {
		m.resetCountLocked(groupInfo)
	}
	return newGroupState(groupInfo), nil
}

// UpdateAffinityGroupPolicy updates the balance policy while preserving the target.
func (m *Manager) UpdateAffinityGroupPolicy(groupID string, policy BalancePolicy) (*GroupState, error) {
	return m.UpdateAffinityGroupPolicyWithContext(context.Background(), groupID, policy)
}

// UpdateAffinityGroupPolicyWithContext updates one policy if the request is still active.
func (m *Manager) UpdateAffinityGroupPolicyWithContext(reqCtx context.Context, groupID string, policy BalancePolicy) (*GroupState, error) {
	states, err := m.UpdateAffinityGroupPoliciesWithContext(reqCtx, []string{groupID}, policy)
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
	return m.UpdateAffinityGroupPoliciesWithContext(context.Background(), groupIDs, policy)
}

// UpdateAffinityGroupPoliciesWithContext updates policies and checks cancellation between transactions.
func (m *Manager) UpdateAffinityGroupPoliciesWithContext(reqCtx context.Context, groupIDs []string, policy BalancePolicy) ([]*GroupState, error) {
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
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return nil, err
	}
	if err := m.allGroupsExist(ids); err != nil {
		return nil, err
	}
	states := make([]*GroupState, 0, len(ids))
	// Each group takes one comparison and at most one write in the transaction.
	for chunk := range slices.Chunk(ids, maxAffinityTxnOps) {
		updated, err := m.saveGroupPoliciesLocked(reqCtx, chunk, policy)
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
func (m *Manager) saveGroupPoliciesLocked(reqCtx context.Context, groupIDs []string, policy BalancePolicy) ([]*GroupState, error) {
	updated := make([]*Group, len(groupIDs))
	if err := m.runAdminTxn(reqCtx, func(txn kv.Txn) error {
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
	value, _, err := m.storage.LoadAffinityGroupInTxn(txn, groupID)
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

type plannedRangeUpdate struct {
	ranges    GroupKeyRanges
	labelRule *labeler.LabelRule
	isRemove  bool
}

// UpdateAffinityGroupKeyRanges batch modifies key ranges for multiple affinity groups.
// It validates the full request before writing the adds followed by the removes.
// A batch may span multiple transactions. Only acknowledged transactions are
// published in memory; an ambiguous commit error is reconciled on reload.
// Add validation checks existing ranges before applying removals, so moving a
// range between groups requires separate requests.
func (m *Manager) UpdateAffinityGroupKeyRanges(addOps, removeOps []GroupKeyRanges) error {
	return m.UpdateAffinityGroupKeyRangesWithContext(context.Background(), addOps, removeOps)
}

// UpdateAffinityGroupKeyRangesWithContext modifies ranges and checks cancellation between transactions.
func (m *Manager) UpdateAffinityGroupKeyRangesWithContext(reqCtx context.Context, addOps, removeOps []GroupKeyRanges) error {
	// Validate IDs for the full batch, including empty operations.
	for _, op := range addOps {
		if err := ValidateGroupID(op.GroupID); err != nil {
			return err
		}
	}
	for _, op := range removeOps {
		if err := ValidateExistingGroupID(op.GroupID); err != nil {
			return err
		}
	}

	// Each group can have at most one nonempty operation.
	seenGroups := make(map[string]struct{}, len(addOps)+len(removeOps))
	for _, ops := range [][]GroupKeyRanges{addOps, removeOps} {
		for _, op := range ops {
			if len(op.KeyRanges) == 0 {
				continue
			}
			if _, exists := seenGroups[op.GroupID]; exists {
				return errs.ErrAffinityGroupExist.GenWithStackByArgs(op.GroupID)
			}
			seenGroups[op.GroupID] = struct{}{}
		}
	}
	if len(seenGroups) == 0 {
		return nil
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := combineCtxErr(reqCtx, m.ctx); err != nil {
		return err
	}

	planned := make([]plannedRangeUpdate, 0, len(seenGroups))
	for _, op := range addOps {
		if len(op.KeyRanges) == 0 {
			continue
		}
		current, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}
		current.KeyRanges = slices.Concat(current.KeyRanges, op.KeyRanges)
		planned = append(planned, plannedRangeUpdate{ranges: current})
	}
	if len(planned) > 0 {
		if err := m.validateNoOverlapLocked(addOps); err != nil {
			return err
		}
	}
	for _, op := range removeOps {
		if len(op.KeyRanges) == 0 {
			continue
		}
		current, err := m.getCurrentRanges(op.GroupID)
		if err != nil {
			return err
		}
		remaining, err := applyRemoveOps(current, op)
		if err != nil {
			return err
		}
		planned = append(planned, plannedRangeUpdate{ranges: remaining, isRemove: true})
	}

	plan := m.regionLabeler.NewPlan()
	for i := range planned {
		item := &planned[i]
		// Removals may leave legacy ranges in place for incremental repair.
		if !item.isRemove {
			if err := validateGroupKeyRanges(item.ranges); err != nil {
				return err
			}
		}
		if len(item.ranges.KeyRanges) > 0 {
			item.labelRule = MakeLabelRule(item.ranges)
			if err := plan.SetLabelRule(item.labelRule); err != nil {
				return err
			}
		} else if err := plan.DeleteLabelRule(GetLabelRuleID(item.ranges.GroupID)); err != nil {
			return err
		}
	}

	// Each planned update writes one label rule in request order.
	groupOps := make([]groupTxnOp, 0, len(planned))
	for _, op := range plan.CommitOps() {
		groupOps = append(groupOps, groupTxnOp{writes: 1, apply: op})
	}
	committed, err := m.runGroupOpsInTxn(reqCtx, groupOps)
	if committed == 0 {
		return err
	}
	plan.ApplyPrefix(committed)
	planned = planned[:committed]
	for _, item := range planned {
		if len(item.ranges.KeyRanges) > 0 {
			m.keyRanges[item.ranges.GroupID] = item.ranges
		} else {
			delete(m.keyRanges, item.ranges.GroupID)
		}
	}

	var conflicts map[string]struct{}
	if m.hasRangeConflicts {
		conflicts = m.collectRangeConflictsLocked()
	}
	m.Lock()
	defer m.Unlock()
	for _, item := range planned {
		m.updateGroupLabelRuleLocked(item.ranges.GroupID, item.labelRule, item.isRemove)
	}
	if m.hasRangeConflicts {
		m.reconcileRangeConflictsLocked(conflicts)
	}
	return err
}

// getCurrentRanges retrieves the current key ranges for a group.
func (m *Manager) getCurrentRanges(groupID string) (GroupKeyRanges, error) {
	if !m.IsGroupExist(groupID) {
		return GroupKeyRanges{}, errs.ErrAffinityGroupNotFound.FastGenByArgs(groupID)
	}
	// Try cache first
	if gkr, ok := m.keyRanges[groupID]; ok {
		return gkr, nil
	}

	// Parse from label rule
	labelRule := m.regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
	if labelRule == nil {
		// Allow starting from empty when the group exists but has no ranges yet.
		return GroupKeyRanges{GroupID: groupID}, nil
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
// logs the groups that start or stop conflicting, at the warn level if any
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
		log.Warn("overlapping affinity key ranges; delete or update conflicting groups to restore affinity scheduling",
			zap.Strings("newly-conflicting-group-ids", conflicting),
			zap.Strings("resolved-group-ids", resolved))
	} else if len(resolved) > 0 {
		log.Info("affinity key range conflicts resolved", zap.Strings("resolved-group-ids", resolved))
	}
}

// loadRegionLabel rebuilds the mapping between groups and label rules after restart.
// Both metaMutex and the manager write lock must be held.
func (m *Manager) loadRegionLabel() {
	totalRanges := 0
	var leftoverGroupIDs []string

	m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
		if !ok {
			// Not an affinity label rule, skip
			return true
		}

		if _, exists := m.groups[groupID]; !exists {
			leftoverGroupIDs = append(leftoverGroupIDs, groupID)
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
			totalRanges += len(gkr.KeyRanges)
			// Populate in-memory key ranges cache
			m.keyRanges[groupID] = gkr
		}

		// Associate the label rule with the group
		m.updateGroupLabelRuleLocked(groupID, rule, false)

		return true
	})

	if len(leftoverGroupIDs) > 0 {
		// Old orphan rules can still split Regions and block merges even though
		// they are excluded from overlap checks.
		slices.Sort(leftoverGroupIDs)
		log.Warn("found affinity label rules without affinity groups, delete each with pd-ctl config affinity delete --force",
			zap.Int("count", len(leftoverGroupIDs)),
			zap.Strings("group-ids", leftoverGroupIDs[:min(len(leftoverGroupIDs), maxLoggedLeftoverGroupIDs)]))
	}

	m.reconcileRangeConflictsLocked(m.collectRangeConflictsLocked())

	log.Info("rebuilt group-label mapping",
		zap.Int("total-groups", len(m.keyRanges)),
		zap.Int("total-ranges", totalRanges))
}
