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

package affinity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

const manualBalanceBuildPageSize = 256

const manualBalancePlanningTimeout = 30 * time.Second
const manualBalanceExecutionTimeout = 30 * time.Minute

// BalanceApplyResult preserves per-group execution state when apply stops.
// Submitted groups may still be converging; applied groups have converged.
type BalanceApplyResult struct {
	Plans []BalancePlan `json:"plans"`
	Error string        `json:"error"`
}

func (m *Manager) rebalanceTable(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	executionCtx, cancelExecution := context.WithTimeout(ctx, manualBalanceExecutionTimeout)
	stopExecutionManagerContext := context.AfterFunc(m.ctx, cancelExecution)
	defer stopExecutionManagerContext()
	defer cancelExecution()
	planningCtx, cancelPlanning := context.WithTimeout(ctx, manualBalancePlanningTimeout)
	stopPlanningManagerContext := context.AfterFunc(m.ctx, cancelPlanning)
	defer stopPlanningManagerContext()
	defer cancelPlanning()
	if err := planningCtx.Err(); err != nil {
		return nil, err
	}
	if m.GetAffinityGroupState(groupID) == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	tableID := TableGroupPrefix(groupID)
	tableLock := m.acquireBalanceTableLock(tableID)
	if !tableLock.TryLock() {
		m.releaseBalanceTableLock(tableID, tableLock)
		return nil, ErrBalanceBusy
	}
	defer m.releaseBalanceTableLock(tableID, tableLock)
	defer tableLock.Unlock()

	snapshot := m.newBalanceSnapshot(groupID, true)
	if snapshot == nil {
		return nil, ErrBalanceNotReady
	}
	groups := make([]*Group, 0, len(snapshot.groups))
	for _, group := range snapshot.groups {
		groups = append(groups, &group.Group)
	}
	autoGroups := snapshot.autoGroups
	autoByID := make(map[string]*GroupState, len(autoGroups))
	for _, group := range autoGroups {
		autoByID[group.ID] = group
		if group.Phase != PhaseStable || !group.AffinitySchedulingAllowed {
			return nil, ErrBalanceNotReady
		}
	}
	if len(autoGroups) > 0 && len(snapshot.stores) == 0 {
		return nil, ErrBalanceNotReady
	}

	// Scan every automatic group's ranges before capturing durable dependencies.
	// This request-local snapshot leaves automatic worker state untouched.
	actions := make(map[string]BalanceActionSet, len(autoGroups))
	blockedGroups := make(map[string]struct{})
	for _, group := range autoGroups {
		built, ready, err := m.buildManualBalanceActions(planningCtx, snapshot, group, &group.Group, blockedGroups)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, ErrBalanceNotReady
		}
		actions[group.ID] = built
	}
	if err := m.captureBalanceDependencies(planningCtx, snapshot); err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, ErrBalanceConflict
		}
		return nil, err
	}

	var rebuildErr error
	plans, err := PlanTableRebalance(planningCtx, groups, BalanceOptions{
		Stores: snapshot.stores, ActionSets: actions,
		GlobalLeaderCounts: snapshot.globalLeaders, GlobalVoterCounts: snapshot.globalVoters,
	}, func(buildCtx context.Context, original, virtual *Group) (BalanceActionSet, bool) {
		state := autoByID[original.ID]
		if state == nil {
			rebuildErr = errors.New("manual balance target has no table group state")
			return BalanceActionSet{}, false
		}
		built, ready, buildErr := m.buildManualBalanceActions(buildCtx, snapshot, state, virtual, blockedGroups)
		if buildErr != nil {
			rebuildErr = buildErr
		}
		return built, ready
	})
	if rebuildErr != nil {
		return nil, rebuildErr
	}
	if err != nil {
		return nil, err
	}
	if err := planningCtx.Err(); err != nil {
		return nil, err
	}

	changed := false
	for _, plan := range plans {
		if !plan.Changed {
			continue
		}
		changed = true
		group := autoByID[plan.GroupID]
		if group == nil || group.BalancePolicy == BalancePolicyFixed {
			return nil, ErrBalanceConflict
		}
		candidate := group.Group
		candidate.LeaderStoreID = plan.LeaderStoreID
		candidate.VoterStoreIDs = slices.Clone(plan.VoterStoreIDs)
		if err := m.AdjustGroup(&candidate); err != nil {
			return nil, err
		}
		if err := m.checkBalanceTargetStores(candidate.LeaderStoreID, candidate.VoterStoreIDs); err != nil {
			return nil, ErrBalanceConflict
		}
	}
	if !changed && len(blockedGroups) > 0 {
		for i := range plans {
			if _, blocked := blockedGroups[plans[i].GroupID]; blocked {
				plans[i].Status = "temporarily_blocked"
				plans[i].Reason = "some balance candidates are temporarily unavailable"
			}
		}
		return plans, ErrBalanceNotReady
	}

	current, err := m.balanceDependenciesCurrent(planningCtx, snapshot)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, ErrBalanceConflict
	}
	revision, err := manualBalanceRevision(snapshot)
	if err != nil {
		return nil, err
	}
	for i := range plans {
		plans[i].Revision = revision
	}
	if !changed {
		return plans, nil
	}
	if !apply {
		for _, plan := range plans {
			if !plan.Changed {
				continue
			}
			if err := validateManualBalanceTarget(planningCtx, snapshot, autoByID[plan.GroupID], planTarget(plan)); err != nil {
				return nil, err
			}
		}
		return plans, nil
	}
	return m.applyManualBalancePlans(executionCtx, snapshot, plans)
}

func manualBalanceRevision(snapshot *balanceSnapshot) (string, error) {
	revision := struct {
		Table    string `json:"table"`
		Metadata string `json:"metadata"`
	}{
		Table:    snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.tableID)],
		Metadata: snapshot.dependencies[endpoint.AffinityTableMetadataRevisionKey(snapshot.tableID)],
	}
	encoded, err := json.Marshal(revision)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (m *Manager) applyManualBalancePlans(ctx context.Context, snapshot *balanceSnapshot, plans []BalancePlan) ([]BalancePlan, error) {
	changed := make([]int, 0, len(plans))
	for i := range plans {
		if plans[i].Changed {
			plans[i].Status = "not_submitted"
			changed = append(changed, i)
		}
	}
	batchSize := max(1, endpoint.MaxAffinityTxnBatchOps)
	submitted := 0
	for start := 0; start < len(changed); start += batchSize {
		end := min(start+batchSize, len(changed))
		batch := make([]BalancePlan, 0, end-start)
		for _, index := range changed[start:end] {
			batch = append(batch, plans[index])
		}
		fail := func(err error, index int) ([]BalancePlan, error) {
			plans[index].Reason = err.Error()
			return plans, manualBalancePartialError(err, submitted)
		}
		if err := ctx.Err(); err != nil {
			return fail(err, changed[start])
		}
		current, err := m.balanceDependenciesCurrent(ctx, snapshot)
		if err != nil {
			return fail(err, changed[start])
		}
		if !current || !m.manualBalanceTargetsCurrent(snapshot) {
			return fail(ErrBalanceConflict, changed[start])
		}
		for _, index := range changed[start:end] {
			group := findManualBalanceGroup(snapshot.groups, plans[index].GroupID)
			if err := validateManualBalanceTarget(ctx, snapshot, group, planTarget(plans[index])); err != nil {
				return fail(err, index)
			}
		}

		committed, err := m.commitBalancePlans(ctx, snapshot, batch)
		if err != nil {
			for _, index := range changed[start:end] {
				plans[index].Status = "unknown"
				plans[index].Reason = err.Error()
			}
			return plans, manualBalancePartialError(err, submitted)
		}
		if !committed {
			return fail(ErrBalanceConflict, changed[start])
		}
		for _, index := range changed[start:end] {
			plans[index].Status = "submitted"
		}
		submitted += len(batch)
	}

	if err := m.waitManualBalanceConvergence(ctx, snapshot, plans); err != nil {
		return plans, manualBalancePartialError(err, submitted)
	}
	return plans, nil
}

func planTarget(plan BalancePlan) *Group {
	return &Group{ID: plan.GroupID, LeaderStoreID: plan.LeaderStoreID, VoterStoreIDs: slices.Clone(plan.VoterStoreIDs)}
}

func findManualBalanceGroup(groups []*GroupState, groupID string) *GroupState {
	for _, group := range groups {
		if group != nil && group.ID == groupID {
			return group
		}
	}
	return nil
}

func validateManualBalanceTarget(ctx context.Context, snapshot *balanceSnapshot, actual *GroupState, target *Group) error {
	if actual == nil || target == nil {
		return ErrBalanceConflict
	}
	page, ready, err := buildManualBalanceActionMode(ctx, snapshot, actual, target, BalanceValidateTarget, nil)
	if err != nil {
		return err
	}
	if page.InvalidTarget {
		return ErrBalanceConflict
	}
	if !ready {
		return ErrBalanceNotReady
	}
	return nil
}

func (m *Manager) waitManualBalanceConvergence(ctx context.Context, snapshot *balanceSnapshot, plans []BalancePlan) error {
	changed := make([]int, 0, len(plans))
	for i := range plans {
		if plans[i].Changed {
			changed = append(changed, i)
		}
	}
	cursors := make(map[string]any, len(changed))
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	next := 0
	failPending := func(err error) error {
		for _, index := range changed {
			if plans[index].Status == "submitted" {
				plans[index].Reason = err.Error()
				break
			}
		}
		return err
	}
	for remaining := len(changed); remaining > 0; {
		current, err := m.balanceDependenciesCurrent(ctx, snapshot)
		if err != nil {
			return failPending(err)
		}
		if !current || !m.manualBalanceTargetsCurrent(snapshot) {
			return failPending(ErrBalanceConflict)
		}
		progress := false
		for step := range changed {
			index := changed[(next+step)%len(changed)]
			if plans[index].Status == "applied" {
				continue
			}
			// Recheck before each group so a range or target change observed
			// while an earlier group was being inspected stops the same pass.
			// This keeps the result genuinely partial instead of marking later
			// groups converged against an obsolete range snapshot.
			current, err := m.balanceDependenciesCurrent(ctx, snapshot)
			if err != nil {
				return failPending(err)
			}
			if !current || !m.manualBalanceTargetsCurrent(snapshot) {
				return failPending(ErrBalanceConflict)
			}
			fail := func(err error) error {
				plans[index].Reason = err.Error()
				return err
			}
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			plan := plans[index]
			actual := m.GetAffinityGroupState(plan.GroupID)
			if actual == nil {
				return fail(ErrBalanceConflict)
			}
			cursor := cursors[plan.GroupID]
			page := snapshot.builder(ctx, actual, snapshot.ranges[plan.GroupID], planTarget(plan), snapshot.stores,
				BalanceCheckConvergence, cursor, manualBalanceBuildPageSize)
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			if page.Checks < 0 || page.Checks > manualBalanceBuildPageSize {
				return fail(errors.New("manual balance convergence checker returned an invalid check count"))
			}
			if page.InvalidTarget {
				return fail(ErrBalanceConflict)
			}
			if page.BlockTable {
				delete(cursors, plan.GroupID)
				continue
			}
			if !page.Ready {
				delete(cursors, plan.GroupID)
				continue
			}
			if page.Complete {
				plans[index].Status = "applied"
				delete(cursors, plan.GroupID)
				remaining--
				progress = true
				continue
			}
			if page.Checks == 0 || page.Cursor == nil {
				return fail(errors.New("manual balance convergence checker made no paging progress"))
			}
			cursors[plan.GroupID] = page.Cursor
			progress = true
		}
		if remaining == 0 {
			current, err := m.balanceDependenciesCurrent(ctx, snapshot)
			if err != nil {
				return failPending(err)
			}
			if !current || !m.manualBalanceTargetsCurrent(snapshot) {
				return failPending(ErrBalanceConflict)
			}
			return nil
		}
		next = (next + 1) % len(changed)
		if progress {
			continue
		}
		select {
		case <-ctx.Done():
			return failPending(ctx.Err())
		case <-m.ctx.Done():
			return failPending(context.Canceled)
		case <-ticker.C:
		}
	}
	return nil
}

func (m *Manager) manualBalanceTargetsCurrent(snapshot *balanceSnapshot) bool {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.RLock()
	defer m.RUnlock()
	for _, expected := range snapshot.groups {
		actual := m.groups[expected.ID]
		if actual == nil || actual.LeaderStoreID != expected.LeaderStoreID ||
			!slices.Equal(normalizeVoters(actual.VoterStoreIDs), normalizeVoters(expected.VoterStoreIDs)) ||
			!balanceRangesEqual(m.keyRanges[expected.ID], snapshot.ranges[expected.ID]) {
			return false
		}
	}
	return true
}

func manualBalancePartialError(err error, submitted int) error {
	if submitted == 0 {
		return err
	}
	return fmt.Errorf("%w: already committed %d groups", err, submitted)
}

func (m *Manager) buildManualBalanceActions(ctx context.Context, snapshot *balanceSnapshot,
	actual *GroupState, target *Group, blockedGroups map[string]struct{}) (BalanceActionSet, bool, error) {
	leaders, ready, err := buildManualBalanceActionMode(ctx, snapshot, actual, target, BalanceBuildLeaders, blockedGroups)
	if err != nil || !ready {
		return BalanceActionSet{}, ready, err
	}
	if leaders.InvalidTarget {
		return BalanceActionSet{}, false, ErrBalanceConflict
	}
	voters, ready, err := buildManualBalanceActionMode(ctx, snapshot, actual, target, BalanceBuildVoters, blockedGroups)
	if err != nil || !ready {
		return BalanceActionSet{}, ready, err
	}
	if voters.InvalidTarget {
		return BalanceActionSet{}, false, ErrBalanceConflict
	}
	voters.Actions.LeaderTargets = slices.Clone(leaders.Actions.LeaderTargets)
	return voters.Actions, true, nil
}

func buildManualBalanceActionMode(ctx context.Context, snapshot *balanceSnapshot, actual *GroupState,
	target *Group, mode BalanceActionMode, blockedGroups map[string]struct{}) (BalanceActionBuildResult, bool, error) {
	var cursor any
	for {
		if err := ctx.Err(); err != nil {
			return BalanceActionBuildResult{}, false, err
		}
		page := snapshot.builder(ctx, actual, snapshot.ranges[actual.ID], target, snapshot.stores, mode, cursor, manualBalanceBuildPageSize)
		if err := ctx.Err(); err != nil {
			return BalanceActionBuildResult{}, false, err
		}
		if page.Checks < 0 || page.Checks > manualBalanceBuildPageSize {
			return BalanceActionBuildResult{}, false, errors.New("manual balance action builder returned an invalid check count")
		}
		if page.InvalidTarget {
			return page, true, nil
		}
		if page.BlockTable || !page.Ready {
			return page, false, nil
		}
		if page.TemporaryBlocked && blockedGroups != nil {
			blockedGroups[actual.ID] = struct{}{}
		}
		if page.Complete {
			return page, true, nil
		}
		if page.Checks == 0 || page.Cursor == nil {
			return BalanceActionBuildResult{}, false, errors.New("manual balance action builder made no paging progress")
		}
		cursor = page.Cursor
	}
}

func (m *Manager) balanceDependenciesCurrent(ctx context.Context, snapshot *balanceSnapshot) (bool, error) {
	m.RLock()
	ruleRevision := m.balanceRuleRevision
	m.RUnlock()
	if ruleRevision != nil && ruleRevision() != snapshot.ruleRevision {
		return false, nil
	}
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		for key, expected := range snapshot.dependencies {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			if value != expected {
				return errs.ErrEtcdTxnConflict.FastGenByArgs()
			}
		}
		return nil
	})
	if err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return false, nil
		}
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}
