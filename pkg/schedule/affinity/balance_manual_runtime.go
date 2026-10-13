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
	"strings"
	"time"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

const manualBalanceBuildPageSize = 256

// Manual table rebalancing builds every group's legal actions before it can
// publish one complete final plan. Keep an unbounded request context from
// holding the table lock forever while still allowing normal-sized tables to
// finish in one request.
const manualBalancePlanningTimeout = 30 * time.Second

// Execution has a separate limit because Region migrations can outlive planning.
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
	planningCtx, cancel := context.WithTimeout(ctx, manualBalancePlanningTimeout)
	stopManagerContext := context.AfterFunc(m.ctx, cancel)
	defer stopManagerContext()
	defer cancel()
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
	for {
		ready, _, pending := m.checkBalanceConvergence(planningCtx, tableID, manualBalanceBuildPageSize)
		if err := planningCtx.Err(); err != nil {
			return nil, err
		}
		if ready {
			break
		}
		if !pending {
			return nil, ErrBalanceNotReady
		}
	}

	var (
		snapshot       *balanceSnapshot
		wasStable      bool
		restoreStable  bool
		originalFinger balanceFingerprint
	)
	m.refreshBalanceStores()
	m.Lock()
	if state := m.balanceTables[tableID]; state != nil {
		originalFinger = m.balanceFingerprintLocked(state)
		wasStable = state.phase == balanceStable && state.fingerprint == originalFinger
		if wasStable {
			state.phase = balanceIdle
		}
	}
	m.Unlock()
	defer func() {
		if snapshot != nil {
			m.releaseBalanceDependencies(snapshot)
		}
		if !wasStable || !restoreStable {
			return
		}
		m.Lock()
		if state := m.balanceTables[tableID]; state != nil && state.phase == balanceIdle &&
			m.balanceFingerprintLocked(state) == originalFinger {
			state.phase = balanceStable
			state.fingerprint = originalFinger
		}
		m.Unlock()
	}()

	snapshot = m.balanceSnapshot(groupID)
	if snapshot == nil {
		return nil, ErrBalanceNotReady
	}
	if snapshot.virtual == nil {
		return nil, errors.New("virtual balance action builder is not configured")
	}
	if err := m.captureBalanceDependencies(planningCtx, snapshot); err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, ErrBalanceConflict
		}
		return nil, err
	}
	if !m.balanceSnapshotCurrent(snapshot) {
		return nil, ErrBalanceConflict
	}

	slices.SortFunc(snapshot.groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	groups := make([]*Group, 0, len(snapshot.groups))
	autoGroups := make([]*GroupState, 0, len(snapshot.groups))
	groupStates := make(map[string]*GroupState, len(snapshot.groups))
	var selected *GroupState
	for _, group := range snapshot.groups {
		groupStates[group.ID] = group
		groups = append(groups, &group.Group)
		if group.ID == groupID {
			selected = group
		}
		if group.BalancePolicy == BalancePolicyFixed {
			continue
		}
		if group.Phase != PhaseStable || !group.AffinitySchedulingAllowed {
			return nil, ErrBalanceNotReady
		}
		autoGroups = append(autoGroups, group)
	}
	if selected == nil {
		return nil, ErrBalanceConflict
	}
	if len(autoGroups) == 0 {
		current, err := m.balanceDependenciesCurrent(planningCtx, snapshot)
		if err != nil {
			return nil, err
		}
		if !current || !m.balanceSnapshotCurrent(snapshot) {
			return nil, ErrBalanceConflict
		}
		plans := unchangedManualBalancePlans(groups)
		revision, err := manualBalanceRevision(snapshot)
		if err != nil {
			return nil, err
		}
		for i := range plans {
			plans[i].Revision = revision
		}
		m.Lock()
		if state := m.balanceTables[tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
			state.phase = balanceStable
			state.fingerprint = snapshot.fingerprint
		}
		m.Unlock()
		return plans, nil
	}
	if len(snapshot.stores) == 0 {
		return nil, ErrBalanceNotReady
	}

	actions := make(map[string]BalanceActionSet, len(autoGroups))
	for _, group := range autoGroups {
		built, ready, err := m.buildManualBalanceActions(planningCtx, snapshot, group, &group.Group)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, ErrBalanceNotReady
		}
		actions[group.ID] = built
	}
	var rebuildErr error
	plans, err := PlanTableRebalance(planningCtx, groups, BalanceOptions{
		Stores: snapshot.stores, ActionSets: actions,
		GlobalLeaderCounts: snapshot.globalLeaders, GlobalVoterCounts: snapshot.globalVoters,
	}, func(buildCtx context.Context, original, virtual *Group) (BalanceActionSet, bool) {
		state := groupStates[original.ID]
		if state == nil {
			rebuildErr = errors.New("virtual balance target has no table group state")
			return BalanceActionSet{}, false
		}
		built, ready, buildErr := m.buildManualBalanceActions(buildCtx, snapshot, state, virtual)
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
		group := groupStates[plan.GroupID]
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
		if snapshot.validator != nil {
			ready, legal := snapshot.validator(planningCtx, group, snapshot.ranges[group.ID], plan)
			if !ready {
				return nil, ErrBalanceNotReady
			}
			if !legal {
				return nil, ErrBalanceConflict
			}
		}
	}
	if !changed && len(snapshot.blockedGroups) > 0 {
		for i := range plans {
			if _, blocked := snapshot.blockedGroups[plans[i].GroupID]; blocked {
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
	if err := planningCtx.Err(); err != nil {
		return nil, err
	}
	if !current || !m.balanceSnapshotCurrent(snapshot) {
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
		// A reconstruction with no better final layout does not prove that
		// the current layout has no improving automatic single-step action.
		restoreStable = true
		return plans, nil
	}
	if !apply {
		restoreStable = true
		return plans, nil
	}
	if snapshot.validator == nil {
		return nil, ErrBalanceNotReady
	}
	expectedGroups := make(map[string]Group, len(snapshot.groups))
	for _, group := range snapshot.groups {
		expectedGroups[group.ID] = group.Group
	}
	committedGroups := 0
	for i := range plans {
		if plans[i].Changed {
			plans[i].Status = "not_submitted"
		}
	}
	fail := func(err error, index int) ([]BalancePlan, error) {
		plans[index].Reason = err.Error()
		return plans, manualBalancePartialError(err, committedGroups)
	}
	for planIndex, plan := range plans {
		if !plan.Changed {
			continue
		}
		if err := executionCtx.Err(); err != nil {
			return fail(err, planIndex)
		}
		current := snapshot
		if committedGroups != 0 {
			current, err = m.nextManualBalanceSnapshot(executionCtx, groupID, snapshot, expectedGroups)
			if err != nil {
				return fail(err, planIndex)
			}
		}
		group := findManualBalanceGroup(current.groups, plan.GroupID)
		if group == nil {
			if current != snapshot {
				m.releaseBalanceDependencies(current)
			}
			return fail(ErrBalanceConflict, planIndex)
		}
		ready, legal := current.validator(executionCtx, group, current.ranges[plan.GroupID], plan)
		if err := executionCtx.Err(); err != nil {
			if current != snapshot {
				m.releaseBalanceDependencies(current)
			}
			return fail(err, planIndex)
		}
		if !ready || !legal {
			if current != snapshot {
				m.releaseBalanceDependencies(current)
			}
			reason := ErrBalanceConflict
			if !ready {
				reason = ErrBalanceNotReady
			}
			return fail(reason, planIndex)
		}
		committed, commitErr := m.commitBalancePlans(executionCtx, current, []BalancePlan{plan})
		if commitErr != nil {
			plans[planIndex].Status = "unknown"
			return fail(commitErr, planIndex)
		}
		if !committed {
			return fail(ErrBalanceConflict, planIndex)
		}
		committedGroups++
		plans[planIndex].Status = "submitted"
		expected := expectedGroups[plan.GroupID]
		expected.LeaderStoreID = plan.LeaderStoreID
		expected.VoterStoreIDs = normalizeVoters(plan.VoterStoreIDs)
		expectedGroups[plan.GroupID] = expected
		// Capture our new durable table generation before waiting. Metadata
		// must still match the original plan, including its range generation.
		waitSnapshot, err := m.nextManualBalanceSnapshot(executionCtx, groupID, snapshot, expectedGroups)
		if err != nil {
			return fail(err, planIndex)
		}
		waitErr := m.waitManualBalanceConvergence(executionCtx, waitSnapshot, plan)
		m.releaseBalanceDependencies(waitSnapshot)
		if waitErr != nil {
			return fail(waitErr, planIndex)
		}
		plans[planIndex].Status = "applied"
	}
	return plans, nil
}

func unchangedManualBalancePlans(groups []*Group) []BalancePlan {
	original := make(map[string]*Group, len(groups))
	for _, group := range groups {
		original[group.ID] = group
	}
	leaders, voters := countsForGroups(groups)
	score := calcBalanceScore(leaders, voters)
	return finalBalancePlans(original, groups, score, score, nil, false)
}

func manualBalanceRevision(snapshot *balanceSnapshot) (string, error) {
	revision := struct {
		Table    string `json:"table"`
		Metadata string `json:"metadata"`
	}{
		Table:    snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.tableID)],
		Metadata: snapshot.dependencies[endpoint.AffinityMetadataRevisionKey()],
	}
	encoded, err := json.Marshal(revision)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (m *Manager) nextManualBalanceSnapshot(ctx context.Context, groupID string, original *balanceSnapshot,
	expectedGroups map[string]Group) (*balanceSnapshot, error) {
	current := m.balanceSnapshot(groupID)
	if current == nil {
		return nil, ErrBalanceConflict
	}
	if err := m.captureBalanceDependencies(ctx, current); err != nil {
		m.releaseBalanceDependencies(current)
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, ErrBalanceConflict
		}
		return nil, err
	}
	metadataKey := endpoint.AffinityMetadataRevisionKey()
	if current.fingerprint.stores != original.fingerprint.stores || current.fingerprint.rules != original.fingerprint.rules ||
		current.dependencies[metadataKey] != original.dependencies[metadataKey] || !manualBalanceTargetsMatch(current.groups, expectedGroups) {
		m.releaseBalanceDependencies(current)
		return nil, ErrBalanceConflict
	}
	current.conditions = slices.Clone(original.conditions)
	if !m.balanceSnapshotCurrent(current) {
		m.releaseBalanceDependencies(current)
		return nil, ErrBalanceConflict
	}
	return current, nil
}

func manualBalanceTargetsMatch(groups []*GroupState, expected map[string]Group) bool {
	if len(groups) != len(expected) {
		return false
	}
	for _, group := range groups {
		want, ok := expected[group.ID]
		if !ok || group.String() != want.String() {
			return false
		}
	}
	return true
}

func findManualBalanceGroup(groups []*GroupState, groupID string) *GroupState {
	for _, group := range groups {
		if group.ID == groupID {
			return group
		}
	}
	return nil
}

func (m *Manager) waitManualBalanceConvergence(ctx context.Context, snapshot *balanceSnapshot, plan BalancePlan) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := m.balancePersistentDependenciesCurrent(ctx, snapshot)
		if err != nil {
			return err
		}
		if !current {
			return ErrBalanceConflict
		}
		group := m.GetAffinityGroupState(plan.GroupID)
		if group == nil || group.LeaderStoreID != plan.LeaderStoreID ||
			!slices.Equal(normalizeVoters(group.VoterStoreIDs), normalizeVoters(plan.VoterStoreIDs)) {
			return ErrBalanceConflict
		}
		ready, legal := snapshot.validator(ctx, group, snapshot.ranges[plan.GroupID], plan)
		if err := ctx.Err(); err != nil {
			return err
		}
		if ready && legal {
			current, err := m.balancePersistentDependenciesCurrent(ctx, snapshot)
			if err != nil {
				return err
			}
			if !current {
				return ErrBalanceConflict
			}
			if current := m.GetAffinityGroupState(plan.GroupID); current != nil && current.Phase == PhaseStable {
				m.Lock()
				if state := m.balanceTables[snapshot.tableID]; state != nil {
					state.converging = nil
					state.phase = balanceIdle
				}
				m.Unlock()
				return nil
			}
		}
		if ready && !legal {
			return ErrBalanceConflict
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.ctx.Done():
			return context.Canceled
		case <-ticker.C:
		}
	}
}

func manualBalancePartialError(err error, committed int) error {
	if committed == 0 {
		return err
	}
	return fmt.Errorf("%w: already committed %d groups", err, committed)
}

func (*Manager) buildManualBalanceActions(ctx context.Context, snapshot *balanceSnapshot,
	actual *GroupState, target *Group) (BalanceActionSet, bool, error) {
	leaders, ready, err := buildManualBalanceActionMode(ctx, snapshot, actual, target, BalanceBuildLeaders)
	if err != nil || !ready {
		return BalanceActionSet{}, ready, err
	}
	voters, ready, err := buildManualBalanceActionMode(ctx, snapshot, actual, target, BalanceBuildVoters)
	if err != nil || !ready {
		return BalanceActionSet{}, ready, err
	}
	voters.Actions.LeaderTargets = slices.Clone(leaders.Actions.LeaderTargets)
	return voters.Actions, true, nil
}

func buildManualBalanceActionMode(ctx context.Context, snapshot *balanceSnapshot, actual *GroupState,
	target *Group, mode BalanceActionMode) (BalanceActionBuildResult, bool, error) {
	var cursor any
	for {
		if err := ctx.Err(); err != nil {
			return BalanceActionBuildResult{}, false, err
		}
		page := snapshot.virtual(ctx, actual, snapshot.ranges[actual.ID], target, snapshot.stores, mode, cursor, manualBalanceBuildPageSize)
		if err := ctx.Err(); err != nil {
			return BalanceActionBuildResult{}, false, err
		}
		if page.BlockTable || !page.Ready {
			return BalanceActionBuildResult{}, false, nil
		}
		if page.Checks < 0 || page.Checks > manualBalanceBuildPageSize {
			return BalanceActionBuildResult{}, false, errors.New("virtual balance action builder returned an invalid check count")
		}
		if page.TemporaryBlocked {
			snapshot.blockedGroups[actual.ID] = struct{}{}
		}
		if page.Complete {
			return page, true, nil
		}
		if page.Checks == 0 || page.Cursor == nil {
			return BalanceActionBuildResult{}, false, errors.New("virtual balance action builder made no paging progress")
		}
		cursor = page.Cursor
	}
}

func (m *Manager) balanceDependenciesCurrent(ctx context.Context, snapshot *balanceSnapshot) (bool, error) {
	if !m.balanceSnapshotCurrent(snapshot) {
		return false, nil
	}
	current, err := m.balancePersistentDependenciesCurrent(ctx, snapshot)
	return current && m.balanceSnapshotCurrent(snapshot), err
}

// Heartbeats change the local action fingerprint during normal convergence.
// Only durable management generations and the captured term fence this wait.
func (m *Manager) balancePersistentDependenciesCurrent(ctx context.Context, snapshot *balanceSnapshot) (bool, error) {
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
