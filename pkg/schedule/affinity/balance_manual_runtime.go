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
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

const manualBalanceBuildPageSize = 256

func (m *Manager) rebalanceTable(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.GetAffinityGroupState(groupID) == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	tableID := TableGroupPrefix(groupID)
	m.Lock()
	tableLock := m.balanceLocks[tableID]
	if tableLock == nil {
		tableLock = new(sync.Mutex)
		m.balanceLocks[tableID] = tableLock
	}
	m.Unlock()
	tableLock.Lock()
	defer tableLock.Unlock()

	var (
		wasStable      bool
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
		if !wasStable {
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

	snapshot := m.balanceSnapshot(groupID)
	if snapshot == nil {
		return nil, nil
	}
	if snapshot.virtual == nil {
		return nil, errors.New("virtual balance action builder is not configured")
	}
	if err := m.captureBalanceDependencies(ctx, snapshot); err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, nil
		}
		return nil, err
	}
	if !m.balanceSnapshotCurrent(snapshot) {
		return nil, nil
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
			return nil, nil
		}
		autoGroups = append(autoGroups, group)
	}
	if selected == nil {
		return nil, nil
	}
	if len(autoGroups) == 0 || len(snapshot.stores) == 0 {
		return nil, nil
	}

	actions := make(map[string]BalanceActionSet, len(autoGroups))
	for _, group := range autoGroups {
		built, ready, err := m.buildManualBalanceActions(ctx, snapshot, group, &group.Group)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, nil
		}
		actions[group.ID] = built
	}
	var rebuildErr error
	plans, err := PlanTableRebalance(ctx, groups, BalanceOptions{
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
	if err := ctx.Err(); err != nil {
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
			return nil, errors.New("manual balance plan contains an ineligible group")
		}
		candidate := group.Group
		candidate.LeaderStoreID = plan.LeaderStoreID
		candidate.VoterStoreIDs = slices.Clone(plan.VoterStoreIDs)
		if err := m.AdjustGroup(&candidate); err != nil {
			return nil, nil
		}
		if err := m.checkBalanceTargetStores(candidate.LeaderStoreID, candidate.VoterStoreIDs); err != nil {
			return nil, nil
		}
		if snapshot.validator != nil {
			ready, legal := snapshot.validator(ctx, group, snapshot.ranges[group.ID], plan)
			if !ready || !legal {
				return nil, nil
			}
		}
	}

	current, err := m.balanceDependenciesCurrent(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if !current || !m.balanceSnapshotCurrent(snapshot) {
		return nil, nil
	}
	if !changed {
		m.Lock()
		if state := m.balanceTables[tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
			state.phase = balanceStable
			state.fingerprint = snapshot.fingerprint
		}
		m.Unlock()
		return plans, nil
	}
	if !apply {
		return plans, nil
	}
	committed, err := m.commitBalancePlans(ctx, snapshot, plans)
	if err != nil {
		return nil, err
	}
	if !committed {
		return nil, nil
	}
	return plans, nil
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
	return m.balanceSnapshotCurrent(snapshot), nil
}
