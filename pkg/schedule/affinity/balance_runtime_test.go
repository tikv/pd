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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newBalanceRuntimeManager(t *testing.T) *Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	store := storage.NewStorageWithMemoryBackend()
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	conf := mockconfig.NewTestOptions()
	labelerManager, err := labeler.NewRegionLabeler(ctx, store, time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, conf, labelerManager)
	require.NoError(t, err)
	return manager
}

func seedBalanceGroups(t *testing.T, manager *Manager, leaders []uint64) []string {
	t.Helper()
	ids := []string{"_tidb_pt_10_p1", "_tidb_pt_10_p2", "_tidb_pt_10_p3"}
	for i, leader := range leaders {
		start, end := []byte{byte(i)}, []byte{byte(i + 1)}
		require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{
			GroupID: ids[i], KeyRanges: []keyutil.KeyRange{{StartKey: start, EndKey: end}},
		}}))
		_, err := manager.UpdateAffinityGroupPeers(ids[i], leader, []uint64{1, 2, 3})
		require.NoError(t, err)
		peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
		region := core.NewRegionInfo(&metapb.Region{Id: uint64(i + 1), StartKey: start, EndKey: end, Peers: peers,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, peers[leader-1])
		_, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
		require.True(t, aligned)
	}
	return ids[:len(leaders)]
}

func TestBalanceRuntimeConvergesBeforeNextPlan(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, nil)
	snapshot := manager.balanceSnapshot(ids[0])
	require.NoError(t, manager.captureBalanceDependencies(t.Context(), snapshot))
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
	require.Equal(t, balanceConverging, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans)
	_, err = manager.UpdateAffinityGroupPeers(ids[0], 3, []uint64{1, 2, 3})
	require.NoError(t, err)
	applied, err := manager.commitBalancePlans(t.Context(), snapshot,
		[]BalancePlan{{GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true}})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(3), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
}

func TestBalanceRuntimeStableCacheAndInvalidation(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 2, 3})
	var builds atomic.Int64
	var rules atomic.Uint64
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		builds.Add(1)
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, rules.Load, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Zero(t, changedPlans(plans))
	require.Equal(t, int64(3), builds.Load())
	for range 10 {
		_, err = manager.AutoBalanceGroup(ids[1])
		require.NoError(t, err)
	}
	require.False(t, manager.balanceTables[TableGroupPrefix(ids[0])].queued)
	require.Equal(t, int64(3), builds.Load())
	manager.InvalidateBalance(ids[1])
	_, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, int64(4), builds.Load(), "only the invalidated group should be rebuilt")
	rules.Add(1)
	_, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, int64(7), builds.Load())
}

func TestBalanceRuntimeRejectsChangedTableSnapshot(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		if group.ID == ids[1] {
			manager.InvalidateBalance(ids[2])
		}
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans, "stale plans must not be reported as applied")
	for _, id := range ids {
		require.Equal(t, uint64(1), manager.GetAffinityGroupState(id).LeaderStoreID)
	}
}

func TestBalanceRuntimeBuildBudgetRetainsProgress(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 2, 3})
	var builds int
	var rules atomic.Uint64
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		builds++
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, rules.Load, nil)
	for i := 1; i <= 3; i++ {
		_, err := manager.balanceTableWithBudget(t.Context(), ids[0], true, 1)
		require.NoError(t, err)
		require.Equal(t, i, builds)
	}
	require.Equal(t, balanceStable, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
	rules.Add(1)
	for i := 4; i <= 6; i++ {
		_, err := manager.balanceTableWithBudget(t.Context(), ids[0], true, 1)
		require.NoError(t, err)
		require.Equal(t, i, builds)
		if i < 6 {
			require.Equal(t, balanceIdle, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
		}
	}
	require.Equal(t, balanceStable, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
}

func TestBalanceRuntimeChecksSelectedAction(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	ready, legal := false, false
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, func(context.Context, *GroupState, GroupKeyRanges, BalancePlan) (bool, bool) { return ready, legal })
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans)
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	ready = true
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans)
	require.NotContains(t, manager.balanceTables[TableGroupPrefix(ids[0])].actions[ids[0]].LeaderTargets, uint64(2))
	legal = true
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
}

func TestBalanceRuntimeRejectedActionCannotReturnFromCanonicalCache(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, nil)
	snapshot := manager.balanceSnapshot(ids[0])
	group := manager.GetAffinityGroupState(ids[0])
	actions := BalanceActionSet{LeaderTargets: []uint64{1, 2, 3},
		VoterReplacements: map[uint64]map[uint64][]uint64{3: {4: {1, 2}}}, MoveBytes: 123}
	manager.Lock()
	state := manager.balanceTables[snapshot.tableID]
	state.fingerprint = snapshot.fingerprint
	state.actions[group.ID] = cloneBalanceActionSet(actions)
	state.leaderTargets[group.ID] = []uint64{1, 2, 3}
	state.voterActions[group.ID] = cloneBalanceActionSet(actions)
	manager.Unlock()
	manager.removeIllegalBalanceAction(snapshot, group,
		BalancePlan{LeaderStoreID: 2, VoterStoreIDs: group.VoterStoreIDs})
	manager.RLock()
	require.NotContains(t, state.leaderTargets[group.ID], uint64(2))
	require.NotContains(t, state.voterActions[group.ID].LeaderTargets, uint64(2))
	manager.RUnlock()
	manager.removeIllegalBalanceAction(snapshot, group,
		BalancePlan{LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 4}})
	manager.RLock()
	require.Equal(t, []uint64{1}, state.voterActions[group.ID].VoterReplacements[3][4])
	require.Equal(t, uint64(123), state.voterActions[group.ID].MoveBytes)
	manager.RUnlock()
	require.Contains(t, actions.VoterReplacements[3][4], uint64(2), "input action sets must remain immutable")
}

func TestBalanceRuntimeRejectsUnobservedPersistentUpdate(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], false)
	require.NoError(t, err)
	selected := changedPlan(plans).GroupID
	group := manager.GetAffinityGroupState(selected).Group
	group.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, selected, &group)
	}))
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans, "a remote update must be protected before the watcher observes it")
	value, err := manager.storage.LoadAffinityGroup(selected)
	require.NoError(t, err)
	require.Contains(t, value, `"balance_policy":"fixed"`)
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(selected).LeaderStoreID)
}

func TestFixedGroupRecoversFromHealthySeed(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	group := manager.GetAffinityGroupState(ids[0]).Group
	group.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, group.ID, &group)
	}))
	manager.SyncGroupFromEtcd(&group)
	unavailable := map[uint64]storeCondition{3: storeDown}
	_, changes := manager.getGroupAvailabilityChanges(unavailable)
	manager.setGroupAvailabilityChanges(unavailable, changes)
	require.True(t, manager.GetAffinityGroupState(ids[0]).RegularSchedulingAllowed)
	region := manager.groups[ids[0]].Regions[1].region.Clone(core.WithReplacePeerStore(3, 4))
	manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState(ids[0]))
	require.Equal(t, []uint64{1, 2, 4}, manager.GetAffinityGroupState(ids[0]).VoterStoreIDs)
	require.Equal(t, BalancePolicyFixed, manager.GetAffinityGroupState(ids[0]).BalancePolicy)
	_, changes = manager.getGroupAvailabilityChanges(map[uint64]storeCondition{})
	manager.setGroupAvailabilityChanges(map[uint64]storeCondition{}, changes)
	require.True(t, manager.GetAffinityGroupState(ids[0]).AffinitySchedulingAllowed)
}

func TestBalanceRuntimeOrdinaryHeartbeatKeepsActions(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	region := manager.groups[ids[0]].Regions[1].region
	manager.Lock()
	state := manager.balanceTables[TableGroupPrefix(ids[0])]
	state.phase = balanceStable
	state.actions[ids[0]] = BalanceActionSet{LeaderTargets: []uint64{1}}
	version := state.version
	manager.Unlock()
	heartbeat := region.Clone(core.SetApproximateSize(100))
	manager.ObserveBalanceRegion(heartbeat)
	manager.GetAndCacheRegionAffinityGroupState(heartbeat)
	require.Equal(t, version, state.version)
	require.Equal(t, balanceStable, state.phase)
	changed := heartbeat.Clone(core.WithIncConfVer())
	manager.ObserveBalanceRegion(changed)
	require.Greater(t, state.version, version)
	require.Empty(t, state.actions)
}

func TestBalanceTableLockReleasedAfterLastUser(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	first := manager.acquireBalanceTableLock("table")
	second := manager.acquireBalanceTableLock("table")
	require.Same(t, first, second)
	manager.releaseBalanceTableLock("table", first)
	require.Contains(t, manager.balanceLocks, "table")
	manager.releaseBalanceTableLock("table", second)
	require.Empty(t, manager.balanceLocks)
}

func TestBalanceBuildFailureDropsMutatedCursor(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(map[bool]string{false: "unready", true: "blocked"}[block], func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			ids := seedBalanceGroups(t, manager, []uint64{1})
			manager.SetBalanceActionBuilders(nil, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ []uint64,
				_ BalanceActionMode, cursor any, _ int) BalanceActionBuildResult {
				if cursor == nil {
					return BalanceActionBuildResult{Cursor: new(int), Ready: true, Checks: 1}
				}
				*cursor.(*int)++
				return BalanceActionBuildResult{BlockTable: block, Checks: 1}
			}, nil, nil, nil)
			snapshot := manager.balanceSnapshot(ids[0])
			complete, _, _ := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
			require.False(t, complete)
			require.Contains(t, manager.balanceTables[snapshot.tableID].cursors, ids[0])
			manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
			state := manager.balanceTables[snapshot.tableID]
			require.NotContains(t, state.cursors, ids[0])
			require.NotContains(t, state.leaderTargets, ids[0])
			require.NotContains(t, state.actions, ids[0])
		})
	}
}

func TestBalanceUnreadyWorkConsumesSharedBudget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	calls := 0
	manager.SetBalanceActionBuilders(nil, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ []uint64,
		_ BalanceActionMode, _ any, budget int) BalanceActionBuildResult {
		calls++
		return BalanceActionBuildResult{Checks: budget}
	}, nil, nil, nil)
	snapshot := manager.balanceSnapshot(ids[0])
	complete, remaining, _ := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 2)
	require.False(t, complete)
	require.Zero(t, remaining)
	require.Equal(t, 1, calls)
}

func TestBalanceSelectedTargetValidationSharesPageBudget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	validationCalls := 0
	manager.SetBalanceActionBuilders(nil, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ []uint64,
		_ BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group, _ []uint64,
		mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		require.Equal(t, BalanceValidateTarget, mode)
		require.Equal(t, 1, budget)
		validationCalls++
		visited, _ := cursor.(int)
		visited++
		return BalanceActionBuildResult{Ready: true, Complete: visited == 3, Cursor: visited, Checks: 1}
	}, nil, func(context.Context, *GroupState, GroupKeyRanges, BalancePlan) (bool, bool) {
		t.Fatal("paged validation must not fall back to the full-group validator")
		return false, false
	})
	var plans []BalancePlan
	for range 10 {
		before := validationCalls
		var err error
		var incomplete bool
		plans, err, incomplete = manager.balanceTableWithProgress(t.Context(), ids[0], true, 1)
		require.NoError(t, err)
		require.LessOrEqual(t, validationCalls-before, 1)
		if findChangedPlan(plans) != nil {
			require.False(t, incomplete)
			break
		}
		require.True(t, incomplete)
		for _, id := range ids {
			require.Equal(t, uint64(1), manager.GetAffinityGroupState(id).LeaderStoreID, "partial validation must not submit a target")
		}
	}
	require.NotNil(t, findChangedPlan(plans))
	require.Equal(t, 3, validationCalls)
}

func TestTemporaryCandidateDoesNotPreventOtherGroupImprovement(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	manager.SetBalanceActionBuilders(nil, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ []uint64,
		_ BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if group.ID == ids[0] {
			return BalanceActionBuildResult{Ready: true, Complete: true, TemporaryBlocked: true}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Actions: BalanceActionSet{LeaderTargets: []uint64{2}}}
	}, nil, nil, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, ids[1], changedPlan(plans).GroupID)
	state := manager.balanceTables[TableGroupPrefix(ids[0])]
	require.NotContains(t, state.leaderTargets, ids[0], "temporary omissions must be rebuilt after convergence")
}

func TestBalanceWaitsForFullCoverageBeforeCapturingDependencies(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	fullyConverged := false
	metadataReads := 0
	manager.SetBalanceCommitGuard(nil, func() map[string]string {
		metadataReads++
		return nil
	})
	manager.SetBalanceActionBuilders(nil, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ []uint64,
		mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence {
			return BalanceActionBuildResult{Ready: fullyConverged, Complete: fullyConverged, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1, Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, nil, nil, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	plan := changedPlan(plans)
	require.NotNil(t, plan)
	region := manager.groups[plan.GroupID].Regions[1].region
	heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(plan.LeaderStoreID)))
	manager.ObserveBalanceRegion(heartbeat)
	manager.GetAndCacheRegionAffinityGroupState(heartbeat)
	require.Equal(t, PhaseStable, manager.GetAffinityGroupState(plan.GroupID).Phase)
	require.NotNil(t, manager.balanceTables[TableGroupPrefix(ids[0])].converging)
	before := metadataReads
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans)
	require.Equal(t, before, metadataReads, "convergence waiting must not scan global rule metadata")
	fullyConverged = true
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
}

func TestBalancePageBytesAccumulateIncrementally(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	manager.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	snapshot := manager.balanceSnapshot(ids[0])
	for _, page := range []BalanceActionBuildResult{
		{RegionBytes: map[uint64]uint64{10: 100}},
		{RegionBytes: map[uint64]uint64{20: 200}},
		{RegionBytes: map[uint64]uint64{10: 150}},
		{Complete: true},
	} {
		_, ok := manager.cacheBalanceBuildPage(snapshot, ids[0], page)
		require.True(t, ok)
	}
	state := manager.balanceTables[snapshot.tableID]
	require.Equal(t, uint64(350), state.actions[ids[0]].MoveBytes)
	require.Equal(t, uint64(350), state.moveBytes[ids[0]])
}
