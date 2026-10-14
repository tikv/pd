// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package affinity

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
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
	labelerManager, err := labeler.NewRegionLabeler(ctx, store, time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labelerManager)
	require.NoError(t, err)
	return manager
}

func seedBalanceGroups(t *testing.T, manager *Manager, leaders []uint64) []string {
	t.Helper()
	ids := []string{"_tidb_pt_10_p1", "_tidb_pt_10_p2", "_tidb_pt_10_p3"}
	for i, leader := range leaders {
		start, end := []byte{byte(i)}, []byte{byte(i + 1)}
		require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[i], KeyRanges: []keyutil.KeyRange{{StartKey: start, EndKey: end}}}}))
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

func setTestBalanceBuilder(manager *Manager, builder func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool), revision func() uint64) {
	manager.SetBalanceActionBuilder(func(ctx context.Context, actual *GroupState, ranges GroupKeyRanges, _ *Group, _ []uint64,
		mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence || mode == BalanceValidateTarget {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		actions, ready := builder(ctx, actual, ranges)
		if !ready {
			return BalanceActionBuildResult{Checks: 1}
		}
		if mode == BalanceBuildLeaders {
			return BalanceActionBuildResult{Actions: BalanceActionSet{LeaderTargets: actions.LeaderTargets}, Ready: true, Complete: true, Checks: 1}
		}
		return BalanceActionBuildResult{Actions: BalanceActionSet{VoterReplacements: actions.VoterReplacements}, Ready: true, Complete: true, Checks: 1}
	}, revision)
}

func installTestBalanceBuilder(manager *Manager, builder BalanceActionProgressBuilder, revision func() uint64) {
	manager.Lock()
	manager.balanceBuilder = builder
	manager.balanceRuleRevision = revision
	manager.Unlock()
}

type balanceSamplingStorage struct {
	storage.Storage
	after func()
	once  sync.Once
}

func (s *balanceSamplingStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.Storage.RunInTxn(ctx, f)
	if err == nil && s.after != nil {
		s.once.Do(s.after)
	}
	return err
}

func manualBalanceSnapshot(t *testing.T, manager *Manager, groupID string) *balanceSnapshot {
	t.Helper()
	snapshot := manager.newBalanceSnapshot(groupID, true)
	require.NotNil(t, snapshot)
	return snapshot
}

func TestBalanceRuntimeConvergesBeforeNextPlan(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	setTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
	require.Equal(t, balanceConverging, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
	plans, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans)
}

func TestBalanceRuntimeStableCacheAndRuleInvalidation(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 2, 3})
	var builds atomic.Int64
	var rules atomic.Uint64
	setTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		builds.Add(1)
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, rules.Load)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Zero(t, changedPlans(plans))
	require.Equal(t, int64(6), builds.Load())
	rules.Add(1)
	_, err = manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, int64(12), builds.Load())
}

func TestBalanceRuntimeBuildBudgetAndCursor(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	calls := 0
	manager.SetBalanceActionBuilder(func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group, _ []uint64,
		_ BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		calls++
		if cursor == nil {
			return BalanceActionBuildResult{Ready: true, Cursor: 1, Checks: min(1, budget)}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: min(1, budget), Actions: BalanceActionSet{LeaderTargets: []uint64{1}}}
	}, nil)
	for i := 0; i < 2; i++ {
		_, err := manager.balanceTableWithBudget(t.Context(), ids[0], false, 1)
		require.NoError(t, err)
	}
	require.Equal(t, 2, calls)
}

func TestBalanceRuntimeRejectsChangedTableSnapshot(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	installTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceBuildLeaders && group.ID == ids[1] {
			manager.InvalidateBalance(ids[2])
		}
		if mode == BalanceCheckConvergence || mode == BalanceValidateTarget {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Empty(t, plans, "stale plans must not be reported as applied")
	for _, id := range ids {
		require.Equal(t, uint64(1), manager.GetAffinityGroupState(id).LeaderStoreID)
	}
}

func TestBalanceRuntimeBuildFailureDropsSnapshotCursor(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "unready", true: "blocked"}[blocked], func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			ids := seedBalanceGroups(t, manager, []uint64{1})
			installTestBalanceBuilder(manager, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group,
				_ []uint64, _ BalanceActionMode, cursor any, _ int) BalanceActionBuildResult {
				if cursor == nil {
					return BalanceActionBuildResult{Cursor: new(int), Ready: true, Checks: 1}
				}
				return BalanceActionBuildResult{BlockTable: blocked, Checks: 1}
			}, nil)
			snapshot := manager.newBalanceSnapshot(ids[0], true)
			require.NotNil(t, snapshot)
			snapshot.mode = BalanceBuildLeaders
			complete, _, resume := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
			require.False(t, complete)
			require.True(t, resume)
			require.NotNil(t, snapshot.cursor)
			require.Empty(t, manager.balanceTables[snapshot.tableID].actions)

			complete, _, resume = manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
			require.False(t, complete)
			require.False(t, resume)
			require.Nil(t, snapshot.cursor)
			require.Empty(t, manager.balanceTables[snapshot.tableID].actions)
		})
	}
}

func TestBalanceRuntimeUnreadyWorkConsumesSharedBudget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	calls := 0
	installTestBalanceBuilder(manager, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, _ BalanceActionMode, _ any, budget int) BalanceActionBuildResult {
		calls++
		return BalanceActionBuildResult{Checks: budget}
	}, nil)
	snapshot := manager.newBalanceSnapshot(ids[0], true)
	require.NotNil(t, snapshot)
	snapshot.mode = BalanceBuildLeaders
	complete, remaining, _ := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 2)
	require.False(t, complete)
	require.Zero(t, remaining)
	require.Equal(t, 1, calls)
}

func TestBalanceRuntimeSelectedTargetValidationSharesPageBudget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	validationCalls := 0
	installTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		if mode == BalanceValidateTarget {
			require.Equal(t, 1, budget)
			validationCalls++
			visited, _ := cursor.(int)
			visited++
			return BalanceActionBuildResult{Ready: true, Complete: visited == 3, Cursor: visited, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, nil)
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
			require.Equal(t, uint64(1), manager.GetAffinityGroupState(id).LeaderStoreID,
				"partial validation must not submit a target")
		}
	}
	require.NotNil(t, findChangedPlan(plans))
	require.Equal(t, 3, validationCalls)
}

func TestBalanceRuntimePendingInvalidationDiscardsWorkerPage(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	installTestBalanceBuilder(manager, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, _ BalanceActionMode, cursor any, _ int) BalanceActionBuildResult {
		if cursor == nil {
			return BalanceActionBuildResult{Ready: true, Cursor: true, Checks: 1}
		}
		enteredOnce.Do(func() { close(entered) })
		<-release
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: []uint64{1, 2}}}
	}, nil)
	snapshot := manager.newBalanceSnapshot(ids[0], true)
	require.NotNil(t, snapshot)
	snapshot.mode = BalanceBuildLeaders
	complete, _, resume := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
	require.False(t, complete)
	require.True(t, resume)
	require.True(t, manager.retainBalanceSnapshot(snapshot))

	result := make(chan bool, 1)
	go func() {
		complete, _, _ := manager.buildBalanceActionStage(t.Context(), snapshot, snapshot.groups, 1)
		result <- complete
	}()
	<-entered
	manager.InvalidateBalance(ids[1])
	close(release)
	require.False(t, <-result)
	manager.RLock()
	state := manager.balanceTables[snapshot.tableID]
	require.Nil(t, state.pending)
	require.Empty(t, state.actions)
	manager.RUnlock()
}

func TestBalanceRuntimeTemporaryBlockDoesNotBecomeStable(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	// A temporary omission must not prevent another confirmed group from being selected.
	installTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ *Group, _ []uint64,
		mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence || mode == BalanceValidateTarget {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		if group.ID == ids[0] {
			return BalanceActionBuildResult{Ready: true, Complete: true, TemporaryBlocked: true, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Actions: BalanceActionSet{LeaderTargets: []uint64{2}}, Checks: 1}
	}, nil)
	plans, err := manager.balanceTable(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Equal(t, ids[1], changedPlan(plans).GroupID)
	require.NotEqual(t, balanceStable, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
}

func TestBalanceRuntimeRuleRevisionEnqueuesImmediately(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 2, 3})
	var rules atomic.Uint64
	rules.Store(1)
	installTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence || mode == BalanceValidateTarget {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, rules.Load)
	_, err := manager.balanceTable(t.Context(), ids[0], false)
	require.NoError(t, err)
	state := manager.balanceTables[TableGroupPrefix(ids[0])]
	require.Equal(t, balanceStable, state.phase)
	require.False(t, state.queued)

	rules.Add(1)
	_, err = manager.AutoBalanceGroup(ids[0])
	require.NoError(t, err)
	require.True(t, state.queued)
	select {
	case queued := <-manager.balanceQueue:
		require.Equal(t, ids[0], queued)
	case <-time.After(time.Second):
		t.Fatal("rule revision did not enqueue the table")
	}
}

func TestAutoBalanceFastPathDoesNotWaitForMetadataOrHash(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 2, 3})
	var rules atomic.Uint64
	rules.Store(1)
	var fingerprints atomic.Int64
	installTestBalanceBuilder(manager, func(_ context.Context, group *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		if mode == BalanceCheckConvergence || mode == BalanceValidateTarget {
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1,
			Actions: BalanceActionSet{LeaderTargets: group.VoterStoreIDs}}
	}, rules.Load)
	manager.SetBalanceRuleFingerprint(func([]GroupKeyRanges) uint64 {
		fingerprints.Add(1)
		return 1
	})
	_, err := manager.balanceTable(t.Context(), ids[0], false)
	require.NoError(t, err)
	before := fingerprints.Load()

	manager.metaMutex.Lock()
	result := make(chan error, 1)
	go func() {
		_, err := manager.AutoBalanceGroup(ids[0])
		result <- err
	}()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		manager.metaMutex.Unlock()
		t.Fatal("automatic balance fast path waited for metadata lock")
	}
	manager.metaMutex.Unlock()
	require.Equal(t, before, fingerprints.Load())
}

func TestBalanceDependenciesRejectRangeSamplingRace(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	setTestBalanceBuilder(manager, func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	manager.storage = &balanceSamplingStorage{Storage: base, after: func() {
		rule := MakeLabelRule(GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte("changed")}}})
		require.NoError(t, manager.regionLabeler.SetLabelRule(rule))
	}}
	err := manager.captureBalanceDependencies(t.Context(), snapshot)
	require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
	require.Nil(t, snapshot.dependencies)
}

func TestBalanceRuntimeHeartbeatRefreshesBytes(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	region := manager.groups[ids[0]].Regions[1].region
	manager.Lock()
	state := manager.balanceTables[TableGroupPrefix(ids[0])]
	state.actions[ids[0]] = &balanceGroupActions{set: BalanceActionSet{MoveBytes: 10}, regionBytes: map[uint64]uint64{1: 10}}
	manager.Unlock()
	manager.ObserveBalanceRegion(region.Clone(core.SetApproximateSize(100)))
	manager.RLock()
	require.Equal(t, uint64(100*1024*1024), state.actions[ids[0]].set.MoveBytes)
	manager.RUnlock()
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
