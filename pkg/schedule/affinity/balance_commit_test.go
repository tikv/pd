// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package affinity

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestBalanceCommitProtectsTableDependencies(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	remote := m.GetAffinityGroupState(ids[1]).Group
	remote.LeaderStoreID = 2
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{
		GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
	}})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(1), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
}

func TestBalanceCommitProtectsTableMembership(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	remote := Group{ID: "_tidb_pt_10_p99", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{
		GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
	}})
	require.NoError(t, err)
	require.False(t, applied)
}

func TestBalanceCommitBatchUsesExactTargetToken(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	plans := []BalancePlan{
		{GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true},
		{GroupID: ids[1], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true},
	}
	applied, err := m.commitBalancePlans(t.Context(), snapshot, plans)
	require.NoError(t, err)
	require.True(t, applied)
	key := endpoint.AffinityTableRevisionKey(snapshot.tableID)
	var stored string
	err = m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		var loadErr error
		stored, loadErr = txn.Load(key)
		return loadErr
	})
	require.NoError(t, err)
	require.Equal(t, stored, snapshot.dependencies[key])
	require.Equal(t, uint64(2), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(2), m.GetAffinityGroupState(ids[1]).LeaderStoreID)

	// An external write after our batch must fence a retry that still carries
	// the old snapshot token.
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(key, "external-target")
	}))
	applied, err = m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{
		GroupID: ids[0], LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
	}})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(2), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
}

func TestBalanceCommitRejectsPlacementChange(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(endpoint.AffinityPlacementRevisionKey(), "changed")
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{
		GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
	}})
	require.NoError(t, err)
	require.False(t, applied)
}

func TestBalanceDependenciesDoNotReuseChangedLabelRevision(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	setTestBalanceBuilder(m, func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	require.NotNil(t, snapshot.dependencies)
	require.NoError(t, m.regionLabeler.DeleteLabelRule(GetLabelRuleID(ids[0])))
	require.True(t, errs.ErrEtcdTxnConflict.Equal(m.captureBalanceDependencies(t.Context(), snapshot)))
	// A fresh snapshot must also wait for the affinity range watcher to catch up.
	fresh := manualBalanceSnapshot(t, m, ids[0])
	require.True(t, errs.ErrEtcdTxnConflict.Equal(m.captureBalanceDependencies(t.Context(), fresh)))
	m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(ids[0]))
	require.NoError(t, m.captureBalanceDependencies(t.Context(), manualBalanceSnapshot(t, m, ids[0])))
}

func TestBalanceDependenciesIgnoreRangeOrder(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ranges := GroupKeyRanges{GroupID: "_tidb_pt_10_p1", KeyRanges: []keyutil.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}}, {StartKey: []byte{2}, EndKey: []byte{3}},
	}}
	require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	setTestBalanceBuilder(m, func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil)
	reordered := cloneBalanceRanges(ranges)
	slices.Reverse(reordered.KeyRanges)
	require.NoError(t, m.regionLabeler.SetLabelRule(MakeLabelRule(reordered)))
	snapshot := manualBalanceSnapshot(t, m, ranges.GroupID)
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	require.Equal(t, ranges, snapshot.ranges[ranges.GroupID], "comparison must not reorder the planner's ranges")
}

func TestBalancePreviewNoopDoesNotAdvanceTargetRevision(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	key := endpoint.AffinityTableRevisionKey(ids[0])
	load := func() string {
		var value string
		require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
			var err error
			value, err = txn.Load(key)
			return err
		}))
		return value
	}
	before := load()
	plans, err := m.balanceTable(t.Context(), ids[0], false)
	require.NoError(t, err)
	require.Zero(t, changedPlans(plans))
	require.Equal(t, before, load())
}

func TestBalanceCommitRejectsExternalPlacementWriteAfterBatch(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	setTestBalanceBuilder(m, func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil)
	snapshot := manualBalanceSnapshot(t, m, ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	placementKey := endpoint.AffinityPlacementRevisionKey()
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(placementKey, "external-placement")
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{
		GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
	}})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(1), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
}
