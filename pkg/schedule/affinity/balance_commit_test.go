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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestBalanceCommitProtectsOtherPartition(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	m.SetBalanceActionBuilder(func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
	}, nil, nil)
	snapshot := m.balanceSnapshot(ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	// The remote administration changes B before the local watcher receives it.
	remote := m.GetAffinityGroupState(ids[1]).Group
	remote.LeaderStoreID = 2
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true}})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, uint64(1), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
	// A fresh snapshot also refuses to plan with inputs lagging behind storage.
	require.Error(t, m.captureBalanceDependencies(t.Context(), m.balanceSnapshot(ids[0])))
}

func TestBalanceCommitProtectsMembership(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	snapshot := m.balanceSnapshot(ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	remote := Group{ID: "_tidb_pt_10_p99", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true}})
	require.NoError(t, err)
	require.False(t, applied)
}

func TestBalanceDependenciesRejectLaggingRangeWatcher(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached_metadata", true: "shared_metadata_cache"}[cached], func(t *testing.T) {
			m := newBalanceRuntimeManager(t)
			ids := seedBalanceGroups(t, m, []uint64{1, 1})
			m.SetBalanceActionBuilder(func(_ context.Context, g *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
				return BalanceActionSet{LeaderTargets: g.VoterStoreIDs}, true
			}, func() uint64 { return 1 }, nil)
			metadataReads := 0
			m.SetBalanceCommitGuard(nil, func() map[string]string {
				metadataReads++
				return nil
			})
			other := GroupKeyRanges{GroupID: "_tidb_pt_20_p1",
				KeyRanges: []keyutil.KeyRange{{StartKey: []byte{20}, EndKey: []byte{21}}}}
			require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{other}))
			// Only RegionLabeler has observed the expanded range. The affinity
			// watcher's Manager cache still ends at key 2.
			rule := MakeLabelRule(GroupKeyRanges{GroupID: ids[1],
				KeyRanges: []keyutil.KeyRange{{StartKey: []byte{1}, EndKey: []byte{3}}}})
			require.NoError(t, m.regionLabeler.SetLabelRule(rule))
			if cached {
				require.NoError(t, m.captureBalanceDependencies(t.Context(), m.balanceSnapshot(other.GroupID)))
				require.Equal(t, 1, metadataReads)
			}
			before := metadataReads
			snapshot := m.balanceSnapshot(ids[0])
			err := m.captureBalanceDependencies(t.Context(), snapshot)
			require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
			require.Nil(t, snapshot.dependencies)
			require.Equal(t, before, metadataReads)
			plans, err := m.balanceTable(t.Context(), ids[0], true)
			require.NoError(t, err)
			require.Empty(t, plans)
			require.NotEqual(t, balanceStable, m.balanceTables[snapshot.tableID].phase)
			for _, id := range ids {
				require.Equal(t, uint64(1), m.GetAffinityGroupState(id).LeaderStoreID)
			}
			// Once the independent affinity watcher catches up, the same
			// metadata generation can be used safely.
			require.NoError(t, m.SyncKeyRangesFromEtcd(rule))
			require.NoError(t, m.captureBalanceDependencies(t.Context(), m.balanceSnapshot(ids[0])))
			if cached {
				require.Equal(t, before, metadataReads, "valid tables still share the global metadata comparison")
			}
		})
	}
}

func TestBalanceDependenciesDoNotReuseChangedLabelRevision(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	snapshot := m.balanceSnapshot(ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	require.NotNil(t, snapshot.dependencies)
	require.NoError(t, m.regionLabeler.DeleteLabelRule(GetLabelRuleID(ids[0])))
	require.True(t, errs.ErrEtcdTxnConflict.Equal(m.captureBalanceDependencies(t.Context(), snapshot)))
	// A fresh label revision must also reject the stale Manager ranges.
	require.True(t, errs.ErrEtcdTxnConflict.Equal(m.captureBalanceDependencies(t.Context(), m.balanceSnapshot(ids[0]))))
	m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(ids[0]))
	require.NoError(t, m.captureBalanceDependencies(t.Context(), m.balanceSnapshot(ids[0])))
}

func TestBalanceDependenciesIgnoreRangeOrder(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ranges := GroupKeyRanges{GroupID: "_tidb_pt_10_p1", KeyRanges: []keyutil.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}}, {StartKey: []byte{2}, EndKey: []byte{3}},
	}}
	require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	reordered := cloneBalanceRanges(ranges)
	slices.Reverse(reordered.KeyRanges)
	require.NoError(t, m.regionLabeler.SetLabelRule(MakeLabelRule(reordered)))
	snapshot := m.balanceSnapshot(ranges.GroupID)
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	require.Equal(t, ranges, snapshot.ranges[ranges.GroupID], "comparison must not reorder the planner's ranges")
}

func TestFixedGroupRestoresAvailabilityAfterLoad(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	group := m.GetAffinityGroupState(ids[0]).Group
	group.BalancePolicy = BalancePolicyFixed
	m.SyncGroupFromEtcd(&group)
	m.Lock()
	m.updateGroupAvailabilityLocked(group.ID, groupDegraded)
	m.Unlock()
	changed, changes := m.getGroupAvailabilityChanges(map[uint64]storeCondition{})
	require.True(t, changed, "availability must refresh even when both store maps are empty")
	m.setGroupAvailabilityChanges(map[uint64]storeCondition{}, changes)
	require.True(t, m.GetAffinityGroupState(group.ID).AffinitySchedulingAllowed)
}

type blockingBalanceStorage struct {
	storage.Storage
	entered chan struct{}
	release chan struct{}
}

func (s *blockingBalanceStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	close(s.entered)
	select {
	case <-s.release:
		return s.Storage.RunInTxn(ctx, f)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestBalanceCommitDoesNotBlockRegionObservation(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	snapshot := m.balanceSnapshot(ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	region := m.groups[ids[0]].Regions[1].region
	blocked := &blockingBalanceStorage{Storage: m.storage.(storage.Storage), entered: make(chan struct{}), release: make(chan struct{})}
	m.storage = blocked
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = m.commitBalancePlans(t.Context(), snapshot, []BalancePlan{{GroupID: ids[0], LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true}})
	}()
	<-blocked.entered
	observed := make(chan struct{})
	go func() {
		m.ObserveBalanceRegion(region)
		close(observed)
	}()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Error("Region safety checks waited for network IO")
	}
	close(blocked.release)
	<-done
}

func TestBalanceCommitRejectsMultipleGroups(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1, 1})
	plans := make([]BalancePlan, len(ids))
	for i, id := range ids {
		plans[i] = BalancePlan{GroupID: id, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true}
	}
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	snapshot := m.balanceSnapshot(ids[0])
	require.NoError(t, m.captureBalanceDependencies(t.Context(), snapshot))
	applied, err := m.commitBalancePlans(t.Context(), snapshot, plans)
	require.Error(t, err)
	require.False(t, applied)
	for _, plan := range plans {
		require.Equal(t, uint64(1), m.GetAffinityGroupState(plan.GroupID).LeaderStoreID)
	}
}

func TestBalanceSnapshotUsesLiveStoreAvailability(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	m.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, nil)
	m.Lock()
	// The availability loop has not observed store 4's recovery yet.
	m.unavailableStores[4] = storeLowSpace
	m.Unlock()
	snapshot := m.balanceSnapshot(ids[0])
	require.Contains(t, snapshot.stores, uint64(4))
}

func TestAffinityTablePrefixRetainsNonPartitionID(t *testing.T) {
	require.Equal(t, "_tidb_pt_10", TableGroupPrefix("_tidb_pt_10"))
	require.Equal(t, "_tidb_pt_10", TableGroupPrefix("_tidb_pt_10_p1"))
}

func TestBalanceHeartbeatRefreshesMoveBytesWithoutReplanning(t *testing.T) {
	m := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, m, []uint64{1})
	region := m.groups[ids[0]].Regions[1].region.Clone(core.SetApproximateSize(10))
	m.ObserveBalanceRegion(region)
	m.Lock()
	table := m.balanceTables[TableGroupPrefix(ids[0])]
	table.phase = balanceStable
	table.actions[ids[0]] = BalanceActionSet{MoveBytes: 10 * 1024 * 1024}
	table.regionBytes[ids[0]] = map[uint64]uint64{1: 10 * 1024 * 1024}
	table.moveBytes[ids[0]] = 10 * 1024 * 1024
	version := table.version
	m.Unlock()
	m.ObserveBalanceRegion(region.Clone(core.SetApproximateSize(100)))
	require.Equal(t, uint64(100*1024*1024), table.actions[ids[0]].MoveBytes)
	require.Equal(t, balanceStable, table.phase)
	require.Equal(t, version, table.version)
}
