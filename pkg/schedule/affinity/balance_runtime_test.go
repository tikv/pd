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

func TestBalanceRuntimeRejectsUnobservedPersistentUpdate(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	manager.SetBalanceActionBuilder(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, nil)
	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.NoError(t, err)
	selected := changedPlan(plans).GroupID
	group := manager.GetAffinityGroupState(selected).Group
	group.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, selected, &group)
	}))
	plans, err = manager.RebalanceAffinityGroup(t.Context(), ids[0], true)
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
