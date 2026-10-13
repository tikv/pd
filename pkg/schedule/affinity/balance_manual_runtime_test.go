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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestManualRebalanceReportsBusyAndNotReady(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	tableID := TableGroupPrefix(ids[0])
	tableLock := manager.acquireBalanceTableLock(tableID)
	tableLock.Lock()
	_, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.ErrorIs(t, err, ErrBalanceBusy)
	tableLock.Unlock()
	manager.releaseBalanceTableLock(tableID, tableLock)

	manager.SetBalanceActionBuilders(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, func(context.Context, *GroupState, GroupKeyRanges, *Group, []uint64, BalanceActionMode, any, int) BalanceActionBuildResult {
		return BalanceActionBuildResult{}
	}, nil, nil)
	_, err = manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.ErrorIs(t, err, ErrBalanceNotReady)
}

func TestManualRebalanceReportsDependencyConflict(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	var updated bool
	manager.SetBalanceActionBuilders(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: group.VoterStoreIDs}, true
	}, nil, func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, target *Group, stores []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		if !updated {
			updated = true
			_, err := manager.UpdateAffinityGroupPeers(ids[0], 2, group.VoterStoreIDs)
			require.NoError(t, err)
		}
		return manualBalanceVirtualBuilder(ctx, group, ranges, target, stores, mode, cursor, budget)
	}, nil, nil)

	_, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.ErrorIs(t, err, ErrBalanceConflict)
}

func TestManualRebalanceAllFixedReturnsFullUnchangedPlan(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	group := manager.GetAffinityGroupState(ids[0]).Group
	group.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, group.ID, &group)
	}))
	manager.SyncGroupFromEtcd(&group)
	manager.SetBalanceActionBuilders(func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, manualBalanceVirtualBuilder, nil, nil)

	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.False(t, plans[0].Changed)
	require.Equal(t, "unchanged", plans[0].Status)
	require.Equal(t, group.LeaderStoreID, plans[0].LeaderStoreID)
	require.Equal(t, group.VoterStoreIDs, plans[0].VoterStoreIDs)
	require.NotEmpty(t, plans[0].Revision)
}

func TestManualRebalanceCommitsAndWaitsForEachGroup(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 1, 1, 1})
	originalLeaders := make(map[string]uint64, len(ids))
	for _, id := range ids {
		originalLeaders[id] = manager.GetAffinityGroupState(id).LeaderStoreID
	}
	var converged []string
	validator := func(_ context.Context, group *GroupState, _ GroupKeyRanges, plan BalancePlan) (bool, bool) {
		if group.LeaderStoreID == plan.LeaderStoreID && slices.Equal(group.VoterStoreIDs, plan.VoterStoreIDs) &&
			originalLeaders[group.ID] != plan.LeaderStoreID {
			manager.RLock()
			var region *core.RegionInfo
			for _, cached := range manager.groups[group.ID].Regions {
				region = cached.balanceInput()
				break
			}
			manager.RUnlock()
			require.NotNil(t, region)
			heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(plan.LeaderStoreID)))
			manager.ObserveBalanceRegion(heartbeat)
			_, aligned := manager.GetAndCacheRegionAffinityGroupState(heartbeat)
			require.True(t, aligned)
			converged = append(converged, group.ID)
			changed := 0
			for id, original := range originalLeaders {
				if manager.GetAffinityGroupState(id).LeaderStoreID != original {
					changed++
				}
			}
			require.Equal(t, len(converged), changed, "a later group was committed before the prior group converged")
		}
		return true, true
	}
	manager.SetBalanceActionBuilders(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{LeaderTargets: slices.Clone(group.VoterStoreIDs)}, true
	}, nil, manualBalanceVirtualBuilder, nil, validator)

	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(changedPlanIDs(plans)), 2)
	require.Len(t, converged, len(changedPlanIDs(plans)))
	for _, plan := range plans {
		require.NotEmpty(t, plan.Revision)
	}
}

func TestManualNoImprovementDoesNotCacheAutomaticStable(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 1, 1, 1, 2, 2})
	manager.SetBalanceActionBuilders(func(_ context.Context, group *GroupState, _ GroupKeyRanges) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	}, nil, func(_ context.Context, _ *GroupState, _ GroupKeyRanges, target *Group,
		_ []uint64, _ BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		var actions BalanceActionSet
		if target.ID == ids[0] {
			actions.LeaderTargets = []uint64{2}
		}
		return BalanceActionBuildResult{Actions: actions, Complete: true, Ready: true}
	}, nil, nil)
	// Reconstruction retains the first group's original target on an empty
	// count tie, although moving its leader would improve the current table.
	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.NoError(t, err)
	require.Empty(t, changedPlanIDs(plans))
	require.Equal(t, balanceIdle, manager.balanceTables[TableGroupPrefix(ids[0])].phase)
}

func manualBalanceVirtualBuilder(_ context.Context, _ *GroupState, _ GroupKeyRanges, target *Group,
	_ []uint64, _ BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
	return BalanceActionBuildResult{
		Actions:  BalanceActionSet{LeaderTargets: slices.Clone(target.VoterStoreIDs)},
		Complete: true,
		Ready:    true,
		Checks:   1,
	}
}

func seedManualBalanceGroups(t *testing.T, manager *Manager, leaders []uint64) []string {
	t.Helper()
	ids := make([]string, len(leaders))
	ranges := make([]GroupKeyRanges, len(leaders))
	for i := range leaders {
		ids[i] = fmt.Sprintf("_tidb_pt_10_p%d", i+1)
		ranges[i] = GroupKeyRanges{
			GroupID:   ids[i],
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte{byte(i + 1)}, EndKey: []byte{byte(i + 2)}}},
		}
	}
	require.NoError(t, manager.CreateAffinityGroups(ranges))
	for i, id := range ids {
		_, err := manager.UpdateAffinityGroupPeers(id, leaders[i], []uint64{1, 2, 3})
		require.NoError(t, err)
		peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
		region := core.NewRegionInfo(&metapb.Region{
			Id: uint64(i + 1), StartKey: ranges[i].KeyRanges[0].StartKey, EndKey: ranges[i].KeyRanges[0].EndKey,
			Peers: peers, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}, peers[leaders[i]-1])
		_, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
		require.True(t, aligned)
	}
	return ids
}
