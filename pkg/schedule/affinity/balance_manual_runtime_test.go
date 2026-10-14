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
	"time"

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

	manager.SetBalanceActionBuilder(func(context.Context, *GroupState, GroupKeyRanges, *Group,
		[]uint64, BalanceActionMode, any, int) BalanceActionBuildResult {
		return BalanceActionBuildResult{}
	}, nil)
	_, err = manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.ErrorIs(t, err, ErrBalanceNotReady)
}

func TestManualRebalanceReportsDependencyConflict(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1, 1})
	var updated bool
	manager.SetBalanceActionBuilder(func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, target *Group,
		stores []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		if !updated {
			updated = true
			_, err := manager.UpdateAffinityGroupPeers(ids[0], 2, group.VoterStoreIDs)
			require.NoError(t, err)
		}
		return manualBalanceVirtualBuilder(ctx, group, ranges, target, stores, mode, cursor, budget)
	}, nil)

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
	manager.SetBalanceActionBuilder(manualBalanceVirtualBuilder, nil)

	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.False(t, plans[0].Changed)
	require.Equal(t, "unchanged", plans[0].Status)
	require.Equal(t, group.LeaderStoreID, plans[0].LeaderStoreID)
	require.Equal(t, group.VoterStoreIDs, plans[0].VoterStoreIDs)
	require.NotEmpty(t, plans[0].Revision)
}

func TestManualRebalanceCommitsAllTargetsBeforeWaiting(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 1, 1, 1})
	validated := make(map[string]*Group)
	var converged []string
	manager.SetBalanceActionBuilder(func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, target *Group,
		stores []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		switch mode {
		case BalanceValidateTarget:
			validated[target.ID] = &Group{ID: target.ID, LeaderStoreID: target.LeaderStoreID,
				VoterStoreIDs: slices.Clone(target.VoterStoreIDs)}
			return BalanceActionBuildResult{Ready: true, Complete: true}
		case BalanceCheckConvergence:
			if len(converged) == 0 {
				require.NotEmpty(t, validated)
				for id, expected := range validated {
					current := manager.GetAffinityGroupState(id)
					require.NotNil(t, current)
					require.Equal(t, expected.LeaderStoreID, current.LeaderStoreID,
						"all targets must be committed before convergence is checked")
					require.Equal(t, expected.VoterStoreIDs, current.VoterStoreIDs)
				}
			}
			manager.RLock()
			var region *core.RegionInfo
			for _, cached := range manager.groups[group.ID].Regions {
				region = cached.balanceInput()
				break
			}
			manager.RUnlock()
			require.NotNil(t, region)
			heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(target.LeaderStoreID)))
			manager.ObserveBalanceRegion(heartbeat)
			_, aligned := manager.GetAndCacheRegionAffinityGroupState(heartbeat)
			require.True(t, aligned)
			converged = append(converged, group.ID)
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		case BalanceBuildLeaders:
			return BalanceActionBuildResult{Ready: true, Complete: true,
				Actions: BalanceActionSet{LeaderTargets: slices.Clone(target.VoterStoreIDs)}, Checks: 1}
		case BalanceBuildVoters:
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		default:
			return BalanceActionBuildResult{}
		}
	}, nil)

	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(changedPlanIDs(plans)), 2)
	require.Len(t, converged, len(changedPlanIDs(plans)))
	require.Len(t, validated, len(changedPlanIDs(plans)))
	for _, plan := range plans {
		require.NotEmpty(t, plan.Revision)
	}
}

func TestManualFallbackUsesAutomaticImprovement(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 1, 1, 1, 2, 2})
	for _, id := range []string{"other-table-1", "other-table-2"} {
		require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
		_, err := manager.UpdateAffinityGroupPeers(id, 2, []uint64{1, 2, 3})
		require.NoError(t, err)
	}
	manager.SetBalanceActionBuilder(func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, target *Group,
		stores []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		var actions BalanceActionSet
		if mode == BalanceBuildLeaders && target.ID == ids[0] {
			actions.LeaderTargets = []uint64{2}
		}
		return BalanceActionBuildResult{Actions: actions, Complete: true, Ready: true, Checks: 1}
	}, nil)
	groups := make([]*Group, 0, len(ids))
	for _, id := range ids {
		groups = append(groups, &manager.GetAffinityGroupState(id).Group)
	}
	auto, err := PlanBalance(groups, BalanceOptions{
		Stores:     []uint64{1, 2, 3},
		ActionSets: map[string]BalanceActionSet{ids[0]: {LeaderTargets: []uint64{2}}},
	})
	require.NoError(t, err)
	require.Equal(t, ids[0], changedPlan(auto).GroupID)
	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.NoError(t, err)
	require.Equal(t, []string{ids[0]}, changedPlanIDs(plans))
}

func TestManualWaitRejectsRangeChanges(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "added", true: "removed"}[remove], func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			id := seedBalanceGroups(t, manager, []uint64{1})[0]
			manager.SetBalanceActionBuilder(manualBalanceVirtualBuilder, nil)
			snapshot := manager.newBalanceSnapshot(id, true)
			require.NoError(t, manager.captureBalanceDependencies(t.Context(), snapshot))
			change := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{1}, EndKey: []byte{2}}}}
			if remove {
				require.NoError(t, manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{snapshot.ranges[id]}))
			} else {
				require.NoError(t, manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{change}, nil))
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := manager.waitManualBalanceConvergence(ctx, snapshot,
				[]BalancePlan{{GroupID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true, Status: "submitted"}})
			require.ErrorIs(t, err, ErrBalanceConflict)
		})
	}
}

func TestManualConvergenceDropsFailedCursor(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	id := seedBalanceGroups(t, manager, []uint64{1})[0]
	var calls int
	reset := false
	manager.SetBalanceActionBuilder(func(_ context.Context, _ *GroupState, _ GroupKeyRanges, _ *Group,
		_ []uint64, mode BalanceActionMode, cursor any, _ int) BalanceActionBuildResult {
		if mode != BalanceCheckConvergence {
			return BalanceActionBuildResult{Ready: true, Complete: true}
		}
		calls++
		if calls == 1 {
			return BalanceActionBuildResult{Ready: false, Cursor: new(struct{}), Checks: 1}
		}
		require.Nil(t, cursor, "an unready page must not reuse its mutated cursor")
		reset = true
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
	}, nil)
	snapshot := manager.newBalanceSnapshot(id, true)
	require.NoError(t, manager.captureBalanceDependencies(t.Context(), snapshot))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := manager.waitManualBalanceConvergence(ctx, snapshot, []BalancePlan{{
		GroupID: id, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true, Status: "submitted",
	}})
	require.NoError(t, err)
	require.True(t, reset)
}

func TestManualNoImprovementReportsTemporaryCandidates(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 2, 3})
	manager.SetBalanceActionBuilder(func(_ context.Context, actual *GroupState, _ GroupKeyRanges, target *Group,
		_ []uint64, mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
		actions := BalanceActionSet{}
		if mode == BalanceBuildLeaders {
			actions.LeaderTargets = slices.Clone(target.VoterStoreIDs)
		}
		return BalanceActionBuildResult{Ready: true, Complete: true, Actions: actions, Checks: 1,
			TemporaryBlocked: actual.ID == ids[0] && mode == BalanceBuildLeaders}
	}, nil)
	plans, err := manager.RebalanceAffinityGroup(t.Context(), ids[0], false)
	require.ErrorIs(t, err, ErrBalanceNotReady)
	require.Len(t, plans, 3)
	require.Equal(t, "temporarily_blocked", plans[0].Status)
	require.NotEmpty(t, plans[0].Reason)
	require.Empty(t, changedPlanIDs(plans))
}

func TestManualPartialApplyRetainsSubmittedStates(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedManualBalanceGroups(t, manager, []uint64{1, 1, 1, 1})
	validated := make(map[string]*Group)
	var firstConverged string
	manager.SetBalanceActionBuilder(func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, target *Group,
		stores []uint64, mode BalanceActionMode, cursor any, budget int) BalanceActionBuildResult {
		switch mode {
		case BalanceBuildLeaders:
			return BalanceActionBuildResult{Ready: true, Complete: true,
				Actions: BalanceActionSet{LeaderTargets: slices.Clone(target.VoterStoreIDs)}, Checks: 1}
		case BalanceBuildVoters:
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		case BalanceValidateTarget:
			validated[target.ID] = planTarget(BalancePlan{GroupID: target.ID, LeaderStoreID: target.LeaderStoreID,
				VoterStoreIDs: target.VoterStoreIDs})
			return BalanceActionBuildResult{Ready: true, Complete: true}
		case BalanceCheckConvergence:
			if firstConverged == "" {
				require.NotEmpty(t, validated)
				for id, expected := range validated {
					current := manager.GetAffinityGroupState(id)
					require.NotNil(t, current)
					require.Equal(t, expected.LeaderStoreID, current.LeaderStoreID,
						"all targets must be submitted before convergence starts")
				}
				firstConverged = group.ID
				manager.RLock()
				var region *core.RegionInfo
				for _, cached := range manager.groups[group.ID].Regions {
					region = cached.balanceInput()
					break
				}
				manager.RUnlock()
				require.NotNil(t, region)
				heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(target.LeaderStoreID)))
				manager.ObserveBalanceRegion(heartbeat)
				manager.GetAndCacheRegionAffinityGroupState(heartbeat)
				require.NoError(t, manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{ranges}))
				return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
			}
			return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
		default:
			return BalanceActionBuildResult{}
		}
	}, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	plans, err := manager.RebalanceAffinityGroup(ctx, ids[0], true)
	require.ErrorIs(t, err, ErrBalanceConflict)
	require.Len(t, plans, len(ids))
	require.NotEmpty(t, firstConverged)
	applied, submitted, failures := 0, 0, 0
	for _, plan := range plans {
		if !plan.Changed {
			continue
		}
		if plan.GroupID == firstConverged {
			require.Equal(t, "applied", plan.Status)
			applied++
		} else {
			require.Equal(t, "submitted", plan.Status)
			if plan.Reason != "" {
				failures++
			}
			submitted++
		}
	}
	require.Equal(t, 1, applied)
	require.Positive(t, submitted)
	require.Equal(t, 1, failures)
}

func manualBalanceVirtualBuilder(_ context.Context, _ *GroupState, _ GroupKeyRanges, target *Group,
	_ []uint64, mode BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
	if mode == BalanceValidateTarget || mode == BalanceCheckConvergence {
		return BalanceActionBuildResult{Ready: true, Complete: true, Checks: 1}
	}
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
