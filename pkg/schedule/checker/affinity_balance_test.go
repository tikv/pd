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

package checker

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func (c *AffinityChecker) buildBalanceActions(ctx context.Context, group *affinity.GroupState, ranges affinity.GroupKeyRanges) (affinity.BalanceActionSet, bool) {
	storeIDs := c.balanceStoreIDs()
	leaders := c.buildBalanceActionsProgress(ctx, group, ranges, storeIDs, affinity.BalanceBuildLeaders, nil, 0)
	if !leaders.Ready || !leaders.Complete {
		return affinity.BalanceActionSet{}, false
	}
	voters := c.buildBalanceActionsProgress(ctx, group, ranges, storeIDs, affinity.BalanceBuildVoters, nil, 0)
	if !voters.Ready || !voters.Complete {
		return affinity.BalanceActionSet{}, false
	}
	actions := voters.Actions
	actions.LeaderTargets = leaders.Actions.LeaderTargets
	return actions, true
}

// Exercise the real checker entry point, persisted target change, operator
// generation and subsequent Region heartbeat, rather than just PlanBalance.
func TestAffinityBalanceLeaderConvergence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	for i := uint64(1); i <= 3; i++ {
		start, end := []byte{byte(i)}, []byte{byte(i + 1)}
		id := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}[i-1]
		require.NoError(t, createAffinityGroupForTest(manager, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}, start, end))
		tc.AddLeaderRegion(i, 1, 2, 3)
		tc.PutRegion(tc.GetRegion(i).Clone(core.WithStartKey(start), core.WithEndKey(end)))
	}
	for i := uint64(1); i <= 3; i++ {
		checker.Check(tc.GetRegion(i))
	}
	checker.Check(tc.GetRegion(1))
	require.Eventually(t, func() bool {
		checker.Check(tc.GetRegion(1))
		return manager.GetAffinityGroupState("_tidb_pt_100_p1").LeaderStoreID != 1
	}, 3*time.Second, 10*time.Millisecond)
	target := manager.GetAffinityGroupState("_tidb_pt_100_p1")
	require.NotEqual(t, uint64(1), target.LeaderStoreID)
	ops := checker.Check(tc.GetRegion(1))
	require.Len(t, ops, 1, "a target change must produce a transfer operator, not merge using stale isAffinity")
	require.Equal(t, operator.OpLeader|operator.OpAffinity, ops[0].Kind())
	region := tc.GetRegion(1)
	peer := region.GetStorePeer(target.LeaderStoreID)
	require.NotNil(t, peer)
	tc.PutRegion(region.Clone(core.WithLeader(peer)))
	checker.Check(tc.GetRegion(1))
	require.Equal(t, affinity.PhaseStable, manager.GetAffinityGroupState(target.ID).Phase)
}

func TestAffinityManualRebalanceTableWithFixedRepresentative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	ids := []string{"_tidb_pt_200_p1", "_tidb_pt_200_p2", "_tidb_pt_200_p3"}
	for i, id := range ids {
		start, end := []byte{byte(i + 1)}, []byte{byte(i + 2)}
		require.NoError(t, createAffinityGroupForTest(manager, &affinity.Group{
			ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3},
		}, start, end))
		tc.AddLeaderRegion(uint64(i+1), 1, 2, 3)
		tc.PutRegion(tc.GetRegion(uint64(i+1)).Clone(core.WithStartKey(start), core.WithEndKey(end)))
	}
	for i := range ids {
		group, aligned := manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(uint64(i + 1)))
		require.True(t, aligned)
		if i == 0 {
			fixed := group.Group
			fixed.BalancePolicy = affinity.BalancePolicyFixed
			require.NoError(t, tc.GetStorage().RunInTxn(ctx, func(txn kv.Txn) error {
				return tc.GetStorage().SaveAffinityGroup(txn, fixed.ID, &fixed)
			}))
			manager.SyncGroupFromEtcd(&fixed)
		}
	}
	for i := range ids {
		checker.Check(tc.GetRegion(uint64(i + 1)))
	}
	for i := range ids {
		_, aligned := manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(uint64(i + 1)))
		require.True(t, aligned)
		group := manager.GetAffinityGroupState(ids[i])
		require.Equal(t, affinity.PhaseStable, group.Phase)
	}

	before := make(map[string]affinity.Group, len(ids))
	for _, id := range ids {
		before[id] = manager.GetAffinityGroupState(id).Group
	}
	preview, err := manager.RebalanceAffinityGroup(ctx, ids[0], false)
	require.NoError(t, err)
	require.Len(t, preview, len(ids))
	changed := 0
	for _, plan := range preview {
		require.Equal(t, before[plan.GroupID].ID, plan.GroupID)
		if plan.Changed {
			changed++
		}
		if plan.GroupID == ids[0] {
			require.False(t, plan.Changed)
			require.Equal(t, before[plan.GroupID].LeaderStoreID, plan.LeaderStoreID)
			require.Equal(t, before[plan.GroupID].VoterStoreIDs, plan.VoterStoreIDs)
		}
	}
	require.NotZero(t, changed)
	for _, id := range ids {
		require.Equal(t, before[id], manager.GetAffinityGroupState(id).Group)
	}

	applyCtx, stopApply := context.WithTimeout(ctx, 5*time.Second)
	defer stopApply()
	type applyResult struct {
		plans []affinity.BalancePlan
		err   error
	}
	result := make(chan applyResult, 1)
	go func() {
		plans, err := manager.RebalanceAffinityGroup(applyCtx, ids[0], true)
		result <- applyResult{plans: plans, err: err}
	}()
	// Deliver the operator's resulting heartbeats one group at a time. The
	// request must wait for these before publishing the next group's target.
	for planIndex, plan := range preview {
		if !plan.Changed {
			continue
		}
		require.Eventually(t, func() bool {
			return manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID == plan.LeaderStoreID
		}, time.Second, 10*time.Millisecond)
		for _, later := range preview[planIndex+1:] {
			require.Equal(t, before[later.GroupID], manager.GetAffinityGroupState(later.GroupID).Group,
				"the next target must wait for the current group to converge")
		}
		index := slices.Index(ids, plan.GroupID)
		region := tc.GetRegion(uint64(index + 1))
		ops := checker.Check(region)
		require.NotEmpty(t, ops)
		tc.PutRegion(region.Clone(core.WithLeader(region.GetStorePeer(plan.LeaderStoreID))))
		checker.Check(tc.GetRegion(region.GetID()))
	}
	completed := <-result
	require.NoError(t, completed.err)
	applied := completed.plans
	require.Len(t, applied, len(preview))
	for i := range preview {
		require.Equal(t, preview[i].GroupID, applied[i].GroupID)
		require.Equal(t, preview[i].LeaderStoreID, applied[i].LeaderStoreID)
		require.Equal(t, preview[i].VoterStoreIDs, applied[i].VoterStoreIDs)
	}
	for _, plan := range applied {
		state := manager.GetAffinityGroupState(plan.GroupID)
		require.Equal(t, plan.LeaderStoreID, state.LeaderStoreID)
		require.Equal(t, plan.VoterStoreIDs, state.VoterStoreIDs)
	}
	require.Equal(t, before[ids[0]].LeaderStoreID, manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, before[ids[0]].VoterStoreIDs, manager.GetAffinityGroupState(ids[0]).VoterStoreIDs)
}

func TestAffinityBalanceRequiresCompleteHealthyCoverage(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	require.NoError(t, createAffinityGroupForTest(manager, &affinity.Group{ID: "coverage", LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3}}, []byte("a"), []byte("c")))
	ranges := affinity.GroupKeyRanges{GroupID: "coverage",
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}}
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))
	_, ready := checker.buildBalanceActions(t.Context(), manager.GetAffinityGroupState("coverage"), ranges)
	require.False(t, ready, "cached affinity counts must not hide a coverage hole")
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(2).Clone(core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c"))))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(2))
	actions, ready := checker.buildBalanceActions(t.Context(), manager.GetAffinityGroupState("coverage"), ranges)
	require.True(t, ready)
	require.Contains(t, actions.LeaderTargets, uint64(2))
	require.NotEmpty(t, actions.VoterReplacements)
	tc.PutRegion(tc.GetRegion(2).Clone(core.WithPendingPeers([]*metapb.Peer{tc.GetRegion(2).GetStorePeer(2)})))
	_, ready = checker.buildBalanceActions(t.Context(), manager.GetAffinityGroupState("coverage"), ranges)
	require.False(t, ready, "one pending Region must prevent a group target change")
}

func TestAffinityBalanceProgressBuilderBoundsFits(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "progress", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("c")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(2).Clone(core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c"))))
	for _, id := range []uint64{1, 2} {
		manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(id))
	}
	state := manager.GetAffinityGroupState(group.ID)
	ranges := affinity.GroupKeyRanges{GroupID: group.ID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}}
	storeIDs := checker.balanceStoreIDs()
	const budget = 2
	for _, mode := range []affinity.BalanceActionMode{affinity.BalanceBuildLeaders, affinity.BalanceBuildVoters} {
		var cursor any
		var result affinity.BalanceActionBuildResult
		for {
			result = checker.buildBalanceActionsProgress(t.Context(), state, ranges, storeIDs, mode, cursor, budget)
			require.True(t, result.Ready)
			require.LessOrEqual(t, result.Checks, budget)
			if result.Complete {
				break
			}
			require.NotEmpty(t, result.Cursor)
			require.Empty(t, result.Actions.LeaderTargets)
			require.Empty(t, result.Actions.VoterReplacements, "partial candidates must not escape a page")
			cursor = result.Cursor
		}
		if mode == affinity.BalanceBuildLeaders {
			require.Contains(t, result.Actions.LeaderTargets, uint64(2))
		} else {
			require.Contains(t, result.Actions.VoterReplacements, uint64(1))
			require.Contains(t, result.Actions.VoterReplacements[1], uint64(4))
			require.True(t, slices.Contains(result.Actions.VoterReplacements[1][4], uint64(2)))
		}
	}
}

func TestAffinityBalanceAdmissionBlocksBuild(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "admission", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("b")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))
	checker.PauseOrResume(60)
	result := checker.buildBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID),
		affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}},
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.True(t, result.BlockTable)
	require.False(t, result.Ready)
}

func TestAffinityBalanceRoleSwitchAndStoreFilters(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLabelProperty("reject-leader", "reject", "yes")
	tc := mockcluster.NewCluster(t.Context(), opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	for id := uint64(1); id <= 5; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	tc.SetStoreLabel(1, map[string]string{"zone": "a"})
	tc.SetStoreLabel(2, map[string]string{"zone": "a"})
	tc.SetStoreLabel(3, map[string]string{"zone": "b"})
	tc.SetStoreLabel(4, map[string]string{"zone": "b", "reject": "yes"})
	tc.SetStoreLabel(5, map[string]string{"zone": "b", filter.SpecialUseKey: filter.SpecialUseHotRegion})
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "leader-a", Index: 1, Override: true, Role: placement.Leader, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"a"}}},
	}))
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2,
	}))
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "role-switch", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("b")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))
	state := manager.GetAffinityGroupState(group.ID)
	ranges := affinity.GroupKeyRanges{GroupID: group.ID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	stores := checker.balanceStoreIDs()

	voters := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores, affinity.BalanceBuildVoters, nil, 0)
	require.True(t, voters.Ready)
	require.True(t, voters.Complete)
	require.Contains(t, voters.Actions.VoterReplacements, uint64(1))
	require.Contains(t, voters.Actions.VoterReplacements[1], uint64(4))
	require.Contains(t, voters.Actions.VoterReplacements[1][4], uint64(2),
		"the replacement voter is a follower while another A store takes the leader role")
	require.NotContains(t, voters.Actions.VoterReplacements[1], uint64(5), "special-use stores are not voter destinations")

	leaders := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores, affinity.BalanceBuildLeaders, nil, 0)
	require.True(t, leaders.Complete)
	require.Contains(t, leaders.Actions.LeaderTargets, uint64(2))
	require.NotContains(t, leaders.Actions.LeaderTargets, uint64(4), "reject-leader stores cannot become leaders")

	require.NoError(t, tc.GetBasicCluster().PauseLeaderTransfer(1, constant.Out))
	blockedLeaderSource := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores,
		affinity.BalanceBuildVoters, nil, 0)
	require.NotContains(t, blockedLeaderSource.Actions.VoterReplacements[1][4], uint64(2),
		"a voter replacement that requires transferring from a paused leader is not actionable")

	tc.SetStoreBusy(2, true)
	blockedVoterSource := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores,
		affinity.BalanceBuildVoters, nil, 0)
	require.NotContains(t, blockedVoterSource.Actions.VoterReplacements, uint64(2),
		"busy source stores are excluded from voter replacements")

	tc.SetStoreOffline(4)
	offlineStores := checker.balanceStoreIDs()
	offline := checker.buildBalanceActionsProgress(t.Context(), state, ranges, offlineStores,
		affinity.BalanceBuildVoters, nil, 0)
	require.NotContains(t, offline.Actions.VoterReplacements[1], uint64(4))
	tc.SetStoreUp(4)
	tc.GetBasicCluster().ResumeLeaderTransfer(1, constant.Out)
	tc.SetStoreBusy(2, false)
	recovered := checker.buildBalanceActionsProgress(t.Context(), state, ranges, checker.balanceStoreIDs(),
		affinity.BalanceBuildVoters, nil, 0)
	require.Contains(t, recovered.Actions.VoterReplacements[1], uint64(4), "a recovered destination can be considered again")
}

func TestAffinityBalanceScheduleDenyBlocksAndRecovers(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "deny", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("c")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("c"))))
	region := tc.GetRegion(1)
	manager.GetAndCacheRegionAffinityGroupState(region)
	ranges := affinity.GroupKeyRanges{GroupID: group.ID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}}

	rule := &labeler.LabelRule{ID: "affinity-balance-deny", Index: 0,
		Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: labeler.KeyRange,
		Data: []any{map[string]any{"start_key": "61", "end_key": "63"}}}
	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(rule))
	manager.ObserveBalanceRegion(region)
	denied := checker.buildBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID), ranges,
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.False(t, denied.Ready)

	rule = &labeler.LabelRule{ID: rule.ID, Index: 0,
		Labels: []labeler.RegionLabel{{Key: "schedule", Value: "allow"}}, RuleType: labeler.KeyRange,
		Data: []any{map[string]any{"start_key": "61", "end_key": "63"}}}
	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(rule))
	manager.ObserveBalanceRegion(region)
	recovered := checker.buildBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID), ranges,
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.True(t, recovered.Ready)
	require.True(t, recovered.Complete)
}

func TestAffinityBalanceScheduleLimitZeroBlocksBuild(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetAffinityScheduleLimit(0)
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "limit-zero", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("b")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))
	result := checker.buildBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID),
		affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}},
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.True(t, result.BlockTable)
	require.False(t, result.Ready)
}
