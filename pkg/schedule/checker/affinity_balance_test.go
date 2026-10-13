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
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type scanCountingAffinityCluster struct {
	sche.CheckerCluster
	scanCalls int
}

func (c *scanCountingAffinityCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	c.scanCalls++
	return c.CheckerCluster.ScanRegions(startKey, endKey, limit)
}

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
		oneShot := checker.buildBalanceActionsProgress(t.Context(), state, ranges, storeIDs, mode, nil, 0)
		require.True(t, oneShot.Ready)
		require.True(t, oneShot.Complete)
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
		require.Equal(t, oneShot.Actions, result.Actions, "paged work must produce the same actions as one-shot work")
		if mode == affinity.BalanceBuildLeaders {
			require.ElementsMatch(t, []uint64{1, 2, 3}, result.Actions.LeaderTargets,
				"every feasible leader target must survive the first-region intersection")
			require.Contains(t, result.Actions.LeaderTargets, uint64(2))
		} else {
			require.Contains(t, result.Actions.VoterReplacements, uint64(1))
			require.Contains(t, result.Actions.VoterReplacements[1], uint64(4))
			require.True(t, slices.Contains(result.Actions.VoterReplacements[1][4], uint64(2)))
		}
	}
}

func TestAffinityBalanceProgressBudgetBoundsRegionScans(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "scan-budget", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("z")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("d"))))
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(2).Clone(core.WithStartKey([]byte("d")), core.WithEndKey([]byte("z"))))
	for _, id := range []uint64{1, 2} {
		manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(id))
	}
	countingCluster := &scanCountingAffinityCluster{CheckerCluster: tc}
	checker.cluster = countingCluster
	ranges := affinity.GroupKeyRanges{GroupID: group.ID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}, {StartKey: []byte("c"), EndKey: []byte("z")}}}
	storeIDs := checker.balanceStoreIDs()
	const budget = 1
	for _, mode := range []affinity.BalanceActionMode{affinity.BalanceBuildLeaders, affinity.BalanceValidateTarget} {
		var cursor any
		var result affinity.BalanceActionBuildResult
		scanCalls := countingCluster.scanCalls
		target := *group
		target.LeaderStoreID = 2
		for range 128 {
			previousScans := countingCluster.scanCalls
			result = checker.buildVirtualBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID), ranges,
				&target, storeIDs, mode, cursor, budget)
			require.LessOrEqual(t, countingCluster.scanCalls-previousScans, budget,
				"one page must not hide multiple ScanRegions calls inside the cursor")
			require.LessOrEqual(t, result.Checks, budget)
			require.True(t, result.Ready)
			require.False(t, result.InvalidTarget)
			if result.Complete {
				break
			}
			require.NotNil(t, result.Cursor)
			require.Empty(t, result.Actions.LeaderTargets)
			cursor = result.Cursor
		}
		require.True(t, result.Complete)
		require.GreaterOrEqual(t, countingCluster.scanCalls-scanCalls, 3, "the overlapping ranges revisit one Region before the next")
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
	checker.PauseOrResume(0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result = checker.buildBalanceActionsProgress(ctx, manager.GetAffinityGroupState(group.ID),
		affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}},
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.True(t, result.BlockTable)
	require.False(t, result.Ready)
	require.Empty(t, result.Actions.LeaderTargets)
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
	require.True(t, blockedLeaderSource.Ready)
	require.True(t, blockedLeaderSource.Complete)
	require.NotContains(t, blockedLeaderSource.Actions.VoterReplacements[1][4], uint64(2),
		"a voter replacement that requires transferring from a paused leader is not actionable")
	tc.GetBasicCluster().ResumeLeaderTransfer(1, constant.Out)

	tc.SetStoreBusy(2, true)
	blockedVoterSource := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores,
		affinity.BalanceBuildVoters, nil, 0)
	require.True(t, blockedVoterSource.Ready)
	require.True(t, blockedVoterSource.Complete)
	require.True(t, blockedVoterSource.TemporaryBlocked)
	require.False(t, blockedVoterSource.BlockTable)
	require.NotContains(t, blockedVoterSource.Actions.VoterReplacements, uint64(2),
		"busy source stores are excluded from voter replacements")

	tc.SetStoreOffline(4)
	offlineStores := checker.balanceStoreIDs()
	offline := checker.buildBalanceActionsProgress(t.Context(), state, ranges, offlineStores,
		affinity.BalanceBuildVoters, nil, 0)
	require.NotContains(t, offline.Actions.VoterReplacements[1], uint64(4))
	tc.SetStoreUp(4)
	tc.SetStoreBusy(2, false)
	recovered := checker.buildBalanceActionsProgress(t.Context(), state, ranges, checker.balanceStoreIDs(),
		affinity.BalanceBuildVoters, nil, 0)
	require.Contains(t, recovered.Actions.VoterReplacements[1], uint64(4), "a recovered destination can be considered again")
}

func TestAffinityBalanceTemporaryLeaderLimitKeepsOtherCandidates(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "temporary-candidate", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("c")))
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("c")), core.SetApproximateSize(10)))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))
	state := manager.GetAffinityGroupState(group.ID)
	ranges := affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}}
	stores := checker.balanceStoreIDs()
	page := checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores, affinity.BalanceBuildLeaders, nil, 1)
	require.NotNil(t, page.Cursor)
	exhaust := func(id uint64, typ storelimit.Type) {
		tc.SetStoreLimit(id, typ, 0.00006)
		tc.ResetStoreLimit(id, typ, 0.000001)
		require.True(t, tc.GetStore(id).GetStoreLimit().Take(storelimit.RegionInfluence[typ], typ, constant.Medium))
		require.False(t, tc.GetStore(id).IsAvailable(typ, constant.Medium))
	}
	exhaust(2, storelimit.TransferLeaderIn)
	cursor := page.Cursor
	for range 50 {
		page = checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores, affinity.BalanceBuildLeaders, cursor, 1)
		require.True(t, page.Ready)
		if page.Complete {
			break
		}
		cursor = page.Cursor
	}
	require.True(t, page.Complete)
	require.True(t, page.TemporaryBlocked)
	require.NotContains(t, page.Actions.LeaderTargets, uint64(2))
	require.Contains(t, page.Actions.LeaderTargets, uint64(3))
	ready, _ := checker.validateBalanceAction(t.Context(), state, ranges,
		affinity.BalancePlan{LeaderStoreID: 2, VoterStoreIDs: group.VoterStoreIDs})
	require.False(t, ready, "cached actions must recheck the receiving store's limit")
	ready, legal := checker.validateBalanceAction(t.Context(), state, ranges,
		affinity.BalancePlan{LeaderStoreID: 3, VoterStoreIDs: group.VoterStoreIDs})
	require.True(t, ready)
	require.True(t, legal)
	tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
	page = checker.buildBalanceActionsProgress(t.Context(), state, ranges, stores, affinity.BalanceBuildLeaders, nil, 0)
	require.True(t, page.Complete)
	require.False(t, page.TemporaryBlocked)
	require.Contains(t, page.Actions.LeaderTargets, uint64(2))
	plan := affinity.BalancePlan{LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}}
	exhaust(4, storelimit.AddPeer)
	ready, _ = checker.validateBalanceAction(t.Context(), state, ranges, plan)
	require.False(t, ready, "voter targets also need a fresh execution check")
}

func TestAffinityBalancePagesReturnOnlyNewRegionBytes(t *testing.T) {
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "page-bytes", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("c")))
	for i := uint64(1); i <= 2; i++ {
		tc.AddLeaderRegion(i, 1, 2, 3)
		tc.PutRegion(tc.GetRegion(i).Clone(core.WithStartKey([]byte{byte('a' + i - 1)}),
			core.WithEndKey([]byte{byte('a' + i)}), core.SetApproximateSize(10)))
		manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(i))
	}
	state := manager.GetAffinityGroupState(group.ID)
	ranges := affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}}
	var cursor any
	observed := 0
	var result affinity.BalanceActionBuildResult
	for range 100 {
		result = checker.buildBalanceActionsProgress(t.Context(), state, ranges, checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, cursor, 1)
		require.True(t, result.Ready)
		observed += len(result.RegionBytes)
		if result.Complete {
			break
		}
		cursor = result.Cursor
	}
	require.True(t, result.Complete)
	require.Equal(t, 2, observed, "each Region size should be reported once, regardless of page count")
	require.Equal(t, uint64(20*1024*1024), result.Actions.MoveBytes)
}

func TestAffinityBalanceTemporaryBestLocationBlocksTable(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLocationLabels([]string{"region", "zone", "host"})
	tc := mockcluster.NewCluster(t.Context(), opt)
	tc.AddLabelsStore(1, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(3, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h2"})
	tc.AddLabelsStore(4, 0, map[string]string{"region": "r1", "zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h1"})
	tc.AddLabelsStore(6, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h2"})
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	group := &affinity.Group{ID: "best-location-block", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 3, 4}}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte("a"), []byte("b")))
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.PutRegion(tc.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b"))))
	region := tc.GetRegion(1)
	manager.GetAndCacheRegionAffinityGroupState(region)
	tc.SetStoreBusy(1, true)
	tc.SetStoreBusy(3, true)
	result := checker.buildBalanceActionsProgress(t.Context(), manager.GetAffinityGroupState(group.ID),
		affinity.GroupKeyRanges{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}},
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 8)
	require.True(t, result.BlockTable)
	require.False(t, result.Ready)
	require.Empty(t, result.Actions.LeaderTargets)
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
		checker.balanceStoreIDs(), affinity.BalanceBuildLeaders, nil, 0)
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
