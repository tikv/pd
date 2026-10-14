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
	"encoding/hex"
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
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type affinityBalanceScanCluster struct {
	sche.CheckerCluster
	delay      time.Duration
	starts     [][]byte
	labelReads int
	beforeScan func()
}

func (c *affinityBalanceScanCluster) GetRegionLabeler() *labeler.RegionLabeler {
	c.labelReads++
	return c.CheckerCluster.GetRegionLabeler()
}

func (c *affinityBalanceScanCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	if c.beforeScan != nil {
		c.beforeScan()
	}
	c.starts = append(c.starts, slices.Clone(startKey))
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.CheckerCluster.ScanRegions(startKey, endKey, limit)
}

func newAffinityBalanceTestCluster(t *testing.T, storeCount uint64) (*mockcluster.Cluster, *AffinityChecker) {
	t.Helper()
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= storeCount; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	return tc, newTestAffinityChecker(t.Context(), tc, opt)
}

func createBalanceGroup(t *testing.T, manager *affinity.Manager, id, start, end string,
	leader uint64, voters []uint64, policy affinity.BalancePolicy) {
	t.Helper()
	group := &affinity.Group{ID: id, LeaderStoreID: leader, VoterStoreIDs: voters}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte(start), []byte(end)))
	if policy == affinity.BalancePolicyFixed {
		_, err := manager.UpdateAffinityGroupPolicy(id, affinity.BalancePolicyFixed)
		require.NoError(t, err)
	}
}

func addBalanceRegion(t *testing.T, tc *mockcluster.Cluster, manager *affinity.Manager, id uint64,
	start, end string, leader uint64, voters ...uint64) *core.RegionInfo {
	t.Helper()
	followers := slices.DeleteFunc(slices.Clone(voters), func(id uint64) bool { return id == leader })
	tc.AddLeaderRegion(id, leader, followers...)
	tc.PutRegion(tc.GetRegion(id).Clone(core.WithStartKey([]byte(start)), core.WithEndKey([]byte(end))))
	region := tc.GetRegion(id)
	group, _ := manager.GetAndCacheRegionAffinityGroupState(region)
	require.NotNil(t, group)
	return region
}

func runAffinityBalanceTask(t *testing.T, ctx context.Context, checker *AffinityChecker, task *affinityBalanceTask) bool {
	t.Helper()
	for range 1000 {
		stable, resume, err := checker.balancer.step(ctx, "_tidb_pt_100", task)
		require.NoError(t, err)
		if !resume {
			return stable
		}
	}
	t.Fatal("affinity balance task did not finish after repeated resumes")
	return false
}

func TestAffinityBalanceLeaderConvergence(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	starts := []string{"a", "b", "c"}
	ends := []string{"b", "c", "d"}
	for i := uint64(1); i <= 3; i++ {
		id := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}[i-1]
		createBalanceGroup(t, manager, id, starts[i-1], ends[i-1], 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
		addBalanceRegion(t, tc, manager, i, starts[i-1], ends[i-1], 1, 1, 2, 3)
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

func TestAffinityBalanceRequiresCompleteRangeCoverage(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "d", "e", 1, []uint64{1, 2, 4}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 3, "d", "e", 1, 1, 2, 4)

	task := &affinityBalanceTask{}
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"a gap in the group's key range must prevent a target update")

	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	task = &affinityBalanceTask{}
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"the improvement may commit once the complete range is covered")
}

func TestAffinityBalancePendingRegionAndOperatorBlockOtherGroup(t *testing.T) {
	for _, blocker := range []string{"pending-region", "waiting-operator"} {
		t.Run(blocker, func(t *testing.T) {
			tc, checker := newAffinityBalanceTestCluster(t, 4)
			manager := tc.GetAffinityManager()
			createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
			createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 2, 4}, affinity.BalancePolicyAuto)
			addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
			region := addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 4)
			if blocker == "pending-region" {
				region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)}))
				tc.PutRegion(region)
				manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(region.GetID()))
			} else {
				controller := operator.NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
				checker.opController = controller
				op, err := operator.CreateTransferLeaderOperator("affinity-balance-test", tc, region, 2,
					[]uint64{1, 2, 4}, operator.OpAffinity)
				require.NoError(t, err)
				require.Equal(t, 1, controller.AddWaitingOperator(op))
			}

			require.False(t, runAffinityBalanceTask(t, t.Context(), checker, &affinityBalanceTask{}))
			require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
				"an unready Region in another group must block committing this group's candidate")
		})
	}
}

func TestAffinityBalanceSynchronousValidationStatuses(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "validation", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	actual := manager.GetAffinityGroupState("validation")
	target := actual.Group
	target.LeaderStoreID = 2
	target.VoterStoreIDs = []uint64{1, 2, 4}

	require.Equal(t, balanceAllowed, checker.validateBalanceRegion(actual, &target, region, nil))
	require.Equal(t, balanceUnready, checker.validateBalanceRegion(actual, &target, region,
		map[uint64]struct{}{region.GetID(): {}}), "a waiting operator must keep the Region out of a group commit")
	invalid := target
	invalid.LeaderStoreID = 4
	invalid.VoterStoreIDs = []uint64{1, 2, 3}
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &invalid, region, nil))

	actualGroup := actual.Group
	require.Equal(t, balanceAllowed, checker.balanceTargetExecutionStatus(&actualGroup, &target))
	tc.SetStoreBusy(3, true)
	require.Equal(t, balanceTemporarilyBlocked, checker.balanceTargetExecutionStatus(&actualGroup, &target),
		"a busy source store must temporarily block the move")
	tc.SetStoreBusy(3, false)
	tc.SetStoreBusy(2, true)
	require.Equal(t, balanceTemporarilyBlocked, checker.balanceTargetExecutionStatus(&actualGroup, &target))
	tc.SetStoreBusy(2, false)
	tc.SetStoreOffline(4)
	require.Equal(t, balanceRejected, checker.balanceTargetExecutionStatus(&actualGroup, &target))
}

func TestAffinityBalanceRejectsIsolationRegression(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLocationLabels([]string{"zone", "host"})
	tc := mockcluster.NewCluster(t.Context(), opt)
	stores := map[uint64]map[string]string{
		1: {"zone": "z1", "host": "h1"},
		2: {"zone": "z1", "host": "h2"},
		3: {"zone": "z1", "host": "h3"},
		4: {"zone": "z2", "host": "h1"},
		5: {"zone": "z3", "host": "h1"},
	}
	for id, labels := range stores {
		tc.AddLabelsStore(id, 10, labels)
		tc.SetStoreUp(id)
	}
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	require.NotNil(t, rule)
	rule.LocationLabels = []string{"zone", "host"}
	rule.IsolationLevel = "zone"
	require.NoError(t, tc.GetRuleManager().SetRule(rule))

	checker := newTestAffinityChecker(t.Context(), tc, opt)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "isolation", "a", "b", 1, []uint64{1, 4, 5}, affinity.BalancePolicyAuto)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 4, 5)
	actual := manager.GetAffinityGroupState("isolation")
	target := actual.Group
	target.VoterStoreIDs = []uint64{1, 3, 5}

	require.Equal(t, balanceAllowed, checker.validateBalanceRegion(actual, nil, region, nil))
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &target, region, nil),
		"a target with worse zone isolation must be rejected")
}

func TestAffinityBalanceRejectsLearnerStoreAsVoter(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 5)
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
	}))
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 2, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	region = region.Clone(core.SetPeers(append(slices.Clone(region.GetPeers()), &metapb.Peer{
		Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner,
	})))
	tc.PutRegion(region)
	actual, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
	require.True(t, aligned)
	require.True(t, tc.GetRuleManager().FitRegionWithoutCache(tc, region).IsSatisfied())
	target := actual.Group
	target.VoterStoreIDs = []uint64{1, 2, 4}
	require.Nil(t, cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...))
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &target, region, nil))

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, &affinityBalanceTask{}))
	targetState := manager.GetAffinityGroupState(actual.ID)
	require.NotContains(t, targetState.VoterStoreIDs, uint64(4), "a preserved learner must not become a target voter")
	require.Contains(t, targetState.VoterStoreIDs, uint64(5), "another legal destination remains usable")
	require.NotNil(t, checker.createAffinityOperator(region, targetState))
}

func setupLeaderLimitBalanceTable(t *testing.T, tc *mockcluster.Cluster) *affinity.Manager {
	t.Helper()
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 4, 5}, affinity.BalancePolicyFixed)
	createBalanceGroup(t, manager, "_tidb_pt_100_p2", "c", "d", 4, []uint64{4, 5, 6}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 4, 5)
	addBalanceRegion(t, tc, manager, 3, "c", "d", 4, 4, 5, 6)
	return manager
}

func exhaustLeaderTransferIn(t *testing.T, tc *mockcluster.Cluster, id uint64) {
	t.Helper()
	tc.SetStoreLimit(id, storelimit.TransferLeaderIn, 0.00006)
	tc.ResetStoreLimit(id, storelimit.TransferLeaderIn, 0.000001)
	require.True(t, tc.GetStore(id).GetStoreLimit().Take(
		storelimit.RegionInfluence[storelimit.TransferLeaderIn], storelimit.TransferLeaderIn, constant.Medium))
	require.False(t, tc.GetStore(id).IsAvailable(storelimit.TransferLeaderIn, constant.Medium))
}

func restoreLeaderTransferIn(tc *mockcluster.Cluster, id uint64) {
	tc.SetStoreLimit(id, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(id, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
}

func TestAffinityBalanceTemporaryLeaderLimitUsesNextCandidate(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 6)
	manager := setupLeaderLimitBalanceTable(t, tc)
	exhaustLeaderTransferIn(t, tc, 2)

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, &affinityBalanceTask{}))
	require.Equal(t, uint64(3), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"a temporarily rate-limited candidate must not hide another legal improvement")
}

func TestAffinityBalanceAllLeaderLimitsRetryBeforeVoterMoves(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 6)
	manager := setupLeaderLimitBalanceTable(t, tc)
	exhaustLeaderTransferIn(t, tc, 2)
	exhaustLeaderTransferIn(t, tc, 3)
	task := &affinityBalanceTask{}
	stable, resume, err := checker.balancer.step(t.Context(), affinity.TableGroupPrefix("_tidb_pt_100_p0"), task)
	require.NoError(t, err)
	require.False(t, stable)
	require.False(t, resume)
	require.NotNil(t, task.search)
	require.Equal(t, balanceFindLeader, task.search.phase,
		"temporarily blocked leader candidates must not be treated as stable or fall through to voter replacements")
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)

	// The background worker drops terminal unfinished searches before retrying.
	task.search = nil
	restoreLeaderTransferIn(tc, 2)
	restoreLeaderTransferIn(tc, 3)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"the next attempt can use the recovered candidate")
}

func TestAffinityBalanceBusyVoterDestinationRetries(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 2, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 2, 1, 2, 3)
	tc.SetStoreBusy(4, true)
	task := &affinityBalanceTask{}
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.True(t, task.search.blocked, "a busy destination must not make a complete no-action result")
	require.Equal(t, balanceFindVoter, task.search.phase)
	require.NotContains(t, manager.GetAffinityGroupState("_tidb_pt_100_p0").VoterStoreIDs, uint64(4))

	tc.SetStoreBusy(4, false)
	task.search = nil
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.Contains(t, manager.GetAffinityGroupState("_tidb_pt_100_p0").VoterStoreIDs, uint64(4))
}

func TestAffinityBalanceRecoveredDestinationInvalidatesStable(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	tc.SetMaxStoreDownTime(time.Minute)
	tc.SetStoreLastHeartbeatInterval(4, 2*time.Minute)
	require.False(t, tc.GetStore(4).IsUnhealthy(), "the configured down threshold is below the fixed unhealthy threshold")
	manager := tc.GetAffinityManager()
	ids := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}
	keys := []string{"a", "b", "c", "d"}
	for i, id := range ids {
		leader := uint64(i + 1)
		createBalanceGroup(t, manager, id, keys[i], keys[i+1], leader, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
		addBalanceRegion(t, tc, manager, leader, keys[i], keys[i+1], leader, 1, 2, 3)
	}

	task := &affinityBalanceTask{}
	require.True(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	task.version, task.fingerprint, task.stable = task.search.snapshot.Version, task.search.fingerprint, true
	task.search = nil
	balancer := &affinityBalancer{checker: checker, tables: map[string]*affinityBalanceTask{"_tidb_pt_100": task}, queue: make(chan string, 1)}
	balancer.enqueue(ids[0])
	require.Empty(t, balancer.queue, "an unchanged stable table should remain cached")

	tc.SetStoreUp(4)
	// Snapshot creation samples the same store fingerprint as the availability loop.
	recovered := manager.GetBalanceSnapshot(ids[0])
	require.Greater(t, recovered.Version.Stores, task.version.Stores)
	require.Equal(t, task.version.Table, recovered.Version.Table, "recovery of an unused destination does not change the table's Regions")
	balancer.enqueue(ids[0])
	require.Len(t, balancer.queue, 1, "destination recovery must wake the stable table")
	<-balancer.queue
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.Contains(t, manager.GetAffinityGroupState(ids[0]).VoterStoreIDs, uint64(4))
}

func TestAffinityBalanceFingerprintLabelRanges(t *testing.T) {
	for _, test := range []struct {
		name       string
		ranges     []keyutil.KeyRange
		labelKeys  []string
		intersects bool
	}{
		{"disjoint", []keyutil.KeyRange{keyutil.NewKeyRange("c", "d")}, []string{"a", "b"}, false},
		{"adjacent before", []keyutil.KeyRange{keyutil.NewKeyRange("b", "d")}, []string{"a", "b"}, false},
		{"adjacent after", []keyutil.KeyRange{keyutil.NewKeyRange("b", "d")}, []string{"d", "e"}, false},
		{"overlap", []keyutil.KeyRange{keyutil.NewKeyRange("b", "d")}, []string{"c", "e"}, true},
		{"equal", []keyutil.KeyRange{keyutil.NewKeyRange("b", "d")}, []string{"b", "d"}, true},
		{"unsorted adjacent", []keyutil.KeyRange{keyutil.NewKeyRange("d", "f"), keyutil.NewKeyRange("b", "d")}, []string{"c", "e"}, true},
		{"nested overlaps", []keyutil.KeyRange{keyutil.NewKeyRange("d", "e"), keyutil.NewKeyRange("b", "h"), keyutil.NewKeyRange("c", "d")}, []string{"f", "g"}, true},
		{"gap", []keyutil.KeyRange{keyutil.NewKeyRange("f", "h"), keyutil.NewKeyRange("b", "d")}, []string{"d", "f"}, false},
		{"crossing gap", []keyutil.KeyRange{keyutil.NewKeyRange("f", "h"), keyutil.NewKeyRange("b", "d")}, []string{"c", "g"}, true},
		{"unbounded start", []keyutil.KeyRange{keyutil.NewKeyRange("", "b")}, []string{"", "a"}, true},
		{"unbounded end", []keyutil.KeyRange{keyutil.NewKeyRange("b", "")}, []string{"z", ""}, true},
		{"unbounded nested", []keyutil.KeyRange{keyutil.NewKeyRange("f", "h"), keyutil.NewKeyRange("b", "")}, []string{"z", ""}, true},
		{"later label range", []keyutil.KeyRange{keyutil.NewKeyRange("d", "f")}, []string{"a", "b", "e", "g"}, true},
		{"empty table", nil, []string{"a", "b"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			tc, checker := newAffinityBalanceTestCluster(t, 3)
			snapshot := &affinity.BalanceSnapshot{
				Groups: []*affinity.GroupState{{Group: affinity.Group{ID: "fingerprint"}}},
				Ranges: map[string]affinity.GroupKeyRanges{"fingerprint": {KeyRanges: test.ranges}},
			}
			previous := checker.balanceInputFingerprint(snapshot)
			keys := make([]string, len(test.labelKeys))
			for i, key := range test.labelKeys {
				keys[i] = hex.EncodeToString([]byte(key))
			}
			for _, value := range []string{"first", "second"} {
				require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
					ID: "fingerprint-label", RuleType: labeler.KeyRange,
					Labels: []labeler.RegionLabel{{Key: "balance-test", Value: value}}, Data: labeler.MakeKeyRanges(keys...),
				}))
				current := checker.balanceInputFingerprint(snapshot)
				if test.intersects {
					require.NotEqual(t, previous, current, "intersecting label additions and updates must invalidate the search")
				} else {
					require.Equal(t, previous, current, "unrelated label changes must preserve the search")
				}
				previous = current
			}
		})
	}
}

func TestAffinityBalanceYieldPreservesRangeCursorAndLocalProgress(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "d", "e", 1, []uint64{1, 2, 4}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 3, "d", "e", 1, 1, 2, 4)

	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc, delay: affinityBalanceTurn + 5*time.Millisecond}
	checker.cluster = scanCluster
	task := &affinityBalanceTask{}
	_, resume, err := checker.balancer.step(t.Context(), affinity.TableGroupPrefix("_tidb_pt_100_p0"), task)
	require.NoError(t, err)
	require.True(t, resume)
	require.Len(t, scanCluster.starts, 1)
	require.Equal(t, []byte("b"), task.search.scan.nextKey,
		"the scan should retain the first completed Region when the outer turn expires")

	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "unrelated-balance-input", Index: 1,
		Labels:   []labeler.RegionLabel{{Key: "balance-test", Value: "unrelated"}},
		RuleType: labeler.KeyRange, Data: labeler.MakeKeyRanges("79", "7a"),
	}))
	previous := task.search
	search, err := checker.balancer.prepareSearch(affinity.TableGroupPrefix("_tidb_pt_100_p0"), task)
	require.NoError(t, err)
	require.Same(t, previous, search, "an unrelated label rule update must preserve this table's in-progress search")
	require.Equal(t, []byte("b"), search.scan.nextKey)

	scanCluster.delay = 0
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.GreaterOrEqual(t, len(scanCluster.starts), 2)
	require.Equal(t, []byte("b"), scanCluster.starts[1], "resume should continue at the next key")
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)
}

func TestAffinityBalanceStableResultUsesCachedVersion(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc}
	checker.cluster = scanCluster
	task := &affinityBalanceTask{}
	require.True(t, runAffinityBalanceTask(t, t.Context(), checker, task))
	require.NotNil(t, task.search)
	require.Empty(t, scanCluster.starts, "fixed groups need no Region candidate scan")

	task.version, task.fingerprint, task.stable = task.search.snapshot.Version, task.search.fingerprint, true
	task.search = nil // the worker discards a completed search
	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "unrelated-stable-input", Index: 2,
		Labels:   []labeler.RegionLabel{{Key: "balance-test", Value: "unrelated"}},
		RuleType: labeler.KeyRange, Data: labeler.MakeKeyRanges("79", "7a"),
	}))
	stable, resume, err := checker.balancer.step(t.Context(), affinity.TableGroupPrefix("_tidb_pt_100_p0"), task)
	require.NoError(t, err)
	require.True(t, stable)
	require.False(t, resume)
	require.Empty(t, scanCluster.starts, "an unchanged table with an unrelated global label update should remain cached")
}

func TestAffinityBalanceReusesFingerprintAfterRegionOrStoreChange(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 2, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc}
	checker.cluster = scanCluster
	task := &affinityBalanceTask{}
	search, err := checker.balancer.prepareSearch("_tidb_pt_100", task)
	require.NoError(t, err)
	require.Equal(t, 1, scanCluster.labelReads)

	manager.ObserveBalanceRegion(region.Clone(core.WithLeader(region.GetStorePeer(2))))
	current, err := checker.balancer.prepareSearch("_tidb_pt_100", task)
	require.NoError(t, err)
	require.NotSame(t, search, current, "Region changes must restart validation")
	require.Equal(t, 1, scanCluster.labelReads, "Region changes need no metadata rescan")

	tc.SetStoreLabel(4, map[string]string{"zone": "new"})
	manager.GetBalanceSnapshot("_tidb_pt_100") // The availability loop samples the same store fingerprint.
	search, err = checker.balancer.prepareSearch("_tidb_pt_100", task)
	require.NoError(t, err)
	require.NotSame(t, current, search, "store changes must restart candidate search")
	require.Equal(t, 1, scanCluster.labelReads, "store changes need no metadata rescan")

	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "changed-balance-metadata", RuleType: labeler.KeyRange,
		Labels: []labeler.RegionLabel{{Key: "balance-test", Value: "changed"}}, Data: labeler.MakeKeyRanges("61", "62"),
	}))
	current, err = checker.balancer.prepareSearch("_tidb_pt_100", task)
	require.NoError(t, err)
	require.Equal(t, 2, scanCluster.labelReads, "label changes must refresh the metadata fingerprint")
	require.NotEqual(t, search.fingerprint, current.fingerprint)
}

func TestAffinityBalanceRoleReassignmentCanReplaceOldLeader(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLabelProperty("reject-leader", "reject", "yes")
	tc := mockcluster.NewCluster(t.Context(), opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	for id := uint64(1); id <= 6; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	for _, id := range []uint64{1, 2, 5} {
		tc.SetStoreLabel(id, map[string]string{"zone": "a"})
	}
	for _, id := range []uint64{3, 4, 6} {
		tc.SetStoreLabel(id, map[string]string{"zone": "b"})
	}
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "leader-a", Index: 1, Override: true, Role: placement.Leader, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"a"}}},
	}))
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2,
	}))
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 5, []uint64{1, 5, 6}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 5, 1, 5, 6)

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker, &affinityBalanceTask{}))
	group := manager.GetAffinityGroupState("_tidb_pt_100_p0")
	require.Equal(t, uint64(2), group.LeaderStoreID)
	require.ElementsMatch(t, []uint64{2, 3, 4}, group.VoterStoreIDs,
		"the improving role move may remove the old leader once the new leader remains a voter")
}
