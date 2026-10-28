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
	"fmt"
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

const balanceTestTable affinity.TableID = "_tidb_pt_100"

type affinityBalanceTest struct {
	*mockcluster.Cluster
	*AffinityChecker
	tb testing.TB
}

func newAffinityBalanceTest(t testing.TB, stores uint64, zones ...string) *affinityBalanceTest {
	t.Helper()
	re := require.New(t)
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	for id := uint64(1); id <= stores; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
		// Keep stores connected through long benchmark runs.
		tc.SetStoreLastHeartbeatInterval(id, -time.Hour)
	}
	// Directly driven tests and benchmarks do not start a worker goroutine.
	c := &AffinityChecker{cluster: tc, ruleManager: tc.GetRuleManager(), affinityManager: tc.GetAffinityManager(), conf: tc.PersistOptions}
	c.balancer = &affinityBalancer{checker: c, tables: make(map[affinity.TableID]struct{}), idle: make(map[affinity.TableID]balanceIdle)}
	if len(zones) > 0 {
		for id := uint64(1); id <= stores; id++ {
			tc.SetStoreLabel(id, map[string]string{"zone": zones[(id-1)%uint64(len(zones))]})
		}
		rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
		rule.LocationLabels = []string{"zone"}
		re.NoError(c.ruleManager.SetRule(rule))
	}
	return &affinityBalanceTest{Cluster: tc, AffinityChecker: c, tb: t}
}

func (f *affinityBalanceTest) group(id string, firstRegion, leader uint64, voters []uint64, keys ...string) {
	f.tb.Helper()
	re := require.New(f.tb)
	group := &affinity.Group{ID: id, LeaderStoreID: leader, VoterStoreIDs: voters}
	re.NoError(createAffinityGroupForTest(f.affinityManager, group, []byte(keys[0]), []byte(keys[len(keys)-1])))
	followers := slices.DeleteFunc(slices.Clone(voters), func(id uint64) bool { return id == leader })
	for i := 0; i+1 < len(keys); i++ {
		regionID := firstRegion + uint64(i)
		f.AddLeaderRegionWithRange(regionID, keys[i], keys[i+1], leader, followers...)
		_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(f.GetRegion(regionID))
		re.True(aligned)
	}
}

func (f *affinityBalanceTest) seed(table string, firstRegion uint64, leaders ...uint64) []string {
	f.tb.Helper()
	ids := make([]string, 0, len(leaders))
	for i, leader := range leaders {
		id := fmt.Sprintf("_tidb_pt_%s_p%d", table, i)
		f.group(id, firstRegion+uint64(i), leader, []uint64{1, 2, 3}, fmt.Sprintf("%s-%04d", table, i), fmt.Sprintf("%s-%04d", table, i+1))
		ids = append(ids, id)
	}
	return ids
}

func (f *affinityBalanceTest) policy(policy affinity.BalancePolicy, ids ...string) {
	f.tb.Helper()
	re := require.New(f.tb)
	for _, id := range ids {
		_, err := f.affinityManager.UpdateAffinityGroupPolicy(id, policy)
		re.NoError(err)
	}
}

func (f *affinityBalanceTest) snapshot() *affinity.BalanceSnapshot {
	return f.affinityManager.GetBalanceSnapshot(balanceTestTable, f.affinityManager.GetBalanceCounts())
}

func (f *affinityBalanceTest) step() bool {
	f.tb.Helper()
	re := require.New(f.tb)
	written, err := f.balancer.step(f.tb.Context(), balanceTestTable, f.affinityManager.GetBalanceCounts())
	re.NoError(err)
	return written
}

func balanceTableScore(m *affinity.Manager, table affinity.TableID) [2]int {
	voters, leaders := make(map[uint64]int), make(map[uint64]int)
	for _, g := range m.GetBalanceSnapshot(table, m.GetBalanceCounts()).Groups {
		leaders[g.LeaderStoreID]++
		for _, id := range g.VoterStoreIDs {
			voters[id]++
		}
	}
	var score [2]int
	for i, counts := range []map[uint64]int{voters, leaders} {
		for _, n := range counts {
			score[i] += n * n
		}
	}
	return score
}

func TestAffinityBalanceLeaderConvergence(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	ids := f.seed("100", 1, 1, 1, 1)
	m := f.affinityManager
	c := newTestAffinityChecker(t.Context(), f.Cluster, f.PersistOptions)
	custom := []string{"custom", "_tidb_pt_100"}
	for _, id := range custom {
		re.NoError(createAffinityGroupForTest(m, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}, nil, nil))
		c.balancer.enqueue(id)
	}
	for i := range ids {
		re.Empty(c.Check(f.GetRegion(uint64(i + 1))))
	}
	re.Eventually(func() bool { return balanceTableScore(m, balanceTestTable) == [2]int{27, 3} }, 10*time.Second, 10*time.Millisecond)
	c.balancer.mu.Lock()
	registered := len(c.balancer.tables)
	c.balancer.mu.Unlock()
	re.Equal(1, registered)
	for i, id := range ids {
		group, region := m.GetAffinityGroupState(id), f.GetRegion(uint64(i+1))
		if group.LeaderStoreID != region.GetLeader().GetStoreId() {
			ops := c.Check(region)
			re.Len(ops, 1)
			re.Equal(operator.OpLeader|operator.OpAffinity, ops[0].Kind())
			operator.ApplyOperator(f.Cluster, ops[0])
			c.Check(f.GetRegion(region.GetID()))
		}
		re.Equal(affinity.PhaseStable, m.GetAffinityGroupState(id).Phase)
	}
	for _, id := range custom {
		state := m.GetAffinityGroupState(id)
		re.Equal(uint64(1), state.LeaderStoreID)
		re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
	}
}

func TestAffinityBalancePlacement(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "c", "a")
	f.SetLabelProperty("reject-leader", "zone", "c")
	f.SetLabelProperty("reject-leader", "zone", "a")
	f.PutStore(f.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
	ids := f.seed("100", 1, 1, 2, 3)
	f.policy(affinity.BalancePolicyFixed, ids[1:]...)
	region := f.GetRegion(1)
	current := f.affinityManager.GetAffinityGroupState(ids[0])
	fit, status := f.validateBalanceCurrentRegion(current, region)
	re.Equal(balanceAllowed, status)
	// A target may satisfy its rules and still lose isolation.
	target := current.Group
	target.VoterStoreIDs = []uint64{1, 2, 4}
	worse := f.ruleManager.FitRegionWithoutCache(f.Cluster, cloneRegionWithReplacePeerStores(region, 1, target.VoterStoreIDs...))
	re.True(worse.IsSatisfied())
	re.False(placementNotWorse(fit, worse))
	re.Equal(balanceRejected, f.validateBalanceTarget(&target, region, fit))
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.IsolationLevel = "zone"
	re.NoError(f.ruleManager.SetRule(rule))
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable))
	re.True(f.step())
	group := f.affinityManager.GetAffinityGroupState(ids[0])
	re.Equal([]uint64{2, 3, 4}, group.VoterStoreIDs)
	re.Equal(uint64(2), group.LeaderStoreID)
	re.Equal([2]int{23, 5}, balanceTableScore(f.affinityManager, balanceTestTable))
}

func TestAffinityBalanceLearnerInLaterRegion(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 6)
	f.SetEnableUseJointConsensus(false)
	f.SetStoreLabel(6, map[string]string{"engine": "tiflash"})
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"other"}}}}))
	ids := []string{"_tidb_pt_100_p0", "_tidb_pt_100_p1", "_tidb_pt_100_p2"}
	f.group(ids[0], 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
	f.group(ids[1], 3, 2, []uint64{1, 2, 3}, "c", "d", "e")
	f.group(ids[2], 5, 3, []uint64{1, 2, 3}, "f", "g")
	f.policy(affinity.BalancePolicyFixed, ids[2])
	for _, region := range f.GetRegions() {
		f.PutRegion(region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100 + region.GetID(), StoreId: 6, Role: metapb.PeerRole_Learner})))
	}
	initial, err := f.planBalanceTargets(t.Context(), f.snapshot())
	re.NoError(err)
	index := slices.IndexFunc(initial.targets, func(g *affinity.Group) bool { return slices.Contains(g.VoterStoreIDs, uint64(4)) })
	re.NotEqual(-1, index)
	blocked := initial.targets[index].ID
	groupIndex := slices.Index(ids, blocked)
	// Only the later Region of this group occupies the otherwise preferred store.
	later := f.GetRegion(uint64(2*groupIndex + 2))
	learner := &metapb.Peer{Id: 200, StoreId: 4, Role: metapb.PeerRole_Learner}
	later = later.Clone(core.WithRemoveStorePeer(6), core.WithAddPeer(learner))
	f.PutRegion(later)
	re.True(f.step())
	target := f.affinityManager.GetAffinityGroupState(blocked)
	re.NotContains(target.VoterStoreIDs, uint64(4))
	re.Contains(f.affinityManager.GetAffinityGroupState(ids[1-groupIndex]).VoterStoreIDs, uint64(4))
	op := f.createAffinityOperator(later, target)
	re.NotNil(op)
	operator.ApplyOperator(f.Cluster, op)
	re.Equal(learner, f.GetRegion(later.GetID()).GetStoreLearner(4))
}

func TestAffinityBalanceKeepsUnmodelledGroups(t *testing.T) {
	for _, reason := range []string{"same rules", "different rules", "missing label"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 5, "a", "b", "c", "a", "")
			voters := []uint64{1, 2, 3}
			if reason == "missing label" {
				voters = []uint64{1, 2, 5}
			} else {
				rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.ID, rule.Index, rule.Override = "subrange", 1, true
				rule.StartKeyHex, rule.EndKeyHex = "61", "62"
				if reason == "different rules" {
					rule.IsolationLevel = "zone"
				}
				re.NoError(f.ruleManager.SetRule(rule))
			}
			f.group("_tidb_pt_100_p0", 1, 1, voters, "a", "b", "c")
			f.group("_tidb_pt_100_p1", 3, 1, []uint64{1, 2, 3}, "d", "e")
			before := f.snapshot()
			re.True(f.step())
			re.Equal(before.Groups[0].Group, f.affinityManager.GetAffinityGroupState(before.Groups[0].ID).Group)
			re.NotContains(f.affinityManager.GetAffinityGroupState("_tidb_pt_100_p1").VoterStoreIDs, uint64(5))
		})
	}
}

func TestAffinityBalanceRetries(t *testing.T) {
	for _, reason := range []string{"pending", "gap", "schedule denied", "converging", "busy", "store limit"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 4)
			f.PutStore(f.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
			ids := f.seed("100", 1, 1, 2)
			f.policy(affinity.BalancePolicyFixed, ids[1])
			region := f.GetRegion(1)
			switch reason {
			case "pending":
				f.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
			case "gap":
				f.PutRegion(region.Clone(core.WithStartKey(append(region.GetStartKey(), 'x'))))
			case "schedule denied":
				re.NoError(f.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{ID: "deny", Index: 100,
					Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: "key-range",
					Data: []any{map[string]any{"start_key": "", "end_key": ""}}}))
			case "converging":
				_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[0], 3, []uint64{1, 2, 3})
				re.NoError(err)
			case "busy":
				f.SetStoreBusy(4, true)
			case "store limit":
				f.SetStoreLimit(4, storelimit.AddPeer, 0.00006)
				f.ResetStoreLimit(4, storelimit.AddPeer, 0.000001)
				re.True(f.GetStore(4).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
				re.False(f.GetStore(4).IsAvailable(storelimit.AddPeer, constant.Medium))
			}
			before := f.snapshot()
			re.False(f.step())
			re.Equal(before.Groups, f.snapshot().Groups)
			re.WithinDuration(time.Now().Add(30*time.Second), f.balancer.idle[balanceTestTable].until, 2*time.Second)
			if reason == "busy" || reason == "store limit" {
				f.SetStoreBusy(4, false)
				f.SetStoreLimit(4, storelimit.AddPeer, storelimit.Unlimited)
				f.ResetStoreLimit(4, storelimit.AddPeer, storelimit.Unlimited/time.Minute.Seconds())
				re.True(f.step())
				re.Contains(f.affinityManager.GetAffinityGroupState(ids[0]).VoterStoreIDs, uint64(4))
			}
		})
	}
}

func TestAffinityBalanceRechecksStoreFilters(t *testing.T) {
	for _, reason := range []string{"busy", "store limit", "special use", "low space"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 5)
			ids := f.seed("100", 1, 1, 2)
			f.policy(affinity.BalancePolicyFixed, ids[1])
			current := f.affinityManager.GetAffinityGroupState(ids[0])
			target := current.Group
			target.VoterStoreIDs = []uint64{1, 2, 4}
			check, action := f.balanceStoreActionChecker(), balanceStoreAction{id: 4}
			re.Equal(balanceAllowed, check(action))
			re.Equal(balanceAllowed, f.balanceTargetExecutionStatus(&current.Group, &target))
			want := balanceTemporarilyBlocked
			switch reason {
			case "busy":
				f.SetStoreBusy(4, true)
			case "store limit":
				f.SetStoreLimit(4, storelimit.AddPeer, 0.00006)
				f.ResetStoreLimit(4, storelimit.AddPeer, 0.000001)
				re.True(f.GetStore(4).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
			case "special use":
				f.SetStoreLabel(4, map[string]string{filter.SpecialUseKey: filter.SpecialUseReserved})
				want = balanceRejected
			case "low space":
				f.UpdateRegionCount(4, core.InitClusterRegionThreshold)
				f.UpdateStorageRatio(4, 0.9, 0.1)
				re.True(f.GetStore(4).IsLowSpace(f.GetLowSpaceRatio()))
				want = balanceRejected
			}
			re.Equal(balanceAllowed, check(action))
			re.Equal(want, f.balanceTargetExecutionStatus(&current.Group, &target))
			re.True(f.step())
			for _, group := range f.snapshot().Groups {
				re.NotContains(group.VoterStoreIDs, uint64(4))
			}
		})
	}
}

type affinityBalanceScanCluster struct {
	sche.CheckerCluster
	beforeFinal func()
}

func (c *affinityBalanceScanCluster) BatchScanRegions(ranges *keyutil.KeyRanges, opts ...core.BatchScanRegionsOptionFunc) ([]*core.RegionInfo, error) {
	c.beforeFinal()
	return c.CheckerCluster.BatchScanRegions(ranges, opts...)
}

func TestAffinityBalanceCommitRejectsChangedInputs(t *testing.T) {
	for _, change := range []string{"rule", "conf version", "split", "limit", "pause", "halt", "placement disabled", "cancel"} {
		t.Run(change, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 3)
			ids := f.seed("100", 1, 1, 1)
			f.policy(affinity.BalancePolicyFixed, ids[1])
			snapshot, revision := f.snapshot(), f.ruleManager.GetRevision()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			res, err := f.planBalanceTargets(ctx, snapshot)
			re.NoError(err)
			re.Len(res.targets, 1)
			called := false
			f.cluster = &affinityBalanceScanCluster{CheckerCluster: f.Cluster, beforeFinal: func() {
				re.False(called)
				called = true
				region := f.GetRegion(1)
				switch change {
				case "rule":
					rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
					re.NoError(f.ruleManager.SetRule(rule))
				case "conf version":
					f.PutRegion(region.Clone(core.WithIncConfVer()))
				case "split":
					mid := append(slices.Clone(region.GetStartKey()), 'm')
					f.PutRegion(region.Clone(core.WithEndKey(mid), core.WithIncVersion()))
					f.PutRegion(region.Clone(core.WithNewRegionID(3), core.WithStartKey(mid), core.WithIncVersion()))
				case "limit":
					f.SetAffinityScheduleLimit(0)
				case "pause":
					f.PauseOrResume(60)
				case "halt":
					f.SetHaltScheduling(true, "test")
				case "placement disabled":
					f.SetEnablePlacementRules(false)
				case "cancel":
					cancel()
				}
			}}
			written, err := f.commitBalancePlan(ctx, snapshot, res, revision)
			if change == "cancel" {
				re.ErrorIs(err, context.Canceled)
			} else {
				re.NoError(err)
			}
			re.True(called)
			re.Empty(written)
			re.Equal(snapshot.Groups, f.snapshot().Groups)
		})
	}
}

func TestAffinityBalanceModelsLeaderAndFollowerRules(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 6, "a", "a", "b", "b", "c", "c")
	for _, rule := range []*placement.Rule{
		{GroupID: placement.DefaultGroupID, ID: "leader", Index: 1, Override: true, Role: placement.Leader, Count: 1,
			LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"a"}}}},
		{GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2,
			LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"b", "c"}}}},
	} {
		re.NoError(f.ruleManager.SetRule(rule))
	}
	for i := range 4 {
		f.group(fmt.Sprintf("_tidb_pt_100_p%d", i), uint64(i+1), 1, []uint64{1, 3, 5}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
	}
	re.True(f.step())
	re.Equal([2]int{24, 8}, balanceTableScore(f.affinityManager, balanceTestTable))
	for i := range 4 {
		group := f.affinityManager.GetAffinityGroupState(fmt.Sprintf("_tidb_pt_100_p%d", i))
		region := cloneRegionWithReplacePeerStores(f.GetRegion(uint64(i+1)), group.LeaderStoreID, group.VoterStoreIDs...)
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		re.Equal(bestLocationSatisfied, f.regionPlacementBestLocation(region, fit))
	}
}

func TestAffinityBalanceOverlappingRulesAllowLeaderBalance(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "a", "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	re.NoError(f.ruleManager.SetRule(rule))
	second := rule.Clone()
	second.ID, second.Index = "second", 1
	re.NoError(f.ruleManager.SetRule(second))
	for i := range 2 {
		id := fmt.Sprintf("_tidb_pt_100_p%d", i)
		f.group(id, uint64(i+1), 1, []uint64{1, 2, 3, 4}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
		group, region := f.affinityManager.GetAffinityGroupState(id), f.GetRegion(uint64(i+1))
		fit, status := f.validateBalanceCurrentRegion(group, region)
		re.Equal(balanceAllowed, status)
		re.True(fit.IsSatisfied())
		target := group.Group
		target.LeaderStoreID = 2
		re.Equal(balanceAllowed, f.validateBalanceTarget(&target, region, fit))
	}
	re.Equal([2]int{16, 4}, balanceTableScore(f.affinityManager, balanceTestTable))
	re.True(f.step())
	re.Equal([2]int{16, 2}, balanceTableScore(f.affinityManager, balanceTestTable))
	for _, group := range f.snapshot().Groups {
		re.Equal([]uint64{1, 2, 3, 4}, group.VoterStoreIDs)
	}
}

func TestAffinityBalanceCreationGrace(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	created := uint64(now.Unix())
	for i, group := range []affinity.Group{
		{CreateTimestamp: created}, // Only a new auto group without a target waits.
		{CreateTimestamp: created - 120},
		{CreateTimestamp: created, BalancePolicy: affinity.BalancePolicyFixed},
		{CreateTimestamp: created, VoterStoreIDs: []uint64{1, 2, 3}},
	} {
		snapshot := &affinity.BalanceSnapshot{Groups: []*affinity.GroupState{{Group: group}}}
		re.Equal(i == 0, balanceCreatingTable(snapshot, now), "group: %+v", group)
	}
}
