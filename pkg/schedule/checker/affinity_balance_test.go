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
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func balanceTestTable() affinity.TableID {
	return affinity.TableID(balanceTestGroupID("_tidb_pt_100"))
}

func balanceTestGroupID(id string) string {
	if kerneltype.IsNextGen() {
		return strings.Replace(id, "_tidb_", "_tidb_ks42_", 1)
	}
	return id
}

func balanceTestKey(key string) string {
	if !kerneltype.IsNextGen() {
		return key
	}
	prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 42)
	return string(codec.EncodeBytes(append(prefix, []byte(key)...)))
}

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
		// Keep stores connected however long a test runs.
		tc.SetStoreLastHeartbeatInterval(id, -time.Hour)
	}
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	if len(zones) > 0 {
		for id := uint64(1); id <= stores; id++ {
			tc.SetStoreLabel(id, map[string]string{"zone": zones[(id-1)%uint64(len(zones))], "id": strconv.FormatUint(id, 10)})
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
	keys = slices.Clone(keys)
	for i := range keys {
		keys[i] = balanceTestKey(keys[i])
	}
	group := &affinity.Group{ID: id, LeaderStoreID: leader, VoterStoreIDs: voters}
	re.NoError(createAffinityGroupForTest(f.affinityManager, group, []byte(keys[0]), []byte(keys[len(keys)-1])))
	followers := slices.DeleteFunc(slices.Clone(voters), func(id uint64) bool { return id == leader })
	for i := 0; i+1 < len(keys); i++ {
		regionID := firstRegion + uint64(i)
		f.AddLeaderRegionWithRange(regionID, keys[i], keys[i+1], leader, followers...)
		f.PutRegion(f.GetRegion(regionID).Clone(core.SetApproximateSize(1)))
		_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(f.GetRegion(regionID))
		re.True(aligned)
	}
}

func (f *affinityBalanceTest) seed(table string, firstRegion uint64, leaders ...uint64) []string {
	f.tb.Helper()
	ids := make([]string, 0, len(leaders))
	for i, leader := range leaders {
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_%s_p%d", table, i+1))
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
	return f.affinityManager.GetBalanceSnapshot(balanceTestTable(), f.affinityManager.GetBalanceCounts())
}

func (f *affinityBalanceTest) targetOperator(region *core.RegionInfo, group *affinity.GroupState) *operator.Operator {
	op, _ := f.createAffinityOperator(region, group, f.ruleManager.FitRegionWithoutCache(f.Cluster, region))
	return op
}

func (f *affinityBalanceTest) step() bool {
	f.tb.Helper()
	re := require.New(f.tb)
	written, err := f.balancer.step(f.tb.Context(), balanceTestTable(), f.affinityManager.GetBalanceCounts())
	re.NoError(err)
	return written
}

func (f *affinityBalanceTest) blockStore(id uint64, condition string) balanceStatus {
	f.tb.Helper()
	switch condition {
	case "removing":
		f.PutStore(f.GetStore(id).Clone(core.SetNodeState(metapb.NodeState_Removing)))
	case "down":
		f.SetStoreDown(id)
	case "special use":
		f.SetStoreLabel(id, map[string]string{filter.SpecialUseKey: filter.SpecialUseReserved})
	case "low space":
		f.UpdateRegionCount(id, core.InitClusterRegionThreshold)
		f.UpdateStorageRatio(id, 0.9, 0.1)
		require.True(f.tb, f.GetStore(id).IsLowSpace(f.GetLowSpaceRatio()))
	case "reject-leader":
		f.SetStoreLabel(id, map[string]string{"leader": "reject"})
		f.SetLabelProperty("reject-leader", "leader", "reject")
	case "evict-leader":
		require.NoError(f.tb, f.PauseLeaderTransfer(id, constant.In))
	case "busy":
		f.SetStoreBusy(id, true)
		return balanceTemporarilyBlocked
	case "store limit":
		f.SetStoreLimit(id, storelimit.AddPeer, 0.00006)
		f.ResetStoreLimit(id, storelimit.AddPeer, 0.000001)
		require.True(f.tb, f.GetStore(id).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
		require.False(f.tb, f.GetStore(id).IsAvailable(storelimit.AddPeer, constant.Medium))
		return balanceTemporarilyBlocked
	default:
		f.tb.Fatalf("unknown store condition: %s", condition)
	}
	return balanceRejected
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
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() { c.RunBalancer(ctx) })
	defer func() {
		cancel()
		wg.Wait()
	}()
	custom := []string{"custom", balanceTestGroupID("_tidb_t_100")}
	for _, id := range custom {
		re.NoError(createAffinityGroupForTest(m, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}, nil, nil))
		c.balancer.enqueue(id)
	}
	for i := range ids {
		re.Empty(c.Check(f.GetRegion(uint64(i + 1))))
	}
	re.Eventually(func() bool { return balanceTableScore(m, balanceTestTable()) == [2]int{27, 3} }, 10*time.Second, 10*time.Millisecond)
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

type balanceBenchShape struct {
	name            string
	groups, regions int
	improves        bool
}

// BenchmarkAffinityBalance covers execution of a useful plan, a pinned table,
// and late learner conflicts. The solver benchmarks cover placement topologies.
func BenchmarkAffinityBalance(b *testing.B) {
	level := log.GetLevel()
	log.SetLevel(zapcore.FatalLevel)
	b.Cleanup(func() { log.SetLevel(level) })
	for _, shape := range []balanceBenchShape{
		{name: "one_per_zone", groups: 1000, regions: 1, improves: true},
		{name: "no_improvement", groups: 1000, regions: 1},
		{name: "learner_conflicts", groups: 24, regions: 30, improves: true},
	} {
		b.Run(shape.name, func(b *testing.B) { benchmarkAffinityBalanceStep(b, shape) })
	}
}

func benchmarkAffinityBalanceStep(b *testing.B, shape balanceBenchShape) {
	groups, regions := shape.groups, shape.regions
	b.StopTimer()
	re := require.New(b)
	f := newAffinityBalanceTest(b, 300)
	for _, store := range f.GetStores() {
		id := store.GetID()
		labels := map[string]string{"zone": fmt.Sprintf("z%d", (id-1)%3)}
		if shape.name == "learner_conflicts" && id <= 30 {
			labels["pool"] = "voter"
		}
		if shape.name == "no_improvement" {
			if id == 1 {
				labels["pin"] = "leader"
			} else if id <= 3 {
				labels["pin"] = "follower"
			}
		}
		f.SetStoreLabel(id, labels)
	}
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"zone"}
	if shape.name == "no_improvement" {
		rule.Role, rule.Count, rule.Index, rule.Override = placement.Leader, 1, 1, true
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "pin", Op: placement.In, Values: []string{"leader"}}}
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "pin", Op: placement.In, Values: []string{"follower"}}}}))
	}
	if shape.name == "learner_conflicts" {
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "pool", Op: placement.In, Values: []string{"voter"}}}
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	}
	re.NoError(f.ruleManager.SetRule(rule))
	ids := make([]string, 0, groups)
	for g := range groups {
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1))
		keys := make([]string, 0, regions+1)
		for r := range regions + 1 {
			keys = append(keys, fmt.Sprintf("100-%04d-%05d", g, r))
		}
		firstRegion := uint64(g*regions + 1)
		f.group(id, firstRegion, 1, []uint64{1, 2, 3}, keys...)
		for r := 0; shape.name == "learner_conflicts" && r < regions; r++ {
			store := uint64(31 + r%270)
			if r >= regions-27 {
				store = uint64(4 + r - (regions - 27))
			}
			peer, err := f.AllocPeer(store)
			re.NoError(err)
			peer.Role = metapb.PeerRole_Learner
			region := f.GetRegion(firstRegion + uint64(r)).Clone(core.WithAddPeer(peer))
			f.PutRegion(region)
			f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
		}
		ids = append(ids, id)
	}
	stepRounds := func() float64 {
		var rounds float64
		for _, counter := range affinityBalancePlannerStops {
			rounds += testutil.ToFloat64(counter)
		}
		return rounds
	}
	var rounds, written float64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		for _, id := range ids {
			_, err := f.affinityManager.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
			re.NoError(err)
		}
		f.settle()
		clear(f.balancer.idle)
		before, writtenBefore := stepRounds(), testutil.ToFloat64(affinityBalanceTargetsWritten)
		b.StartTimer()
		re.Equal(shape.improves, f.step())
		b.StopTimer()
		rounds += stepRounds() - before
		written += testutil.ToFloat64(affinityBalanceTargetsWritten) - writtenBefore
	}
	b.ReportMetric(rounds/float64(b.N), "rounds/op")
	b.ReportMetric(written/float64(b.N), "targets/op")
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
	status, _ = f.validateBalanceTarget(&target, region, fit)
	re.Equal(balanceRejected, status)
	feedback := f.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4}, []*core.RegionInfo{region})
	re.Equal(balanceplan.TargetRejected, feedback([]int{0, 1, 3}, 0, balanceplan.DefaultWorkBudget).Status)
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.IsolationLevel = "zone"
	re.NoError(f.ruleManager.SetRule(rule))
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable()))
	re.True(f.step())
	group := f.affinityManager.GetAffinityGroupState(ids[0])
	re.Equal([]uint64{2, 3, 4}, group.VoterStoreIDs)
	re.Equal(uint64(2), group.LeaderStoreID)
	re.Equal([2]int{23, 5}, balanceTableScore(f.affinityManager, balanceTestTable()))
}

func TestBalanceRuleCacheHardIsolation(t *testing.T) {
	f := newAffinityBalanceTest(t, 3, "a", "b", "c")
	cache := newBalanceRuleCache(f.AffinityChecker, []uint64{1, 2, 3}, func(uint64) bool { return true })
	for _, tc := range []struct {
		level   string
		witness bool
		depth   int
		ok      bool
	}{{"", false, 0, true}, {"zone", false, 2, true}, {"dc", false, 1, true}, {"rack", false, 0, false}, {"zone", true, 0, false}} {
		rule := &placement.Rule{Role: placement.Voter, Count: 3, LocationLabels: []string{"dc", "zone"}, IsolationLevel: tc.level, IsWitness: tc.witness}
		for range 2 {
			got, ok := cache.get(rule)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.depth, got.IsolationDepth)
			} else {
				require.Nil(t, got)
				require.Contains(t, cache.rules, balanceRuleSignature(rule))
			}
		}
	}
}

func TestAffinityBalanceLearnerInLaterRegion(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 6)
	f.SetEnableUseJointConsensus(false)
	f.SetStoreLabel(6, map[string]string{"engine": "tiflash"})
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"other"}}}}))
	ids := []string{balanceTestGroupID("_tidb_pt_100_p1"), balanceTestGroupID("_tidb_pt_100_p2"), balanceTestGroupID("_tidb_pt_100_p3")}
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
	// A temporary store action must not hide a later Region's learner conflict.
	f.SetStoreBusy(4, true)
	snapshot := f.snapshot()
	status, learners, reason, retry := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(blocked), initial.targets[index])
	re.Equal(balanceRejected, status)
	re.Equal([]uint64{4}, learners)
	re.Equal(balanceTargetLearnerConflict, reason)
	re.True(retry)
	f.SetStoreBusy(4, false)
	re.True(f.step())
	// The TiFlash store holds a learner of every Region and is never a voter target.
	for _, group := range f.snapshot().Groups {
		re.NotContains(group.VoterStoreIDs, uint64(6))
	}
	target := f.affinityManager.GetAffinityGroupState(blocked)
	re.NotContains(target.VoterStoreIDs, uint64(4))
	re.Contains(f.affinityManager.GetAffinityGroupState(ids[1-groupIndex]).VoterStoreIDs, uint64(4))
	op := f.targetOperator(later, target)
	re.NotNil(op)
	operator.ApplyOperator(f.Cluster, op)
	re.Equal(learner, f.GetRegion(later.GetID()).GetStoreLearner(4))
}

type heartbeatTargetWriter struct {
	targets []affinity.GroupTarget
}

func (w *heartbeatTargetWriter) WriteGroupTargets(_ context.Context, targets []affinity.GroupTarget) ([]string, error) {
	w.targets = append(w.targets, targets...)
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.Expected.ID)
	}
	return ids, nil
}

func newMissingHeartbeatBalanceTest(t *testing.T) (*affinityBalanceTest, *core.RegionInfo, *heartbeatTargetWriter) {
	t.Helper()
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	group := &affinity.Group{ID: balanceTestGroupID("_tidb_pt_100_p1"), LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(createAffinityGroupForTest(f.affinityManager, group, []byte(balanceTestKey("a")), []byte(balanceTestKey("c"))))
	f.AddLeaderRegionWithRange(1, balanceTestKey("a"), balanceTestKey("b"), 1, 2, 3)
	f.PutRegion(f.GetRegion(1).Clone(core.SetApproximateSize(1)))
	f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 2, 3}, "d", "e")
	f.group(balanceTestGroupID("_tidb_pt_100_p3"), 4, 2, []uint64{1, 2, 3}, "f", "g")
	f.policy(affinity.BalancePolicyFixed, balanceTestGroupID("_tidb_pt_100_p2"), balanceTestGroupID("_tidb_pt_100_p3"))
	for _, region := range f.GetRegions() {
		region = region.Clone(core.SetSource(core.Heartbeat))
		f.PutRegion(region)
		f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	}
	// This Region has never entered the cluster cache; the reported [a,b)
	// Region keeps its original boundaries throughout the test.
	missing := f.GetRegion(1).Clone(core.WithNewRegionID(2), core.WithStartKey([]byte(balanceTestKey("b"))), core.WithEndKey([]byte(balanceTestKey("c"))), core.SetSource(core.Heartbeat))
	writer := &heartbeatTargetWriter{}
	f.affinityManager.SetTargetWriter(writer)
	return f, missing, writer
}

func TestAffinityBalanceWithRegionGaps(t *testing.T) {
	re := require.New(t)
	f, missing, writer := newMissingHeartbeatBalanceTest(t)
	f.SetMaxAffinityMergeRegionSize(0)
	re.Nil(f.GetRegion(missing.GetID()))
	re.True(f.step())
	re.Len(writer.targets, 1)
	re.Equal(uint64(3), writer.targets[0].LeaderStoreID)
	// Deliver the persisted target before the missing Region's first heartbeat.
	written, err := f.affinityManager.UpdateAffinityGroupTargets(t.Context(), writer.targets)
	re.NoError(err)
	re.Equal([]string{writer.targets[0].Expected.ID}, written)
	f.PutRegion(missing)
	ops := f.Check(missing)
	re.Len(ops, 1)
	operator.ApplyOperator(f.Cluster, ops[0])
	moved := f.GetRegion(missing.GetID())
	re.Equal(uint64(3), moved.GetLeader().GetStoreId())
	re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
}

func TestAffinityBalanceWaitsForStorageRegionHeartbeat(t *testing.T) {
	re := require.New(t)
	f, heartbeat, writer := newMissingHeartbeatBalanceTest(t)
	stored := core.NewRegionInfo(heartbeat.GetMeta(), nil, core.SetSource(core.Storage))
	f.PutRegion(stored)
	re.True(stored.LoadedFromStorage())
	re.Nil(stored.GetLeader())
	before := f.snapshot()
	re.False(f.step())
	re.Empty(writer.targets)
	re.Equal(before.Groups, f.snapshot().Groups)
	f.PutRegion(heartbeat)
	f.affinityManager.GetAndCacheRegionAffinityGroupState(heartbeat)
	re.False(f.GetRegion(heartbeat.GetID()).LoadedFromStorage())
	re.True(f.step())
	re.Len(writer.targets, 1)
	re.Equal(uint64(3), writer.targets[0].LeaderStoreID)
}

func TestAffinityBalanceTiFlashRecordRules(t *testing.T) {
	for _, differentVoterRules := range []bool{false, true} {
		t.Run(fmt.Sprintf("different-voter-rules=%t", differentVoterRules), func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 5)
			f.SetEnableUseJointConsensus(false)
			f.SetStoreLabel(5, map[string]string{"engine": "tiflash"})
			encode := func(key []byte) []byte {
				if kerneltype.IsNextGen() {
					key = append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 42), key...)
				}
				return codec.EncodeBytes(key)
			}
			for i := range 3 {
				partition := int64(101 + i)
				start := encode(codec.GenerateTableKey(partition))
				record := encode(codec.GenerateRecordKeyPrefix(partition))
				end := encode(codec.GenerateTableKey(partition + 1))
				// TiDB's TiFlash rule starts at the record prefix, inside the
				// affinity range, leaving the index Region without a learner.
				re.NoError(f.ruleManager.SetRule(&placement.Rule{
					GroupID: "tiflash", ID: fmt.Sprintf("table-%d-r", partition), Role: placement.Learner, Count: 1,
					StartKeyHex: hex.EncodeToString(record), EndKeyHex: hex.EncodeToString(end),
					LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}},
				}))
				if differentVoterRules {
					rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
					rule.ID, rule.Index, rule.Override = fmt.Sprintf("record-%d", partition), 1, true
					rule.StartKeyHex, rule.EndKeyHex = hex.EncodeToString(record), hex.EncodeToString(end)
					rule.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"tiflash"}}}
					re.NoError(f.ruleManager.SetRule(rule))
				}
				id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", partition))
				re.NoError(createAffinityGroupForTest(f.affinityManager,
					&affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}, start, end))
				keys := [][]byte{start, record, end}
				for j := range 2 {
					regionID := uint64(2*i + j + 1)
					f.AddLeaderRegionWithRange(regionID, string(keys[j]), string(keys[j+1]), 1, 2, 3)
					region := f.GetRegion(regionID).Clone(core.SetApproximateSize(1))
					if j == 1 {
						region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100 + regionID, StoreId: 5, Role: metapb.PeerRole_Learner}))
					}
					f.PutRegion(region)
					_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
					re.True(aligned)
					re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, region).IsSatisfied())
				}
			}
			snapshot := f.snapshot()
			revision := f.ruleManager.GetRevision()
			res, err := f.planBalanceTargets(t.Context(), snapshot)
			re.NoError(err)
			if differentVoterRules {
				re.Empty(res.targets)
				re.Equal(3, res.groupCounts[balanceGroupUnsupported])
				re.False(f.step())
				re.Equal(snapshot.Groups, f.snapshot().Groups)
				return
			}
			re.Equal(3, res.groupCounts[balanceGroupMutable])
			re.Zero(res.groupCounts[balanceGroupUnsupported])
			re.NotEmpty(res.targets)
			written, reason, err := f.commitBalancePlan(t.Context(), snapshot, res, revision)
			re.NoError(err)
			re.Empty(reason)
			re.Len(written, len(res.targets))
			for _, target := range res.targets {
				re.Contains(written, target.ID)
				re.NotContains(target.VoterStoreIDs, uint64(5))
				re.Equal(*target, f.affinityManager.GetAffinityGroupState(target.ID).Group)
			}
			score := balanceTableScore(f.affinityManager, balanceTestTable())
			re.Less(score[0], 27)
			re.Less(score[1], 9)
			for _, region := range f.GetRegions() {
				group, _ := f.affinityManager.GetRegionAffinityGroupState(region)
				learners := region.GetLearners()
				if op := f.targetOperator(region, group); op != nil {
					operator.ApplyOperator(f.Cluster, op)
				}
				moved := f.GetRegion(region.GetID())
				re.Equal(learners, moved.GetLearners())
				re.Equal(group.LeaderStoreID, moved.GetLeader().GetStoreId())
				re.Len(moved.GetVoters(), len(group.VoterStoreIDs))
				for _, id := range group.VoterStoreIDs {
					re.NotNil(moved.GetStoreVoter(id))
				}
				re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
			}
		})
	}
}

func TestAffinityBalanceKeepsUnmodelledGroups(t *testing.T) {
	for _, reason := range []string{"same rules", "different rules", "missing label", "unknown policy"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 5, "a", "b", "c", "a", "")
			voters := []uint64{1, 2, 3}
			switch reason {
			case "missing label":
				voters = []uint64{1, 2, 5}
			case "same rules", "different rules":
				rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.ID, rule.Index, rule.Override = "subrange", 1, true
				rule.StartKeyHex, rule.EndKeyHex = hex.EncodeToString([]byte(balanceTestKey("a"))), hex.EncodeToString([]byte(balanceTestKey("b")))
				if reason == "different rules" {
					rule.IsolationLevel = "zone"
				}
				re.NoError(f.ruleManager.SetRule(rule))
			}
			f.group(balanceTestGroupID("_tidb_pt_100_p1"), 1, 1, voters, "a", "b", "c")
			f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 2, 3}, "d", "e")
			if reason == "unknown policy" {
				// A policy that a newer version writes keeps the target like fixed.
				group := f.affinityManager.GetAffinityGroupState(balanceTestGroupID("_tidb_pt_100_p1")).Group
				group.BalancePolicy = "manual"
				f.affinityManager.SyncGroupFromEtcd(&group)
			}
			before := f.snapshot()
			re.True(f.step())
			re.Equal(before.Groups[0].Group, f.affinityManager.GetAffinityGroupState(before.Groups[0].ID).Group)
			re.NotContains(f.affinityManager.GetAffinityGroupState(balanceTestGroupID("_tidb_pt_100_p2")).VoterStoreIDs, uint64(5))
		})
	}
}

func TestAffinityBalanceRetries(t *testing.T) {
	for _, reason := range []string{"pending", "schedule denied", "converging", "busy", "store limit"} {
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
			case "schedule denied":
				re.NoError(f.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{ID: "deny", Index: 100,
					Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: "key-range",
					Data: []any{map[string]any{"start_key": "", "end_key": ""}}}))
			case "converging":
				_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[0], 3, []uint64{1, 2, 3})
				re.NoError(err)
			default:
				f.blockStore(4, reason)
			}
			before := f.snapshot()
			blocked := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceBlocked])
			re.False(f.step())
			re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceBlocked])-blocked)
			re.Equal(before.Groups, f.snapshot().Groups)
			re.WithinDuration(time.Now().Add(30*time.Second), f.balancer.idle[balanceTestTable()].until, 2*time.Second)
			if reason == "converging" {
				// The worker plans the table again only once the record expires.
				b := f.balancer
				expire := func() {
					idle := b.idle[balanceTestTable()]
					idle.until = time.Now().Add(-time.Second)
					b.idle[balanceTestTable()] = idle
				}
				b.enqueue(ids[0])
				idle := b.idle[balanceTestTable()]
				re.False(b.balance(t.Context()))
				re.Equal(idle, b.idle[balanceTestTable()])
				expire()
				re.False(b.balance(t.Context()))
				re.WithinDuration(time.Now().Add(30*time.Second), b.idle[balanceTestTable()].until, 2*time.Second)
				f.settle()
				expire()
				re.True(b.balance(t.Context()))
			}
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
			want := f.blockStore(4, reason)
			re.Equal(balanceAllowed, check(action))
			re.Equal(want, f.balanceTargetExecutionStatus(&current.Group, &target))
			re.True(f.step())
			for _, group := range f.snapshot().Groups {
				re.NotContains(group.VoterStoreIDs, uint64(4))
			}
		})
	}
}

// Unschedulable target stores cannot receive affinity operators.
func TestAffinityOperatorTargetStores(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leader bool
	}{
		{"removing", false}, {"down", false}, {"special use", false}, {"low space", false},
		{"busy", false}, {"store limit", false}, {"removing", true}, {"busy", true},
		{"reject-leader", true}, {"evict-leader", true},
	} {
		t.Run(fmt.Sprintf("%s/leader=%t", tc.name, tc.leader), func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 4)
			ids := f.seed("100", 1, 1)
			// Move a voter from store 3 to store 4, or the leader to store 2.
			leader, voters, store := uint64(1), []uint64{1, 2, 4}, uint64(4)
			if tc.leader {
				leader, voters, store = 2, []uint64{1, 2, 3}, 2
			}
			_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[0], leader, voters)
			re.NoError(err)
			region, group := f.GetRegion(1), f.affinityManager.GetAffinityGroupState(ids[0])
			re.NotNil(f.targetOperator(region, group))
			counter := affinityCheckerTargetStoreRejectedCounter
			if f.blockStore(store, tc.name) == balanceTemporarilyBlocked {
				counter = affinityCheckerTargetStoreBlockedCounter
			}
			before := testutil.ToFloat64(counter)
			re.Nil(f.targetOperator(region, group))
			re.Equal(1.0, testutil.ToFloat64(counter)-before)
		})
	}
}

func beforeAffinityBalanceCommit(t *testing.T, fn func()) {
	t.Helper()
	const fp = "github.com/tikv/pd/pkg/schedule/checker/beforeAffinityBalanceCommit"
	require.NoError(t, failpoint.EnableCall(fp, fn))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
}

func TestAffinityBalanceCommitRejectsChangedInputs(t *testing.T) {
	for _, change := range []string{"rule", "limit", "pause", "halt", "placement disabled", "policy", "cancel"} {
		t.Run(change, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 3)
			ids := f.seed("100", 1, 1, 1)
			f.policy(affinity.BalancePolicyFixed, ids[1])
			snapshot := f.snapshot()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			res, err := f.planBalanceTargets(ctx, snapshot)
			re.NoError(err)
			re.Len(res.targets, 1)
			called := false
			beforeAffinityBalanceCommit(t, func() {
				re.False(called)
				called = true
				switch change {
				case "rule":
					rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
					re.NoError(f.ruleManager.SetRule(rule))
				case "limit":
					f.SetAffinityScheduleLimit(0)
				case "pause":
					f.PauseOrResume(60)
				case "halt":
					f.SetHaltScheduling(true, "test")
				case "placement disabled":
					f.SetEnablePlacementRules(false)
				case "policy":
					f.policy(affinity.BalancePolicyFixed, ids[0])
				case "cancel":
					cancel()
				}
			})
			result := affinityBalanceRejected
			switch change {
			case "cancel":
				result = affinityBalanceError
			case "limit", "pause", "halt", "placement disabled":
				result = affinityBalanceDisabled
			}
			plans := testutil.ToFloat64(affinityBalancePlanCounters[result])
			plannedTargets := testutil.ToFloat64(affinityBalanceTargetsPlanned)
			writtenTargets := testutil.ToFloat64(affinityBalanceTargetsWritten)
			written, err := f.balancer.step(ctx, balanceTestTable(), snapshot.Counts)
			if change == "cancel" {
				re.ErrorIs(err, context.Canceled)
			} else {
				re.NoError(err)
			}
			re.True(called)
			re.False(written)
			expectedPlans := 1.0
			if change == "cancel" {
				expectedPlans = 0
			}
			re.Equal(expectedPlans, testutil.ToFloat64(affinityBalancePlanCounters[result])-plans)
			re.Equal(float64(len(res.targets)), testutil.ToFloat64(affinityBalanceTargetsPlanned)-plannedTargets)
			re.Equal(writtenTargets, testutil.ToFloat64(affinityBalanceTargetsWritten))
			if change == "policy" {
				snapshot.Groups[0].BalancePolicy = affinity.BalancePolicyFixed
			}
			re.Equal(snapshot.Groups, f.snapshot().Groups)
		})
	}
}

func TestAffinityBalanceCommitAllowsOtherGroupChanges(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	ids := f.seed("100", 1, 1, 1)
	f.policy(affinity.BalancePolicyFixed, ids[1])
	snapshot := f.snapshot()
	revision := f.ruleManager.GetRevision()
	res, err := f.planBalanceTargets(t.Context(), snapshot)
	re.NoError(err)
	re.Len(res.targets, 1)
	re.Equal(ids[0], res.targets[0].ID)
	re.NotEqual(uint64(1), res.targets[0].LeaderStoreID)
	called := false
	var changedVersion uint64
	beforeAffinityBalanceCommit(t, func() {
		if called {
			return
		}
		called = true
		_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[1], res.targets[0].LeaderStoreID, []uint64{1, 2, 3})
		re.NoError(err)
		var ok bool
		changedVersion, ok = f.affinityManager.GetBalanceVersion(balanceTestTable())
		re.True(ok)
		re.Greater(changedVersion, snapshot.Version)
	})
	written, reason, err := f.commitBalancePlan(t.Context(), snapshot, res, revision)
	re.NoError(err)
	re.True(called)
	re.Equal([]string{ids[0]}, written)
	re.Empty(reason)
	re.Equal(*res.targets[0], f.affinityManager.GetAffinityGroupState(ids[0]).Group)
	re.Equal(res.targets[0].LeaderStoreID, f.affinityManager.GetAffinityGroupState(ids[1]).LeaderStoreID)
	// The other group moved to the planned leader, so this round need not improve the table.
	re.Equal([2]int{12, 4}, balanceTableScore(f.affinityManager, balanceTestTable()))
	version, ok := f.affinityManager.GetBalanceVersion(balanceTestTable())
	re.True(ok)
	re.Greater(version, changedVersion)

	// Once Regions reach their targets, the next round can improve the table
	// without moving the fixed group that changed during the previous commit.
	f.settle()
	fixed := f.affinityManager.GetAffinityGroupState(ids[1]).Group
	snapshot = f.snapshot()
	res, err = f.planBalanceTargets(t.Context(), snapshot)
	re.NoError(err)
	re.Len(res.targets, 1)
	re.Equal(ids[0], res.targets[0].ID)
	written, reason, err = f.commitBalancePlan(t.Context(), snapshot, res, f.ruleManager.GetRevision())
	re.NoError(err)
	re.Empty(reason)
	re.Equal([]string{ids[0]}, written)
	score := balanceTableScore(f.affinityManager, balanceTestTable())
	re.LessOrEqual(score[0], 12)
	if score[0] == 12 {
		re.LessOrEqual(score[1], 2)
	}
	re.Equal(fixed, f.affinityManager.GetAffinityGroupState(ids[1]).Group)
}

type balanceExecutionTargetWriter struct {
	write func([]affinity.GroupTarget) ([]string, error)
}

func (w balanceExecutionTargetWriter) WriteGroupTargets(_ context.Context, targets []affinity.GroupTarget) ([]string, error) {
	return w.write(targets)
}

func TestAffinityBalanceExecutionAfterRegionChanges(t *testing.T) {
	// Check split and merge execution, and both target rejection gates.
	for _, tc := range []struct{ event, gate string }{
		{"split", "allowed"}, {"merge", "allowed"}, {"split", "placement"}, {"merge", "best-location"},
	} {
		t.Run(tc.event+"/"+tc.gate, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 4, "a", "b", "c", "a")
			f.SetEnableUseJointConsensus(false)
			f.SetMaxAffinityMergeRegionSize(0)
			for id := uint64(1); id <= 4; id++ {
				zone := f.GetStore(id).GetLabelValue("zone")
				f.SetStoreLabel(id, map[string]string{"zone": zone, "disk": "ssd"})
			}
			rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.LabelConstraints = []placement.LabelConstraint{{Key: "disk", Op: placement.In, Values: []string{"ssd"}}}
			re.NoError(f.ruleManager.SetRule(rule))
			id := balanceTestGroupID("_tidb_pt_100_p1")
			f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
			fixedID := balanceTestGroupID("_tidb_pt_100_p2")
			f.group(fixedID, 3, 1, []uint64{1, 2, 3}, "d", "e")
			f.policy(affinity.BalancePolicyFixed, fixedID)
			snapshot := f.snapshot()
			revision := f.ruleManager.GetRevision()
			res, err := f.planBalanceTargets(t.Context(), snapshot)
			re.NoError(err)
			re.Len(res.targets, 1)
			target := res.targets[0]
			re.Equal(id, target.ID)
			re.Contains(target.VoterStoreIDs, uint64(4))
			var changed []*core.RegionInfo
			beforeAffinityBalanceCommit(t, func() {
				first := f.GetRegion(1)
				if tc.event == "split" {
					mid := []byte(balanceTestKey("am"))
					changed = []*core.RegionInfo{
						first.Clone(core.WithEndKey(mid), core.WithIncVersion()),
						first.Clone(core.WithNewRegionID(4), core.WithStartKey(mid), core.WithIncVersion()),
					}
				} else {
					changed = []*core.RegionInfo{first.Clone(core.WithEndKey(f.GetRegion(2).GetEndKey()), core.WithIncVersion())}
				}
				for _, region := range changed {
					f.PutRegion(region)
				}
				switch tc.gate {
				case "placement":
					f.SetStoreLabel(4, map[string]string{"zone": "a", "disk": "hdd"})
				case "best-location":
					f.SetStoreLabel(4, map[string]string{"zone": "b", "disk": "ssd"})
				}
			})
			written, reason, err := f.commitBalancePlan(t.Context(), snapshot, res, revision)
			re.NoError(err)
			re.Empty(reason)
			re.Equal([]string{id}, written)
			re.Equal(*target, f.affinityManager.GetAffinityGroupState(id).Group)
			re.Equal(revision, f.ruleManager.GetRevision())
			if tc.event == "merge" {
				re.Nil(f.GetRegion(2))
			}
			if tc.gate == "allowed" {
				for _, region := range changed {
					ops := f.Check(region)
					re.Len(ops, 1)
					re.NotEqual(operator.OpKind(0), ops[0].Kind()&operator.OpAffinity)
					operator.ApplyOperator(f.Cluster, ops[0])
					moved := f.GetRegion(region.GetID())
					re.Equal(target.LeaderStoreID, moved.GetLeader().GetStoreId())
					re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
					re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(moved, true))
				}
				return
			}
			region := changed[0]
			re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
			virtual := cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
			fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, virtual)
			if tc.gate == "placement" {
				re.False(fit.IsSatisfied())
			} else {
				re.True(fit.IsSatisfied())
				re.Equal(affinityPlacementRejected, f.checkRegionPlacement(virtual, fit, false).status)
			}
			type firstWin struct {
				expired *affinity.GroupState
				targets []affinity.GroupTarget
				err     error
			}
			firstWins := make(chan firstWin, 1)
			var firstWinCount atomic.Int32
			f.affinityManager.SetTargetWriter(balanceExecutionTargetWriter{write: func(targets []affinity.GroupTarget) ([]string, error) {
				firstWinCount.Add(1)
				expired := f.affinityManager.GetAffinityGroupState(id)
				ids, err := f.affinityManager.UpdateAffinityGroupTargets(t.Context(), targets)
				select {
				case firstWins <- firstWin{expired: expired, targets: targets, err: err}:
				default:
				}
				return ids, err
			}})
			re.Empty(f.Check(region))
			var observed firstWin
			select {
			case observed = <-firstWins:
			case <-time.After(10 * time.Second):
				re.FailNow("first-win did not write a target")
			}
			re.NoError(observed.err)
			re.True(observed.expired.RegularSchedulingAllowed)
			re.False(observed.expired.AffinitySchedulingAllowed)
			re.Len(observed.targets, 1)
			re.Equal(observed.targets[0].Expected, *target)
			re.Equal(uint64(1), observed.targets[0].LeaderStoreID)
			re.Equal([]uint64{1, 2, 3}, observed.targets[0].VoterStoreIDs)
			learned := f.affinityManager.GetAffinityGroupState(id)
			re.True(learned.AffinitySchedulingAllowed)
			re.False(learned.RegularSchedulingAllowed)
			re.Equal(uint64(1), learned.LeaderStoreID)
			re.Equal([]uint64{1, 2, 3}, learned.VoterStoreIDs)
			re.Equal(int32(1), firstWinCount.Load())
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
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)), uint64(i+1), 1, []uint64{1, 3, 5}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
	}
	re.True(f.step())
	re.Equal([2]int{24, 8}, balanceTableScore(f.affinityManager, balanceTestTable()))
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
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1))
		f.group(id, uint64(i+1), 1, []uint64{1, 2, 3, 4}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
		group, region := f.affinityManager.GetAffinityGroupState(id), f.GetRegion(uint64(i+1))
		fit, status := f.validateBalanceCurrentRegion(group, region)
		re.Equal(balanceAllowed, status)
		re.True(fit.IsSatisfied())
		target := group.Group
		target.LeaderStoreID = 2
		status, _ = f.validateBalanceTarget(&target, region, fit)
		re.Equal(balanceAllowed, status)
	}
	re.Equal([2]int{16, 4}, balanceTableScore(f.affinityManager, balanceTestTable()))
	re.True(f.step())
	re.Equal([2]int{16, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
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

func TestAffinityBalanceIncludesPersistedLeader(t *testing.T) {
	f := newAffinityBalanceTest(t, 0)
	group := &affinity.GroupState{ID: "g", LeaderStoreID: 99, BalancePolicy: affinity.BalancePolicyFixed}
	plan := f.balancePlanInput(&affinity.BalanceSnapshot{Groups: []*affinity.GroupState{group}}, nil, nil)
	require.Equal(t, []uint64{99}, plan.stores)
	require.Equal(t, []int{1}, plan.input.BaseLeaders)
}

func TestAffinityFeedbackConflictingRegions(t *testing.T) {
	f := newAffinityBalanceTest(t, 7, "a", "d", "b", "c", "b", "c", "d")
	for id := uint64(1); id <= 7; id++ {
		f.UpdateRegionCount(id, 10+int(id)*10)
	}
	tc, c := f.Cluster, f.AffinityChecker
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	regions := []*core.RegionInfo{tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner})), tc.GetRegion(2).Clone(core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))}
	ids := []uint64{1, 2, 3, 4, 5, 6, 7}
	check := c.balanceTargetCheck(t.Context(), ids, regions)
	loc := [][]int32{{0}, {3}, {1}, {2}, {1}, {2}, {3}}
	in := &balanceplan.Input{Stores: 7, BaseVoters: []int{0, 5, 0, 0, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Role: balanceplan.RoleVoter, Count: 2, Stores: []int{0, 1, 2, 3, 6}, Location: loc}}, Voters: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 7), Exclude: []int{4, 5}, CheckTarget: check}}}
	legal := 0
	for _, s := range []int{1, 2, 3, 6} {
		f := check([]int{0, s}, 0, balanceplan.DefaultWorkBudget)
		if f.Status == balanceplan.TargetSatisfied {
			legal++
		}
	}
	require.Equal(t, 2, legal)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 6)
	for i, v := range r.Voters {
		require.Equal(t, balanceplan.TargetSatisfied, check(v, r.Leaders[i], balanceplan.DefaultWorkBudget).Status)
	}
}

func TestAffinityFeedbackRuleReassignment(t *testing.T) {
	f := newAffinityBalanceTest(t, 6, "x", "a", "a", "b", "d", "d")
	for id := uint64(1); id <= 6; id++ {
		f.UpdateRegionCount(id, int(id)*10)
	}
	tc, c := f.Cluster, f.AffinityChecker
	for i, ids := range [][]string{{"1"}, {"2", "3", "4", "6"}, {"2", "3", "5"}} {
		count, role := 1, placement.Voter
		if i == 0 {
			role = placement.Leader
		}
		if i == 1 {
			count = 2
		}
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: role, Count: count, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	// The real fitter can assign a voter to a different rule than the solver.
	tc.AddLeaderRegion(1, 5, 1, 2, 4)
	region := tc.GetRegion(1).Clone(core.WithReplaceLeaderStore(1))
	ids := []uint64{1, 2, 3, 4, 5, 6}
	loc := [][]int32{{0}, {1}, {1}, {2}, {3}, {3}}
	rules := []*balanceplan.Rule{{Role: balanceplan.RoleLeader, Count: 1, Stores: []int{0}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 2, Stores: []int{1, 2, 3, 5}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 1, Stores: []int{1, 2, 4}, Location: loc}}
	check := c.balanceTargetCheck(t.Context(), ids, []*core.RegionInfo{region})
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: rules, Voters: []int{0, 1, 3, 4}, RuleOf: []int{0, 1, 1, 2}, Leader: 0, LeaderPinned: true, CanLead: []bool{true, false, false, false, false, false}}}}
	raw := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.Equal(t, balanceplan.TargetRejected, check(raw.Voters[0], raw.Leaders[0], balanceplan.DefaultWorkBudget).Status)
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 1, 3, 5}, 0, balanceplan.DefaultWorkBudget).Status)
	in.Groups[0].CheckTarget = check
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSource(t *testing.T) {
	f := newAffinityBalanceTest(t, 5, "a", "a", "b", "b", "b")
	tc, c := f.Cluster, f.AffinityChecker
	for i, ids := range [][]string{{"1", "3"}, {"2", "4", "5"}} {
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	tc.AddLeaderRegion(1, 1, 4)
	region := tc.GetRegion(1)
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5}, []*core.RegionInfo{region})
	loc := [][]int32{{0}, {0}, {1}, {1}, {1}}
	in := &balanceplan.Input{Stores: 5, BaseVoters: []int{0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 1, Stores: []int{0, 2}, Location: loc}, {Count: 1, Stores: []int{1, 3, 4}, Location: loc}}, Voters: []int{0, 3}, RuleOf: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 5), CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 4}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 0)
	require.Equal(t, 0, r.Leaders[0])
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSingleRule(t *testing.T) {
	f := newAffinityBalanceTest(t, 6, "a", "b", "c", "c", "a", "b")
	tc, c := f.Cluster, f.AffinityChecker
	tc.UpdateRegionCount(1, 100)
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learners", Role: placement.Learner, Count: 2}))
	tc.AddLeaderRegion(1, 1, 3)
	region := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}), core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6}, []*core.RegionInfo{region})
	// The PD removal tie break chooses the pinned leader in [0,1]. Moving 1 also fixes it.
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 5, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 2, Stores: []int{0, 1, 2, 3}, Location: [][]int32{{0}, {1}, {2}, {2}, {0}, {1}}}}, Voters: []int{0, 2}, Leader: 0, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 6), Exclude: []int{4, 5}, CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 3}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, []int{0, 3}, r.Voters[0])
}

func TestAffinityFeedbackRegionRejectionDominatesTemporary(t *testing.T) {
	f := newAffinityBalanceTest(t, 7, "a", "a", "b", "c", "b", "c", "d")
	tc, c := f.Cluster, f.AffinityChecker
	tc.UpdateRegionCount(2, 100)
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	r1 := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}))
	r2 := tc.GetRegion(2).Clone(core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))
	for _, id := range []uint64{4, 6, 7} {
		tc.SetStoreBusy(id, true)
	}
	require.Equal(t, affinityPlacementTemporary, c.regionPlacementFeedback(r1, c.ruleManager.FitRegionWithoutCache(tc, r1)).status)
	require.Equal(t, affinityPlacementRejected, c.regionPlacementFeedback(r2, c.ruleManager.FitRegionWithoutCache(tc, r2)).status)
	for _, regions := range [][]*core.RegionInfo{{r1, r2}, {r2, r1}} {
		f := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6, 7}, regions)([]int{0, 1}, 0, balanceplan.DefaultWorkBudget)
		require.Equal(t, balanceplan.TargetRejected, f.Status)
	}
}

func TestAffinityFeedbackStopsBetweenRegions(t *testing.T) {
	f := newAffinityBalanceTest(t, 3, "a", "b", "c")
	tc, c := f.Cluster, f.AffinityChecker
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	tc.AddLeaderRegion(1, 1, 2, 3)
	regions := slices.Repeat([]*core.RegionInfo{tc.GetRegion(1)}, 100)
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3}, regions)
	limited := check([]int{0, 1, 2}, 0, 1)
	full := check([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Equal(t, balanceplan.TargetRejected, limited.Status)
	require.Equal(t, balanceplan.TargetSatisfied, full.Status)
	require.Equal(t, limited.Work*100, full.Work)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := c.balanceTargetCheck(ctx, []uint64{1, 2, 3}, regions)([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Zero(t, canceled.Work)
}

func TestAffinityBalanceSameRoleIsolation(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3, "a", "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 1
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1"}}}
	re.NoError(f.ruleManager.SetRule(rule))
	second := rule.Clone()
	second.ID, second.Index = "second", 1
	second.LabelConstraints[0].Values = []string{"2", "3"}
	re.NoError(f.ruleManager.SetRule(second))
	for i := range 2 {
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)), uint64(i+1), 1, []uint64{1, 3}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
	}
	snapshot := f.snapshot()
	region := f.GetRegion(1)
	currentFit, status := f.validateBalanceCurrentRegion(snapshot.Groups[0], region)
	re.Equal(balanceAllowed, status)
	virtual := cloneRegionWithReplacePeerStores(region, 1, 1, 2)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, virtual)
	re.True(fit.IsSatisfied())
	re.True(placementNotWorse(currentFit, fit))
	re.Equal(affinityPlacementRejected, f.checkRegionPlacement(virtual, fit, false).status)
	strategy := f.strategy(virtual, second, false)
	old, next, temporary := strategy.getBetterLocation(f.Cluster, virtual, fit, fit.RuleFits[1])
	re.Equal(uint64(2), old)
	re.Equal(uint64(3), next)
	re.False(temporary)
	res, err := f.planBalanceTargets(t.Context(), snapshot)
	re.NoError(err)
	re.Equal(balanceplan.Score{Voter: 8, Leader: 2}, res.planner.Score)
	re.Len(res.targets, 1)
	re.Equal(2, res.rounds)
	re.Equal(2, res.groupCounts[balanceGroupMutable])
	re.True(f.step())
	re.Equal([2]int{8, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
	for _, group := range f.snapshot().Groups {
		re.Equal([]uint64{1, 3}, group.VoterStoreIDs)
	}
	f.settle()
	re.False(f.step())
	// A new store in zone b is usable even though store 2 is cheaper to try.
	f.AddRegionStore(4, 10)
	f.SetStoreLabel(4, map[string]string{"zone": "b", "id": "4"})
	second.LabelConstraints[0].Values = []string{"2", "3", "4"}
	re.NoError(f.ruleManager.SetRule(second))
	re.True(f.step())
	re.Equal([2]int{6, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
	for _, group := range f.snapshot().Groups {
		re.NotContains(group.VoterStoreIDs, uint64(2))
	}
}

func TestAffinityBalanceLaterRegionPlacement(t *testing.T) {
	f := newAffinityBalanceTest(t, 5, "a", "a", "b", "c", "b")
	f.UpdateRegionCount(3, 100)
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1", "2", "3", "4", "5"}}}
	require.NoError(t, f.ruleManager.SetRule(rule))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
		StartKeyHex: hex.EncodeToString([]byte(balanceTestKey("b"))), EndKeyHex: hex.EncodeToString([]byte(balanceTestKey("c"))), LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"5"}}}}))
	f.group(balanceTestGroupID("_tidb_pt_100_p1"), 1, 1, []uint64{1, 4}, "a", "b", "c")
	f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 4}, "d", "e")
	f.policy(affinity.BalancePolicyFixed, balanceTestGroupID("_tidb_pt_100_p2"))
	later := f.GetRegion(2)
	f.PutRegion(later.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner})))
	snapshot := f.snapshot()
	rawPlan := f.balancePlanInput(snapshot, nil, nil)
	raw := balanceplan.Plan(&rawPlan.input, balanceplan.Options{Seed: snapshot.Version})
	for _, target := range rawPlan.changedTargets(&raw) {
		for _, id := range []uint64{1, 2} {
			region := f.GetRegion(id)
			fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
			status, reason := f.validateBalanceTarget(target, region, fit)
			if id == 1 {
				require.Equal(t, balanceAllowed, status)
			} else {
				require.Equal(t, balanceRejected, status)
				require.Equal(t, balanceTargetLocation, reason)
			}
		}
	}
	// A heartbeat after the snapshot makes the first Region temporarily
	// unready. A later Region still rejects the relaxed target's location.
	first := f.GetRegion(1)
	extra := &metapb.Peer{Id: 200, StoreId: 3}
	f.PutRegion(first.Clone(core.WithAddPeer(extra), core.WithPendingPeers([]*metapb.Peer{extra})))
	blocked, err := f.planBalanceTargets(t.Context(), snapshot)
	require.NoError(t, err)
	require.Empty(t, blocked.targets)
	require.Equal(t, 2, blocked.rounds)
	require.False(t, blocked.planner.TemporarilyBlocked)
	require.False(t, blocked.planner.PlacementLimited)
	require.Equal(t, balanceplan.StopGreedy, blocked.planner.Stop)
	require.True(t, blocked.retry)
	target := rawPlan.changedTargets(&raw)[0]
	status, _, reason, retry := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
	require.Equal(t, balanceRejected, status)
	require.Equal(t, balanceTargetLocation, reason)
	require.True(t, retry)
	f.PutRegion(first)
	res, err := f.planBalanceTargets(t.Context(), snapshot)
	require.NoError(t, err)
	require.NotEmpty(t, res.targets)
	require.Greater(t, res.rounds, 1)
}

func TestAffinityPlacementChecksEveryCompetingSource(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "a", "c")
	for i, ids := range [][]string{{"1", "2", "4"}, {"3"}} {
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i),
			Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 2 - i, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	f.AddLeaderRegion(1, 1, 2, 3)
	region := f.GetRegion(1)
	for _, high := range []uint64{1, 2} {
		f.UpdateRegionCount(1, 10)
		f.UpdateRegionCount(2, 10)
		f.UpdateRegionCount(high, 100)
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		re.True(fit.IsSatisfied())
		if high == 2 {
			strategy := f.strategy(region, fit.RuleFits[0].Rule, false)
			old, candidates, temporary := strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
			re.Equal(uint64(2), old)
			re.Nil(candidates, "the sampled source alone misses the improvement")
			re.False(temporary)
		}
		for _, detailed := range []bool{false, true} {
			feedback := f.checkRegionPlacement(region, fit, detailed)
			re.Equal(affinityPlacementRejected, feedback.status, "RegionScore must not change placement validity")
			if detailed {
				re.Contains(feedback.replacements, bestLocationReplacement{old: 1, candidates: []uint64{4}})
			}
		}
	}
}

func TestAffinityHardIsolationDominatesTemporary(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3, "a", "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count, rule.IsolationLevel = 2, "zone"
	re.NoError(f.ruleManager.SetRule(rule))
	f.AddLeaderRegion(1, 1, 2)
	f.SetStoreBusy(3, true)
	region := f.GetRegion(1)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
	strategy := f.strategy(region, rule, false)
	_, candidates, temporary := strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
	re.Nil(candidates)
	re.True(temporary)
	re.Equal(affinityPlacementRejected, f.checkRegionPlacement(region, fit, false).status)
}

func TestBalanceRuleCacheUnicodeFold(t *testing.T) {
	values := []string{"S", "s", "ſ", "Σ", "σ", "ς", "K", "k", "K", "İ", "i", "ı", "a"}
	f := newAffinityBalanceTest(t, uint64(len(values)), values...)
	ids := make([]uint64, len(values))
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	cache := newBalanceRuleCache(f.AffinityChecker, ids, func(uint64) bool { return true })
	locations := cache.location([]string{"zone"})
	for i, a := range values {
		for j, b := range values {
			require.Equal(t, strings.EqualFold(a, b), locations[i][0] == locations[j][0], "%q and %q", a, b)
		}
	}
}

func TestAffinityBalanceWitnessRulesStayUnsupported(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "c", "d")
	id := balanceTestGroupID("_tidb_pt_100_p1")
	f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.IsWitness, rule.Count = true, 1
	re.NoError(f.ruleManager.SetRule(rule))
	plan := f.balancePlanInput(f.snapshot(), nil, nil)
	re.Empty(plan.input.Groups)
	re.Equal(1, plan.groupCounts[balanceGroupUnsupported])
}

func TestAffinityPlacementUnlabeledCoLocationRule(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "z1", "z1", "z2", "z3")
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "r1", Index: 1,
		Override: true, Role: placement.Voter, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z1"}}}}))
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "r2", Index: 2,
		Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
	f.AddLeaderRegion(1, 1, 2, 3)
	region := f.GetRegion(1)
	for _, high := range []uint64{2, 3} {
		f.UpdateRegionCount(2, 10)
		f.UpdateRegionCount(3, 10)
		f.UpdateRegionCount(high, 100)
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		re.True(fit.IsSatisfied())
		re.Empty(fit.RuleFits[0].Rule.LocationLabels)
		for _, detailed := range []bool{false, true} {
			feedback := f.checkRegionPlacement(region, fit, detailed)
			re.Equal(affinityPlacementRejected, feedback.status, "an unlabeled co-located rule must not hide a better source")
			if detailed {
				re.Contains(feedback.replacements, bestLocationReplacement{old: 2, candidates: []uint64{4}})
			}
		}
	}
}

func TestBalanceRuleSignatureBoundaries(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 2, "foo bar", "foo")
	cache := newBalanceRuleCache(f.AffinityChecker, []uint64{1, 2}, func(uint64) bool { return true })
	first := &placement.Rule{Role: placement.Voter, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"foo bar", "a"}}}}
	second := first.Clone()
	second.LabelConstraints[0].Values = []string{"foo", "bar a"}
	re.NotEqual(balanceRuleSignature(first), balanceRuleSignature(second))
	a, ok := cache.get(first)
	re.True(ok)
	b, ok := cache.get(second)
	re.True(ok)
	re.Equal([]int{0}, a.Stores)
	re.Equal([]int{1}, b.Stores)
	first.LocationLabels, second.LocationLabels = []string{"foo bar", "a"}, []string{"foo", "bar a"}
	first.LabelConstraints, second.LabelConstraints = nil, nil
	re.NotEqual(balanceRuleSignature(first), balanceRuleSignature(second))
}

func TestBalanceTargetCheckCanceledBeforeRegionFits(t *testing.T) {
	f := newAffinityBalanceTest(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A nil Region would panic if preparation tried to fit it after cancellation.
	check := f.balanceTargetCheck(ctx, []uint64{1, 2, 3}, []*core.RegionInfo{nil})
	feedback := check([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Equal(t, balanceplan.TargetRejected, feedback.Status)
	require.Zero(t, feedback.Work)
}

func TestAffinityPlacementSingleRuleAndLearnerScanOnce(t *testing.T) {
	for _, learner := range []bool{false, true} {
		f := newAffinityBalanceTest(t, 4, "z1", "z2", "z3", "z4")
		f.AddLeaderRegion(1, 1, 2, 3)
		region := f.GetRegion(1)
		if learner {
			f.SetStoreLabel(4, map[string]string{"engine": "tiflash"})
			require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner",
				Role: placement.Learner, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}))
			region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 4, Role: metapb.PeerRole_Learner}))
		}
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		feedback := f.checkRegionPlacement(region, fit, false)
		require.Equal(t, affinityPlacementReady, feedback.status)
		require.Equal(t, 1, feedback.scans)
	}
}
