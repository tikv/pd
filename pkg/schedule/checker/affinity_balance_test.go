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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

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
	"github.com/tikv/pd/pkg/utils/keyutil"
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

func enableFastAffinityAvailabilityCheck(t testing.TB) {
	t.Helper()
	const fp = "github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"
	require.NoError(t, failpoint.Enable(fp, "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
}

func (f *affinityBalanceTest) waitForStoreScan(id string) {
	f.tb.Helper()
	leader := f.affinityManager.GetAffinityGroupState(id).LeaderStoreID
	for _, busy := range []bool{true, false} {
		f.SetStoreBusy(leader, busy)
		require.Eventually(f.tb, func() bool {
			return f.affinityManager.GetAffinityGroupState(id).AffinitySchedulingAllowed != busy
		}, 2*time.Second, 10*time.Millisecond)
	}
	f.SetStoreLastHeartbeatInterval(leader, -time.Hour)
	for _, region := range f.GetRegions() {
		f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	}
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

func (f *affinityBalanceTest) addSizedStore(id uint64, regions, zone int, lowSpace bool) {
	f.tb.Helper()
	f.AddRegionStore(id, regions)
	f.SetStoreUp(id)
	store := f.GetStore(id)
	capacity := uint64(1) << 40
	available := capacity - store.GetStoreStats().GetUsedSize()
	if lowSpace {
		available = 1
	}
	f.PutStore(store.Clone(
		core.SetStoreStats(&pdpb.StoreStats{Capacity: capacity, UsedSize: capacity - available, Available: available}),
		core.SetLastHeartbeatTS(time.Now().Add(time.Hour)),
	))
	f.SetStoreLabel(id, map[string]string{"zone": fmt.Sprintf("z%d", zone)})
	if lowSpace {
		require.True(f.tb, f.GetStore(id).IsLowSpace(f.GetLowSpaceRatio()))
	}
}

func (f *affinityBalanceTest) group(id string, firstRegion, leader uint64, voters []uint64, keys ...string) {
	f.tb.Helper()
	keys = slices.Clone(keys)
	for i := range keys {
		keys[i] = balanceTestKey(keys[i])
	}
	group := &affinity.Group{ID: id, LeaderStoreID: leader, VoterStoreIDs: voters}
	require.NoError(f.tb, createAffinityGroupForTest(f.affinityManager, group, keyutil.KeyRange{
		StartKey: []byte(keys[0]),
		EndKey:   []byte(keys[len(keys)-1]),
	}))
	f.putAlignedRegions(firstRegion, leader, voters, keys...)
}

func (f *affinityBalanceTest) putAlignedRegions(firstRegion, leader uint64, voters []uint64, encodedKeys ...string) {
	f.tb.Helper()
	followers := slices.DeleteFunc(slices.Clone(voters), func(id uint64) bool { return id == leader })
	for i := 0; i+1 < len(encodedKeys); i++ {
		regionID := firstRegion + uint64(i)
		f.AddLeaderRegionWithRange(regionID, encodedKeys[i], encodedKeys[i+1], leader, followers...)
		f.PutRegion(f.GetRegion(regionID).Clone(core.SetApproximateSize(1)))
		_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(f.GetRegion(regionID))
		require.True(f.tb, aligned)
	}
}

func (f *affinityBalanceTest) seed(table string, firstRegion uint64, leaders ...uint64) []string {
	f.tb.Helper()
	return f.seedRegions(table, firstRegion, 1, []uint64{1, 2, 3}, leaders...)
}

func (f *affinityBalanceTest) seedVoters(voters []uint64, leaders ...uint64) []string {
	f.tb.Helper()
	return f.seedRegions("100", 1, 1, voters, leaders...)
}

func (f *affinityBalanceTest) seedRegions(table string, firstRegion uint64, regions int, voters []uint64, leaders ...uint64) []string {
	f.tb.Helper()
	re := require.New(f.tb)
	ids := make([]string, len(leaders))
	changes := make([]affinity.GroupKeyRanges, len(leaders))
	keyAt := func(g, r int) string { return balanceTestKey(fmt.Sprintf("%s-%04d-%05d", table, g, r)) }
	for i := range leaders {
		ids[i] = balanceTestGroupID(fmt.Sprintf("_tidb_pt_%s_p%d", table, i+1))
		changes[i] = affinity.GroupKeyRanges{
			GroupID:   ids[i],
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte(keyAt(i, 0)), EndKey: []byte(keyAt(i+1, 0))}},
		}
	}
	re.NoError(f.affinityManager.CreateAffinityGroups(changes))
	keys := make([]string, regions+1)
	for i, leader := range leaders {
		_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[i], leader, voters)
		re.NoError(err)
		for r := range regions {
			keys[r] = keyAt(i, r)
		}
		keys[regions] = keyAt(i+1, 0)
		f.putAlignedRegions(firstRegion+uint64(i*regions), leader, voters, keys...)
	}
	return ids
}

func (f *affinityBalanceTest) policy(policy affinity.BalancePolicy, ids ...string) {
	f.tb.Helper()
	for _, id := range ids {
		_, err := f.affinityManager.UpdateAffinityGroupPolicy(id, policy)
		require.NoError(f.tb, err)
	}
}

func (f *affinityBalanceTest) snapshot() *affinity.BalanceSnapshot {
	return f.affinityManager.GetBalanceSnapshot(balanceTestTable(), f.affinityManager.GetBalanceCounts())
}

func (f *affinityBalanceTest) plan(snapshot *affinity.BalanceSnapshot) balancePlanResult {
	f.tb.Helper()
	res, err := f.planBalanceTargets(f.tb.Context(), snapshot)
	require.NoError(f.tb, err)
	return res
}

func (f *affinityBalanceTest) setRules(rules ...*placement.Rule) {
	f.tb.Helper()
	for _, rule := range rules {
		if rule.GroupID == "" {
			rule.GroupID = placement.DefaultGroupID
		}
		require.NoError(f.tb, f.ruleManager.SetRule(rule))
	}
}

func (f *affinityBalanceTest) denySchedule(startKey, endKey []byte) {
	f.tb.Helper()
	require.NoError(f.tb, f.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "deny", Index: 100, Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: "key-range",
		Data: []any{map[string]any{"start_key": hex.EncodeToString(startKey), "end_key": hex.EncodeToString(endKey)}},
	}))
}

func (f *affinityBalanceTest) disableBalance(reason string) {
	switch reason {
	case "limit":
		f.SetAffinityScheduleLimit(0)
	case "pause":
		f.PauseOrResume(60)
	case "halt":
		f.SetHaltScheduling(true, "test")
	case "placement disabled":
		f.SetEnablePlacementRules(false)
	}
}

func (f *affinityBalanceTest) setVoters(count int) {
	f.tb.Helper()
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = count
	f.setRules(rule)
}

func (f *affinityBalanceTest) addLearnerRule(count int) {
	f.tb.Helper()
	f.setRules(&placement.Rule{ID: "learners", Role: placement.Learner, Count: count})
}

func learnerAt(region *core.RegionInfo, store uint64) *core.RegionInfo {
	return region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100 + region.GetID(), StoreId: store, Role: metapb.PeerRole_Learner}))
}

func (f *affinityBalanceTest) targetOperator(region *core.RegionInfo, group *affinity.GroupState) *operator.Operator {
	op, _ := f.createAffinityOperator(region, group, f.ruleManager.FitRegionWithoutCache(f.Cluster, region))
	return op
}

func (f *affinityBalanceTest) applyBalanceMove(op *operator.Operator, leader uint64) {
	f.tb.Helper()
	operator.ApplyOperator(f.Cluster, op)
	moved := f.GetRegion(op.RegionID())
	require.Equal(f.tb, leader, moved.GetLeader().GetStoreId())
	require.True(f.tb, f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
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
	case "store limit", "store limit and snapshots":
		f.SetStoreLimit(id, storelimit.AddPeer, 0.00006)
		f.ResetStoreLimit(id, storelimit.AddPeer, 0.000001)
		require.True(f.tb, f.GetStore(id).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
		require.False(f.tb, f.GetStore(id).IsAvailable(storelimit.AddPeer, constant.Medium))
		if condition == "store limit and snapshots" {
			f.SetMaxSnapshotCount(1)
			f.UpdateSnapshotCount(id, 2)
		}
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
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 3)
	ids := f.seed("100", 1, 1, 1, 1)
	f.waitForStoreScan(ids[0])
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() { f.RunBalancer(ctx) })
	defer func() {
		cancel()
		wg.Wait()
	}()
	custom := []string{"custom", balanceTestGroupID("_tidb_t_100")}
	for _, id := range custom {
		re.NoError(createAffinityGroupForTest(f.affinityManager, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}))
		f.balancer.enqueue(id)
	}
	for i := range ids {
		re.Empty(f.Check(f.GetRegion(uint64(i + 1))))
	}
	re.Eventually(func() bool { return balanceTableScore(f.affinityManager, balanceTestTable()) == [2]int{27, 3} }, 10*time.Second, 10*time.Millisecond)
	f.balancer.mu.Lock()
	registered := len(f.balancer.tables)
	f.balancer.mu.Unlock()
	re.Equal(1, registered)
	for i, id := range ids {
		group, region := f.affinityManager.GetAffinityGroupState(id), f.GetRegion(uint64(i+1))
		if group.LeaderStoreID != region.GetLeader().GetStoreId() {
			ops := f.Check(region)
			re.Len(ops, 1)
			re.Equal(operator.OpLeader|operator.OpAffinity, ops[0].Kind())
			operator.ApplyOperator(f.Cluster, ops[0])
			f.Check(f.GetRegion(region.GetID()))
		}
		re.Equal(affinity.PhaseStable, f.affinityManager.GetAffinityGroupState(id).Phase)
	}
	for _, id := range custom {
		state := f.affinityManager.GetAffinityGroupState(id)
		re.Equal(uint64(1), state.LeaderStoreID)
		re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
	}
}

func TestAffinityBalancePlacement(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "c", "a")
	f.PutStore(f.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
	ids := f.seed("100", 1, 1, 2, 3)
	f.policy(affinity.BalancePolicyFixed, ids[1:]...)
	// Keep the initial leaders in zones a and c, but reject those zones for new leaders.
	f.SetLabelProperty("reject-leader", "zone", "c")
	f.SetLabelProperty("reject-leader", "zone", "a")
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
	f.setRules(rule)
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable()))
	re.True(f.step())
	group := f.affinityManager.GetAffinityGroupState(ids[0])
	re.Equal([]uint64{2, 3, 4}, group.VoterStoreIDs)
	re.Equal(uint64(2), group.LeaderStoreID)
	re.Equal([2]int{23, 5}, balanceTableScore(f.affinityManager, balanceTestTable()))
}

func TestBalanceRuleCache(t *testing.T) {
	t.Run("location-label-boundaries", func(t *testing.T) {
		f := newAffinityBalanceTest(t, 1)
		f.SetStoreLabel(1, map[string]string{"zone": "a", "rack": "b"})
		cache := newBalanceRuleCache(f.AffinityChecker, []uint64{1}, func(uint64) bool { return true })
		require.Equal(t, [][]int32{{-1}}, cache.location([]string{"zone,rack"}))
		require.Equal(t, [][]int32{{0, 0}}, cache.location([]string{"zone", "rack"}))
	})

	t.Run("hard-isolation", func(t *testing.T) {
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
	})

	t.Run("unicode-fold", func(t *testing.T) {
		values := []string{"S", "s", "ſ", "Σ", "σ", "ς", "K", "k", "K", "İ", "i", "ı", "a"}
		f := newAffinityBalanceTest(t, uint64(len(values)), values...)
		ids := make([]uint64, len(values))
		for i := range ids {
			ids[i] = uint64(i + 1)
		}
		locations := newBalanceRuleCache(f.AffinityChecker, ids, func(uint64) bool { return true }).location([]string{"zone"})
		for i, a := range values {
			for j, b := range values {
				require.Equal(t, strings.EqualFold(a, b), locations[i][0] == locations[j][0], "%q and %q", a, b)
			}
		}
	})

	t.Run("signature-boundaries", func(t *testing.T) {
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
	})
}

func TestAffinityBalanceLearnerInLaterRegion(t *testing.T) {
	re := require.New(t)
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 6)
	f.SetEnableUseJointConsensus(false)
	f.SetStoreLabel(6, map[string]string{"engine": "tiflash"})
	f.setRules(&placement.Rule{ID: "learner", Role: placement.Learner, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"other"}}}})
	ids := []string{balanceTestGroupID("_tidb_pt_100_p1"), balanceTestGroupID("_tidb_pt_100_p2"), balanceTestGroupID("_tidb_pt_100_p3")}
	f.group(ids[0], 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
	f.group(ids[1], 3, 2, []uint64{1, 2, 3}, "c", "d", "e")
	f.group(ids[2], 5, 3, []uint64{1, 2, 3}, "f", "g")
	f.policy(affinity.BalancePolicyFixed, ids[2])
	for _, region := range f.GetRegions() {
		f.PutRegion(learnerAt(region, 6))
	}
	f.waitForStoreScan(ids[0])
	initial := f.plan(f.snapshot())
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

func TestAffinityBalanceRegionHeartbeat(t *testing.T) {
	for _, source := range []string{"missing", "storage"} {
		t.Run(source, func(t *testing.T) {
			re := require.New(t)
			enableFastAffinityAvailabilityCheck(t)
			f := newAffinityBalanceTest(t, 3)
			f.SetMaxAffinityMergeRegionSize(0)
			firstID := balanceTestGroupID("_tidb_pt_100_p1")
			re.NoError(createAffinityGroupForTest(f.affinityManager,
				&affinity.Group{ID: firstID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
				keyutil.KeyRange{StartKey: []byte(balanceTestKey("a")), EndKey: []byte(balanceTestKey("c"))}))
			f.putAlignedRegions(1, 1, []uint64{1, 2, 3}, balanceTestKey("a"), balanceTestKey("b"))
			f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 2, 3}, "d", "e")
			f.group(balanceTestGroupID("_tidb_pt_100_p3"), 4, 2, []uint64{1, 2, 3}, "f", "g")
			f.waitForStoreScan(firstID)
			f.policy(affinity.BalancePolicyFixed, balanceTestGroupID("_tidb_pt_100_p2"), balanceTestGroupID("_tidb_pt_100_p3"))
			for _, region := range f.GetRegions() {
				region = region.Clone(core.SetSource(core.Heartbeat))
				f.PutRegion(region)
				f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
			}
			// This Region has never entered the cluster cache; the reported [a,b)
			// Region keeps its original boundaries throughout the test.
			heartbeat := f.GetRegion(1).Clone(core.WithNewRegionID(2), core.WithStartKey([]byte(balanceTestKey("b"))), core.WithEndKey([]byte(balanceTestKey("c"))), core.SetSource(core.Heartbeat))

			var targets []affinity.GroupTarget
			f.affinityManager.SetTargetWriter(targetWriterFunc(func(_ context.Context, writes []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
				targets = append(targets, writes...)
				ids := make([]string, 0, len(writes))
				for _, target := range writes {
					ids = append(ids, target.Expected.ID)
				}
				return affinity.GroupTargetsResponse{WrittenGroupIDs: ids}, nil
			}))
			if source == "storage" {
				// Region storage persists metadata without a heartbeat's leader.
				stored := core.NewRegionInfo(heartbeat.GetMeta(), nil, core.SetSource(core.Storage))
				f.PutRegion(stored)
				re.True(f.GetRegion(stored.GetID()).LoadedFromStorage())
				re.Nil(f.GetRegion(stored.GetID()).GetLeader())
				before := f.snapshot()
				re.False(f.step())
				re.Empty(targets)
				re.Equal(before.Groups, f.snapshot().Groups)
				f.PutRegion(heartbeat)
				f.affinityManager.GetAndCacheRegionAffinityGroupState(heartbeat)
				re.False(f.GetRegion(heartbeat.GetID()).LoadedFromStorage())
			} else {
				re.Nil(f.GetRegion(heartbeat.GetID()))
			}
			re.True(f.step())
			re.Len(targets, 1)
			re.Equal(uint64(3), targets[0].LeaderStoreID)
			written, err := f.affinityManager.UpdateAffinityGroupTargets(t.Context(), targets)
			re.NoError(err)
			re.Equal([]string{targets[0].Expected.ID}, written.WrittenGroupIDs)
			if source == "missing" {
				// Publish the target before the missing Region's first heartbeat.
				f.PutRegion(heartbeat)
			}
			ops := f.Check(heartbeat)
			re.Len(ops, 1)
			f.applyBalanceMove(ops[0], 3)
		})
	}
}

func TestAffinityBalanceTiFlashRecordRules(t *testing.T) {
	for _, differentVoterRules := range []bool{false, true} {
		t.Run(fmt.Sprintf("different-voter-rules=%t", differentVoterRules), func(t *testing.T) {
			re := require.New(t)
			enableFastAffinityAvailabilityCheck(t)
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
				recordHex, endHex := hex.EncodeToString(record), hex.EncodeToString(end)
				// TiDB's TiFlash rule starts at the record prefix, inside the
				// affinity range, leaving the index Region without a learner.
				f.setRules(&placement.Rule{
					GroupID: "tiflash", ID: fmt.Sprintf("table-%d-r", partition), Role: placement.Learner, Count: 1,
					StartKeyHex: recordHex, EndKeyHex: endHex,
					LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}},
				})
				if differentVoterRules {
					rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
					rule.ID, rule.Index, rule.Override = fmt.Sprintf("record-%d", partition), 1, true
					rule.StartKeyHex, rule.EndKeyHex = recordHex, endHex
					rule.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"tiflash"}}}
					f.setRules(rule)
				}
				id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", partition))
				re.NoError(createAffinityGroupForTest(f.affinityManager,
					&affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
					keyutil.KeyRange{StartKey: start, EndKey: end}))
				f.putAlignedRegions(uint64(2*i+1), 1, []uint64{1, 2, 3}, string(start), string(record), string(end))
				recordRegion := learnerAt(f.GetRegion(uint64(2*i+2)), 5)
				f.PutRegion(recordRegion)
				_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(recordRegion)
				re.True(aligned)
				for j := range 2 {
					re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, f.GetRegion(uint64(2*i+j+1))).IsSatisfied())
				}
			}
			f.waitForStoreScan(balanceTestGroupID("_tidb_pt_100_p101"))
			snapshot := f.snapshot()
			revision := f.ruleManager.GetRevision()
			res := f.plan(snapshot)
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
				_, aligned := f.affinityManager.GetRegionAffinityGroupState(moved)
				re.True(aligned)
				re.Equal(learners, moved.GetLearners())
				re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
			}
		})
	}
}

func TestAffinityBalanceKeepsUnmodelledGroups(t *testing.T) {
	for _, reason := range []string{"same rules", "different rules", "missing label", "witness", "unknown policy"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 5, "a", "b", "c", "a", "")
			voters := []uint64{1, 2, 3}
			switch reason {
			case "missing label":
				voters = []uint64{1, 2, 5}
			case "same rules", "different rules", "witness":
				rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.ID, rule.Index, rule.Override = "subrange", 1, true
				endKey := "b"
				switch reason {
				case "different rules":
					rule.IsolationLevel = "zone"
				case "witness":
					rule.IsWitness, rule.Count, endKey = true, 1, "c"
				}
				rule.StartKeyHex = hex.EncodeToString([]byte(balanceTestKey("a")))
				rule.EndKeyHex = hex.EncodeToString([]byte(balanceTestKey(endKey)))
				f.setRules(rule)
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
			plan := f.balancePlanInput(before, nil, nil)
			re.Len(plan.input.Groups, 1)
			if reason == "unknown policy" {
				re.Equal(1, plan.groupCounts[balanceGroupFixed])
			} else {
				re.Equal(1, plan.groupCounts[balanceGroupUnsupported])
			}
			re.True(f.step())
			re.Equal(before.Groups[0].Group, f.affinityManager.GetAffinityGroupState(before.Groups[0].ID).Group)
			re.NotContains(f.affinityManager.GetAffinityGroupState(balanceTestGroupID("_tidb_pt_100_p2")).VoterStoreIDs, uint64(5))
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
			called := false
			beforeAffinityBalanceCommit(t, func() {
				if called {
					return
				}
				called = true
				switch change {
				case "rule":
					f.setRules(f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID))
				case "policy":
					f.policy(affinity.BalancePolicyFixed, ids[0])
				case "cancel":
					cancel()
				default:
					f.disableBalance(change)
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
			re.Equal(1.0, testutil.ToFloat64(affinityBalanceTargetsPlanned)-plannedTargets)
			re.Equal(writtenTargets, testutil.ToFloat64(affinityBalanceTargetsWritten))
			if change == "policy" {
				snapshot.Groups[0].BalancePolicy = affinity.BalancePolicyFixed
			}
			re.Equal(snapshot.Groups, f.snapshot().Groups)
			if change == "limit" {
				re.Empty(f.balancer.idle)
				f.SetAffinityScheduleLimit(4)
				re.True(f.step())
			}
		})
	}
}

func TestAffinityBalanceCommitChanges(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fixedGroups int
	}{
		{name: "other group", fixedGroups: 2},
		{name: "partial write", fixedGroups: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			enableFastAffinityAvailabilityCheck(t)
			f := newAffinityBalanceTest(t, 3)
			ids := f.seed("100", 1, 1, 1, 1)
			fixed := ids[1 : 1+tc.fixedGroups]
			f.policy(affinity.BalancePolicyFixed, fixed...)
			f.waitForStoreScan(ids[0])
			snapshot, revision := f.snapshot(), f.ruleManager.GetRevision()
			res := f.plan(snapshot)
			re.Len(res.targets, len(ids)-len(fixed))
			first := res.targets[0]
			changedID := ids[1]
			if tc.name == "partial write" {
				second := res.targets[1]
				re.NotEqual(first.ID, second.ID)
				re.NotEqual(first.LeaderStoreID, second.LeaderStoreID)
				changedID = second.ID
			}
			var beforeCommit *affinity.BalanceSnapshot
			beforeAffinityBalanceCommit(t, func() {
				if beforeCommit != nil {
					return
				}
				_, err := f.affinityManager.UpdateAffinityGroupPeers(changedID, first.LeaderStoreID, []uint64{1, 2, 3})
				re.NoError(err)
				beforeCommit = f.snapshot()
			})
			written, reason, err := f.commitBalancePlan(t.Context(), snapshot, res, revision)
			re.NoError(err)
			re.Empty(reason)
			re.Equal([]string{first.ID}, written)
			re.NotNil(beforeCommit)
			re.Greater(beforeCommit.Version, snapshot.Version)
			afterCommit := f.snapshot()
			re.Greater(afterCommit.Version, beforeCommit.Version)
			re.Equal(*first, afterCommit.Group(first.ID).Group)
			re.Equal(beforeCommit.Group(changedID).Group, afterCommit.Group(changedID).Group)
			// Both written layouts use the same leader until the next plan.
			re.Equal([2]int{27, 5}, balanceTableScore(f.affinityManager, balanceTestTable()))

			f.settle()
			snapshot = f.snapshot()
			res = f.plan(snapshot)
			re.Len(res.targets, 1)
			re.NotContains(fixed, res.targets[0].ID)
			written, reason, err = f.commitBalancePlan(t.Context(), snapshot, res, f.ruleManager.GetRevision())
			re.NoError(err)
			re.Empty(reason)
			re.Equal([]string{res.targets[0].ID}, written)
			re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable()))
			for _, id := range fixed {
				re.Equal(afterCommit.Group(id).Group, f.affinityManager.GetAffinityGroupState(id).Group)
			}
		})
	}
}

func TestAffinityBalanceExecutionAfterRegionChanges(t *testing.T) {
	// Check split and merge execution, and both target rejection gates.
	for _, tc := range []struct{ event, gate string }{
		{"split", "allowed"}, {"merge", "allowed"}, {"split", "placement"}, {"merge", "best-location"},
	} {
		t.Run(tc.event+"/"+tc.gate, func(t *testing.T) {
			re := require.New(t)
			enableFastAffinityAvailabilityCheck(t)
			f := newAffinityBalanceTest(t, 4, "a", "b", "c", "a")
			f.SetEnableUseJointConsensus(false)
			f.SetMaxAffinityMergeRegionSize(0)
			for id := uint64(1); id <= 4; id++ {
				f.SetStoreLabel(id, map[string]string{"zone": f.GetStore(id).GetLabelValue("zone"), "disk": "ssd"})
			}
			rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.LabelConstraints = []placement.LabelConstraint{{Key: "disk", Op: placement.In, Values: []string{"ssd"}}}
			f.setRules(rule)
			id := balanceTestGroupID("_tidb_pt_100_p1")
			f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
			fixedID := balanceTestGroupID("_tidb_pt_100_p2")
			f.group(fixedID, 3, 1, []uint64{1, 2, 3}, "d", "e")
			f.policy(affinity.BalancePolicyFixed, fixedID)
			f.waitForStoreScan(id)
			snapshot := f.snapshot()
			revision := f.ruleManager.GetRevision()
			res := f.plan(snapshot)
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
					f.applyBalanceMove(ops[0], target.LeaderStoreID)
					re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(f.GetRegion(region.GetID()), true))
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
			var expired *affinity.GroupState
			var writtenTargets []affinity.GroupTarget
			var firstWinCount atomic.Int32
			done := make(chan error, 1)
			f.affinityManager.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
				if firstWinCount.Add(1) == 1 {
					expired, writtenTargets = f.affinityManager.GetAffinityGroupState(id), targets
				}
				ids, err := f.affinityManager.UpdateAffinityGroupTargets(t.Context(), targets)
				select {
				case done <- err:
				default:
				}
				return ids, err
			}))
			re.Empty(f.Check(region))
			select {
			case err := <-done:
				re.NoError(err)
			case <-time.After(10 * time.Second):
				re.FailNow("first-win did not write a target")
			}
			re.True(expired.RegularSchedulingAllowed)
			re.False(expired.AffinitySchedulingAllowed)
			re.Len(writtenTargets, 1)
			re.Equal(writtenTargets[0].Expected, *target)
			re.Equal(uint64(1), writtenTargets[0].LeaderStoreID)
			re.Equal([]uint64{1, 2, 3}, writtenTargets[0].VoterStoreIDs)
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
	f := newAffinityBalanceTest(t, 6, "a", "a", "b", "b", "c", "c")
	f.setRules(
		&placement.Rule{ID: "leader", Index: 1, Override: true, Role: placement.Leader, Count: 1, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"a"}}}},
		&placement.Rule{ID: "followers", Index: 2, Role: placement.Follower, Count: 2, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"b", "c"}}}},
	)
	f.seedVoters([]uint64{1, 3, 5}, 1, 1, 1, 1)
	require.True(t, f.step())
	require.Equal(t, [2]int{24, 8}, balanceTableScore(f.affinityManager, balanceTestTable()))
}

func TestAffinityBalanceOverlappingRulesAllowLeaderBalance(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "a", "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	second := rule.Clone()
	second.ID, second.Index = "second", 1
	f.setRules(rule, second)
	for i, id := range f.seedVoters([]uint64{1, 2, 3, 4}, 1, 1) {
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

func TestAffinityBalanceIncludesPersistedLeader(t *testing.T) {
	f := newAffinityBalanceTest(t, 0)
	group := &affinity.GroupState{ID: "g", LeaderStoreID: 99, BalancePolicy: affinity.BalancePolicyFixed}
	plan := f.balancePlanInput(&affinity.BalanceSnapshot{Groups: []*affinity.GroupState{group}}, nil, nil)
	require.Equal(t, []uint64{99}, plan.stores)
	require.Equal(t, []int{1}, plan.input.BaseLeaders)
}

func TestAffinityBalanceRejectsCoLocatedVoterWithTiKVLearner(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "a", "c")
	voters := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	voters.Count = 2
	f.setRules(voters, &placement.Rule{ID: "learner", Role: placement.Learner, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"3"}}}})
	f.AddLeaderRegion(1, 2, 4)
	region := learnerAt(f.GetRegion(1), 3)
	f.UpdateRegionCount(2, 100)
	currentFit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
	re.True(currentFit.IsSatisfied())
	target := &affinity.Group{LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}
	virtual := cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, virtual)
	re.Equal(affinityPlacementReady, f.checkPlacement(virtual, fit, false, false).status)
	status, reason := f.validateBalanceTarget(target, region, currentFit)
	re.Equal(balanceRejected, status)
	re.Equal(balanceTargetLocation, reason)
}

func TestAffinityBalanceSameRoleIsolation(t *testing.T) {
	setup := func(t *testing.T) (*affinityBalanceTest, *placement.Rule, []string) {
		t.Helper()
		f := newAffinityBalanceTest(t, 3, "a", "a", "b")
		first := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
		first.Count = 1
		first.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1"}}}
		second := first.Clone()
		second.ID, second.Index = "second", 1
		second.LabelConstraints[0].Values = []string{"2", "3"}
		f.setRules(first, second)
		return f, second, f.seedVoters([]uint64{1, 3}, 1, 1)
	}

	t.Run("healthy-and-expanded", func(t *testing.T) {
		re := require.New(t)
		enableFastAffinityAvailabilityCheck(t)
		f, second, ids := setup(t)
		f.waitForStoreScan(ids[0])
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
		res := f.plan(snapshot)
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
		f.setRules(second)
		re.True(f.step())
		re.Equal([2]int{6, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
		for _, group := range f.snapshot().Groups {
			re.NotContains(group.VoterStoreIDs, uint64(2))
		}
	})

	t.Run("temporarily-blocked-leader-only", func(t *testing.T) {
		re := require.New(t)
		f, _, ids := setup(t)
		f.blockStore(3, "store limit")
		snapshot := f.snapshot()
		input := f.balancePlanInput(snapshot, nil, nil)
		relaxed := balanceplan.Plan(&input.input, balanceplan.Options{Seed: snapshot.Version})
		targets := input.changedTargets(&relaxed)
		re.Len(targets, 1)
		target := targets[0]
		status, _, failure, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
		re.Equal(balanceTemporarilyBlocked, status)
		re.Equal(balanceTargetLocation, failure)

		res := f.plan(snapshot)
		re.Equal(2, res.rounds)
		re.Len(res.targets, 1)
		re.Contains(ids, res.targets[0].ID)
		re.Equal([]uint64{1, 3}, res.targets[0].VoterStoreIDs)
		re.Equal(uint64(3), res.targets[0].LeaderStoreID)
	})
}

func TestAffinityBalanceLaterRegionPlacement(t *testing.T) {
	f := newAffinityBalanceTest(t, 5, "a", "a", "b", "c", "b")
	f.UpdateRegionCount(3, 100)
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1", "2", "3", "4", "5"}}}
	f.setRules(rule, &placement.Rule{
		ID: "learner", Role: placement.Learner, Count: 1,
		StartKeyHex: hex.EncodeToString([]byte(balanceTestKey("b"))), EndKeyHex: hex.EncodeToString([]byte(balanceTestKey("c"))),
		LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"5"}}},
	})
	f.group(balanceTestGroupID("_tidb_pt_100_p1"), 1, 1, []uint64{1, 4}, "a", "b", "c")
	f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 4}, "d", "e")
	f.policy(affinity.BalancePolicyFixed, balanceTestGroupID("_tidb_pt_100_p2"))
	f.PutRegion(learnerAt(f.GetRegion(2), 5))
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
	blocked := f.plan(snapshot)
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
	res := f.plan(snapshot)
	require.NotEmpty(t, res.targets)
	require.Greater(t, res.rounds, 1)
}
