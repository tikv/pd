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
	"encoding/json"
	"fmt"
	"os"
	"slices"
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
	// The tests drive the balancer directly instead of running it.
	c := NewAffinityChecker(t.Context(), tc, tc.PersistOptions)
	c.startTime = time.Now().Add(-2 * recentMergeTTL)
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

func (f *affinityBalanceTest) step() bool {
	f.tb.Helper()
	re := require.New(f.tb)
	written, err := f.balancer.step(f.tb.Context(), balanceTestTable(), f.affinityManager.GetBalanceCounts())
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

// balanceBenchShape is a table of BenchmarkAffinityBalance. All its groups
// start with the same leader and voters, as the concentrated inputs of
// BenchmarkPlan in balanceplan do.
type balanceBenchShape struct {
	name   string
	stores uint64
	leader uint64
	voters []uint64
	// setup labels the stores and sets the placement rules of a table of groups.
	setup func(f *affinityBalanceTest, groups int)
	// learner, if set, returns the store of a learner added to Region r of a
	// group of n Regions.
	learner func(r, n int) uint64
	// improves reports whether the first plan of the table writes targets.
	improves bool
}

// balanceBenchKey is the start key of Region r of group g in the benchmark
// tables, before balanceTestKey encodes it.
func balanceBenchKey(g, r int) string {
	return fmt.Sprintf("100-%04d-%05d", g, r)
}

func balanceBenchShapes() []balanceBenchShape {
	label := func(f *affinityBalanceTest, labels func(id uint64) map[string]string) {
		for _, store := range f.GetStores() {
			f.SetStoreLabel(store.GetID(), labels(store.GetID()))
		}
	}
	zones := func(n uint64, extra func(id uint64, labels map[string]string)) func(id uint64) map[string]string {
		return func(id uint64) map[string]string {
			labels := map[string]string{"zone": fmt.Sprintf("z%d", (id-1)%n)}
			if extra != nil {
				extra(id, labels)
			}
			return labels
		}
	}
	setRules := func(f *affinityBalanceTest, rules ...*placement.Rule) {
		for _, rule := range rules {
			require.NoError(f.tb, f.ruleManager.SetRule(rule))
		}
	}
	zone := []string{"zone"}
	voterRule := func(f *affinityBalanceTest, count int, labels []string, constraints ...placement.LabelConstraint) *placement.Rule {
		rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
		rule.Count, rule.LocationLabels, rule.LabelConstraints = count, labels, constraints
		return rule
	}
	in := func(key string, values ...string) []placement.LabelConstraint {
		return []placement.LabelConstraint{{Key: key, Op: placement.In, Values: values}}
	}
	// leaderAnd returns a leader rule that overrides the default rule and a rule
	// for two more voters with the given role.
	leaderAnd := func(leader []placement.LabelConstraint, role placement.PeerRoleType, others []placement.LabelConstraint) []*placement.Rule {
		return []*placement.Rule{
			{GroupID: placement.DefaultGroupID, ID: "leader", Index: 1, Override: true, Role: placement.Leader, Count: 1,
				LocationLabels: zone, LabelConstraints: leader},
			{GroupID: placement.DefaultGroupID, ID: "others", Index: 2, Role: role, Count: 2,
				LocationLabels: zone, LabelConstraints: others},
		}
	}
	return []balanceBenchShape{
		{name: "one_per_zone", stores: 300, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, nil))
				setRules(f, voterRule(f, 3, zone))
			}},
		{name: "leader_on_3_stores", stores: 12, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, func(id uint64, labels map[string]string) {
					if id > 3 {
						labels["leader"] = "reject"
					}
				}))
				f.SetLabelProperty("reject-leader", "leader", "reject")
				setRules(f, voterRule(f, 3, zone))
			}},
		{name: "3_replicas_2_zones", stores: 300, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(2, nil))
				setRules(f, voterRule(f, 3, zone))
			}},
		{name: "5_replicas_3_zones", stores: 300, leader: 1, voters: []uint64{1, 2, 3, 4, 5}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, nil))
				setRules(f, voterRule(f, 5, zone))
			}},
		// The first 90% of the groups may place their zone z0 voter only on
		// stores 1 and 4, the other 98 stores of z0 are open to the rest.
		{name: "restricted_90pct", stores: 300, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, groups int) {
				label(f, zones(3, func(id uint64, labels map[string]string) {
					if labels["zone"] == "z0" && id != 1 && id != 4 {
						labels["pool"] = "other"
					}
				}))
				restricted := voterRule(f, 3, zone, placement.LabelConstraint{Key: "pool", Op: placement.NotIn, Values: []string{"other"}})
				restricted.ID, restricted.Index, restricted.Override = "restricted", 1, true
				restricted.StartKeyHex = hex.EncodeToString([]byte(balanceTestKey(balanceBenchKey(0, 0))))
				restricted.EndKeyHex = hex.EncodeToString([]byte(balanceTestKey(balanceBenchKey(groups*9/10, 0))))
				setRules(f, voterRule(f, 3, zone), restricted)
			}},
		// Each of two zones has 50 hosts of 3 stores.
		{name: "zone_host", stores: 300, leader: 1, voters: []uint64{1, 4, 151}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, func(id uint64) map[string]string {
					return map[string]string{"zone": fmt.Sprintf("z%d", (id-1)/150), "host": fmt.Sprintf("h%d", (id-1)/3)}
				})
				setRules(f, voterRule(f, 3, []string{"zone", "host"}))
			}},
		{name: "leader_rule_followers", stores: 300, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, nil))
				setRules(f, leaderAnd(in("zone", "z0"), placement.Follower, in("zone", "z1", "z2"))...)
			}},
		{name: "leader_rule_overlapping_voters", stores: 300, leader: 1, voters: []uint64{1, 2, 3}, improves: true,
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, nil))
				setRules(f, leaderAnd(in("zone", "z0"), placement.Voter, nil)...)
			}},
		// The rules allow only the initial leader and voters.
		{name: "no_improvement", stores: 300, leader: 1, voters: []uint64{1, 2, 3},
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, func(id uint64, labels map[string]string) {
					switch id {
					case 1:
						labels["pin"] = "leader"
					case 2, 3:
						labels["pin"] = "follower"
					}
				}))
				setRules(f, leaderAnd(in("pin", "leader"), placement.Follower, in("pin", "follower"))...)
			}},
		// Voters may use stores 1-30, and a learner rule without constraints
		// allows a learner on any store. The last 27 Regions of a group have
		// their learners on stores 4-30 and the other Regions on stores 31-300,
		// so a target that moves a voter conflicts with a learner near the end
		// of the group, and every round of planning and validation fails until
		// the round limit.
		{name: "learner_conflicts", stores: 300, leader: 1, voters: []uint64{1, 2, 3},
			setup: func(f *affinityBalanceTest, _ int) {
				label(f, zones(3, func(id uint64, labels map[string]string) {
					if id <= 30 {
						labels["pool"] = "voter"
					}
				}))
				learner := &placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}
				setRules(f, voterRule(f, 3, zone, in("pool", "voter")...), learner)
			},
			learner: func(r, n int) uint64 {
				if r >= n-27 {
					return uint64(4 + r - (n - 27))
				}
				return uint64(31 + r%270)
			}},
	}
}

// BenchmarkAffinityBalance measures one step of the balance worker for a
// partition table: the snapshot, the planner input, planning, the Region
// validation of the changed groups and the commit. plan=first plans the
// initial layout of the table, plan=replan the table after it has converged;
// a table that cannot improve has only plan=first. Each step reports its rounds
// of planning and validation and the targets it wrote, and the stops of the
// planner are logged. Tables of 10000 Regions or more, whose first steps take
// seconds on 300 stores, run only with PD_AFFINITY_BALANCE_BENCH_LARGE=1:
//
//	PD_AFFINITY_BALANCE_BENCH_LARGE=1 go test -tags without_dashboard ./pkg/schedule/checker/ \
//		-run '^$' -bench '^BenchmarkAffinityBalance$' -benchmem -benchtime=1x -count=10 -timeout 30m
func BenchmarkAffinityBalance(b *testing.B) {
	level := log.GetLevel()
	log.SetLevel(zapcore.FatalLevel)
	b.Cleanup(func() { log.SetLevel(level) })
	shapes := make(map[string]balanceBenchShape)
	type benchCase struct {
		shape           string
		stores          uint64
		groups, regions int
		large           bool
	}
	var cases []benchCase
	for _, shape := range balanceBenchShapes() {
		shapes[shape.name] = shape
		if shape.learner == nil {
			cases = append(cases, benchCase{shape: shape.name, stores: shape.stores, groups: 1000, regions: 1})
		}
	}
	cases = append(cases,
		benchCase{shape: "one_per_zone", stores: 300, groups: 100, regions: 1},
		benchCase{shape: "one_per_zone", stores: 300, groups: 100, regions: 10},
		benchCase{shape: "learner_conflicts", stores: 300, groups: 24, regions: 30},
		benchCase{shape: "one_per_zone", stores: 300, groups: 100, regions: 100, large: true},
		benchCase{shape: "one_per_zone", stores: 300, groups: 1000, regions: 10, large: true},
		benchCase{shape: "one_per_zone", stores: 30, groups: 24, regions: 500, large: true},
		benchCase{shape: "one_per_zone", stores: 300, groups: 24, regions: 500, large: true},
		benchCase{shape: "learner_conflicts", stores: 300, groups: 24, regions: 500, large: true},
	)
	large := os.Getenv("PD_AFFINITY_BALANCE_BENCH_LARGE") != ""
	for _, c := range cases {
		shape := shapes[c.shape]
		if c.large && !large {
			continue
		}
		plans := []string{"first", "replan"}
		if !shape.improves {
			plans = plans[:1]
		}
		for _, plan := range plans {
			name := fmt.Sprintf("%s/stores=%d/groups=%d/regions=%d/plan=%s", c.shape, c.stores, c.groups, c.regions, plan)
			b.Run(name, func(b *testing.B) {
				benchmarkAffinityBalanceStep(b, shape, c.stores, c.groups, c.regions, plan == "replan")
			})
		}
	}
}

func benchmarkAffinityBalanceStep(b *testing.B, shape balanceBenchShape, stores uint64, groups, regions int, replan bool) {
	b.StopTimer()
	re := require.New(b)
	f := newAffinityBalanceTest(b, stores)
	shape.setup(f, groups)
	ids := make([]string, 0, groups)
	for g := range groups {
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1))
		keys := make([]string, 0, regions+1)
		for r := range regions + 1 {
			keys = append(keys, balanceBenchKey(g, r))
		}
		firstRegion := uint64(g*regions + 1)
		f.group(id, firstRegion, shape.leader, shape.voters, keys...)
		for r := 0; shape.learner != nil && r < regions; r++ {
			peer, err := f.AllocPeer(shape.learner(r, regions))
			re.NoError(err)
			peer.Role = metapb.PeerRole_Learner
			region := f.GetRegion(firstRegion + uint64(r)).Clone(core.WithAddPeer(peer))
			f.PutRegion(region)
			f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
		}
		ids = append(ids, id)
	}
	for rounds := 0; replan && f.step(); rounds++ {
		re.Less(rounds, 100, "table did not converge")
		f.settle()
	}
	stops := func() map[string]float64 {
		counts := make(map[string]float64, len(affinityBalancePlannerStops))
		for stop, counter := range affinityBalancePlannerStops {
			counts[string(stop)] = testutil.ToFloat64(counter)
		}
		return counts
	}
	var rounds, written float64
	stopped := make(map[string]float64)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if !replan {
			for _, id := range ids {
				_, err := f.affinityManager.UpdateAffinityGroupPeers(id, shape.leader, shape.voters)
				re.NoError(err)
			}
			f.settle()
		}
		clear(f.balancer.idle)
		before, writtenBefore := stops(), testutil.ToFloat64(affinityBalanceTargetsWritten)
		b.StartTimer()
		re.Equal(!replan && shape.improves, f.step())
		b.StopTimer()
		for stop, count := range stops() {
			stopped[stop] += count - before[stop]
			rounds += count - before[stop]
		}
		written += testutil.ToFloat64(affinityBalanceTargetsWritten) - writtenBefore
	}
	b.ReportMetric(rounds/float64(b.N), "rounds/op")
	b.ReportMetric(written/float64(b.N), "targets/op")
	reasons := make([]string, 0, len(stopped))
	for stop, count := range stopped {
		if count > 0 {
			reasons = append(reasons, fmt.Sprintf("%s=%g", stop, count/float64(b.N)))
		}
	}
	slices.Sort(reasons)
	b.Logf("planner stops per step: %s", strings.Join(reasons, " "))
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
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable()))
	re.True(f.step())
	group := f.affinityManager.GetAffinityGroupState(ids[0])
	re.Equal([]uint64{2, 3, 4}, group.VoterStoreIDs)
	re.Equal(uint64(2), group.LeaderStoreID)
	re.Equal([2]int{23, 5}, balanceTableScore(f.affinityManager, balanceTestTable()))
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
	re.True(f.step())
	// The TiFlash store holds a learner of every Region and is never a voter target.
	for _, group := range f.snapshot().Groups {
		re.NotContains(group.VoterStoreIDs, uint64(6))
	}
	target := f.affinityManager.GetAffinityGroupState(blocked)
	re.NotContains(target.VoterStoreIDs, uint64(4))
	re.Contains(f.affinityManager.GetAffinityGroupState(ids[1-groupIndex]).VoterStoreIDs, uint64(4))
	op := f.createAffinityOperator(later, target)
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
	written, err := f.affinityManager.UpdateAffinityGroupTargets(writer.targets)
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
	// Region storage persists metadata, not the leader supplied by a heartbeat.
	stored := core.NewRegionInfo(heartbeat.GetMeta(), nil, core.SetSource(core.Storage))
	f.PutRegion(stored)
	re.True(f.GetRegion(stored.GetID()).LoadedFromStorage())
	re.Nil(f.GetRegion(stored.GetID()).GetLeader())
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
			t.Logf("targets=%d mutable=%d unsupported=%d", len(res.targets),
				res.groupCounts[balanceGroupMutable], res.groupCounts[balanceGroupUnsupported])
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
				raw, err := f.GetStorage().LoadAffinityGroup(target.ID)
				re.NoError(err)
				var stored affinity.Group
				re.NoError(json.Unmarshal([]byte(raw), &stored))
				re.Equal(*target, stored)
				re.Equal(stored, f.affinityManager.GetAffinityGroupState(target.ID).Group)
			}
			score := balanceTableScore(f.affinityManager, balanceTestTable())
			re.Less(score[0], 27)
			re.Less(score[1], 9)
			for _, region := range f.GetRegions() {
				group, _ := f.affinityManager.GetRegionAffinityGroupState(region)
				learners := region.GetLearners()
				if op := f.createAffinityOperator(region, group); op != nil {
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
			case "busy":
				f.SetStoreBusy(4, true)
			case "store limit":
				f.SetStoreLimit(4, storelimit.AddPeer, 0.00006)
				f.ResetStoreLimit(4, storelimit.AddPeer, 0.000001)
				re.True(f.GetStore(4).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
				re.False(f.GetStore(4).IsAvailable(storelimit.AddPeer, constant.Medium))
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

// TestAffinityOperatorTargetStores tests that no operator moves a Region to a
// new voter or leader store that cannot take it now, as the rule checker and the
// balance schedulers skip such stores.
func TestAffinityOperatorTargetStores(t *testing.T) {
	removing := func(f *affinityBalanceTest, id uint64) {
		f.PutStore(f.GetStore(id).Clone(core.SetNodeState(metapb.NodeState_Removing)))
	}
	busy := func(f *affinityBalanceTest, id uint64) { f.SetStoreBusy(id, true) }
	for _, tc := range []struct {
		name              string
		leader, temporary bool
		set               func(f *affinityBalanceTest, id uint64)
	}{
		{name: "removing", set: removing},
		{name: "down", set: func(f *affinityBalanceTest, id uint64) { f.SetStoreDown(id) }},
		{name: "special use", set: func(f *affinityBalanceTest, id uint64) {
			f.SetStoreLabel(id, map[string]string{filter.SpecialUseKey: filter.SpecialUseReserved})
		}},
		{name: "low space", set: func(f *affinityBalanceTest, id uint64) {
			f.UpdateRegionCount(id, core.InitClusterRegionThreshold)
			f.UpdateStorageRatio(id, 0.9, 0.1)
		}},
		{name: "busy", temporary: true, set: busy},
		{name: "store limit", temporary: true, set: func(f *affinityBalanceTest, id uint64) {
			f.SetStoreLimit(id, storelimit.AddPeer, 0.00006)
			f.ResetStoreLimit(id, storelimit.AddPeer, 0.000001)
			f.GetStore(id).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium)
		}},
		{name: "leader removing", leader: true, set: removing},
		{name: "leader busy", leader: true, temporary: true, set: busy},
		{name: "reject-leader", leader: true, set: func(f *affinityBalanceTest, id uint64) {
			f.SetStoreLabel(id, map[string]string{"leader": "reject"})
			f.SetLabelProperty("reject-leader", "leader", "reject")
		}},
		{name: "evict-leader", leader: true, set: func(f *affinityBalanceTest, id uint64) {
			require.NoError(f.tb, f.PauseLeaderTransfer(id, constant.In))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			re.NotNil(f.createAffinityOperator(region, group))
			counter := affinityCheckerTargetStoreRejectedCounter
			if tc.temporary {
				counter = affinityCheckerTargetStoreBlockedCounter
			}
			before := testutil.ToFloat64(counter)
			tc.set(f, store)
			re.Nil(f.createAffinityOperator(region, group))
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
			if change == "cancel" {
				result = affinityBalanceError
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
			re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[result])-plans)
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
	// Both events are checked with the operators of an allowed target. A
	// rejected target takes the same expiration and first-win path after either
	// event, so each rejecting gate runs once.
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
				re.Equal(bestLocationRejected, f.regionPlacementBestLocation(virtual, fit))
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
				ids, err := f.affinityManager.UpdateAffinityGroupTargets(targets)
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
			raw, err := f.GetStorage().LoadAffinityGroup(id)
			re.NoError(err)
			var stored affinity.Group
			re.NoError(json.Unmarshal([]byte(raw), &stored))
			re.Equal(learned.Group, stored)
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
	for i := range 4 {
		group := f.affinityManager.GetAffinityGroupState(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)))
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
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1))
		f.group(id, uint64(i+1), 1, []uint64{1, 2, 3, 4}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
		group, region := f.affinityManager.GetAffinityGroupState(id), f.GetRegion(uint64(i+1))
		fit, status := f.validateBalanceCurrentRegion(group, region)
		re.Equal(balanceAllowed, status)
		re.True(fit.IsSatisfied())
		target := group.Group
		target.LeaderStoreID = 2
		re.Equal(balanceAllowed, f.validateBalanceTarget(&target, region, fit))
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
