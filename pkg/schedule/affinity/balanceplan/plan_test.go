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

package balanceplan

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// general assigns voters in rule order, starting at store 0.
// A positive zone count gives store s the location s % zones.
func general(groups, stores, zones int, counts ...int) *Input {
	ids := make([]int, stores)
	var loc [][]int32
	if zones > 0 {
		loc = deepPaths(stores, 1, func(s int) int { return s % zones })
	}
	for s := range ids {
		ids[s] = s
	}
	rules := make([]*Rule, len(counts))
	var ruleOf []int
	for ri, count := range counts {
		rules[ri] = &Rule{Count: count, Stores: ids, Location: loc}
		ruleOf = append(ruleOf, slices.Repeat([]int{ri}, count)...)
	}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	can := allTrue(stores)
	for range groups {
		in.Groups = append(in.Groups, Group{
			Rules: rules, Voters: slices.Clone(ids[:len(ruleOf)]), RuleOf: slices.Clone(ruleOf), CanLead: can,
		})
	}
	return in
}

func singleGroup(stores, zones int, counts ...int) (*Input, *Group) {
	in := general(1, stores, zones, counts...)
	return in, &in.Groups[0]
}

func TestOverlappingRulesFindImprovement(t *testing.T) {
	// The cheapest voter store is also the follower's only store. Include
	// duplicate, unsorted stores to exercise normalization of the input.
	in, g := singleGroup(5, 1, 3)
	g.Rules = []*Rule{
		{Role: RoleLeader, Count: 1, Stores: []int{0, 3}},
		{Role: RoleVoter, Count: 1, Stores: []int{2, 1, 2}},
		{Role: RoleFollower, Count: 1, Stores: []int{1}},
	}
	g.Voters, g.RuleOf = []int{0, 2, 1}, []int{0, 1, 2}
	g.CanLead = []bool{true, false, false, true, false}
	in.BaseVoters, in.BaseLeaders = []int{4, 0, 4, 0, 4}, []int{4, 0, 0, 0, 0}
	r := Plan(in, Options{})
	require.Equal(t, Score{67, 25}, r.Baseline)
	require.Equal(t, Score{59, 17}, r.Score)
	require.Equal(t, 3, r.Leaders[0])
	require.ElementsMatch(t, []int{1, 2, 3}, r.Voters[0])
	require.Equal(t, []int{0, 2, 1}, g.Voters)
	require.Equal(t, []int{2, 1, 2}, g.Rules[1].Stores)
	require.Zero(t, g.Rules[1].level)
	require.Nil(t, g.Rules[1].domainOf)
}

func TestOverlappingIsolationAllowsLeaderBalance(t *testing.T) {
	in := general(2, 4, 1, 2, 2)
	in.Groups[0].Rules[0].Location[3][0] = 1
	r := Plan(in, Options{})
	require.Equal(t, Score{16, 4}, r.Baseline)
	require.Equal(t, Score{16, 2}, r.Score)
	for i, g := range in.Groups {
		require.Equal(t, g.Voters, r.Voters[i])
	}
	require.Equal(t, 1, r.Changed)
}

func leaderLayout(stores int, voters ...[]int) *Input {
	in := general(len(voters), stores, 0, 2)
	for i, v := range voters {
		in.Groups[i].Voters, in.Groups[i].Leader = v, v[0]
	}
	return in
}

// Either transfer ties on its own; together they move one leader from 0 to 2.
func TestLeaderChains(t *testing.T) {
	for _, name := range []string{"chain", "cannot lead", "pinned", "follower"} {
		t.Run(name, func(t *testing.T) {
			in := leaderLayout(3, []int{0, 1}, []int{0, 1}, []int{1, 2})
			in.BaseVoters = []int{1, 0, 2}
			g := &in.Groups[2]
			want, changed := Score{27, 5}, 0
			switch name {
			case "chain":
				want, changed = Score{27, 3}, 2
			case "cannot lead":
				g.CanLead = []bool{true, true, false}
			case "pinned":
				g.LeaderPinned = true
			case "follower":
				g.Rules = []*Rule{{Count: 1, Stores: []int{1}}, {Role: RoleFollower, Count: 1, Stores: []int{2}}}
				g.RuleOf = []int{0, 1}
			}
			r := Plan(in, Options{})
			require.Equal(t, Score{27, 5}, r.Baseline)
			require.Equal(t, want, r.Score)
			require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score)
			require.Equal(t, StopGreedy, r.Stop)
			require.Equal(t, changed, r.Changed)
			require.Equal(t, [][]int{{0, 1}, {0, 1}, {1, 2}}, r.Voters)
		})
	}
}

// Both paths pass through store 1, so a cutoff after one round keeps one transfer path.
func TestLeaderChainStops(t *testing.T) {
	for reason, delta := range map[StopReason]int64{"": -6, StopCanceled: -4, StopBudget: -4} {
		t.Run(string(reason), func(t *testing.T) {
			in := leaderLayout(4, []int{0, 1}, []int{0, 1}, []int{0, 1}, []int{1, 2}, []int{1, 3})
			in.BaseVoters = []int{2, 0, 4, 4}
			p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
			_, leaders := p.layout()
			before := p.score()
			switch reason {
			case StopCanceled:
				p.opt.Cancel = func() bool { return p.visits > 0 } // Stop after the first round.
			case StopBudget:
				p.opt.WorkBudget = p.work + 11 // Entry charges ten voters.
			}
			d := p.chainLeaders(leaders)
			require.Equal(t, reason, p.stop)
			require.Equal(t, delta, d)
			voters, working := p.layout()
			require.Equal(t, working, leaders)
			require.Equal(t, Score{before.Voter, before.Leader + d}, p.score())
			require.Equal(t, p.score(), scoreOf(in, voters, leaders))
		})
	}
}

func TestLeaderChainStopsBuildingAdjacency(t *testing.T) {
	const budget = 12
	in := general(1000, 3, 0, 3)
	for i := range in.Groups {
		in.Groups[i].LeaderPinned = i != 0
	}
	p := newPlanner(in, Options{WorkBudget: budget})
	_, leaders := p.layout()
	require.Zero(t, p.chainLeaders(leaders))
	require.Equal(t, StopBudget, p.stop)
	require.EqualValues(t, budget, p.work, "pinned groups after the cutoff must not be scanned")
}

func deepPaths(stores, depth int, domain func(int) int) [][]int32 {
	loc := make([][]int32, stores)
	for s := range loc {
		loc[s] = make([]int32, depth)
		loc[s][depth-1] = int32(domain(s))
	}
	return loc
}

func TestDeepLocationLabels(t *testing.T) {
	in := general(2, 6, 2, 3)
	loc := deepPaths(in.Stores, 9, func(s int) int { return s % 2 })
	in.Groups[0].Rules[0].Location = loc
	r := Plan(in, Options{})
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, 2.0, isolationScore(loc, r.Voters[0], 9))
}

func checkNoGroups(t *testing.T, in *Input) {
	t.Helper()
	r := Plan(in, Options{})
	require.Equal(t, StopNoGroups, r.Stop)
	require.Equal(t, r.Baseline, r.Score)
	for i, g := range in.Groups {
		require.Equal(t, g.Voters, r.Voters[i])
		require.Equal(t, g.Leader, r.Leaders[i])
	}
}

func TestHardIsolationScaleOut(t *testing.T) {
	in := general(4, 6, 1, 4)
	rule := in.Groups[0].Rules[0]
	rule.Location = [][]int32{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 4}, {1, 5}}
	rule.IsolationDepth = 2
	p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
	g := &p.gs[0]
	iso, ok := p.isolation(g, 0)
	require.True(t, ok)
	require.Equal(t, [maxDepth]int32{3}, iso.pairs)
	_, ok = p.pickRule(g, 0, []int{3, 4}, nil, true)
	require.False(t, ok, "two mandatory peers cannot share dc2/z4")
	r := Plan(in, Options{Seed: 7})
	require.Equal(t, int64(64), r.Baseline.Voter)
	require.Equal(t, int64(44), r.Score.Voter)
	loads := make([]int, in.Stores)
	for i, voters := range r.Voters {
		require.True(t, matchesTarget(legalTargets(in, i), voters, nil, r.Leaders[i]))
		for _, s := range voters {
			loads[s]++
		}
	}
	require.Equal(t, []int{3, 3, 3, 2, 2, 3}, loads)
	require.True(t, slices.ContainsFunc(r.Voters, func(v []int) bool {
		return sameSet(v, []int{0, 1, 3, 5}) || sameSet(v, []int{0, 1, 4, 5})
	}), "a target must use z1, z2, z5 and z4")
}

func TestHardIsolationPaths(t *testing.T) {
	deep := deepPaths(4, maxDepth, func(s int) int { return s / 2 })
	for _, c := range []struct {
		name     string
		location [][]int32
		depth    int
		voters   []int
		base     []int
		want     []int
	}{
		{"first level", [][]int32{{0, 0}, {0, 1}, {1, 0}, {1, 1}}, 1, []int{0, 2}, []int{4, 0, 4, 0}, []int{1, 3}},
		{"full path", [][]int32{{0, 0}, {0, 1}, {1, 0}, {1, 1}}, 2, []int{0, 1, 2}, []int{0, 0, 4, 0}, []int{0, 1, 3}},
		{"shared prefix", [][]int32{{0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {0, 1, 1}}, 2, []int{0, 2}, []int{4, 0, 4, 0}, []int{1, 3}},
		{"maximum depth", deep, maxDepth, []int{0, 2}, []int{4, 0, 4, 0}, []int{1, 3}},
	} {
		t.Run(c.name, func(t *testing.T) {
			in, g := singleGroup(len(c.location), 1, len(c.voters))
			g.Voters, g.Rules[0].Location, g.Rules[0].IsolationDepth = c.voters, c.location, c.depth
			in.BaseVoters = c.base
			r := Plan(in, Options{})
			require.ElementsMatch(t, c.want, r.Voters[0])
			require.True(t, matchesTarget(legalTargets(in, 0), r.Voters[0], nil, r.Leaders[0]))
			p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
			combinations(g.Rules[0].Stores, len(c.voters), func(stores []int) {
				if meetsIsolation(g.Rules[0], stores) {
					return
				}
				// A shared prefix cannot be skipped past the hard boundary.
				picks := p.pickAll(&p.gs[0], p.gs[0].Rules[0], stores, nil, 0, len(stores), func(int) bool { return true })
				require.False(t, picks[0][len(stores)].ok)
				require.False(t, picks[1][len(stores)].ok)
				_, ok := p.pickRule(&p.gs[0], 0, stores, nil, true)
				require.False(t, ok, "colliding mandatory peers: %v", stores)
			})
		})
	}
}

func TestUnsupportedGroupsStayFixed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		count    int
		depth    int
		location [][]int32
		forced   []int
	}{
		{"rule count mismatch", 4, 1, [][]int32{{0}, {1}, {2}, {0}, {1}, {2}}, nil},
		{"too many labels", 3, 1, deepPaths(6, maxDepth+1, func(int) int { return 0 }), nil},
		{"negative", 3, -1, [][]int32{{0}, {1}, {2}, {0}, {1}, {2}}, nil},
		{"too deep", 3, 2, [][]int32{{0}, {1}, {2}, {0}, {1}, {2}}, nil},
		{"no labels", 3, 1, nil, nil},
		{"infeasible count", 3, 1, [][]int32{{0}, {1}, {0}, {1}, {0}, {1}}, nil},
		{"current collision", 3, 1, [][]int32{{0}, {0}, {2}, {0}, {1}, {2}}, nil},
		{"pinned collision", 3, 1, [][]int32{{0}, {0}, {2}, {0}, {1}, {2}}, []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := general(2, 6, 0, 3)
			rule := in.Groups[0].Rules[0]
			rule.Count, rule.IsolationDepth, rule.Location = tc.count, tc.depth, tc.location
			for i := range in.Groups {
				in.Groups[i].Forced, in.Groups[i].LeaderPinned = tc.forced, len(tc.forced) > 0
			}
			checkNoGroups(t, in)
		})
	}
	checkNoGroups(t, general(3, 40, 3, 15))

	t.Run("excluded current voter", func(t *testing.T) {
		in, g := singleGroup(5, 2, 2)
		g.Exclude = []int{1, 3} // No eligible voter remains in the second zone.
		in.BaseVoters[1] = 3
		checkNoGroups(t, in)
	})
	t.Run("forced store outside current voters", func(t *testing.T) {
		in, g := singleGroup(3, 0, 2)
		g.Forced = []int{2}
		checkNoGroups(t, in)
	})
}

// General isolation must count scans and stop within one visit of the budget.
func TestGeneralShapesAreBounded(t *testing.T) {
	for _, c := range []struct {
		replicas, zones int
		budget, ceiling int64
		stop            StopReason
	}{
		{3, 2, 0, DefaultWorkBudget, StopGreedy},
		{5, 3, 0, DefaultWorkBudget, StopGreedy},
		{3, 2, 50_000, 51_999, StopBudget},
	} {
		r := Plan(general(100, 300, c.zones, c.replicas), Options{WorkBudget: c.budget})
		require.Equal(t, c.stop, r.Stop, "%+v", c)
		require.LessOrEqual(t, r.Work, c.ceiling, "%+v", c)
		require.False(t, r.Baseline.Less(r.Score), "%+v", c)
	}
}

// Other tables' counts break ties without changing this table's score.
func TestOtherTablesSpreadTies(t *testing.T) {
	in := general(4, 30, 3, 3)
	alone := Plan(in, Options{})
	in.OtherVoters, in.OtherLeaders = make([]int, 30), make([]int, 30)
	for s := 3; s < 15; s++ {
		in.OtherVoters[s], in.OtherLeaders[s] = 5, 2 // used by other tables
	}
	r := Plan(in, Options{})
	require.Positive(t, r.Changed)
	for _, voters := range r.Voters {
		require.False(t, slices.ContainsFunc(voters, func(store int) bool { return in.OtherVoters[store] != 0 }), "busy store in %v", voters)
	}
	require.Equal(t, alone.Score, r.Score) // only ties change
}

func TestGreedyKeepsSweepTieBreaks(t *testing.T) {
	t.Run("other tables' voters", func(t *testing.T) {
		in := leaderLayout(5, []int{0, 1}, []int{0, 2})
		in.Groups[0].LeaderPinned, in.Groups[1].LeaderPinned = true, true
		in.Groups[0].Rules = []*Rule{{Count: 2, Stores: []int{0, 1, 2, 3}}}
		in.Groups[1].Rules = []*Rule{{Count: 2, Stores: []int{0, 2, 4}}}
		in.BaseVoters = []int{0, 2, 1, 1, 0}
		in.OtherVoters = []int{0, 0, 0, 1, 0}

		// The first group moves to 3 before the second group frees store 2.
		p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
		first, _, score := p.greedy(0)
		require.ElementsMatch(t, []int{0, 3}, first[0])
		require.ElementsMatch(t, []int{0, 4}, first[1])

		p = newPlanner(in, Options{Sweeps: 2, WorkBudget: DefaultWorkBudget})
		second, _, finalScore := p.greedy(0)
		require.Equal(t, score, finalScore)
		require.ElementsMatch(t, []int{0, 2}, second[0])
		require.ElementsMatch(t, []int{0, 4}, second[1])
	})

	t.Run("original voter", func(t *testing.T) {
		in := leaderLayout(4, []int{0, 1}, []int{1})
		in.Groups[0].LeaderPinned = true
		in.Groups[0].Rules = []*Rule{{Count: 2, Stores: []int{0, 1, 2}}}
		in.Groups[1].Rules = []*Rule{{Count: 1, Stores: []int{1, 3}}}
		in.Groups[1].RuleOf = nil
		in.BaseLeaders[1] = 1

		// The first group moves off store 1. The second can then trade
		// store 1 for 3 to improve the leader score, so the first can return.
		p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
		first, _, score := p.greedy(0)
		require.ElementsMatch(t, []int{0, 2}, first[0])
		require.Equal(t, []int{3}, first[1])

		p = newPlanner(in, Options{Sweeps: 2, WorkBudget: DefaultWorkBudget})
		second, _, finalScore := p.greedy(0)
		require.Equal(t, score, finalScore)
		require.ElementsMatch(t, in.Groups[0].Voters, second[0])
		require.Equal(t, []int{3}, second[1])
	})
}

func TestScaleOutKeepsExistingTargets(t *testing.T) {
	const groups, stores = 900, 300
	for _, replicas := range []int{3, 5} {
		in := general(groups, stores+1, replicas/2+1, replicas)
		for i := range in.Groups {
			g := &in.Groups[i]
			for k := range g.Voters {
				g.Voters[k] = (i + k) % stores
			}
			g.Leader = i % stores
		}
		r := Plan(in, Options{Seed: 4242})
		require.True(t, r.Score.Less(r.Baseline))
		moves := 0
		for i, g := range in.Groups {
			for _, s := range r.Voters[i] {
				if !slices.Contains(g.Voters, s) {
					moves++
				}
			}
		}
		require.Positive(t, moves)
		require.LessOrEqual(t, moves, 3*groups*replicas/stores)
	}
}

// Forced and unforced searches share bounds across all overlapping rules.
func TestOverlappingSearchBounds(t *testing.T) {
	const stores = 300
	for _, c := range []struct {
		rules, count, forced int
		stop                 StopReason
	}{{3, 4, 11, StopGreedy}, {30, 1, 29, StopGreedy}, {30, 1, 0, StopCanceled}, {30, 1, 0, StopBudget}} {
		t.Run(string(c.stop)+"/"+strconv.Itoa(c.forced), func(t *testing.T) {
			in, g := singleGroup(stores, 0, slices.Repeat([]int{c.count}, c.rules)...)
			g.Forced = g.Voters[:c.forced]
			for _, store := range g.Voters {
				in.BaseVoters[store] = 1
			}
			in.BaseLeaders[0] = 1
			opt := Options{}
			switch c.stop {
			case StopBudget:
				opt.WorkBudget = 1000
			case StopCanceled:
				checks := 0
				opt.Cancel = func() bool {
					checks++
					return checks >= 2
				}
			}
			r := Plan(in, opt)
			if c.forced > 0 {
				require.Less(t, r.Work/int64(r.Visits), int64(DefaultWorkBudget/10))
				require.LessOrEqual(t, r.Work, int64(DefaultWorkBudget+DefaultWorkBudget/10))
				require.True(t, r.Score.Less(r.Baseline))
				require.Subset(t, r.Voters[0], g.Forced)
			} else {
				require.Equal(t, c.stop, r.Stop)
				require.Equal(t, 1, r.Visits)
				require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score)
				require.False(t, r.Baseline.Less(r.Score))
				// One root can finish its scans before the next cutoff check.
				require.LessOrEqual(t, r.Work, int64(2*c.rules*stores+c.rules))
			}
		})
	}
}

// Whole-table balancing and initialization one group at a time attain the optimum.
func TestBroadLifecycle(t *testing.T) {
	for _, c := range []struct {
		n          int
		restricted bool
		want       Score
	}{
		{36, false, Score{972, 108}},
		{36, true, Score{1008, 432}},
		{128, true, Score{12746, 5462}},
	} {
		in := general(c.n, 12, 3, 3)
		if c.restricted {
			clear(in.Groups[0].CanLead[3:]) // One leader store per zone.
		}
		r := Plan(in, Options{})
		require.Equal(t, StopGreedy, r.Stop)
		require.Equal(t, c.want, r.Score)
		placed := &Input{Stores: in.Stores, BaseVoters: make([]int, in.Stores), BaseLeaders: make([]int, in.Stores)}
		for i := range in.Groups {
			placed.Groups = in.Groups[i : i+1]
			one := Plan(placed, Options{Sweeps: 1})
			in.Groups[i].Voters, in.Groups[i].Leader = one.Voters[0], one.Leaders[0]
			for _, store := range one.Voters[0] {
				placed.BaseVoters[store]++
			}
			placed.BaseLeaders[one.Leaders[0]]++
		}
		again := Plan(in, Options{Seed: 1})
		require.Equal(t, StopGreedy, again.Stop)
		require.Equal(t, c.want, again.Score)
	}
}

func BenchmarkPlan(b *testing.B) {
	bench := func(name string, in *Input) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r := Plan(in, Options{Seed: 7})
				b.ReportMetric(float64(r.Work), "work/op")
			}
		})
	}
	const groups = 1000
	bench("one_per_zone/concentrated_1000x300", general(groups, 300, 3, 3))
	restricted := general(groups, 12, 3, 3)
	clear(restricted.Groups[0].CanLead[3:])
	bench("leader_on_3_stores/1000x12", restricted)
	bench("3_replicas_2_zones/1000x300", general(groups, 300, 2, 3))
	bench("5_replicas_3_zones/1000x300", general(groups, 300, 3, 5))
	bench("five_zones/repair_1000x300", fiveZones(false))
	bench("five_zones/balanced_1000x300", fiveZones(true))
}

func fiveZones(balanced bool) *Input {
	in := general(1000, 300, 5, 2, 3)
	first, rest := in.Groups[0].Rules[0], in.Groups[0].Rules[1]
	first.Stores = first.Stores[:120]
	rest.Stores = rest.Stores[120:]
	for s := range in.Stores {
		rest.Location[s][0] = int32(s / 60)
	}
	for i := range in.Groups {
		g := &in.Groups[i]
		g.Voters = []int{0, 60, 120, 180, 240}
		if balanced {
			for zone := range g.Voters {
				g.Voters[zone] += i % 60
			}
			g.Leader = g.Voters[(i/60)%5]
		}
	}
	return in
}

func TestFiveZoneConvergence(t *testing.T) {
	in := fiveZones(false)
	var r Result
	for range 6 {
		r = Plan(in, Options{Seed: 7})
		for i := range in.Groups {
			var zones [5]int
			for k, s := range r.Voters[i] {
				zones[s/60]++
				in.Groups[i].RuleOf[k] = min(1, s/120)
			}
			require.Equal(t, [5]int{1, 1, 1, 1, 1}, zones)
			in.Groups[i].Voters, in.Groups[i].Leader = r.Voters[i], r.Leaders[i]
		}
		if r.Changed == 0 {
			break
		}
	}
	require.Equal(t, Score{83400, 3400}, r.Score)
	require.Equal(t, StopGreedy, r.Stop)
	require.Zero(t, r.Changed)
}

func TestFiveZoneConvergedBudget(t *testing.T) {
	r := Plan(fiveZones(true), Options{Seed: 7})
	require.Equal(t, int64(83400), r.Score.Voter)
	require.Zero(t, r.Changed)
	require.Equal(t, StopGreedy, r.Stop)
	require.Less(t, r.Work, int64(DefaultWorkBudget))
}

func TestCanceledChoiceKeepsLeader(t *testing.T) {
	in := general(1, 2, 0, 2)
	in.BaseLeaders[0] = 2
	checks := 0
	r := Plan(in, Options{Cancel: func() bool {
		checks++
		return checks == 2
	}})
	require.Equal(t, StopCanceled, r.Stop)
	require.Zero(t, r.Changed)
	require.Equal(t, []int{0}, r.Leaders)
}

func TestRevertChain(t *testing.T) {
	const groups = 32
	in := general(groups+1, groups+3, 0, 1)
	for i := range in.Groups {
		g := &in.Groups[i]
		g.Voters[0], g.Leader = i+1, i+1
		g.Rules = []*Rule{{Count: 1, Stores: []int{i, i + 1}}}
	}
	in.Groups[groups].Rules[0].Stores = []int{groups + 1, groups + 2}
	in.BaseVoters[groups+1], in.BaseLeaders[groups+1] = 1, 1

	t.Run("greedy keeps ties", func(t *testing.T) {
		p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
		baseline := p.score()
		voters, leaders, score := p.greedy(0)
		require.True(t, score.Less(baseline))
		for i, g := range in.Groups[:groups] {
			require.Equal(t, g.Voters, voters[i])
			require.Equal(t, g.Leader, leaders[i])
		}
	})
	for _, reason := range []StopReason{StopBudget, StopCanceled} {
		for _, c := range []struct {
			trials      int
			limit, work int64
		}{{0, 0, 0}, {17, 133, 136}, {groups, 252, 252}} {
			t.Run(string(reason)+"/"+strconv.Itoa(c.trials), func(t *testing.T) {
				p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
				origV, origL := p.layout()
				// Only the last shifted group can revert per pass.
				for i := range groups {
					p.move(&p.gs[i], []int{i}, i)
				}
				p.move(&p.gs[groups], []int{groups + 2}, groups+2)
				voters, leaders := p.layout()
				best := p.score()
				if reason == StopCanceled {
					p.opt.Cancel = func() bool { return p.visits >= c.trials }
				} else {
					p.opt.WorkBudget = c.limit
				}
				// Re-entry after stopping must not process another trial.
				score := best
				for range 2 {
					score = p.revert(origV, origL, voters, leaders, score)
					require.Equal(t, best, score)
					require.Equal(t, reason, p.stop)
					require.Equal(t, c.trials, p.visits)
					require.Equal(t, c.work, p.work) // Rejected trials restore four load entries.
					require.Equal(t, best, scoreOf(in, voters, leaders))
				}
				for i := range in.Groups {
					require.True(t, matchesTarget(legalTargets(in, i), voters[i], nil, leaders[i]))
				}
				if c.trials == groups {
					require.Equal(t, origV[groups-1], voters[groups-1])
					require.Equal(t, origL[groups-1], leaders[groups-1])
				}
			})
		}
	}
}

// Revert restores the original rule assignment for subsequent leader transfers.
func TestRevertRestoresRuleAssignment(t *testing.T) {
	rules := []*Rule{
		{Role: RoleLeader, Count: 1, Stores: []int{0, 1}},
		{Role: RoleFollower, Count: 1, Stores: []int{2, 3}},
	}
	in := general(1, 4, 1, 2)
	in.Groups[0].Rules, in.Groups[0].Voters, in.Groups[0].RuleOf = rules, []int{2, 0}, []int{1, 0}
	p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
	origV, origL := p.layout()
	// Another target with the same score, as the chooser returns it.
	g := &p.gs[0]
	p.move(g, []int{1, 3}, 1)
	g.curRule = []int{0, 1}
	voters, leaders := p.layout()
	require.Equal(t, Score{2, 1}, p.revert(origV, origL, voters, leaders, p.score()))
	require.Equal(t, []int{2, 0}, g.cur)
	require.Equal(t, []int{1, 0}, g.curRule)
}

// Fixed voters can assign their leader to any voter rule, not just the first.
func TestInvalidLeaderRulesStayFixed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rules  []*Rule
		ruleOf []int
	}{
		{"unknown role", []*Rule{{Role: RoleFollower + 1, Count: 2}}, []int{0, 0}},
		{"non-leader on second leader rule", []*Rule{{Role: RoleLeader, Count: 1}, {Role: RoleLeader, Count: 1}}, []int{0, 1}},
		{"non-leader sharing leader rule", []*Rule{{Role: RoleLeader, Count: 2}}, []int{0, 0}},
		{"leader on follower rule", []*Rule{{Role: RoleFollower, Count: 1}, {Role: RoleVoter, Count: 1}}, []int{0, 1}},
		{"non-leader alone on leader rule", []*Rule{{Role: RoleVoter, Count: 1}, {Role: RoleLeader, Count: 1}}, []int{0, 1}},
		{"empty leader rule", []*Rule{{Role: RoleVoter, Count: 2}, {Role: RoleLeader, Count: 0}}, []int{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := Group{Rules: tc.rules, RuleOf: tc.ruleOf, Voters: []int{0, 1}, Leader: 0, CanLead: allTrue(2)}
			checkNoGroups(t, &Input{Stores: 2, Groups: []Group{g}})
		})
	}
}

// fullRulePick keeps the full candidate scan as a reference for fixed-rule picks.
func fullRulePick(p *planner, g *groupState, must, forbid []int, host int) choice {
	r := g.Rules[0]
	cands := p.candidates(nil, g, 0, func(s int) bool { return slices.Contains(forbid, s) })
	if slices.ContainsFunc(must, func(s int) bool { return !slices.Contains(cands, s) }) {
		return choice{}
	}
	var lead func(int) bool
	if host == 1 {
		lead = func(s int) bool { return p.leaderCapable(g, r, s) && (!g.LeaderPinned || s == g.Leader) }
	}
	target, ok := p.isolation(g, 0)
	c := p.pickAll(g, r, cands, must, 0, r.Count, lead)[host][r.Count]
	c.ok = ok && c.ok && c.v.cmpIsolation(target) == 0
	return c
}

func TestPickRuleFixed(t *testing.T) {
	membership, g := singleGroup(8, 0, 2, 1)
	g.Rules[0].Stores, g.Rules[1].Stores = []int{3, 4, 5}, []int{6}
	g.Exclude = []int{4}
	p := newPlanner(membership, Options{WorkBudget: DefaultWorkBudget})
	state := &p.gs[0]
	for ri := range g.Rules {
		candidates := p.candidates(nil, state, ri, nil)
		for store := range membership.Stores {
			accepted := p.accepts(state, ri, store) && !slices.Contains(g.Exclude, store)
			require.Equal(t, slices.Contains(candidates, store), accepted, "rule %d store %d", ri, store)
		}
	}
	// Original voter and shared member; then wrong rule, nonmember,
	// excluded, forbidden and duplicate.
	for _, store := range []int{1, 3, 2, 7, 4, 5, 0} {
		_, ok := p.pickRule(state, 0, []int{store, 0}, []int{5}, false)
		require.Equal(t, store == 1 || store == 3, ok, "mandatory store %d", store)
	}

	rng := rand.New(rand.NewPCG(17, 38))
	for i := range 400 {
		in, g := singleGroup(300, 1, 1+rng.IntN(maxCount))
		r := g.Rules[0]
		for store := range in.Stores {
			zone := []int{0, store, store % 5}[i%3] // shared prefix, unique or repeated zones
			r.Location[store] = []int32{int32(zone), int32(store % 25), int32(store)}
			in.BaseVoters[store], in.BaseLeaders[store] = rng.IntN(9), rng.IntN(5)
			g.CanLead[store] = rng.IntN(2) == 0
		}
		r.IsolationDepth = []int{0, 2, 3}[i%3]
		r.Stores = r.Stores[maxCount:] // Original voters remain eligible.
		g.Exclude, g.LeaderPinned = []int{298}, i%4 == 0
		p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
		require.Len(t, p.gs, 1)
		state := &p.gs[0]
		state.startV, state.startLd = g.Voters, g.Leader
		forbid := []int{299}
		for host := range 2 {
			full := fullRulePick(p, state, nil, forbid, host)
			require.True(t, full.ok)
			must := slices.Clone(full.sel)
			if i%2 == 0 {
				must = rng.Perm(in.Stores)[:r.Count]
			}
			rng.Shuffle(len(must), func(a, b int) { must[a], must[b] = must[b], must[a] })
			before := slices.Clone(must)
			want := fullRulePick(p, state, must, forbid, host)
			work := p.work
			got, ok := p.pickRule(state, 0, must, forbid, host == 1)
			require.Equal(t, want.ok, ok, "case %d host=%d must=%v", i, host, must)
			if ok {
				require.Equal(t, want.v, got.v, "case %d host=%d", i, host)
				require.Equal(t, want.leader, got.leader)
				require.ElementsMatch(t, want.sel, got.sel)
			}
			require.Equal(t, before, must)
			require.LessOrEqual(t, p.work-work, int64(r.Count*(r.depth()+1)), "case %d must not scan all stores", i)
		}
	}
}

func TestEmptyRuleOfSlice(t *testing.T) {
	in := general(3, 4, 1, 2)
	in.BaseVoters, in.BaseLeaders = []int{4, 4, 4, 0}, []int{3, 0, 0, 0}
	for i := range in.Groups {
		in.Groups[i].RuleOf = in.Groups[i].RuleOf[:0]
	}
	// Keep one group's voters fixed so visitLeader and chainLeaders read the
	// initial rule slice directly, and give another a tied improvement to revert.
	in.Groups[1].Forced = []int{0, 1}
	in.Groups[2].Rules = []*Rule{{Count: 2, Stores: []int{0, 1, 2}}}
	r := Plan(in, Options{Seed: 7})
	require.Equal(t, StopGreedy, r.Stop)
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score)
	require.ElementsMatch(t, []int{2, 3}, r.Voters[0])
	require.ElementsMatch(t, []int{0, 1}, r.Voters[1])
	require.Equal(t, 1, r.Leaders[1])
	for _, g := range in.Groups {
		require.Equal(t, []int{}, g.RuleOf)
	}
}
