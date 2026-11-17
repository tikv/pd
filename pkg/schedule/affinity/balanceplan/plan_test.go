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

// general starts every group on stores 0..replicas-1; store s is in zone s%zones.
func general(groups, stores, replicas, zones int) *Input {
	loc, ids := make([][]int32, stores), make([]int, stores)
	for s := range stores {
		ids[s], loc[s] = s, []int32{int32(s % zones)}
	}
	rule := &Rule{Role: RoleVoter, Count: replicas, Stores: ids, Location: loc}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	can := allTrue(stores)
	for range groups {
		in.Groups = append(in.Groups, Group{Rules: []*Rule{rule}, Voters: slices.Clone(ids[:replicas]),
			RuleOf: make([]int, replicas), Leader: 0, CanLead: can})
	}
	return in
}

// broad uses one voter per zone, concentrated or round-robin, with optional leader stores.
func broad(n, perzone int, leaderStores []int, concentrated bool) *Input {
	in := general(n, 3*perzone, 3, 3)
	for s := range in.Stores {
		in.Groups[0].Rules[0].Location[s][0] = int32(s / perzone)
	}
	can := allTrue(in.Stores)
	if leaderStores != nil {
		clear(can)
		for _, s := range leaderStores {
			can[s] = true
		}
	}
	for i := range in.Groups {
		v := []int{0, perzone, 2 * perzone}
		if !concentrated {
			v = []int{i % perzone, perzone + (i/3)%perzone, 2*perzone + (i/9)%perzone}
		}
		leader := slices.IndexFunc(v, func(s int) bool { return can[s] })
		if leader < 0 {
			v[0], leader = leaderStores[0], 0
		}
		in.Groups[i].Voters, in.Groups[i].Leader, in.Groups[i].CanLead = v, v[leader], can
	}
	return in
}

// bottleneck lets 90% of the groups use only stores 0 and 1 in zone 0; the
// layout already attains the voter optimum.
func bottleneck(n, perzone int) *Input {
	in := broad(n, perzone, nil, false)
	open := in.Groups[0].Rules[0]
	narrow := &Rule{Role: RoleVoter, Count: 3, Stores: append([]int{0, 1}, open.Stores[perzone:]...), Location: open.Location}
	restricted := n * 9 / 10
	for i := range in.Groups {
		g := &in.Groups[i]
		g.Voters = []int{2 + (i-restricted)%(perzone-2), perzone + i%perzone, 2*perzone + i%perzone}
		if i < restricted {
			g.Rules, g.Voters[0] = []*Rule{narrow}, i%2
		}
		g.Leader = g.Voters[1+i%2]
	}
	return in
}

// hosts places 3 replicas over 2 zones with host isolation: each zone has
// hostsPerZone hosts of 3 stores.
func hosts(groups, hostsPerZone int) *Input {
	stores := 2 * hostsPerZone * 3
	in := general(groups, stores, 3, 2)
	for s, loc := range in.Groups[0].Rules[0].Location {
		loc[0] = int32(s / (stores / 2))
		in.Groups[0].Rules[0].Location[s] = append(loc, int32(s/3))
	}
	for i := range in.Groups {
		in.Groups[i].Voters = []int{0, 3, stores / 2}
	}
	return in
}

// primary is the placement policy shape: a leader in zone 0 and two more
// voters, either outside zone 0 or anywhere (overlapping the leader rule).
func primary(groups, perzone int, overlap bool) *Input {
	in := broad(groups, perzone, nil, true)
	all := in.Groups[0].Rules[0]
	leader := &Rule{Role: RoleLeader, Count: 1, Stores: all.Stores[:perzone], Location: all.Location}
	rest := &Rule{Role: RoleFollower, Count: 2, Stores: all.Stores[perzone:], Location: all.Location}
	if overlap {
		rest = &Rule{Role: RoleVoter, Count: 2, Stores: all.Stores, Location: all.Location}
	}
	for i := range in.Groups {
		in.Groups[i].Rules, in.Groups[i].RuleOf = []*Rule{leader, rest}, []int{0, 1, 1}
	}
	return in
}

// Choosing the voter rule's cheapest store first takes the follower's only store.
func overlapInput() *Input {
	rules := []*Rule{
		{Role: RoleLeader, Count: 1, Stores: []int{0, 3}},
		{Role: RoleVoter, Count: 1, Stores: []int{1, 2}},
		{Role: RoleFollower, Count: 1, Stores: []int{1}},
	}
	return &Input{Stores: 5, BaseVoters: []int{4, 0, 4, 0, 4}, BaseLeaders: []int{4, 0, 0, 0, 0},
		Groups: []Group{{Rules: rules, Voters: []int{0, 2, 1}, RuleOf: []int{0, 1, 2}, Leader: 0,
			CanLead: []bool{true, false, false, true, false}}}}
}

func TestOverlappingRulesFindImprovement(t *testing.T) {
	re := require.New(t)
	r := Plan(overlapInput(), Options{})
	re.Equal(Score{67, 25}, r.Baseline)
	re.Equal(Score{59, 17}, r.Score)
	re.Equal(3, r.Leaders[0])
	re.ElementsMatch([]int{1, 2, 3}, r.Voters[0])
}

func TestOverlappingIsolationAllowsLeaderBalance(t *testing.T) {
	re := require.New(t)
	in := general(2, 4, 4, 1)
	rule := in.Groups[0].Rules[0]
	rule.Count, rule.Location[3][0] = 2, 1
	other := *rule
	for i := range in.Groups {
		in.Groups[i].Rules, in.Groups[i].RuleOf = []*Rule{rule, &other}, []int{0, 0, 1, 1}
	}
	r := Plan(in, Options{})
	re.Equal(Score{16, 4}, r.Baseline)
	re.Equal(Score{16, 2}, r.Score)
	changed := 0
	for i, g := range in.Groups {
		re.Equal(g.Voters, r.Voters[i])
		if r.Leaders[i] != g.Leader {
			changed++
		}
	}
	re.Equal(1, changed)
}

// leaderChain has balanced voters, and its leaders reach the optimum only by two
// transfers in a row: store 0 leads two groups with voters on stores 0 and 1,
// store 1 leads one group with voters on stores 1 and 2. Either transfer alone
// only ties.
func leaderChain() *Input {
	rule := &Rule{Count: 2, Stores: []int{0, 1, 2}}
	in := &Input{Stores: 3, BaseVoters: []int{1, 0, 2}, BaseLeaders: make([]int, 3)}
	for _, v := range [][]int{{0, 1}, {0, 1}, {1, 2}} {
		in.Groups = append(in.Groups, Group{Rules: []*Rule{rule}, Voters: v, RuleOf: []int{0, 0},
			Leader: v[0], CanLead: allTrue(3)})
	}
	return in
}

func TestLeaderChains(t *testing.T) {
	for _, c := range []struct {
		name    string
		modify  func(g *Group)
		want    Score
		stop    StopReason
		changed int
	}{
		{"chain", func(*Group) {}, Score{27, 3}, StopGreedy, 2},
		{"cannot lead", func(g *Group) { g.CanLead = []bool{true, true, false} }, Score{27, 5}, StopGreedy, 0},
		{"pinned", func(g *Group) { g.LeaderPinned = true }, Score{27, 5}, StopGreedy, 0},
		{"placement check", func(g *Group) {
			g.CheckTarget = func(_ []int, leader int, _ int64) TargetFeedback {
				if leader == 2 {
					return TargetFeedback{Status: TargetRejected}
				}
				return TargetFeedback{}
			}
		}, Score{27, 5}, StopGreedy, 0},
		{"follower", func(g *Group) {
			g.Rules = []*Rule{{Count: 1, Stores: []int{1}}, {Role: RoleFollower, Count: 1, Stores: []int{2}}}
			g.RuleOf = []int{0, 1}
		}, Score{27, 5}, StopGreedy, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			re := require.New(t)
			in := leaderChain()
			c.modify(&in.Groups[2])
			r := Plan(in, Options{})
			re.Equal(Score{27, 5}, r.Baseline)
			re.Equal(c.want, r.Score)
			re.Equal(scoreOf(in, r.Voters, r.Leaders), r.Score)
			re.Equal(c.stop, r.Stop)
			re.Equal(c.changed, r.Changed)
			for i, g := range in.Groups {
				re.Equal(g.Voters, r.Voters[i])
			}
		})
	}
}

// A cutoff keeps the transfer paths already moved. Store 0 leads three groups
// with voters on stores 0 and 1, and store 1 leads two groups with voters on
// stores 1 and 2 or 3. Both paths pass store 1, so they take two rounds.
func TestLeaderChainStops(t *testing.T) {
	for _, c := range []struct {
		name   string
		reason StopReason
		delta  int64
	}{{"complete", "", -6}, {"canceled", StopCanceled, -4}, {"budget", StopBudget, -4}} {
		t.Run(c.name, func(t *testing.T) {
			re := require.New(t)
			rule := &Rule{Count: 2, Stores: []int{0, 1, 2, 3}}
			in := &Input{Stores: 4, BaseVoters: []int{2, 0, 4, 4}, BaseLeaders: make([]int, 4)}
			for _, v := range [][]int{{0, 1}, {0, 1}, {0, 1}, {1, 2}, {1, 3}} {
				in.Groups = append(in.Groups, Group{Rules: []*Rule{rule}, Voters: v, RuleOf: []int{0, 0},
					Leader: v[0], CanLead: allTrue(4)})
			}
			p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
			_, leaders := p.layout()
			before := p.score()
			switch c.reason {
			case StopCanceled:
				// Checked on entry, then before every round: one round runs.
				checks := 0
				p.opt.Cancel = func() bool { checks++; return checks > 2 }
			case StopBudget:
				// Entry charges the 2 voters of each of the 5 groups; one round runs.
				p.opt.WorkBudget = p.work + 11
			}
			d := p.chainLeaders(leaders)
			re.Equal(c.reason, p.stop)
			re.Equal(c.delta, d)
			voters, working := p.layout()
			re.Equal(working, leaders)
			re.Equal(Score{before.Voter, before.Leader + d}, p.score())
			re.Equal(p.score(), scoreOf(in, voters, leaders))
		})
	}
}

// Nine location levels are accepted by PD; the planner must not panic.
func TestDeepLocationLabels(t *testing.T) {
	re := require.New(t)
	in := general(2, 6, 3, 2)
	loc := make([][]int32, 6)
	for s := range loc {
		loc[s] = make([]int32, 9)
		loc[s][8] = int32(s % 2)
	}
	in.Groups[0].Rules[0].Location = loc
	r := Plan(in, Options{})
	re.False(r.Baseline.Less(r.Score))
	re.True(r.Score.Less(r.Baseline))
	re.Equal(2.0, isolationScore(loc, r.Voters[0], 9))
}

func TestUnsupportedGroupsStayFixed(t *testing.T) {
	re := require.New(t)
	in := general(3, 40, 15, 3)
	in.Groups[1].Voters, in.Groups[1].RuleOf = in.Groups[1].Voters[:3], in.Groups[1].RuleOf[:3]
	deep := general(1, 40, 3, 3).Groups[0]
	for s := range deep.Rules[0].Location {
		deep.Rules[0].Location[s] = make([]int32, maxDepth+1)
	}
	in.Groups = append(in.Groups, deep)
	r := Plan(in, Options{})
	re.Equal(StopNoGroups, r.Stop)
	for i := range in.Groups {
		re.Equal(in.Groups[i].Voters, r.Voters[i])
		re.Equal(in.Groups[i].Leader, r.Leaders[i])
	}
	in.Groups = []Group{deep}
	re.Equal(StopNoGroups, Plan(in, Options{}).Stop)
}

func TestHardIsolationScaleOut(t *testing.T) {
	re := require.New(t)
	in := general(4, 6, 4, 1)
	rule := in.Groups[0].Rules[0]
	rule.Location = [][]int32{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 4}, {1, 5}}
	rule.IsolationDepth = 2
	for i := range in.Groups {
		in.Groups[i].CheckTarget = func(voters []int, _ int, _ int64) TargetFeedback {
			re.True(meetsIsolation(rule, voters), "illegal candidate: %v", voters)
			return TargetFeedback{}
		}
	}
	p := newPlanner(in, Options{WorkBudget: DefaultWorkBudget})
	g := &p.gs[0]
	iso, ok := p.isolation(g, 0)
	re.True(ok)
	re.Equal([maxDepth]int32{3}, iso.pairs)
	_, ok = p.pickRule(g, 0, []int{3, 4}, nil, true)
	re.False(ok, "two mandatory peers cannot share dc2/z4")
	for _, voters := range [][]int{{0, 1, 3, 4}, {0, 1, 4, 5}} {
		sol := p.assignTarget(g, voters, &ruleSearch{p: p})
		re.Equal(meetsIsolation(rule, voters), sol.ok, "repair: %v", voters)
	}
	r := Plan(in, Options{Seed: 7})
	re.Equal(int64(64), r.Baseline.Voter)
	re.Equal(int64(44), r.Score.Voter)
	loads := make([]int, in.Stores)
	for i, voters := range r.Voters {
		re.True(matchesTarget(legalTargets(in, i), voters, nil, r.Leaders[i]))
		for _, s := range voters {
			loads[s]++
		}
	}
	re.Equal([]int{3, 3, 3, 2, 2, 3}, loads)
	re.True(slices.ContainsFunc(r.Voters, func(v []int) bool {
		return slices.Contains(v, 0) && slices.Contains(v, 1) && slices.Contains(v, 5)
	}), "a target must use z1, z2, z5 and z4")
}

func TestHardIsolationPaths(t *testing.T) {
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
	} {
		t.Run(c.name, func(t *testing.T) {
			in := general(1, len(c.location), len(c.voters), 1)
			g := &in.Groups[0]
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
	// Shared levels may be skipped all the way to the maximum supported depth.
	in := general(1, 4, 2, 1)
	for s := range in.Stores {
		path := make([]int32, maxDepth)
		path[maxDepth-1] = int32(s / 2)
		in.Groups[0].Rules[0].Location[s] = path
	}
	in.Groups[0].Rules[0].IsolationDepth = maxDepth
	in.Groups[0].Voters = []int{0, 2}
	in.BaseVoters = []int{4, 0, 4, 0}
	require.ElementsMatch(t, []int{1, 3}, Plan(in, Options{}).Voters[0])
}

func TestUnsupportedIsolationStaysFixed(t *testing.T) {
	for _, name := range []string{"negative", "too deep", "no labels", "infeasible count", "current collision", "pinned collision"} {
		t.Run(name, func(t *testing.T) {
			in := general(2, 6, 3, 3)
			rule := in.Groups[0].Rules[0]
			rule.IsolationDepth = 1
			switch name {
			case "negative":
				rule.IsolationDepth = -1
			case "too deep":
				rule.IsolationDepth = 2
			case "no labels":
				rule.Location = nil
			case "infeasible count":
				for s := range in.Stores {
					rule.Location[s][0] = int32(s % 2)
				}
			default:
				rule.Location[1][0] = 0
				if name == "pinned collision" {
					for i := range in.Groups {
						in.Groups[i].Forced, in.Groups[i].LeaderPinned = []int{1}, true
					}
				}
			}
			r := Plan(in, Options{})
			require.Equal(t, StopNoGroups, r.Stop)
			require.Equal(t, r.Baseline, r.Score)
			for i, g := range in.Groups {
				require.Equal(t, g.Voters, r.Voters[i])
				require.Equal(t, g.Leader, r.Leaders[i])
			}
		})
	}
}

// General isolation must count candidate scans and respect the work budget.
func TestGeneralShapesAreBounded(t *testing.T) {
	re := require.New(t)
	for _, c := range []struct {
		replicas, zones int
	}{{3, 2}, {5, 3}} {
		r := Plan(general(100, 300, c.replicas, c.zones), Options{})
		re.Equal(StopGreedy, r.Stop, "%+v", c)
		re.LessOrEqual(r.Work, int64(DefaultWorkBudget), "%+v", c)
		re.False(r.Baseline.Less(r.Score), "%+v", c)
	}
	opt := Options{WorkBudget: 50_000}
	r := Plan(general(100, 300, 3, 2), opt)
	re.Equal(StopBudget, r.Stop)
	re.Less(r.Work, opt.WorkBudget+2_000) // stops at the next visit
	re.False(r.Baseline.Less(r.Score))
}

// Other tables' counts break ties without changing this table's score.
func TestOtherTablesSpreadTies(t *testing.T) {
	re := require.New(t)
	alone := Plan(general(4, 30, 3, 3), Options{})
	in := general(4, 30, 3, 3)
	in.OtherVoters, in.OtherLeaders = make([]int, 30), make([]int, 30)
	for s := 3; s < 15; s++ {
		in.OtherVoters[s], in.OtherLeaders[s] = 5, 2 // used by other tables
	}
	r := Plan(in, Options{})
	moved := 0
	for _, v := range r.Voters {
		for _, s := range v {
			if s >= 3 {
				moved++
				re.Zero(in.OtherVoters[s], "store %d is busy in other tables", s)
			}
		}
	}
	re.Positive(moved)
	re.Equal(alone.Score, r.Score) // only ties change
}

func TestGreedyKeepsTiedTargets(t *testing.T) {
	re := require.New(t)
	in := revertChain(32)
	p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
	baseline := p.score()
	voters, leaders, score := p.greedy(0)
	re.True(score.Less(baseline))
	for i, g := range in.Groups[:32] {
		re.Equal(g.Voters, voters[i])
		re.Equal(g.Leader, leaders[i])
	}
}

func TestScaleOutKeepsExistingTargets(t *testing.T) {
	const groups, stores = 900, 300
	for _, replicas := range []int{3, 5} {
		re := require.New(t)
		in := general(groups, stores+1, replicas, replicas/2+1)
		for i := range in.Groups {
			g := &in.Groups[i]
			for k := range g.Voters {
				g.Voters[k] = (i + k) % stores
			}
			g.Leader = i % stores
		}
		r := Plan(in, Options{Seed: 4242})
		re.True(r.Score.Less(r.Baseline))
		moves := 0
		for i, g := range in.Groups {
			for _, s := range r.Voters[i] {
				if !slices.Contains(g.Voters, s) {
					moves++
				}
			}
		}
		re.Positive(moves)
		re.LessOrEqual(moves, 3*groups*replicas/stores)
	}
}

func TestPlanKeepsInput(t *testing.T) {
	re := require.New(t)
	in := overlapInput()
	in.Groups[0].Rules[1].Stores = []int{2, 1, 2} // unsorted with a duplicate
	voters, stores := slices.Clone(in.Groups[0].Voters), slices.Clone(in.Groups[0].Rules[1].Stores)
	Plan(in, Options{})
	re.Equal(voters, in.Groups[0].Voters)
	re.Equal(stores, in.Groups[0].Rules[1].Stores)
	re.Zero(in.Groups[0].Rules[1].level)
	re.Nil(in.Groups[0].Rules[1].domainOf)
}

func TestExcludedStoreKeepsOtherTargets(t *testing.T) {
	re := require.New(t)
	in := general(1, 5, 3, 1)
	in.Groups[0].Rules[0].Location, in.Groups[0].RuleOf = nil, nil
	in.BaseVoters, in.BaseLeaders = []int{1, 1, 1, 0, 0}, []int{1, 0, 0, 0, 0}
	re.Contains(Plan(in, Options{}).Voters[0], 3)
	in.Groups[0].Exclude = []int{3}
	r := Plan(in, Options{})
	re.True(r.Score.Less(r.Baseline))
	re.NotContains(r.Voters[0], 3)
}

// Forced assignments over overlapping rules must share search bounds per visit.
func TestForcedAssignmentsAreBounded(t *testing.T) {
	re := require.New(t)
	const stores = 300
	for _, c := range []struct{ rules, count, forced int }{{3, 4, 11}, {30, 1, 29}} {
		in := general(1, stores, c.rules*c.count, 1)
		g := &in.Groups[0]
		all := g.Rules[0].Stores
		g.Rules = nil
		for range c.rules {
			g.Rules = append(g.Rules, &Rule{Count: c.count, Stores: all})
		}
		g.Forced = slices.Clone(g.Voters[:c.forced])
		for k, s := range g.Voters {
			g.RuleOf[k] = s / c.count
			in.BaseVoters[s] = 1
		}
		in.BaseLeaders[0] = 1
		r := Plan(in, Options{})
		re.Less(r.Work/int64(r.Visits), int64(DefaultWorkBudget/10), "one visit must stay a small part of the budget")
		re.LessOrEqual(r.Work, int64(DefaultWorkBudget+DefaultWorkBudget/10))
		re.True(r.Score.Less(r.Baseline), "%v -> %v", r.Baseline, r.Score)
		for _, s := range g.Forced {
			re.Contains(r.Voters[0], s)
		}
	}
}

func TestUnforcedSearchRespectsStop(t *testing.T) {
	const stores, rules = 300, 30
	for _, stop := range []StopReason{StopCanceled, StopBudget} {
		t.Run(string(stop), func(t *testing.T) {
			re := require.New(t)
			in := general(1, stores, rules, 1)
			g := &in.Groups[0]
			all := g.Rules[0].Stores
			g.Rules = nil
			for ri := range rules {
				g.Rules = append(g.Rules, &Rule{Count: 1, Stores: all})
				g.RuleOf[ri], in.BaseVoters[ri] = ri, 1
			}
			in.BaseLeaders[0] = 1
			checks := 0
			opt := Options{Cancel: func() bool {
				checks++
				return stop == StopCanceled && checks >= 2
			}}
			if stop == StopBudget {
				opt.WorkBudget = 1000
			}
			r := Plan(in, opt)
			t.Logf("visits=%d work=%d checks=%d stop=%s", r.Visits, r.Work, checks, r.Stop)
			re.Equal(stop, r.Stop)
			re.Equal(1, r.Visits)
			// Stopping inside the greedy keeps a complete, consistent layout.
			re.Equal(scoreOf(in, r.Voters, r.Leaders), r.Score)
			re.False(r.Baseline.Less(r.Score))
			// Allow one complete root, its isolation calculation, and the
			// leader-only fallback when the interrupted search finds no target.
			re.LessOrEqual(r.Work, int64(2*rules*stores+rules))
		})
	}
}

// initialize places groups in input order using only the preceding groups' loads.
func initialize(in *Input) {
	placed := &Input{Stores: in.Stores, BaseVoters: slices.Clone(in.BaseVoters), BaseLeaders: slices.Clone(in.BaseLeaders)}
	for i := range in.Groups {
		one := *placed
		one.Groups = []Group{in.Groups[i]}
		r := Plan(&one, Options{Sweeps: 1})
		in.Groups[i].Voters, in.Groups[i].Leader = r.Voters[0], r.Leaders[0]
		for _, s := range r.Voters[0] {
			placed.BaseVoters[s]++
		}
		placed.BaseLeaders[r.Leaders[0]]++
	}
}

// Whole-table balancing and initialization one group at a time attain the optimum.
func TestBroadLifecycle(t *testing.T) {
	for _, c := range []struct {
		n       int
		leaders []int
		want    Score
	}{
		{36, nil, Score{972, 108}},
		{36, []int{0, 4, 8}, Score{1008, 432}},
		{128, []int{0, 4, 8}, Score{12746, 5462}},
	} {
		re := require.New(t)
		in := broad(c.n, 4, c.leaders, true)
		r := Plan(in, Options{})
		re.Equal(StopGreedy, r.Stop)
		re.Equal(c.want, r.Score)
		initialize(in)
		again := Plan(in, Options{Seed: 1})
		re.Equal(StopGreedy, again.Stop)
		re.Equal(c.want, again.Score)
	}
}

func BenchmarkPlan(b *testing.B) {
	for _, bc := range []struct {
		name string
		in   func() *Input
	}{
		{"one_per_zone/concentrated_1000x300", func() *Input { return broad(1000, 100, nil, true) }},
		{"one_per_zone/balanced_1000x300", func() *Input { return broad(1000, 100, nil, false) }},
		{"leader_on_3_stores/1000x12", func() *Input { return broad(1000, 4, []int{0, 4, 8}, true) }},
		{"3_replicas_2_zones/1000x300", func() *Input { return general(1000, 300, 3, 2) }},
		{"5_replicas_3_zones/1000x300", func() *Input { return general(1000, 300, 5, 3) }},
		{"restricted_90pct/1000x300", func() *Input { return bottleneck(1000, 100) }},
		{"zone_host/1000x300", func() *Input { return hosts(1000, 50) }},
		{"leader_rule_followers/1000x300", func() *Input { return primary(1000, 100, false) }},
		{"leader_rule_overlapping_voters/1000x300", func() *Input { return primary(1000, 100, true) }},
		{"five_zones/repair_1000x300", func() *Input { return fiveZones(false, true) }},
		{"five_zones/balanced_1000x300", func() *Input { return fiveZones(true, false) }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			in := bc.in()
			b.ReportAllocs()
			for b.Loop() {
				r := Plan(in, Options{Seed: 7})
				b.ReportMetric(float64(r.Work), "work/op")
			}
		})
	}
}

func TestDirectedPlacementFeedback(t *testing.T) {
	in := general(12, 9, 3, 1)
	for i := range in.Groups {
		in.Groups[i].CheckTarget = func(voters []int, _ int, _ int64) TargetFeedback {
			if slices.Contains(voters, 3) {
				return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: 3, Candidates: []int{4, 5, 6, 7, 8}}}, Work: 9}
			}
			return TargetFeedback{Work: 9}
		}
	}
	r := Plan(in, Options{Seed: 7})
	require.Less(t, r.Score.Voter, r.Baseline.Voter)
	require.Equal(t, StopGreedy, r.Stop)
	for _, voters := range r.Voters {
		require.NotContains(t, voters, 3)
	}
	require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score)
	t.Logf("single-rule feedback: %v -> %v changed=%d stop=%s work=%d", r.Baseline, r.Score, r.Changed, r.Stop, r.Work)
	for i := range in.Groups {
		in.Groups[i].CheckTarget = func(voters []int, _ int, _ int64) TargetFeedback {
			if slices.Contains(voters, 3) {
				return TargetFeedback{Status: TargetTemporarilyBlocked, Work: 9}
			}
			return TargetFeedback{Work: 9}
		}
	}
	blocked := Plan(in, Options{Seed: 7})
	require.True(t, blocked.TemporarilyBlocked)
	require.Equal(t, blocked.Baseline.Voter, blocked.Score.Voter)
	for i := range in.Groups {
		in.Groups[i].CheckTarget = nil
	}
	recovered := Plan(in, Options{Seed: 7})
	require.Less(t, recovered.Score.Voter, blocked.Score.Voter)
	require.False(t, recovered.TemporarilyBlocked)
	// No feedback was persisted in the input, including Exclude.
	for _, g := range in.Groups {
		require.Empty(t, g.Exclude)
	}
}

// Exhausted, cyclic and non-improving branches leave legal siblings reachable.
func TestDirectedRepairAlternatives(t *testing.T) {
	for _, c := range []struct {
		name       string
		candidates []int
		next, want int
	}{
		{"no alternatives", nil, 2, 1},
		{"cycle", []int{3}, 2, 1},
		{"cyclic sibling", []int{3, 4}, 2, 4},
		{"unchanged sibling", []int{3, 4}, 1, 4},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := general(1, 5, 2, 1)
			in.Groups[0].LeaderPinned = true
			in.BaseVoters[1] = 5
			calls := 0
			in.Groups[0].CheckTarget = func(v []int, _ int, _ int64) TargetFeedback {
				calls++
				switch {
				case slices.Contains(v, 2):
					return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: 2, Candidates: c.candidates}}}
				case slices.Contains(v, 3):
					return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: 3, Candidates: []int{c.next}}}}
				default:
					return TargetFeedback{}
				}
			}
			r := Plan(in, Options{Seed: 7})
			require.ElementsMatch(t, []int{0, c.want}, r.Voters[0])
			require.Equal(t, 0, r.Leaders[0])
			require.Equal(t, c.want == 4, r.Score.Less(r.Baseline))
			require.False(t, r.PlacementLimited)
			require.False(t, r.TemporarilyBlocked)
			require.Less(t, calls, 10)
			require.Empty(t, in.Groups[0].Exclude)
		})
	}
}

// A queued sibling needs a second replacement; a 65-step failed chain must not starve it.
func TestDirectedRepairDeepChain(t *testing.T) {
	in := general(1, 72, 2, 1)
	in.Groups[0].LeaderPinned = true
	in.BaseVoters[1] = 5
	calls := 0
	in.Groups[0].CheckTarget = func(v []int, _ int, _ int64) TargetFeedback {
		calls++
		for _, s := range v {
			if s == 2 {
				return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: s, Candidates: []int{3, 4}}}}
			}
			if s == 3 {
				return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: s, Candidates: []int{71}}}}
			}
			if s >= 4 && s < 71 {
				return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: s, Candidates: []int{s + 1}}}}
			}
		}
		return TargetFeedback{}
	}
	r := Plan(in, Options{Seed: 7})
	t.Logf("direct sibling versus long branch: score=%v voters=%v calls=%d limited=%v", r.Score, r.Voters, calls, r.PlacementLimited)
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 71)
	require.Less(t, calls, 10)
	require.False(t, r.PlacementLimited)
}

func TestDirectedRepairStops(t *testing.T) {
	for _, reason := range []string{"nodes", "assignment", "callback", "canceled", "no alternatives"} {
		t.Run(reason, func(t *testing.T) {
			in := general(1, 90, 2, 1)
			in.Groups[0].LeaderPinned = true
			in.BaseVoters[1] = 5
			calls, canceled := 0, false
			in.Groups[0].CheckTarget = func(v []int, _ int, limit int64) TargetFeedback {
				calls++
				if slices.Contains(v, 1) {
					return TargetFeedback{}
				}
				f := TargetFeedback{Status: TargetRejected, Work: 1}
				for _, s := range v {
					if s != 0 && reason != "no alternatives" {
						f.Replace = []StoreReplacement{{Old: s, Candidates: []int{s + 1}}}
					}
				}
				switch reason {
				case "assignment":
					f.Work = limit - 2 // The remaining work ends inside assignTarget.
				case "callback", "no alternatives":
					f.Work = limit
				case "canceled":
					canceled = true
				}
				return f
			}
			budget := int64(1000)
			if reason == "nodes" {
				budget = DefaultWorkBudget
			}
			r := Plan(in, Options{WorkBudget: budget, Cancel: func() bool { return canceled }})
			wantStop := StopBudget
			switch reason {
			case "nodes":
				wantStop = StopGreedy
			case "canceled":
				wantStop = StopCanceled
			}
			require.Equal(t, wantStop, r.Stop)
			require.Equal(t, reason != "no alternatives", r.PlacementLimited)
			require.Equal(t, r.Baseline, r.Score)
			require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score)
			require.False(t, r.TemporarilyBlocked)
			require.LessOrEqual(t, calls, maxSearchNodes)
			require.Empty(t, in.Groups[0].Exclude)
		})
	}
}

// Two overlapping voter rules need two replacements when their relaxed picks
// cover only three zones. The callback offers every store in the missing zones.
func fiveZones(balanced, feedback bool) *Input {
	in := general(1000, 300, 5, 5)
	all := in.Groups[0].Rules[0]
	for s := range in.Stores {
		all.Location[s][0] = int32(s / 60)
	}
	rules := []*Rule{{Count: 2, Stores: all.Stores[:120], Location: all.Location}, {Count: 3, Stores: all.Stores, Location: all.Location}}
	for i := range in.Groups {
		g := &in.Groups[i]
		g.Rules, g.RuleOf, g.Voters = rules, []int{0, 0, 1, 1, 1}, []int{0, 60, 120, 180, 240}
		if balanced {
			for z := range g.Voters {
				g.Voters[z] += i % 60
			}
			g.Leader = g.Voters[(i/60)%5]
		}
		if feedback {
			g.CheckTarget = func(v []int, _ int, _ int64) TargetFeedback {
				var counts [5]int
				for _, s := range v {
					counts[s/60]++
				}
				var candidates []int
				for z, n := range counts {
					if n == 0 {
						candidates = append(candidates, all.Stores[z*60:(z+1)*60]...)
					}
				}
				f := TargetFeedback{}
				var reserved [2]bool
				for _, s := range v {
					z := s / 60
					if z < 2 && !reserved[z] {
						reserved[z] = true
					} else if counts[z] > 1 {
						f.Status, f.Work = TargetRejected, 300
						f.Replace = append(f.Replace, StoreReplacement{Old: s, Candidates: candidates})
					}
				}
				return f
			}
		}
	}
	return in
}

func TestFiveZoneConvergence(t *testing.T) {
	in := fiveZones(false, true)
	var r Result
	for round := range 6 {
		r = Plan(in, Options{Seed: 7})
		t.Logf("round=%d score=%v work=%d stop=%s changed=%d limited=%v", round, r.Score, r.Work, r.Stop, r.Changed, r.PlacementLimited)
		for i := range in.Groups {
			var zones [5]int
			for k, s := range r.Voters[i] {
				zones[s/60]++
				in.Groups[i].RuleOf[k] = 1
				if s < 120 {
					in.Groups[i].RuleOf[k] = 0
				}
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
	r := Plan(fiveZones(true, false), Options{Seed: 7})
	t.Logf("score=%v work=%d stop=%s changed=%d", r.Score, r.Work, r.Stop, r.Changed)
	require.Equal(t, int64(83400), r.Score.Voter)
	require.Zero(t, r.Changed)
	require.Equal(t, StopGreedy, r.Stop)
	require.Less(t, r.Work, int64(DefaultWorkBudget))
}

func TestCompleteLeavesBeforeRepairs(t *testing.T) {
	in := general(1, 70, 2, 1)
	g := &in.Groups[0]
	all := g.Rules[0].Stores
	g.Rules = []*Rule{{Role: RoleLeader, Count: 1, Stores: all}, {Role: RoleFollower, Count: 1, Stores: all}}
	g.Voters, g.RuleOf, g.Leader = []int{69, 68}, []int{0, 1}, 69
	in.BaseVoters[68], in.BaseVoters[69] = 100, 100
	calls := 0
	g.CheckTarget = func(v []int, leader int, _ int64) TargetFeedback {
		calls++
		if sameSet(v, g.Voters) || leader == 1 && sameSet(v, []int{0, 1}) {
			return TargetFeedback{}
		}
		for _, s := range v {
			if s != leader {
				return TargetFeedback{Status: TargetRejected, Replace: []StoreReplacement{{Old: s, Candidates: all[2:68]}}}
			}
		}
		return TargetFeedback{Status: TargetRejected}
	}
	r := Plan(in, Options{Sweeps: 1})
	require.ElementsMatch(t, []int{0, 1}, r.Voters[0])
	require.Equal(t, 1, r.Leaders[0])
	require.Less(t, calls, 5)
}

func TestTwoReplacements(t *testing.T) {
	in := general(1, 6, 3, 1)
	g := &in.Groups[0]
	g.Rules = []*Rule{{Count: 1, Stores: []int{0}}, {Count: 1, Stores: []int{1, 2, 3}}, {Count: 1, Stores: []int{4, 5}}}
	g.Voters, g.RuleOf, g.LeaderPinned = []int{0, 2, 5}, []int{0, 1, 2}, true
	in.BaseVoters, in.BaseLeaders = []int{5, 0, 5, 0, 0, 5}, []int{5, 0, 0, 0, 0, 0}
	for _, depth := range []int{1, 2} {
		g.CheckTarget = func(v []int, _ int, _ int64) TargetFeedback {
			if sameSet(v, g.Voters) || sameSet(v, []int{0, 3, 5}) {
				return TargetFeedback{}
			}
			f := TargetFeedback{Status: TargetRejected}
			if depth == 2 || sameSet(v, []int{0, 1, 4}) {
				f.Replace = []StoreReplacement{{Old: 1, Candidates: []int{2, 3}}, {Old: 4, Candidates: []int{5}}}
			}
			return f
		}
		r := Plan(in, Options{})
		want := Score{108, 36}
		if depth == 2 {
			want.Voter = 98
		}
		require.Equal(t, want, r.Score, "repair depth %d", depth)
	}
}

func TestLeaderChecks(t *testing.T) {
	for name, work := range map[string]int64{"return edge": 0, "accepted over budget": 1000} {
		t.Run(name, func(t *testing.T) {
			in := &Input{Stores: 5, BaseLeaders: []int{1, 1, 2, 1, 0}}
			for _, v := range [][]int{{0, 1}, {0, 4}, {2, 3}, {3, 1}, {0, 4}} {
				g := Group{Rules: []*Rule{{Count: 2, Stores: v}}, Voters: v, Leader: v[0], CanLead: allTrue(5)}
				g.CheckTarget = func(_ []int, leader int, _ int64) TargetFeedback {
					require.NotEqual(t, v[0], leader, "the current leader needs no check")
					return TargetFeedback{Work: work}
				}
				in.Groups = append(in.Groups, g)
			}
			p := newPlanner(in, Options{WorkBudget: 100})
			_, leaders := p.layout()
			delta := p.chainLeaders(leaders)
			require.False(t, p.placementLimited)
			if work > 0 {
				require.Equal(t, StopBudget, p.stop)
			} else {
				require.Equal(t, int64(-10), delta, "later rounds must allow returning to an original leader")
			}
		})
	}
}

// revertChain has a neutral chain and an independent improvement.
func revertChain(groups int) *Input {
	n := groups + 3
	in := &Input{Stores: n, BaseVoters: make([]int, n), BaseLeaders: make([]int, n)}
	can := allTrue(n)
	for i := range groups {
		in.Groups = append(in.Groups, Group{Rules: []*Rule{{Count: 1, Stores: []int{i, i + 1}}},
			Voters: []int{i + 1}, Leader: i + 1, CanLead: can})
	}
	in.BaseVoters[groups+1], in.BaseLeaders[groups+1] = 1, 1
	in.Groups = append(in.Groups, Group{Rules: []*Rule{{Count: 1, Stores: []int{groups + 1, groups + 2}}},
		Voters: []int{groups + 1}, Leader: groups + 1, CanLead: can})
	return in
}

func TestRevertChainStops(t *testing.T) {
	const groups = 32
	for _, reason := range []StopReason{StopBudget, StopCanceled} {
		for _, c := range []struct {
			trials      int
			limit, work int64
		}{{0, 0, 0}, {17, 133, 136}, {groups, 252, 252}} {
			t.Run(string(reason)+"/"+strconv.Itoa(c.trials), func(t *testing.T) {
				re := require.New(t)
				in := revertChain(groups)
				p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
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
				for range 2 {
					re.Equal(best, p.revert(origV, origL, voters, leaders))
					re.Equal(reason, p.stop)
					re.Equal(c.trials, p.visits)
					re.Equal(c.work, p.work) // Rejected trials restore four load entries.
					re.Equal(best, scoreOf(in, voters, leaders))
				}
				for i := range in.Groups {
					re.True(matchesTarget(legalTargets(in, i), voters[i], nil, leaders[i]))
				}
				if c.trials == groups {
					re.Equal(origV[groups-1], voters[groups-1])
					re.Equal(origL[groups-1], leaders[groups-1])
				}
			})
		}
	}
}

// A reversion restores the rule assignment of the input voters, which the
// leader transfers read. The chooser lists voters in rule order; the input
// may not.
func TestRevertRestoresRuleAssignment(t *testing.T) {
	re := require.New(t)
	rules := []*Rule{
		{Role: RoleLeader, Count: 1, Stores: []int{0, 1}},
		{Role: RoleFollower, Count: 1, Stores: []int{2, 3}},
	}
	in := &Input{Stores: 4, BaseVoters: make([]int, 4), BaseLeaders: make([]int, 4), Groups: []Group{{
		Rules: rules, Voters: []int{2, 0}, RuleOf: []int{1, 0}, Leader: 0, CanLead: allTrue(4)}}}
	p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
	origV, origL := p.layout()
	// Another target with the same score, as the chooser returns it.
	g := &p.gs[0]
	p.move(g, []int{1, 3}, 1)
	g.curRule = []int{0, 1}
	voters, leaders := p.layout()
	re.Equal(Score{2, 1}, p.revert(origV, origL, voters, leaders))
	re.Equal([]int{2, 0}, g.cur)
	re.Equal([]int{1, 0}, g.curRule)
}
