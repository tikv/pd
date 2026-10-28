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
	for seed := range uint64(20) {
		r := Plan(overlapInput(), Options{Seed: seed})
		re.Equal(Score{67, 25}, r.Baseline)
		re.Equal(Score{59, 17}, r.Score, "seed %d", seed)
		re.Equal(3, r.Leaders[0])
		re.ElementsMatch([]int{1, 2, 3}, r.Voters[0])
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
	re.Equal([maxDepth]int32{3, 3, 3, 3, 3, 3, 3, 3, 1}, pairsOf(in.Groups[0].Rules[0], r.Voters[0]))
}

func TestUnsupportedGroupsStayFixed(t *testing.T) {
	re := require.New(t)
	in := general(3, 40, 15, 3)
	in.Groups[1].Voters, in.Groups[1].RuleOf = in.Groups[1].Voters[:3], in.Groups[1].RuleOf[:3]
	r := Plan(in, Options{})
	re.Equal(StopNoGroups, r.Stop)
	for i := range in.Groups {
		re.Equal(in.Groups[i].Voters, r.Voters[i])
		re.Equal(in.Groups[i].Leader, r.Leaders[i])
	}
}

// General isolation must count candidate scans and respect the work budget.
func TestGeneralShapesAreBounded(t *testing.T) {
	re := require.New(t)
	for _, c := range []struct{ replicas, zones int }{{3, 2}, {5, 3}} {
		r := Plan(general(100, 300, c.replicas, c.zones), Options{})
		re.Equal(StopBound, r.Stop, "%+v", c)
		re.LessOrEqual(r.Work, int64(DefaultWorkBudget), "%+v", c)
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

// initialize places groups in input order using only the preceding groups' loads.
func initialize(in *Input) {
	placed := &Input{Stores: in.Stores, BaseVoters: slices.Clone(in.BaseVoters), BaseLeaders: slices.Clone(in.BaseLeaders)}
	for i := range in.Groups {
		one := *placed
		one.Groups = []Group{in.Groups[i]}
		r := Plan(&one, Options{Sweeps: 1, MaxAttempts: -1})
		in.Groups[i].Voters, in.Groups[i].Leader = r.Voters[0], r.Leaders[0]
		for _, s := range r.Voters[0] {
			placed.BaseVoters[s]++
		}
		placed.BaseLeaders[r.Leaders[0]]++
	}
}

// Whole-table balancing and initialization one group at a time must attain the bound.
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
		re.Equal(StopBound, r.Stop)
		re.Equal(c.want, r.Score)
		initialize(in)
		again := Plan(in, Options{Seed: 1})
		re.Equal(StopBound, again.Stop)
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
	} {
		b.Run(bc.name, func(b *testing.B) {
			in := bc.in()
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				Plan(in, Options{Seed: uint64(i)})
			}
		})
	}
}
