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
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression cases from the independent reviews of the v2 prototype.

// overlapInput: a leader rule {0,3}, a voter rule {1,2} and a follower rule
// that may only use store 1. Choosing the voter rule's cheapest store first
// takes the follower's only store.
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
}

// Rules beyond the exact isolation comparison stay as they are.
func TestUnsupportedGroupsStayFixed(t *testing.T) {
	re := require.New(t)
	in := general(3, 40, 15, 3)
	in.Groups[1].Voters = in.Groups[1].Voters[:3] // voter count does not match the rule
	in.Groups[1].RuleOf = in.Groups[1].RuleOf[:3]
	r := Plan(in, Options{})
	re.Equal(StopNoGroups, r.Stop)
	for i := range in.Groups {
		re.Equal(in.Groups[i].Voters, r.Voters[i])
		re.Equal(in.Groups[i].Leader, r.Leaders[i])
	}
}

// Fewer zones than voters used to cost seconds per plan because every leader
// option rebuilt the candidates; the work is now counted where it happens.
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

// Other tables' counts break ties, so small tables do not pile up on the
// low-numbered stores.
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

// Plan works on private copies and leaves its input unchanged.
func TestPlanKeepsInput(t *testing.T) {
	re := require.New(t)
	in := overlapInput()
	in.Groups[0].Rules[1].Stores = []int{2, 1, 2} // unsorted with a duplicate
	before := *in
	before.Groups = slices.Clone(in.Groups)
	stores := slices.Clone(in.Groups[0].Rules[1].Stores)
	Plan(in, Options{})
	re.True(reflect.DeepEqual(before.Groups[0].Voters, in.Groups[0].Voters))
	re.Equal(stores, in.Groups[0].Rules[1].Stores)
	re.Zero(in.Groups[0].Rules[1].level)
	re.Nil(in.Groups[0].Rules[1].domainOf)
}

// A rejected store, for example one holding a learner of the group, is
// excluded for that group only; the group still finds another improvement.
func TestExcludedStoreKeepsOtherTargets(t *testing.T) {
	re := require.New(t)
	newInput := func() *Input {
		return &Input{
			Stores: 5, BaseVoters: []int{1, 1, 1, 0, 0}, BaseLeaders: []int{1, 0, 0, 0, 0},
			Groups: []Group{{
				Rules:  []*Rule{{Role: RoleVoter, Count: 3, Stores: []int{0, 1, 2, 3, 4}}},
				Voters: []int{0, 1, 2}, Leader: 0, CanLead: []bool{true, true, true, true, true},
			}},
		}
	}
	free := Plan(newInput(), Options{})
	re.Contains(free.Voters[0], 3)
	in := newInput()
	in.Groups[0].Exclude = []int{3}
	r := Plan(in, Options{})
	re.True(r.Score.Less(r.Baseline))
	re.NotContains(r.Voters[0], 3)
}

// From the v3 reviews: forced voters over overlapping rules on 300 stores. The
// first shape, 11 forced voters over three rules of four, scanned 91 million
// candidates (6.6 s) in one visit without search bounds. The second, 29
// forced voters over 30 rules of one, still scanned 15.7 million (1.0 s) with
// bounds per leader rule. With bounds shared by the whole choice every visit
// stays a small part of the budget, keeps the forced voters and the plan
// still improves the table.
func TestForcedAssignmentsAreBounded(t *testing.T) {
	re := require.New(t)
	const stores = 300
	all, canLead := make([]int, stores), make([]bool, stores)
	for s := range stores {
		all[s], canLead[s] = s, true
	}
	for _, c := range []struct{ rules, count, forced int }{{3, 4, 11}, {30, 1, 29}} {
		in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
		g := Group{Leader: 0, CanLead: canLead}
		for range c.rules {
			g.Rules = append(g.Rules, &Rule{Count: c.count, Stores: all})
		}
		for s := range c.rules * c.count {
			g.Voters, g.RuleOf = append(g.Voters, s), append(g.RuleOf, s/c.count)
			in.BaseVoters[s] = 1
			if s < c.forced {
				g.Forced = append(g.Forced, s)
			}
		}
		in.BaseLeaders[0] = 1
		in.Groups = []Group{g}
		r := Plan(in, Options{})
		t.Logf("%d rules of %d, %d forced: visits=%d work=%d stop=%s %v -> %v",
			c.rules, c.count, c.forced, r.Visits, r.Work, r.Stop, r.Baseline, r.Score)
		re.Less(r.Work/int64(r.Visits), int64(DefaultWorkBudget/10), "one visit must stay a small part of the budget")
		re.LessOrEqual(r.Work, int64(DefaultWorkBudget+DefaultWorkBudget/10))
		re.True(r.Score.Less(r.Baseline), "%v -> %v", r.Baseline, r.Score)
		for _, s := range g.Forced {
			re.Contains(r.Voters[0], s)
		}
	}
}
