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
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// oracleCases is the number of random cases per rule shape. Regular runs use
// 100 (about 12 s); the full check uses 400 (about 45 s):
//
//	go test ./pkg/schedule/affinity/balanceplan -run TestOracle -args -oracle-cases=400
var oracleCases = flag.Int("oracle-cases", 100, "random cases per rule shape in TestOracleBoundChooserAndLegality")

// The oracle below is independent of the planner: legality uses PD's
// placement isolation formula (pairwise, base 100, first differing label) and
// accepts a peer set only when no other candidate set of the same rule scores
// higher, which is what the best-location check demands.

type shape struct {
	name    string
	zones   int // top-level label values
	hosts   int // stores per zone (second label level); 0 = no second level
	depth   int // label levels used by the rule
	counts  []int
	roles   []Role
	overlap bool // rules draw candidates from all stores, so they overlap
}

var shapes = []shape{
	{name: "3zone-c3", zones: 3, hosts: 3, depth: 1, counts: []int{3}, roles: []Role{RoleVoter}},
	{name: "5zone-c3", zones: 5, hosts: 2, depth: 1, counts: []int{3}, roles: []Role{RoleVoter}},
	{name: "2zone-c3-host", zones: 2, hosts: 3, depth: 2, counts: []int{3}, roles: []Role{RoleVoter}},
	{name: "3zone-c5-host", zones: 3, hosts: 3, depth: 2, counts: []int{5}, roles: []Role{RoleVoter}},
	{name: "1zone-c3-host", zones: 1, hosts: 6, depth: 2, counts: []int{3}, roles: []Role{RoleVoter}},
	{name: "nolabel-c3", zones: 1, hosts: 6, depth: 0, counts: []int{3}, roles: []Role{RoleVoter}},
	{name: "leader+followers", zones: 3, hosts: 2, depth: 1, counts: []int{1, 2}, roles: []Role{RoleLeader, RoleFollower}},
	{name: "leader+voters overlap", zones: 3, hosts: 2, depth: 1, counts: []int{1, 2}, roles: []Role{RoleLeader, RoleVoter}, overlap: true},
	{name: "3 rules overlap", zones: 3, hosts: 2, depth: 1, counts: []int{1, 1, 1}, roles: []Role{RoleLeader, RoleVoter, RoleFollower}, overlap: true},
	{name: "2 voter rules overlap", zones: 2, hosts: 3, depth: 1, counts: []int{2, 1}, roles: []Role{RoleVoter, RoleVoter}, overlap: true},
}

func isolationScore(loc [][]int32, stores []int, depth int) float64 {
	var score float64
	for i, a := range stores {
		for _, b := range stores[i+1:] {
			for lv := range depth {
				if loc[a][lv] != loc[b][lv] {
					score += math.Pow(100, float64(depth-lv-1))
					break
				}
			}
		}
	}
	return score
}

func combinations(xs []int, k int, f func([]int)) {
	sel := make([]int, 0, k)
	var rec func(int)
	rec = func(i int) {
		if len(sel) == k {
			f(slices.Clone(sel))
			return
		}
		for j := i; j <= len(xs)-(k-len(sel)); j++ {
			sel = append(sel, xs[j])
			rec(j + 1)
			sel = sel[:len(sel)-1]
		}
	}
	rec(0)
}

type oracleTarget struct {
	voters []int
	rules  []int
	leader int
}

// legalTargets enumerates every legal target of group g of the input.
func legalTargets(in *Input, gi int) []oracleTarget {
	g := &in.Groups[gi]
	per := make([][][]int, len(g.Rules))
	for ri, r := range g.Rules {
		cands := slices.Clone(r.Stores)
		for k, s := range g.Voters {
			if ruleIndex(g, k) == ri && !slices.Contains(cands, s) {
				cands = append(cands, s)
			}
		}
		slices.Sort(cands)
		depth := r.depth()
		best := -1.0
		var sets [][]int
		combinations(cands, r.Count, func(c []int) {
			score := isolationScore(r.Location, c, depth)
			if score > best {
				best, sets = score, nil
			}
			if score == best {
				sets = append(sets, c)
			}
		})
		per[ri] = sets
	}
	var out []oracleTarget
	var rec func(int, []int, []int)
	rec = func(ri int, voters, rules []int) {
		if ri == len(g.Rules) {
			for _, f := range g.Forced {
				if !slices.Contains(voters, f) {
					return
				}
			}
			if g.LeaderPinned && !slices.Contains(voters, g.Leader) {
				return
			}
			for k, s := range voters {
				r := g.Rules[rules[k]]
				if r.Role == RoleFollower || slices.ContainsFunc(g.Rules, func(x *Rule) bool { return x.Role == RoleLeader }) && r.Role != RoleLeader {
					continue
				}
				if g.LeaderPinned && s != g.Leader || s != g.Leader && !g.CanLead[s] {
					continue
				}
				out = append(out, oracleTarget{slices.Clone(voters), slices.Clone(rules), s})
			}
			return
		}
		for _, set := range per[ri] {
			if slices.ContainsFunc(set, func(s int) bool { return slices.Contains(voters, s) }) {
				continue
			}
			nv, nr := append(slices.Clone(voters), set...), slices.Clone(rules)
			for range set {
				nr = append(nr, ri)
			}
			rec(ri+1, nv, nr)
		}
	}
	rec(0, nil, nil)
	return out
}

func randomCase(rng *rand.Rand, sh shape) (*Input, bool) {
	hosts := max(sh.hosts, 1)
	n := sh.zones * hosts
	loc := make([][]int32, n)
	for s := range n {
		path := []int32{int32(s / hosts), int32(s % hosts)}
		loc[s] = path[:sh.depth]
	}
	in := &Input{Stores: n, BaseVoters: make([]int, n), BaseLeaders: make([]int, n)}
	for s := range n {
		in.BaseVoters[s] = rng.IntN(3)
		in.BaseLeaders[s] = rng.IntN(2)
	}
	groups := 1 + rng.IntN(3)
	for range groups {
		var rules []*Rule
		for ri, c := range sh.counts {
			var stores []int
			for s := range n {
				// leader+followers: the leader rule owns zone 0.
				if len(sh.counts) == 2 && !sh.overlap && (ri == 0) != (s/hosts == 0) {
					continue
				}
				p := 0.8
				if sh.overlap {
					p = 0.6
				}
				if rng.Float64() < p {
					stores = append(stores, s)
				}
			}
			rules = append(rules, &Rule{Role: sh.roles[ri], Count: c, Stores: stores, Location: loc})
		}
		g := Group{Rules: rules, CanLead: make([]bool, n)}
		for s := range n {
			g.CanLead[s] = rng.Float64() < 0.5
		}
		in.Groups = append(in.Groups, g)
		gi := len(in.Groups) - 1
		// Pick a random legal current target; leader may stay without CanLead.
		all := legalTargets(&Input{Stores: n, Groups: []Group{{Rules: rules, CanLead: allTrue(n)}}}, 0)
		if len(all) == 0 {
			return nil, false
		}
		t := all[rng.IntN(len(all))]
		in.Groups[gi].Voters, in.Groups[gi].RuleOf, in.Groups[gi].Leader = t.voters, t.rules, t.leader
		if rng.Float64() < 0.2 {
			in.Groups[gi].Forced = []int{t.voters[rng.IntN(len(t.voters))]}
		}
		if rng.Float64() < 0.1 {
			in.Groups[gi].LeaderPinned = true
		}
	}
	return in, true
}

func allTrue(n int) []bool {
	out := make([]bool, n)
	for i := range out {
		out[i] = true
	}
	return out
}

func scoreOf(in *Input, voters [][]int, leaders []int) Score {
	v, l := slices.Clone(in.BaseVoters), slices.Clone(in.BaseLeaders)
	for i := range voters {
		for _, s := range voters[i] {
			v[s]++
		}
		l[leaders[i]]++
	}
	var out Score
	for s := range v {
		out.Voter += int64(v[s] * v[s])
		out.Leader += int64(l[s] * l[s])
	}
	return out
}

func TestOracleBoundChooserAndLegality(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261027, 7))
	for _, sh := range shapes {
		cases, optimal, boundStops, chooserChecks := 0, 0, 0, 0
		for cases < *oracleCases {
			in, ok := randomCase(rng, sh)
			if !ok {
				continue
			}
			legal := make([][]oracleTarget, len(in.Groups))
			product := 1
			for gi := range in.Groups {
				legal[gi] = legalTargets(in, gi)
				require.NotEmpty(t, legal[gi])
				product *= len(legal[gi])
			}
			if product > 200000 {
				continue
			}
			cases++
			// Exhaustive optimum over all legal layouts.
			minV, minL := int64(math.MaxInt64), int64(math.MaxInt64)
			opt := Score{math.MaxInt64, math.MaxInt64}
			idx := make([]int, len(in.Groups))
			for {
				voters, leaders := make([][]int, len(idx)), make([]int, len(idx))
				for gi, k := range idx {
					voters[gi], leaders[gi] = legal[gi][k].voters, legal[gi][k].leader
				}
				s := scoreOf(in, voters, leaders)
				minV, minL = min(minV, s.Voter), min(minL, s.Leader)
				if s.Less(opt) {
					opt = s
				}
				gi := 0
				for gi < len(idx) {
					idx[gi]++
					if idx[gi] < len(legal[gi]) {
						break
					}
					idx[gi] = 0
					gi++
				}
				if gi == len(idx) {
					break
				}
			}
			r := Plan(in, Options{Seed: uint64(cases)})
			name := fmt.Sprintf("%s case %d", sh.name, cases)
			for gi := range in.Groups {
				found := slices.ContainsFunc(legal[gi], func(o oracleTarget) bool {
					return o.leader == r.Leaders[gi] && sameSet(o.voters, r.Voters[gi])
				})
				if !found {
					g := in.Groups[gi]
					t.Logf("stores=%d loc=%v", in.Stores, g.Rules[0].Location)
					for ri, rr := range g.Rules {
						t.Logf("rule %d role=%d count=%d stores=%v", ri, rr.Role, rr.Count, rr.Stores)
					}
					t.Logf("voters=%v ruleOf=%v leader=%d forced=%v pinned=%v canLead=%v", g.Voters, g.RuleOf, g.Leader, g.Forced, g.LeaderPinned, g.CanLead)
					t.Logf("legal=%v", legal[gi])
				}
				require.True(t, found, "%s group %d: illegal %v/%d", name, gi, r.Voters[gi], r.Leaders[gi])
			}
			require.Equal(t, scoreOf(in, r.Voters, r.Leaders), r.Score, name)
			require.False(t, r.Baseline.Less(r.Score), name)
			require.LessOrEqual(t, r.Bound.Voter, minV, "%s: voter bound above optimum", name)
			require.LessOrEqual(t, r.Bound.Leader, minL, "%s: leader bound above optimum", name)
			if r.Stop == StopBound {
				boundStops++
				require.Equal(t, opt, r.Score, "%s: stopped at bound but not optimal", name)
			}
			if r.Score == opt {
				optimal++
			}
			// The single-group choice must minimize (sum V', L' of leader).
			p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
			for gi := range p.gs {
				g := &p.gs[gi]
				g.startV = slices.Clone(g.cur)
				g.startLd = g.lead
				for _, s := range g.cur {
					p.v[s]--
				}
				p.l[g.lead]--
				best := [2]int64{math.MaxInt64, math.MaxInt64}
				for _, o := range legal[gi] {
					var sv int64
					for _, s := range o.voters {
						sv += int64(p.v[s])
					}
					if c := [2]int64{sv, int64(p.l[o.leader])}; c[0] < best[0] || c[0] == best[0] && c[1] < best[1] {
						best = c
					}
				}
				voters, rules, leader, ok := p.choose(g, nil)
				require.True(t, ok, "%s group %d: chooser found nothing", name, gi)
				require.True(t, p.legal(g, voters, rules, leader), name)
				var sv int64
				for _, s := range voters {
					sv += int64(p.v[s])
				}
				require.Equal(t, [2]int64{sv, int64(p.l[leader])}, best, "%s group %d: chooser not optimal", name, gi)
				chooserChecks++
				for _, s := range g.cur {
					p.v[s]++
				}
				p.l[g.lead]++
			}
		}
		t.Logf("%-18s cases=%d plan optimal=%d stopped at bound=%d single-group checks=%d", sh.name, cases, optimal, boundStops, chooserChecks)
	}
}
