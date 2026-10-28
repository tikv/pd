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
	"cmp"
	"slices"
)

func (r *Rule) depth() int {
	if len(r.Location) == 0 {
		return 0
	}
	return len(r.Location[0])
}

// leaf reports whether every store is its own isolation domain.
func (r *Rule) leaf() bool { return r.level >= r.depth() }

// cost orders store choices. Sums compare lexicographically, which is
// compatible with addition. The fields are:
//
//	0 voter count after removing the group
//	1 leader count (leader only)
//	2 voter count before removing the group
//	3 whether the store is new to the target this greedy call started from
//	4 whether the leader leaves that target's leader (leader only)
//	5 voter count of other tables, which spreads tables apart on ties
//	6 leader count of other tables (leader only)
type cost [7]int64

func (a cost) add(b cost) cost {
	for i := range a {
		a[i] += b[i]
	}
	return a
}

func (a cost) sub(b cost) cost {
	for i := range a {
		a[i] -= b[i]
	}
	return a
}

func (a cost) cmp(b cost) int {
	for i := range a {
		if c := cmp.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

func (p *planner) costOf(g *groupState, s int, leader bool) cost {
	c := cost{int64(p.v[s]), 0, int64(p.v[s]), 1}
	if slices.Contains(g.cur, s) {
		c[2]++
	}
	if slices.Contains(g.startV, s) {
		c[3] = 0
	}
	if p.otherV != nil {
		c[5] = int64(p.otherV[s])
	}
	if leader {
		c[1] = int64(p.l[s])
		if s != g.startLd {
			c[4] = 1
		}
		if p.otherL != nil {
			c[6] = int64(p.otherL[s])
		}
	}
	return c
}

func (p *planner) leaderCapable(g *groupState, r *Rule, s int) bool {
	return r.Role != RoleFollower && p.canLead(g, s)
}

// solution is a complete target of one group.
type solution struct {
	ok            bool
	c             cost
	voters, rules []int
	leader        int
}

// Bounds of the rule search of one choice, shared by all leader rules tried:
// maxSearchNodes complete rule picks (one per forced-voter assignment and per
// conflict branch) and maxAssignSteps steps of the assignment enumeration.
// The search also stops once the plan is out of budget or canceled, after its
// first node. Conflicts need overlapping rules and forced voters need
// throttled stores, so the search is complete, and exact, for the usual
// groups; the oracle shapes never reach the bounds. A group with many forced
// voters and overlapping rules is cut off instead of costing the whole plan's
// budget in one visit: 11 forced voters over 3 overlapping rules of 4 on 300
// stores scanned 91 million candidates (6.6 s) without the bounds, and with
// bounds per leader rule 30 overlapping rules of 1 with 29 forced voters
// still scanned 15.7 million (1.0 s).
const (
	maxSearchNodes = 64
	maxAssignSteps = 1024
)

// ruleSearch counts the work of one choice's rule search.
type ruleSearch struct {
	p            *planner
	nodes, steps int
}

func (rs *ruleSearch) done() bool {
	return rs.nodes >= maxSearchNodes || rs.steps >= maxAssignSteps || rs.nodes > 0 && rs.p.stopped()
}

// choose selects the complete target with the lowest cost. The counts must
// already exclude the group's working target. exclude, when set, removes
// stores from the selection but not from the isolation every rule must reach.
//
// It first picks every rule independently. A store claimed by several rules
// is then given to each claimant in turn and forbidden to the others. A
// forced voter must stay but may fit any rule that accepts it, so each such
// assignment is tried, the current one first. The cost is a sum over rules,
// so a search that ends within its bounds is exact. One that is cut off
// returns the best target found; it is never worse than the current target,
// because the current leader's rule is searched first and its search starts
// with the current assignment.
func (p *planner) choose(g *groupState, exclude []bool) (voters, rules []int, leader int, ok bool) {
	var forced []int
	for _, s := range g.Voters {
		if g.isForced(s) {
			forced = append(forced, s)
		}
	}
	if len(g.Rules) == 1 {
		// The common case: no conflicts, every forced voter is the rule's.
		if len(p.leaderHosts(g)) == 0 {
			return nil, nil, 0, false
		}
		c, ok := p.pickRule(g, 0, exclude, forced, nil, true)
		if !ok || c.leader < 0 {
			return nil, nil, 0, false
		}
		return c.sel, make([]int, len(c.sel)), c.leader, true
	}
	var best solution
	hosts := p.leaderHosts(g)
	if k := slices.Index(g.Voters, g.Leader); k >= 0 {
		if i := slices.Index(hosts, ruleIndex(g.Group, k)); i > 0 {
			hosts = slices.Clone(hosts)
			hosts[0], hosts[i] = hosts[i], hosts[0]
		}
	}
	rs := ruleSearch{p: p}
	for _, host := range hosts {
		p.assignForced(g, forced, make([][]int, len(g.Rules)), &rs, func(must [][]int) {
			if g.LeaderPinned && !slices.Contains(must[host], g.Leader) {
				return
			}
			forbid := make([][]int, len(g.Rules))
			for ri := range g.Rules {
				for rj, m := range must {
					if rj != ri {
						forbid[ri] = append(forbid[ri], m...)
					}
				}
			}
			if s := p.searchRules(g, exclude, host, must, forbid, &rs); s.ok && (!best.ok || s.c.cmp(best.c) < 0) {
				best = s
			}
		})
	}
	if !best.ok {
		return nil, nil, 0, false
	}
	return best.voters, best.rules, best.leader, true
}

// assignForced calls f with the assignments of the forced stores to rules
// that accept them, within each rule's count, until the search is cut off.
// Every store tries its current rule first.
func (p *planner) assignForced(g *groupState, forced []int, must [][]int, rs *ruleSearch, f func([][]int)) {
	rs.steps++
	if len(forced) == 0 {
		f(must)
		return
	}
	s := forced[0]
	k := slices.Index(g.Voters, s)
	current := ruleIndex(g.Group, k)
	for i := range g.Rules {
		// Visit the current rule first, then the others in order.
		ri := (current + i) % len(g.Rules)
		if rs.done() {
			return
		}
		if len(must[ri]) >= g.Rules[ri].Count || !p.accepts(g, ri, s) {
			continue
		}
		next := slices.Clone(must)
		next[ri] = append(slices.Clone(must[ri]), s)
		p.assignForced(g, forced[1:], next, rs, f)
	}
}

// accepts reports whether s is a candidate of rule ri for g.
func (*planner) accepts(g *groupState, ri int, s int) bool {
	if g.Rules[ri].isMember(s) {
		return true
	}
	for k, v := range g.Voters {
		if v == s && ruleIndex(g.Group, k) == ri {
			return true
		}
	}
	return false
}

func (p *planner) searchRules(g *groupState, exclude []bool, host int, must, forbid [][]int, rs *ruleSearch) solution {
	rs.nodes++
	sels := make([]choice, len(g.Rules))
	for ri := range g.Rules {
		c, ok := p.pickRule(g, ri, exclude, must[ri], forbid[ri], ri == host)
		if !ok {
			return solution{}
		}
		sels[ri] = c
	}
	for ri := range sels {
		for _, s := range sels[ri].sel {
			var claimants []int
			for rj := range sels {
				if slices.Contains(sels[rj].sel, s) {
					claimants = append(claimants, rj)
				}
			}
			if len(claimants) < 2 {
				continue
			}
			var best solution
			for _, owner := range claimants {
				if rs.done() {
					break
				}
				next := slices.Clone(forbid)
				for _, rj := range claimants {
					if rj != owner {
						next[rj] = append(slices.Clone(forbid[rj]), s)
					}
				}
				if sol := p.searchRules(g, exclude, host, must, next, rs); sol.ok && (!best.ok || sol.c.cmp(best.c) < 0) {
					best = sol
				}
			}
			return best
		}
	}
	sol := solution{ok: true, leader: -1}
	for ri, c := range sels {
		sol.c = sol.c.add(c.v.c)
		sol.voters = append(sol.voters, c.sel...)
		for range c.sel {
			sol.rules = append(sol.rules, ri)
		}
		if ri == host {
			sol.leader = c.leader
		}
	}
	sol.ok = sol.leader >= 0
	return sol
}

// pickRule selects one rule's stores, with the leader when host is set. The
// selection must reach the rule's best isolation over all its candidates.
// This is stricter than PD's best-location check, which accounts for stores
// occupied by other rules. Competing rules may have no complete target that
// meets their independent optima; visit then keeps voters and only picks a leader.
func (p *planner) pickRule(g *groupState, ri int, exclude []bool, must, forbid []int, host bool) (choice, bool) {
	r := g.Rules[ri]
	var skip func(int) bool
	if exclude != nil || len(forbid) > 0 {
		skip = func(s int) bool { return exclude != nil && exclude[s] || slices.Contains(forbid, s) }
	}
	cands := p.candidates(p.candBuf, g, ri, skip)
	p.candBuf = cands // reused: the pick below does not keep cands
	for _, s := range must {
		if _, found := slices.BinarySearch(cands, s); !found {
			return choice{}, false
		}
	}
	var lead func(int) bool
	if host {
		lead = func(s int) bool {
			return p.leaderCapable(g, r, s) && (!g.LeaderPinned || s == g.Leader)
		}
	}
	res := p.pickAll(g, r, cands, must, 0, r.Count, lead)
	c := res[0][r.Count]
	if host {
		c = res[1][r.Count]
	}
	target, ok := p.isolation(g, ri)
	if !ok || !c.ok || c.v.cmpIsolation(target) != 0 {
		return choice{}, false
	}
	return c, true
}

// isolation returns the best isolation rule ri can reach over all its
// candidates. It does not depend on counts, so it is computed once, and once
// per rule for the usual groups whose voters are all shared candidates.
func (p *planner) isolation(g *groupState, ri int) (value, bool) {
	if g.iso == nil {
		g.iso = make([]*isoTarget, len(g.Rules))
	}
	if g.iso[ri] == nil {
		r := g.Rules[ri]
		shared := len(g.Exclude) == 0
		for k, s := range g.Voters {
			if ruleIndex(g.Group, k) == ri && !r.isMember(s) {
				shared = false
			}
		}
		t := &isoTarget{}
		if shared {
			if p.ruleIso == nil {
				p.ruleIso = map[*Rule]*isoTarget{}
			}
			if p.ruleIso[r] == nil {
				p.ruleIso[r] = t
			}
			t = p.ruleIso[r]
		}
		if !t.done {
			c := p.pickAll(g, r, p.candidates(nil, g, ri, nil), nil, 0, r.Count, nil)[0][r.Count]
			t.done, t.ok, t.v = true, c.ok, value{pairs: c.v.pairs}
		}
		g.iso[ri] = t
	}
	return g.iso[ri].v, g.iso[ri].ok
}

// isoTarget caches the isolation a rule must reach.
type isoTarget struct {
	done, ok bool
	v        value
}

// leaderHosts lists the rules that may hold the leader: the leader rule if
// there is one, otherwise every rule that is not a follower rule. A pinned
// leader stays, so only rules that accept its store remain.
func (p *planner) leaderHosts(g *groupState) []int {
	var out []int
	for ri, r := range g.Rules {
		if r.Role == RoleLeader {
			out = []int{ri}
			break
		}
		if r.Role != RoleFollower {
			out = append(out, ri)
		}
	}
	if g.LeaderPinned {
		out = slices.DeleteFunc(out, func(ri int) bool { return !p.accepts(g, ri, g.Leader) })
	}
	return out
}

// candidates lists rule ri's stores for g in ascending order: the rule's
// shared stores and the group's original voters of that rule. It appends to
// dst[:0].
func (*planner) candidates(dst []int, g *groupState, ri int, skip func(int) bool) []int {
	r := g.Rules[ri]
	out := dst[:0]
	if out == nil {
		out = make([]int, 0, len(r.Stores)+len(g.Voters))
	}
	for _, s := range r.Stores {
		if (skip == nil || !skip(s)) && !slices.Contains(g.Exclude, s) {
			out = append(out, s)
		}
	}
	for k, s := range g.Voters {
		if ruleIndex(g.Group, k) != ri || r.isMember(s) || skip != nil && skip(s) || slices.Contains(g.Exclude, s) {
			continue
		}
		if i, found := slices.BinarySearch(out, s); !found {
			out = slices.Insert(out, i, s)
		}
	}
	return out
}

type alternative struct {
	voters, rules []int
	leader        int
}

// alternatives lists cheap legal targets other than the working one: it
// excludes one current voter at a time and chooses again. Trying combinations
// per domain did no better in the parameter study.
func (p *planner) alternatives(g *groupState) []alternative {
	if len(p.exclude) < p.n {
		p.exclude = make([]bool, p.n)
	}
	var out []alternative
	for _, s := range g.cur {
		if g.isForced(s) {
			continue
		}
		p.exclude[s] = true
		if v, rr, l, ok := p.choose(g, p.exclude); ok && (l != g.lead || !sameSet(v, g.cur)) {
			out = append(out, alternative{v, rr, l})
		}
		p.exclude[s] = false
	}
	return out
}
