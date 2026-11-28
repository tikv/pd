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
)

func (r *Rule) depth() int {
	if len(r.Location) == 0 {
		return 0
	}
	return len(r.Location[0])
}

// cost orders store choices. Sums compare lexicographically, which is
// compatible with addition. The fields are:
//
//	0 voter count after removing the group
//	1 leader count (leader only)
//	2 whether the store is new to the target this greedy call started from
//	3 whether the leader leaves that target's leader (leader only)
//	4 voter count of other tables, which spreads tables apart on ties
//	5 leader count of other tables (leader only)
type cost [6]int64

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
	return slices.Compare(a[:], b[:])
}

func (p *planner) costOf(g *groupState, s int, leader bool) cost {
	c := cost{int64(p.v[s]), 0, 1}
	if slices.Contains(g.startV, s) {
		c[2] = 0
	}
	if p.otherV != nil {
		c[4] = int64(p.otherV[s])
	}
	if leader {
		c[1] = int64(p.l[s])
		if s != g.startLd {
			c[3] = 1
		}
		if p.otherL != nil {
			c[5] = int64(p.otherL[s])
		}
	}
	return c
}

func (p *planner) leaderCapable(g *groupState, r *Rule, s int) bool {
	return r.Role != RoleFollower && p.canLead(g, s)
}

// solution is a complete target of one group. rules[i] labels voters[i].
type solution struct {
	ok            bool
	c             cost
	voters, rules []int
	leader        int
}

// Bounds for one rule search.
const (
	maxSearchNodes = 64
	maxAssignSteps = 1024
)

// ruleSearch counts the work of a rule search.
type ruleSearch struct {
	p            *planner
	nodes, steps int
}

func (rs *ruleSearch) done() bool {
	return rs.nodes >= maxSearchNodes || rs.steps >= maxAssignSteps || rs.p.stopped()
}

// choose keeps the lowest-cost complete rule assignment if it improves the
// current target. Counts must already exclude the group's working target.
func (p *planner) choose(g *groupState) (voters, rules []int, leader int, ok bool) {
	best := solution{voters: g.cur, rules: g.curRule, leader: g.lead}
	for _, s := range g.cur {
		best.c = best.c.add(p.costOf(g, s, s == g.lead))
	}
	collect := func(sol solution) {
		best.ok = true
		if sol.c.cmp(best.c) < 0 {
			best = sol
		}
	}
	var forced []int
	for _, s := range g.Voters {
		if g.isForced(s) {
			forced = append(forced, s)
		}
	}
	hosts := p.leaderHosts(g)
	if k := slices.Index(g.Voters, g.Leader); k >= 0 {
		if i := slices.Index(hosts, ruleIndex(g.Group, k)); i > 0 {
			hosts[0], hosts[i] = hosts[i], hosts[0]
		}
	}
	rs := ruleSearch{p: p}
	for _, host := range hosts {
		if rs.done() {
			break
		}
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
			p.searchRules(g, host, must, forbid, make([]choice, len(g.Rules)), &rs, collect)
		})
	}
	return best.voters, best.rules, best.leader, best.ok
}

// assignForced calls f with the assignments of the forced stores to rules
// that accept them, within each rule's count, until the search is cut off.
// Every store tries its current rule first. The callback borrows must until
// it returns, when backtracking can reuse the per-rule slices.
func (p *planner) assignForced(g *groupState, forced []int, must [][]int, rs *ruleSearch, f func([][]int)) {
	rs.steps++
	if len(forced) == 0 {
		if !rs.done() {
			f(must)
		}
		return
	}
	s := forced[0]
	current := 0
	if k := slices.Index(g.Voters, s); k >= 0 {
		current = ruleIndex(g.Group, k)
	}
	for i := range g.Rules {
		ri := (current + i) % len(g.Rules)
		if rs.done() {
			return
		}
		p.work++
		if len(must[ri]) >= g.Rules[ri].Count || !p.accepts(g, ri, s) {
			continue
		}
		must[ri] = append(must[ri], s)
		p.assignForced(g, forced[1:], must, rs, f)
		must[ri] = must[ri][:len(must[ri])-1]
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

func (p *planner) searchRules(g *groupState, host int, must, forbid [][]int, sels []choice, rs *ruleSearch, collect func(solution)) {
	rs.nodes++
	for ri := range g.Rules {
		if sels[ri].sel == nil {
			c, ok := p.pickRule(g, ri, must[ri], forbid[ri], ri == host)
			if !ok {
				return
			}
			sels[ri] = c
		}
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
			for _, owner := range claimants {
				if rs.done() {
					break
				}
				next := slices.Clone(forbid)
				picks := slices.Clone(sels)
				for _, rj := range claimants {
					if rj != owner {
						next[rj] = append(slices.Clone(forbid[rj]), s)
						picks[rj] = choice{} // Only changed restrictions need another pick.
					}
				}
				p.searchRules(g, host, must, next, picks, rs, collect)
			}
			return
		}
	}
	collect(combine(sels))
}

func combine(sels []choice) solution {
	sol := solution{ok: true, leader: -1}
	for ri, c := range sels {
		sol.c = sol.c.add(c.v.c)
		sol.voters = append(sol.voters, c.sel...)
		for range c.sel {
			sol.rules = append(sol.rules, ri)
		}
		if c.leader >= 0 {
			sol.leader = c.leader
		}
	}
	sol.ok = sol.leader >= 0
	return sol
}

// pickRule selects one rule's stores, with the leader when host is set. The
// selection must reach the rule's best isolation over all its candidates.
// The caller validates PD's best location across rules with the same role. Competing rules may
// have no complete target that meets their independent optima; visit then
// keeps voters and only picks a leader.
func (p *planner) pickRule(g *groupState, ri int, must, forbid []int, host bool) (choice, bool) {
	r := g.Rules[ri]
	target, ok := p.isolation(g, ri)
	if !ok {
		return choice{}, false
	}
	var cands []int
	if len(must) == r.Count {
		// The full candidate set is only needed for the cached isolation target.
		// A fixed rule can evaluate its mandatory stores directly.
		cands = slices.Clone(must)
		slices.Sort(cands)
		for i, s := range cands {
			if i > 0 && cands[i-1] == s || slices.Contains(forbid, s) || slices.Contains(g.Exclude, s) || !p.accepts(g, ri, s) {
				return choice{}, false
			}
		}
	} else {
		var skip func(int) bool
		if len(forbid) > 0 {
			skip = func(s int) bool { return slices.Contains(forbid, s) }
		}
		cands = p.candidates(p.candBuf, g, ri, skip)
		p.candBuf = cands // reused: the pick below does not keep cands
		for _, s := range must {
			if _, found := slices.BinarySearch(cands, s); !found {
				return choice{}, false
			}
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
	if !c.ok || c.v.cmpIsolation(target) != 0 {
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
