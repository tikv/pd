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
	"encoding/binary"
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

// Bounds for one rule search. Directed repairs share the node bound but each
// fixed target has its own assignment-step budget.
const (
	maxSearchNodes = 64
	maxAssignSteps = 1024
)

// ruleSearch counts the work of a rule search and records any cutoff.
type ruleSearch struct {
	p            *planner
	nodes, steps int
	limited      bool
}

func (rs *ruleSearch) done() bool {
	if rs.nodes >= maxSearchNodes || rs.steps >= maxAssignSteps || rs.p.stopped() {
		rs.limited = true
		return true
	}
	return false
}

// choose collects improving complete rule assignments before checking or
// repairing any of them. Without CheckTarget, a search within its bounds is
// exact. Counts must already exclude the group's working target.
func (p *planner) choose(g *groupState) (voters, rules []int, leader int, ok bool) {
	current := solution{voters: g.cur, rules: g.curRule, leader: g.lead}
	for _, s := range g.cur {
		current.c = current.c.add(p.costOf(g, s, s == g.lead))
	}
	var leaves []solution
	collect := func(sol solution) {
		current.ok = true
		if sol.c.cmp(current.c) < 0 {
			leaves = append(leaves, sol)
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
		p.assignForced(g, forced, make([][]int, len(g.Rules)), &rs, func(must [][]int) bool {
			if g.LeaderPinned && !slices.Contains(must[host], g.Leader) {
				return false
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
			return false
		})
	}
	if len(leaves) == 0 {
		return current.voters, current.rules, current.leader, current.ok
	}
	slices.SortStableFunc(leaves, func(a, b solution) int { return a.c.cmp(b.c) })
	best := leaves[0]
	if g.CheckTarget != nil {
		best = p.repairTargets(g, leaves, current.c, &rs)
	}
	return best.voters, best.rules, best.leader, best.ok
}

// assignForced calls f with the assignments of the forced stores to rules
// that accept them, within each rule's count, until the search is cut off.
// Every store tries its current rule first. The callback borrows must until
// it returns, when backtracking can reuse the per-rule slices.
func (p *planner) assignForced(g *groupState, forced []int, must [][]int, rs *ruleSearch, f func([][]int) bool) bool {
	rs.steps++
	if len(forced) == 0 {
		return !rs.done() && f(must)
	}
	s := forced[0]
	current := 0
	if k := slices.Index(g.Voters, s); k >= 0 {
		current = ruleIndex(g.Group, k)
	}
	for i := range g.Rules {
		ri := (current + i) % len(g.Rules)
		if rs.done() {
			return false
		}
		p.work++
		if len(must[ri]) >= g.Rules[ri].Count || !p.accepts(g, ri, s) {
			continue
		}
		must[ri] = append(must[ri], s)
		ok := p.assignForced(g, forced[1:], must, rs, f)
		must[ri] = must[ri][:len(must[ri])-1]
		if ok {
			return true
		}
	}
	return false
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

// repairTargets checks every collected leaf before spending the remaining
// bounds on repairs. Each physical target is checked once per choice.
func (p *planner) repairTargets(g *groupState, leaves []solution, current cost, rs *ruleSearch) solution {
	type rejected struct {
		sol      solution
		feedback TargetFeedback
		repairs  []repair
	}
	var queue []rejected
	var checked [][]int
	var targetRejected bool
	// A check can finish the search by accepting a target or observing a stop.
	check := func(sol solution) (solution, bool) {
		key := slices.Clone(sol.voters)
		slices.Sort(key)
		key = append(key, sol.leader)
		if slices.ContainsFunc(checked, func(k []int) bool { return slices.Equal(key, k) }) {
			return solution{}, false
		}
		if p.stopped() {
			p.placementLimited = p.placementLimited || targetRejected
			return solution{}, true
		}
		checked = append(checked, key)
		feedback := p.runTargetCheck(g, sol.voters, sol.leader)
		switch feedback.Status {
		case TargetSatisfied:
			if sol.c.cmp(current) < 0 {
				return sol, true
			}
		case TargetRejected:
			targetRejected = true
			if len(feedback.Replace) > 0 {
				queue = append(queue, rejected{sol: sol, feedback: feedback})
			}
		}
		return solution{}, false
	}
	for _, sol := range leaves {
		if result, done := check(sol); done {
			return result
		}
	}
	// A cutoff while collecting leaves may hide alternatives even without repairs.
	if rs.limited && targetRejected {
		p.placementLimited = true
		return solution{}
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node.repairs == nil {
			node.repairs = p.repairs(g, node.sol, node.feedback)
		}
		// Let children take a second step without letting a deep rejected
		// chain or one wide parent consume every remaining node.
		for range min(4, len(node.repairs)) {
			if rs.done() {
				p.placementLimited = true
				return solution{}
			}
			replacement := node.repairs[0]
			node.repairs = node.repairs[1:]
			voters := slices.Clone(node.sol.voters)
			voters[slices.Index(voters, replacement.old)] = replacement.next
			rs.nodes++
			candidate := p.assignTarget(g, voters, rs)
			if candidate.ok {
				if result, done := check(candidate); done {
					return result
				}
			}
		}
		if len(node.repairs) > 0 {
			queue = append(queue, node)
		}
	}
	return solution{}
}

type repair struct {
	old, next int
	delta     cost
}

// repairs orders the cheapest representative of each (old store, domain
// signature) before the rest. Same-domain alternatives remain reachable.
func (p *planner) repairs(g *groupState, sol solution, feedback TargetFeedback) []repair {
	var repairs []repair
	seen := make(map[[2]int]bool)
	for _, replacement := range feedback.Replace {
		if !slices.Contains(sol.voters, replacement.Old) || g.isForced(replacement.Old) {
			continue
		}
		for _, next := range replacement.Candidates {
			if p.stopped() {
				p.placementLimited = true
				return nil
			}
			p.work++
			key := [2]int{replacement.Old, next}
			if next < 0 || next >= p.n || slices.Contains(sol.voters, next) || slices.Contains(g.Exclude, next) || seen[key] {
				continue
			}
			seen[key] = true
			leader := replacement.Old == sol.leader
			repairs = append(repairs, repair{replacement.Old, next, p.costOf(g, next, leader).sub(p.costOf(g, replacement.Old, leader))})
		}
	}
	slices.SortFunc(repairs, func(a, b repair) int {
		return cmp.Or(a.delta.cmp(b.delta), cmp.Compare(a.old, b.old), cmp.Compare(a.next, b.next))
	})
	var primary, secondary []repair
	domains := make(map[string]bool)
	for _, r := range repairs {
		key := binary.AppendVarint(nil, int64(r.old))
		for ri, rule := range g.Rules {
			domain := int64(-1) // Stores outside this rule share a separate class.
			if p.accepts(g, ri, r.next) {
				domain = 0 // No differing location level means one domain.
				if rule.level < rule.depth() {
					domain = int64(rule.domainOf[r.next])
				}
			}
			key = binary.AppendVarint(key, domain)
		}
		if domains[string(key)] {
			secondary = append(secondary, r)
		} else {
			domains[string(key)] = true
			primary = append(primary, r)
		}
	}
	return append(primary, secondary...)
}

// assignTarget reuses the bounded rule assignment for a fixed physical target.
// The checker's fitted rule need not be the planner's previous assignment.
func (p *planner) assignTarget(g *groupState, voters []int, rs *ruleSearch) solution {
	if p.stopped() {
		p.placementLimited = true
		return solution{}
	}
	hosts := p.leaderHosts(g)
	if len(hosts) == 0 {
		return solution{}
	}
	// Charge one repair attempt to the caller; its internal assignments cannot
	// exhaust the step budget needed to reach subsequent repair candidates.
	rs.steps++
	if len(g.Rules) > 1 {
		// Try the least flexible voters first. A voter with no matching rule
		// rules out the whole target before any assignments are enumerated.
		type voterRules struct{ store, count int }
		order := make([]voterRules, len(voters))
		for i, s := range voters {
			order[i].store = s
			for ri := range g.Rules {
				if p.stopped() {
					p.placementLimited = true
					return solution{}
				}
				p.work++
				if p.accepts(g, ri, s) {
					order[i].count++
				}
			}
			if order[i].count == 0 {
				return solution{}
			}
		}
		slices.SortStableFunc(order, func(a, b voterRules) int { return cmp.Compare(a.count, b.count) })
		voters = make([]int, len(order))
		for i, v := range order {
			voters[i] = v.store
		}
	}
	local := &ruleSearch{p: p}
	var best solution
	assignments := 0
	p.assignForced(g, voters, make([][]int, len(g.Rules)), local, func(must [][]int) bool {
		if p.stopped() {
			p.placementLimited = true
			return true
		}
		sels := make([]choice, len(g.Rules))
		for ri := range g.Rules {
			c, ok := p.pickRule(g, ri, must[ri], nil, false)
			if !ok {
				return false
			}
			sels[ri] = c
		}
		if assignments >= maxSearchNodes {
			p.placementLimited = true
			return true
		}
		assignments++
		for _, host := range hosts {
			if p.stopped() {
				p.placementLimited = true
				return true
			}
			if g.LeaderPinned && !slices.Contains(must[host], g.Leader) {
				continue
			}
			leadChoice, ok := p.pickRule(g, host, must[host], nil, true)
			if !ok {
				continue
			}
			base := sels[host]
			sels[host] = leadChoice
			sol := combine(sels)
			sels[host] = base
			if !best.ok || sol.c.cmp(best.c) < 0 {
				best = sol
			}
		}
		return false
	})
	if local.limited {
		p.placementLimited = true
	}
	return best
}

// pickRule selects one rule's stores, with the leader when host is set. The
// selection must reach the rule's best isolation over all its candidates.
// This does not enforce PD's best-location check across rules with the same
// role; the caller can supply Group.CheckTarget for that. Competing rules may
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
