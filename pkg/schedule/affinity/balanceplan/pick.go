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

// Limits of the exact isolation comparison. value compares, level by level,
// the number of selected pairs whose locations still agree. PD's isolation
// score weights the first differing level by powers of 100, so the two orders
// agree only while a rule has fewer than 100 pairs, that is at most 14 voters.
// Groups with deeper or larger rules are kept fixed by the planner.
const (
	maxDepth = 16
	maxCount = 14
)

// value orders peer sets of one rule: isolation first, then cost.
type value struct {
	pairs [maxDepth]int32
	c     cost
}

func (a value) add(b value) value {
	for i := range a.pairs {
		a.pairs[i] += b.pairs[i]
	}
	a.c = a.c.add(b.c)
	return a
}

func (a value) cmpIsolation(b value) int {
	for i := range a.pairs {
		if c := cmp.Compare(a.pairs[i], b.pairs[i]); c != 0 {
			return c
		}
	}
	return 0
}

func (a value) cmp(b value) int {
	if c := a.cmpIsolation(b); c != 0 {
		return c
	}
	return a.c.cmp(b.c)
}

// choice is the best selection of some stores of one rule. leader is the
// store holding the leader, or -1.
type choice struct {
	ok     bool
	v      value
	sel    []int
	leader int
}

// joinInto stores a+b in cells[i] when it beats the current cell.
func joinInto(cells []choice, i int, a, b *choice) {
	v := a.v.add(b.v)
	if cells[i].ok && v.cmp(cells[i].v) >= 0 {
		return
	}
	leader := a.leader
	if leader < 0 {
		leader = b.leader
	}
	sel := make([]int, 0, len(a.sel)+len(b.sel))
	cells[i] = choice{ok: true, v: v, sel: append(append(sel, a.sel...), b.sel...), leader: leader}
}

// pickAll returns, for every n <= k, the most isolated and then cheapest n
// stores of cands that include must: res[0][n] without the leader, res[1][n]
// with exactly one store accepted by lead holding it. Isolation is exact:
// units are split across the domains of the first differing location level
// by a DP over (domain, units), recursively. The leader is one more DP state,
// so a single pass serves every leader choice.
func (p *planner) pickAll(g *groupState, r *Rule, cands, must []int, level, k int, lead func(int) bool) [2][]choice {
	p.work += int64(len(cands))
	// Results live in scratch of the starting level: the caller consumes them
	// before it starts another pick at the same level.
	sc := &p.results[level]
	for m := range sc {
		sc[m] = slices.Grow(sc[m][:0], k+1)[:k+1]
		clear(sc[m])
	}
	res := *sc
	from, depth := level, r.depth()
	for level < depth && sameValue(r, cands, level) {
		level++
	}
	if level >= depth {
		// One location: only the cost matters.
		pk := p.picker(k, from)
		for _, s := range cands {
			if len(must) > 0 && slices.Contains(must, s) {
				pk.addMust(p.storeItem(g, s, lead))
			} else {
				pk.add(p.storeItem(g, s, lead))
			}
		}
		pk.finish(&res)
	} else {
		domains := p.partition(r, cands, level)
		if k <= len(domains) && spread(r, must, level) {
			// With at most one unit per domain no pair agrees at this level
			// or below, so each domain is represented by its cheapest store.
			pk := p.picker(k, from)
			for _, d := range domains {
				p.domainItem(g, d, must, lead, pk)
			}
			pk.finish(&res)
		} else {
			p.pickSplit(g, r, domains, must, level, k, lead, &res)
		}
	}
	// Every selected pair agrees on the levels all candidates share.
	for lv := from; lv < level; lv++ {
		for m := range res {
			for n := range res[m] {
				if res[m][n].ok {
					res[m][n].v.pairs[lv] += pairCount(n)
				}
			}
		}
	}
	return res
}

// pickSplit distributes units over the domains of level with a DP; dp[1]
// holds the selections that already contain the leader.
func (p *planner) pickSplit(g *groupState, r *Rule, domains [][]int, must []int, level, k int, lead func(int) bool, res *[2][]choice) {
	dp := [2][]choice{make([]choice, k+1), make([]choice, k+1)}
	dp[0][0] = choice{ok: true, leader: -1}
	for _, d := range domains {
		var dm []int
		for _, s := range must {
			if r.Location[s][level] == r.Location[d[0]][level] {
				dm = append(dm, s)
			}
		}
		top := min(len(d), k)
		sub := p.pickAll(g, r, d, dm, level+1, top, lead)
		for m := range sub {
			for n := range sub[m] {
				if sub[m][n].ok {
					sub[m][n].v.pairs[level] += pairCount(n)
				}
			}
		}
		next := [2][]choice{make([]choice, k+1), make([]choice, k+1)}
		for j := range k + 1 {
			for n := 0; n <= top && j+n <= k; n++ {
				if dp[0][j].ok && sub[0][n].ok {
					joinInto(next[0], j+n, &dp[0][j], &sub[0][n])
				}
				if dp[1][j].ok && sub[0][n].ok {
					joinInto(next[1], j+n, &dp[1][j], &sub[0][n])
				}
				if dp[0][j].ok && sub[1][n].ok {
					joinInto(next[1], j+n, &dp[0][j], &sub[1][n])
				}
			}
		}
		dp = next
	}
	*res = dp
}

// item is one unit to choose: a store, or a whole domain represented by its
// cheapest voter store and its cheapest leader store.
type item struct {
	voter, leader int // leader is -1 when the item cannot hold the leader
	vc, lc        cost
}

func (p *planner) storeItem(g *groupState, s int, lead func(int) bool) item {
	it := item{voter: s, leader: -1, vc: p.costOf(g, s, false)}
	if lead != nil && lead(s) {
		it.leader, it.lc = s, p.costOf(g, s, true)
	}
	return it
}

// domainItem feeds one domain to pk: its forced store, or its cheapest voter
// and cheapest leader. A store whose voter count already exceeds the best
// one's cannot win, as the count is the first cost field.
func (p *planner) domainItem(g *groupState, d, must []int, lead func(int) bool, pk *picker) {
	p.work += int64(len(d))
	it := item{voter: -1, leader: -1}
	for _, s := range d {
		if len(must) > 0 && slices.Contains(must, s) {
			// The forced store is the domain's only possible unit.
			pk.addMust(p.storeItem(g, s, lead))
			return
		}
		v := int64(p.v[s])
		if it.voter < 0 || v <= it.vc[0] {
			if c := p.costOf(g, s, false); it.voter < 0 || c.cmp(it.vc) < 0 || c.cmp(it.vc) == 0 && s < it.voter {
				it.voter, it.vc = s, c
			}
		}
		if lead != nil && (it.leader < 0 || v <= it.lc[0]) && lead(s) {
			if c := p.costOf(g, s, true); it.leader < 0 || c.cmp(it.lc) < 0 || c.cmp(it.lc) == 0 && s < it.leader {
				it.leader, it.lc = s, c
			}
		}
	}
	if it.voter >= 0 {
		pk.add(it)
	}
}

// ranked is an item ordered by one of its costs; s is the store it adds.
type ranked struct {
	c  cost
	s  int
	it item
}

func (a *ranked) before(b *ranked) bool {
	if c := a.c.cmp(b.c); c != 0 {
		return c < 0
	}
	return a.s < b.s
}

// keep inserts x into top, a sorted list of at most k entries.
func keep(top []ranked, x ranked, k int) []ranked {
	if k <= 0 || len(top) == k && !x.before(&top[k-1]) {
		return top
	}
	if len(top) < k {
		top = append(top, x)
	} else {
		top[k-1] = x
	}
	for i := len(top) - 1; i > 0 && top[i].before(&top[i-1]); i-- {
		top[i], top[i-1] = top[i-1], top[i]
	}
	return top
}

// picker collects the must items and the k cheapest other items by voter
// cost and by leader cost. One picker is reused: a pick never nests.
type picker struct {
	k, minN         int
	musts           []item
	voters, leaders []ranked
}

// picker starts a pick of k stores. A pick at level 0 serves a rule directly
// and only needs all k; nested picks serve a DP and need every count.
func (p *planner) picker(k, level int) *picker {
	pk := &p.pk
	pk.k, pk.musts, pk.voters, pk.leaders = k, pk.musts[:0], pk.voters[:0], pk.leaders[:0]
	pk.minN = 0
	if level == 0 {
		pk.minN = k
	}
	return pk
}

func (pk *picker) addMust(it item) { pk.musts = append(pk.musts, it) }

func (pk *picker) add(it item) {
	pk.voters = keep(pk.voters, ranked{it.vc, it.voter, it}, pk.k)
	if it.leader >= 0 {
		pk.leaders = keep(pk.leaders, ranked{it.lc, it.leader, it}, pk.k)
	}
}

// finish chooses the must items and the cheapest others for every count.
// With the leader, the leader item is a must item, one of the chosen items,
// or the cheapest leader outside them replacing the last chosen item.
func (pk *picker) finish(res *[2][]choice) {
	free := pk.k - len(pk.musts)
	if free < 0 {
		return
	}
	voters, leaders := pk.voters[:min(len(pk.voters), free)], pk.leaders[:min(len(pk.leaders), free)]
	m0 := len(pk.musts)
	var sum cost
	mustSel := make([]int, 0, pk.k)
	for _, it := range pk.musts {
		sum = sum.add(it.vc)
		mustSel = append(mustSel, it.voter)
	}
	for t := 0; t <= len(voters); t++ {
		n := m0 + t
		if t > 0 {
			sum = sum.add(voters[t-1].c)
		}
		if n < pk.minN {
			continue
		}
		sel := slices.Clone(mustSel)
		for _, x := range voters[:t] {
			sel = append(sel, x.s)
		}
		res[0][n] = choice{ok: true, v: value{c: sum}, sel: sel, leader: -1}
		// The best leader option: its cost, its store and how to build it.
		bestC, bestLeader, kind, at := cost{}, -1, 0, 0
		consider := func(c cost, leader, k, i int) {
			if bestLeader < 0 || c.cmp(bestC) < 0 || c.cmp(bestC) == 0 && leader < bestLeader {
				bestC, bestLeader, kind, at = c, leader, k, i
			}
		}
		for i, it := range pk.musts {
			if it.leader >= 0 {
				consider(sum.sub(it.vc).add(it.lc), it.leader, 0, i)
			}
		}
		for j := range voters[:t] {
			if it := &voters[j].it; it.leader >= 0 {
				consider(sum.sub(it.vc).add(it.lc), it.leader, 1, j)
			}
		}
		if t > 0 {
			prev := sum.sub(voters[t-1].c)
			for j := range leaders {
				x := &leaders[j]
				if !slices.ContainsFunc(voters[:t-1], func(y ranked) bool { return y.it.voter == x.it.voter }) {
					consider(prev.add(x.c), x.s, 2, j)
					break
				}
			}
		}
		if bestLeader < 0 {
			continue
		}
		out := slices.Clone(sel)
		switch kind {
		case 1:
			out[m0+at] = bestLeader
		case 2:
			out[n-1] = bestLeader
		}
		res[1][n] = choice{ok: true, v: value{c: bestC}, sel: out, leader: bestLeader}
	}
}

// partition groups stores by their location value at level, in value order.
// At the rule's first differing level the precomputed domain index replaces
// sorting; stores keep their ascending order inside a domain. The result
// lives in per-level scratch: a recursion only goes deeper, and sibling
// domains are handled one after another.
func (p *planner) partition(r *Rule, stores []int, level int) [][]int {
	sc := &p.parts[level]
	sorted := append(sc.sorted[:0], stores...)
	if level == r.level && r.domainOf != nil {
		start := slices.Grow(sc.start[:0], r.domains+1)[:r.domains+1]
		clear(start)
		for _, s := range stores {
			start[r.domainOf[s]+1]++
		}
		for d := range r.domains {
			start[d+1] += start[d]
		}
		for _, s := range stores {
			d := r.domainOf[s]
			sorted[start[d]] = s
			start[d]++
		}
		sc.start = start
	} else {
		slices.SortFunc(sorted, func(a, b int) int {
			if c := cmp.Compare(r.Location[a][level], r.Location[b][level]); c != 0 {
				return c
			}
			return cmp.Compare(a, b)
		})
	}
	out := sc.out[:0]
	for i := 0; i < len(sorted); {
		j := i + 1
		for j < len(sorted) && r.Location[sorted[j]][level] == r.Location[sorted[i]][level] {
			j++
		}
		out = append(out, sorted[i:j:j])
		i = j
	}
	sc.sorted, sc.out = sorted, out
	return out
}

// partScratch is the reusable storage of one partition level.
type partScratch struct {
	sorted, start []int
	out           [][]int
}

// spread reports whether no two must stores share a domain at level.
func spread(r *Rule, must []int, level int) bool {
	for i, a := range must {
		for _, b := range must[i+1:] {
			if r.Location[a][level] == r.Location[b][level] {
				return false
			}
		}
	}
	return true
}

func sameValue(r *Rule, stores []int, level int) bool {
	for _, s := range stores[min(1, len(stores)):] {
		if r.Location[s][level] != r.Location[stores[0]][level] {
			return false
		}
	}
	return true
}

// pairsOf computes the isolation vector of a peer set.
func pairsOf(r *Rule, stores []int) [maxDepth]int32 {
	var out [maxDepth]int32
	for i, a := range stores {
		for _, b := range stores[i+1:] {
			for lv := range r.depth() {
				if r.Location[a][lv] != r.Location[b][lv] {
					break
				}
				out[lv]++
			}
		}
	}
	return out
}

func pairCount(k int) int32 { return int32(k * (k - 1) / 2) }
