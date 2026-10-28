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

import "slices"

// fill adds units one at a time to the least loaded store, ties to the lower
// ID, which minimizes the sum of squares for a convex per-store cost.
func fill(loads, selected []int, stores []int, units int) bool {
	if units > 0 && len(stores) == 0 {
		return false
	}
	for range units {
		s := stores[0]
		for _, t := range stores[1:] {
			if loads[t] < loads[s] || loads[t] == loads[s] && t < s {
				s = t
			}
		}
		loads[s]++
		selected[s]++
	}
	return true
}

// lowerBound relaxes group identity. Any legal layout places every voter of
// a planned group on one of its candidate stores and leads every group from
// one of its voters on a store that may lead. Keeping only those facts, the
// fixed occupancy and, when every group needs exactly one voter in each of
// the same domains, the per-domain totals gives a valid lower bound for each
// score part separately.
//
// It is used only to stop the search early, and it is worth its size: with
// no bound every plan runs all perturbation attempts, so a 64-group table
// takes about 80 ms instead of 5 ms; keeping only the simple fill over all
// stores misses the leader-occupancy step, and a table whose leaders may use
// 3 of 12 stores runs to the budget (140 ms instead of 3 ms).
func (p *planner) lowerBound() Score {
	n := p.n
	loads := slices.Clone(p.baseV)
	fixedSel := make([]int, n)
	selected := make([]int, n)
	leaderStore := make([]bool, n)
	inUnion := make([]bool, n)
	var union []int
	units := 0
	uniform := len(p.gs) > 0
	var domainRule *Rule
	for i := range p.gs {
		g := &p.gs[i]
		for _, s := range g.Voters {
			if g.isForced(s) {
				loads[s]++
				fixedSel[s]++
			}
		}
		hosts := p.leaderHosts(g)
		for ri, r := range g.Rules {
			host := slices.Contains(hosts, ri)
			p.candBuf = p.candidates(p.candBuf, g, ri, nil)
			for _, s := range p.candBuf {
				if !inUnion[s] {
					inUnion[s] = true
					union = append(union, s)
				}
				// Only the rules that may hold the leader place it.
				if host && p.leaderCapable(g, r, s) && (!g.LeaderPinned || s == g.Leader) {
					leaderStore[s] = true
				}
			}
			units += r.Count
		}
		units -= len(g.Forced)
		if g.LeaderPinned && !slices.Contains(g.Forced, g.Leader) {
			units--
		}
		// For a single rule, candBuf still holds its candidates.
		if len(g.Rules) != 1 || g.Rules[0].leaf() || domainsUsed(g.Rules[0], p.candBuf) != g.Rules[0].Count {
			uniform = false
		} else if domainRule == nil {
			domainRule = g.Rules[0]
		} else if !sameDomains(domainRule, g.Rules[0]) {
			uniform = false
		}
	}
	// Per-domain totals hold only when every group spans the same domains.
	var domains [][]int
	if uniform {
		byDomain := map[int32][]int{}
		var ids []int32
		for _, s := range union {
			d := domainRule.domainOf[s]
			if _, ok := byDomain[d]; !ok {
				ids = append(ids, d)
			}
			byDomain[d] = append(byDomain[d], s)
		}
		if len(byDomain) != domainRule.Count {
			uniform = false
		}
		slices.Sort(ids)
		for _, d := range ids {
			domains = append(domains, byDomain[d])
		}
	}
	if uniform {
		for _, stores := range domains {
			need := len(p.gs)
			for _, s := range stores {
				need -= fixedSel[s]
			}
			if need < 0 || !fill(loads, selected, stores, need) {
				return Score{}
			}
		}
	} else if !fill(loads, selected, union, units) {
		return Score{}
	}
	// Every group leads from one of its voters on a store that may lead.
	occupied := 0
	for _, s := range union {
		if leaderStore[s] {
			occupied += selected[s] + fixedSel[s]
		}
	}
	scopes := [][]int{union}
	if uniform {
		scopes = domains
	}
	for occupied < len(p.gs) {
		bestCost, bestSrc, bestDst := 0, -1, -1
		for _, stores := range scopes {
			src, dst := -1, -1
			for _, s := range stores {
				if !leaderStore[s] && selected[s] > 0 && (src < 0 || loads[s] > loads[src] || loads[s] == loads[src] && s < src) {
					src = s
				}
				if leaderStore[s] && (dst < 0 || loads[s] < loads[dst] || loads[s] == loads[dst] && s < dst) {
					dst = s
				}
			}
			if src < 0 || dst < 0 {
				continue
			}
			if c := 2*(loads[dst]-loads[src]) + 2; bestSrc < 0 || c < bestCost {
				bestCost, bestSrc, bestDst = c, src, dst
			}
		}
		if bestSrc < 0 {
			break
		}
		loads[bestSrc]--
		selected[bestSrc]--
		loads[bestDst]++
		selected[bestDst]++
		occupied++
	}
	var out Score
	for _, x := range loads {
		out.Voter += int64(x) * int64(x)
	}
	leaders := slices.Clone(p.baseL)
	if len(leaders) < n {
		leaders = append(leaders, make([]int, n-len(leaders))...)
	}
	need := 0
	for i := range p.gs {
		if p.gs[i].LeaderPinned {
			leaders[p.gs[i].Leader]++
		} else {
			need++
		}
	}
	var leaderUnion []int
	for _, s := range union {
		if leaderStore[s] {
			leaderUnion = append(leaderUnion, s)
		}
	}
	if !fill(leaders, make([]int, n), leaderUnion, need) {
		return Score{Voter: out.Voter}
	}
	for _, x := range leaders {
		out.Leader += int64(x) * int64(x)
	}
	return out
}

// domainsUsed counts the domains of r that hold one of stores.
func domainsUsed(r *Rule, stores []int) int {
	seen := make([]bool, r.domains)
	n := 0
	for _, s := range stores {
		if !seen[r.domainOf[s]] {
			seen[r.domainOf[s]] = true
			n++
		}
	}
	return n
}

func sameDomains(a, b *Rule) bool {
	if a == b {
		return true
	}
	if a.level != b.level || len(a.domainOf) != len(b.domainOf) {
		return false
	}
	for s := range a.domainOf {
		if a.domainOf[s] != b.domainOf[s] {
			return false
		}
	}
	return true
}
