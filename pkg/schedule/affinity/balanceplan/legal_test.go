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

// legal checks a target against the abstract domain of the group. The planner
// produces legal targets by construction; tests use this as an assertion.
func (p *planner) legal(g *groupState, voters, rules []int, leader int) bool {
	if len(voters) != len(rules) || !slices.Contains(voters, leader) {
		return false
	}
	seen := map[int]bool{}
	for _, s := range voters {
		if seen[s] {
			return false
		}
		seen[s] = true
	}
	for _, f := range g.Forced {
		if !seen[f] {
			return false
		}
	}
	if g.LeaderPinned && leader != g.Leader {
		return false
	}
	for ri, r := range g.Rules {
		cands := p.candidates(nil, g, ri, nil)
		var chosen []int
		for k, s := range voters {
			if rules[k] == ri {
				if !slices.Contains(cands, s) {
					return false
				}
				chosen = append(chosen, s)
			}
		}
		if len(chosen) != r.Count {
			return false
		}
		if best, ok := p.isolation(g, ri); !ok || pairsOf(r, chosen) != best.pairs {
			return false
		}
	}
	k := slices.Index(voters, leader)
	if !p.leaderCapable(g, g.Rules[rules[k]], leader) {
		return false
	}
	for ri, r := range g.Rules {
		if r.Role == RoleLeader && rules[k] != ri {
			return false
		}
	}
	return true
}
