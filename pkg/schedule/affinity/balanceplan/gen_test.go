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

// Generators of planner inputs shared by the tests and benchmarks.

// broad builds n groups over 3 zones of perzone stores, all eligible. With
// concentrated set every group starts on the first store of each zone;
// otherwise targets are round-robin and already balanced.
func broad(n, perzone int, leaderStores []int, concentrated bool) *Input {
	stores := 3 * perzone
	loc := make([][]int32, stores)
	for s := range loc {
		loc[s] = []int32{int32(s / perzone)}
	}
	all := make([]int, stores)
	for s := range all {
		all[s] = s
	}
	r := &Rule{Role: RoleVoter, Count: 3, Stores: all, Location: loc}
	can := make([]bool, stores)
	if leaderStores == nil {
		for s := range can {
			can[s] = true
		}
	}
	for _, s := range leaderStores {
		can[s] = true
	}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	for i := range n {
		v := []int{0, perzone, 2 * perzone}
		if !concentrated {
			v = []int{i % perzone, perzone + (i/3)%perzone, 2*perzone + (i/9)%perzone}
		}
		leader := v[0]
		for _, s := range v {
			if can[s] {
				leader = s
				break
			}
		}
		if !can[leader] {
			// Leaders must start on a store that may lead.
			v[0] = leaderStores[0]
			leader = v[0]
		}
		in.Groups = append(in.Groups, Group{Rules: []*Rule{r}, Voters: v, RuleOf: make([]int, 3), Leader: leader, CanLead: can})
	}
	return in
}

// bottleneck is the frozen model's heterogeneous case: 90% of groups may use
// only stores 0 and 1 in zone 0; the layout already attains the voter optimum.
func bottleneck(n, perzone int) *Input {
	stores := 3 * perzone
	loc := make([][]int32, stores)
	for s := range loc {
		loc[s] = []int32{int32(s / perzone)}
	}
	all := make([]int, stores)
	for s := range all {
		all[s] = s
	}
	limited := append([]int{0, 1}, all[perzone:]...)
	open := &Rule{Role: RoleVoter, Count: 3, Stores: all, Location: loc}
	narrow := &Rule{Role: RoleVoter, Count: 3, Stores: limited, Location: loc}
	can := make([]bool, stores)
	for s := range can {
		can[s] = true
	}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	restricted := n * 9 / 10
	for i := range n {
		r := open
		first := 2 + (i-restricted)%(perzone-2)
		if i < restricted {
			r, first = narrow, i%2
		}
		v := []int{first, perzone + i%perzone, 2*perzone + i%perzone}
		in.Groups = append(in.Groups, Group{Rules: []*Rule{r}, Voters: v, RuleOf: make([]int, 3), Leader: v[1+i%2], CanLead: can})
	}
	return in
}

// general builds groups of one rule with replicas voters over zones; store s
// is in zone s%zones. Every group starts on stores 0..replicas-1, a legal and
// fully concentrated layout. Fewer zones than replicas needs the general DP.
func general(groups, stores, replicas, zones int) *Input {
	loc := make([][]int32, stores)
	ids, lead := make([]int, stores), make([]bool, stores)
	for s := range stores {
		ids[s], lead[s], loc[s] = s, true, []int32{int32(s % zones)}
	}
	rule := &Rule{Role: RoleVoter, Count: replicas, Stores: ids, Location: loc}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	for range groups {
		v := make([]int, replicas)
		for i := range replicas {
			v[i] = i
		}
		in.Groups = append(in.Groups, Group{Rules: []*Rule{rule}, Voters: v, RuleOf: make([]int, replicas), Leader: 0, CanLead: lead})
	}
	return in
}

// hosts builds 3 replicas over 2 zones with host isolation inside a zone:
// zones of hostsPerZone hosts with 3 stores each.
func hosts(groups, hostsPerZone int) *Input {
	stores := 2 * hostsPerZone * 3
	loc := make([][]int32, stores)
	ids, lead := make([]int, stores), make([]bool, stores)
	for s := range stores {
		ids[s], lead[s], loc[s] = s, true, []int32{int32(s / (stores / 2)), int32(s / 3)}
	}
	rule := &Rule{Role: RoleVoter, Count: 3, Stores: ids, Location: loc}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	for range groups {
		v := []int{0, 3, stores / 2}
		in.Groups = append(in.Groups, Group{Rules: []*Rule{rule}, Voters: v, RuleOf: make([]int, 3), Leader: 0, CanLead: lead})
	}
	return in
}

// primary builds the placement-policy shape: one leader in zone 0 and two
// more voters anywhere (overlap) or only outside zone 0 (disjoint), with zone
// isolation for the voters.
func primary(groups, perZone int, overlap bool) *Input {
	stores := 3 * perZone
	loc := make([][]int32, stores)
	lead := make([]bool, stores)
	var zone0, others, all []int
	for s := range stores {
		loc[s], lead[s] = []int32{int32(s / perZone)}, true
		all = append(all, s)
		if s < perZone {
			zone0 = append(zone0, s)
		} else {
			others = append(others, s)
		}
	}
	leader := &Rule{Role: RoleLeader, Count: 1, Stores: zone0, Location: loc}
	rest := &Rule{Role: RoleFollower, Count: 2, Stores: others, Location: loc}
	if overlap {
		rest = &Rule{Role: RoleVoter, Count: 2, Stores: all, Location: loc}
	}
	in := &Input{Stores: stores, BaseVoters: make([]int, stores), BaseLeaders: make([]int, stores)}
	for range groups {
		in.Groups = append(in.Groups, Group{Rules: []*Rule{leader, rest}, Voters: []int{0, perZone, 2 * perZone},
			RuleOf: []int{0, 1, 1}, Leader: 0, CanLead: lead})
	}
	return in
}
