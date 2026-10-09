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

package placement

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func TestRoleCheckerOverlappingRules(t *testing.T) {
	stores := core.NewBasicCluster()
	for i, zone := range []string{"A", "B", "B", "A"} {
		stores.PutStore(core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": zone}))
	}
	peers := []*metapb.Peer{{StoreId: 1}, {StoreId: 2}, {StoreId: 3}}
	rules := []*Rule{{Role: Leader, Count: 1}, {Role: Voter, Count: 2, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"B"}}}}}
	checker := NewRoleChecker(stores, peers, rules, false)
	require.True(t, checker.IsSatisfied(1))
	require.False(t, checker.IsSatisfied(2))
	require.False(t, checker.IsSatisfied(0))
	require.False(t, checker.IsSatisfied(4))
	// Backtracking must free a peer initially assigned to the broad rule.
	rules = []*Rule{{Role: Voter, Count: 2}, {Role: Follower, Count: 1, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"A"}}}}}
	require.True(t, NewRoleChecker(stores, peers, rules, false).IsSatisfied(2))
	require.False(t, NewRoleChecker(stores, peers, rules, false).IsSatisfied(1))
	// All validation data is local; unallocated/duplicate peer IDs are harmless.
	for _, peer := range peers {
		require.Zero(t, peer.Id)
	}
	require.False(t, NewRoleChecker(stores, append(peers, peers[0]), rules, false).IsSatisfied(1))
	require.False(t, NewRoleChecker(stores, peers, []*Rule{{Role: Voter, Count: 4}}, false).IsSatisfied(1))
	// Rebuild after labels change. Existing checker snapshots retain their input.
	stores.PutStore(core.NewStoreInfoWithLabel(2, map[string]string{"zone": "A"}))
	require.True(t, checker.IsSatisfied(1))
	rules = []*Rule{{Role: Leader, Count: 1}, {Role: Voter, Count: 2, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"B"}}}}}
	require.False(t, NewRoleChecker(stores, peers, rules, false).IsSatisfied(1))
}

// Enumerate assignments as an independent oracle, including impossible layouts.
func TestRoleCheckerAgainstExhaustiveAssignments(t *testing.T) {
	rng := rand.New(rand.NewPCG(11283, 2026))
	roles := []PeerRoleType{Leader, Follower, Voter, Learner}
	for range 300 {
		count := 3 + rng.IntN(4)
		stores := core.NewBasicCluster()
		peers := make([]*metapb.Peer, count)
		for i := range count {
			stores.PutStore(core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": []string{"A", "B"}[rng.IntN(2)]}))
			peers[i] = &metapb.Peer{StoreId: uint64(i + 1), Role: metapb.PeerRole(rng.IntN(2)), IsWitness: rng.IntN(4) == 0}
		}
		var rules []*Rule
		remaining := count
		for remaining > 0 {
			n := 1 + rng.IntN(remaining)
			rule := &Rule{Role: roles[rng.IntN(len(roles))], Count: n, IsWitness: rng.IntN(4) == 0}
			if rng.IntN(2) == 0 {
				rule.LabelConstraints = []LabelConstraint{{Key: "zone", Op: In, Values: []string{[]string{"A", "B"}[rng.IntN(2)]}}}
			}
			rules = append(rules, rule)
			remaining -= n
		}
		for _, supportWitness := range []bool{false, true} {
			checker := NewRoleChecker(stores, peers, rules, supportWitness)
			for _, leader := range peers {
				var slots []*Rule
				for _, r := range rules {
					for range r.Count {
						slots = append(slots, r)
					}
				}
				used := make([]bool, count)
				var fill func(int) bool
				fill = func(slot int) bool {
					if slot == count {
						return true
					}
					r := slots[slot]
					for i, peer := range peers {
						if used[i] || !MatchLabelConstraints(stores.GetStore(peer.StoreId), r.LabelConstraints) {
							continue
						}
						isLeader := peer.StoreId == leader.StoreId
						roleOK := false
						switch r.Role {
						case Leader:
							roleOK = isLeader && !core.IsLearner(peer)
						case Follower:
							roleOK = !isLeader && !core.IsLearner(peer)
						case Voter:
							roleOK = !core.IsLearner(peer)
						case Learner:
							roleOK = core.IsLearner(peer)
						}
						witnessOK := !peer.IsWitness
						if supportWitness {
							witnessOK = peer.IsWitness == r.IsWitness
						}
						if !roleOK || !witnessOK {
							continue
						}
						used[i] = true
						if fill(slot + 1) {
							return true
						}
						used[i] = false
					}
					return false
				}
				expected := !core.IsLearner(leader) && !leader.IsWitness && fill(0)
				require.Equal(t, expected, checker.IsSatisfied(leader.StoreId))
			}
		}
	}
}
