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

func TestRoleCheckerMatchesPeers(t *testing.T) {
	stores := core.NewBasicCluster()
	peers := make(map[uint64]*metapb.Peer)
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfoWithLabel(id, map[string]string{"zone": "A"}))
		peers[id] = &metapb.Peer{StoreId: id}
	}
	checker := NewRoleChecker(stores, []*metapb.Peer{peers[3], peers[1], peers[2]}, []*Rule{{Role: Voter, Count: 3}}, false)
	require.True(t, checker.MatchesPeers(stores, peers, false))
	peers[1].Id = 123
	require.True(t, checker.MatchesPeers(stores, peers, false))
	// Snapshot role values must not follow later mutations of the input peers.
	peers[1].Role = metapb.PeerRole_Learner
	require.False(t, checker.MatchesPeers(stores, peers, false))
	peers[1].Role = metapb.PeerRole_Voter
	peers[1].IsWitness = true
	require.False(t, checker.MatchesPeers(stores, peers, false))
	peers[1].IsWitness = false
	require.False(t, checker.MatchesPeers(stores, peers, true))
	peers[1].StoreId = 4
	require.False(t, checker.MatchesPeers(stores, peers, false))
	peers[1].StoreId = 1
	delete(peers, 3)
	require.False(t, checker.MatchesPeers(stores, peers, false))
	peers[3] = &metapb.Peer{StoreId: 3}
	require.True(t, checker.MatchesPeers(stores, peers, false))
	stores.PutStore(core.NewStoreInfoWithLabel(3, map[string]string{"zone": "B"}))
	require.False(t, checker.MatchesPeers(stores, peers, false))
	var absent *RoleChecker
	require.False(t, absent.MatchesPeers(stores, peers, false))
	require.False(t, (&RoleChecker{}).MatchesPeers(stores, peers, false))
}

// Splitting an identical voter rule forces the general matcher without changing
// the legal assignments. Cover all three-peer Raft-role and witness combinations.
func TestRoleCheckerSingleVoterEquivalent(t *testing.T) {
	stores := core.NewBasicCluster()
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfoWithLabel(id, map[string]string{"zone": "A"}))
	}
	for state := range 512 {
		peers := make([]*metapb.Peer, 3)
		for i := range peers {
			peers[i] = &metapb.Peer{StoreId: uint64(i + 1), Role: metapb.PeerRole((state >> (2 * i)) & 3), IsWitness: state&(1<<(6+i)) != 0}
		}
		for _, supportWitness := range []bool{false, true} {
			for _, ruleWitness := range []bool{false, true} {
				rule := &Rule{Role: Voter, Count: 3, IsWitness: ruleWitness}
				fast := NewRoleChecker(stores, peers, []*Rule{rule}, supportWitness)
				first, rest := rule.Clone(), rule.Clone()
				first.Count, rest.Count = 1, 2
				general := NewRoleChecker(stores, peers, []*Rule{first, rest}, supportWitness)
				for leader := range uint64(5) {
					expected := leader >= 1 && leader <= 3
					for _, peer := range peers {
						witnessOK := !peer.IsWitness
						if supportWitness {
							witnessOK = peer.IsWitness == ruleWitness
						}
						expected = expected && !core.IsLearner(peer) && witnessOK
						if peer.StoreId == leader {
							expected = expected && peer.Role != metapb.PeerRole_DemotingVoter && !peer.IsWitness
						}
					}
					require.Equal(t, expected, fast.IsSatisfied(leader), "state=%d, support=%v, witness=%v, leader=%d", state, supportWitness, ruleWitness, leader)
					require.Equal(t, expected, general.IsSatisfied(leader))
				}
			}
		}
	}
}

func TestRoleCheckerSingleVoterConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*core.BasicCluster, []*metapb.Peer, *Rule)
		valid  bool
	}{
		{name: "unallocated peer IDs", valid: true},
		{name: "duplicate peer IDs", change: func(_ *core.BasicCluster, peers []*metapb.Peer, _ *Rule) {
			for _, peer := range peers {
				peer.Id = 1
			}
		}, valid: true},
		{name: "nil peer", change: func(_ *core.BasicCluster, peers []*metapb.Peer, _ *Rule) { peers[1] = nil }},
		{name: "zero store ID", change: func(_ *core.BasicCluster, peers []*metapb.Peer, _ *Rule) { peers[1].StoreId = 0 }},
		{name: "duplicate store ID", change: func(_ *core.BasicCluster, peers []*metapb.Peer, _ *Rule) { peers[1].StoreId = 1 }},
		{name: "missing store", change: func(_ *core.BasicCluster, peers []*metapb.Peer, _ *Rule) { peers[1].StoreId = 4 }},
		{name: "too few slots", change: func(_ *core.BasicCluster, _ []*metapb.Peer, rule *Rule) { rule.Count = 2 }},
		{name: "too many slots", change: func(_ *core.BasicCluster, _ []*metapb.Peer, rule *Rule) { rule.Count = 4 }},
		{name: "zero slots", change: func(_ *core.BasicCluster, _ []*metapb.Peer, rule *Rule) { rule.Count = 0 }},
		{name: "negative slots", change: func(_ *core.BasicCluster, _ []*metapb.Peer, rule *Rule) { rule.Count = -1 }},
		{name: "wrong group", change: func(stores *core.BasicCluster, _ []*metapb.Peer, _ *Rule) {
			stores.PutStore(core.NewStoreInfoWithLabel(2, map[string]string{"$group": "B"}))
		}},
		{name: "missing group", change: func(stores *core.BasicCluster, _ []*metapb.Peer, _ *Rule) {
			stores.PutStore(core.NewStoreInfoWithLabel(2, nil))
		}},
		{name: "uncovered exclusive label", change: func(stores *core.BasicCluster, _ []*metapb.Peer, _ *Rule) {
			stores.PutStore(core.NewStoreInfoWithLabel(2, map[string]string{"$group": "A", "engine": "tiflash"}))
		}},
		{name: "missing exclusive constraint", change: func(_ *core.BasicCluster, _ []*metapb.Peer, rule *Rule) { rule.LabelConstraints = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stores := core.NewBasicCluster()
			peers := []*metapb.Peer{{StoreId: 1}, {StoreId: 2}, {StoreId: 3}}
			for _, peer := range peers {
				stores.PutStore(core.NewStoreInfoWithLabel(peer.StoreId, map[string]string{"$group": "A"}))
			}
			rule := &Rule{Role: Voter, Count: 3, LabelConstraints: []LabelConstraint{{Key: "$group", Op: In, Values: []string{"A"}}}}
			if tc.change != nil {
				tc.change(stores, peers, rule)
			}
			checker := NewRoleChecker(stores, peers, []*Rule{rule}, false)
			require.Equal(t, tc.valid, checker.IsSatisfied(1))
			require.False(t, checker.IsSatisfied(0))
		})
	}
}

func TestRoleCheckerEligibilityStorage(t *testing.T) {
	for _, count := range []int{3, 5, 9, 33} {
		stores := core.NewBasicCluster()
		peers := make([]*metapb.Peer, count)
		for i := range peers {
			id := uint64(i + 1)
			stores.PutStore(core.NewStoreInfoWithLabel(id, map[string]string{"zone": "A"}))
			peers[i] = &metapb.Peer{StoreId: id}
		}
		// Both dense overlap and sparse rows must survive backing-slice growth.
		for _, rules := range [][]*Rule{
			{{Role: Voter, Count: 1}, {Role: Voter, Count: count - 1}},
			{{Role: Leader, Count: 1, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"B"}}}}, {Role: Voter, Count: count - 1, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"A"}}}}},
		} {
			stores.PutStore(core.NewStoreInfoWithLabel(1, map[string]string{"zone": "B"}))
			checker := NewRoleChecker(stores, peers, rules, false)
			require.True(t, checker.IsSatisfied(1), "peers=%d", count)
			require.Equal(t, rules[0].Role == Voter, checker.IsSatisfied(2))
		}
	}
}
