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
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

// RoleChecker checks whether every peer can occupy a distinct rule slot.
// It snapshots label eligibility and roles, and does not optimize isolation.
// A checker can be reused for different leaders of the same planned membership.
// Peer IDs are deliberately ignored: planned peers may not have IDs yet.
// IsSatisfied uses private scratch space and is safe for concurrent calls.
type RoleChecker struct {
	peers    []rolePeer
	slots    []PeerRoleType
	eligible [][]int
	valid    bool
}

type rolePeer struct {
	storeID uint64
	canLead bool
}

// NewRoleChecker prepares a complete-role check against the supplied effective
// rules. Callers must rebuild it when membership, rules or store labels change.
func NewRoleChecker(stores StoreSet, peers []*metapb.Peer, rules []*Rule, supportWitness bool) *RoleChecker {
	c := &RoleChecker{}
	count := 0
	for _, rule := range rules {
		if rule == nil || rule.Count <= 0 || rule.Count > len(peers)-count {
			return c
		}
		count += rule.Count
	}
	if count == 0 || count != len(peers) {
		return c
	}
	c.slots = make([]PeerRoleType, 0, count)
	for _, rule := range rules {
		for range rule.Count {
			c.slots = append(c.slots, rule.Role)
		}
	}
	c.peers = make([]rolePeer, len(peers))
	c.eligible = make([][]int, len(peers))
	seen := make(map[uint64]struct{}, len(peers))
	for i, peer := range peers {
		if peer == nil || peer.GetStoreId() == 0 {
			return c
		}
		if _, ok := seen[peer.GetStoreId()]; ok {
			return c
		}
		seen[peer.GetStoreId()] = struct{}{}
		store := stores.GetStore(peer.GetStoreId())
		if store == nil {
			return c
		}
		c.peers[i] = rolePeer{storeID: peer.GetStoreId(), canLead: !core.IsLearner(peer) && peer.GetRole() != metapb.PeerRole_DemotingVoter && !peer.GetIsWitness()}
		offset := 0
		for _, rule := range rules {
			roleOK := core.IsLearner(peer) == (rule.Role == Learner)
			witnessOK := (!supportWitness && !peer.GetIsWitness()) || (supportWitness && peer.GetIsWitness() == rule.IsWitness)
			if roleOK && witnessOK && MatchLabelConstraints(store, rule.LabelConstraints) {
				for j := range rule.Count {
					c.eligible[i] = append(c.eligible[i], offset+j)
				}
			}
			offset += rule.Count
		}
	}
	c.valid = true
	return c
}

// IsSatisfied reports whether leaderStoreID and the other peers can fill all
// rule slots. Augmenting paths allow peers to move between overlapping rules.
// This takes O(peers^3) time, without enumerating peer subsets for isolation.
func (c *RoleChecker) IsSatisfied(leaderStoreID uint64) bool {
	if !c.valid {
		return false
	}
	leader := -1
	for i, peer := range c.peers {
		if peer.storeID == leaderStoreID && peer.canLead {
			leader = i
			break
		}
	}
	if leader < 0 {
		return false
	}
	owners := make([]int, len(c.slots))
	for i := range owners {
		owners[i] = -1
	}
	visited := make([]bool, len(c.slots))
	var assign func(int) bool
	assign = func(peer int) bool {
		for _, slot := range c.eligible[peer] {
			role := c.slots[slot]
			if visited[slot] || (role == Leader && peer != leader) || (role == Follower && peer == leader) {
				continue
			}
			visited[slot] = true
			if owners[slot] < 0 || assign(owners[slot]) {
				owners[slot] = peer
				return true
			}
		}
		return false
	}
	for i := range c.peers {
		clear(visited)
		if !assign(i) {
			return false
		}
	}
	return true
}
