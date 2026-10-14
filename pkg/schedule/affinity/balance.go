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

package affinity

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

// BalancePolicy controls whether an affinity group may be balanced.
type BalancePolicy string

const (
	// BalancePolicyAuto allows the group's placement to be improved.
	BalancePolicyAuto BalancePolicy = "auto"
	// BalancePolicyFixed keeps the group's placement unchanged.
	BalancePolicyFixed BalancePolicy = "fixed"
)

// BalanceScore measures the leader and voter distribution across affinity groups.
type BalanceScore struct {
	Leader int
	Voter  int
}

// Compare orders scores by leader balance first, then voter balance.
func (s BalanceScore) Compare(other BalanceScore) int {
	if order := cmp.Compare(s.Leader, other.Leader); order != 0 {
		return order
	}
	return cmp.Compare(s.Voter, other.Voter)
}

// BalanceMove describes a leader transfer or a single voter replacement.
// A leader transfer has both FromStoreID and ToStoreID set to zero.
type BalanceMove struct {
	GroupID       string
	LeaderStoreID uint64
	FromStoreID   uint64
	ToStoreID     uint64
}

type balanceGroup struct {
	leaderStoreID uint64
	voters        map[uint64]struct{}
	fixed         bool
}

// BalanceScorer scores placements and evaluates individual candidate moves.
type BalanceScorer struct {
	groups       map[string]balanceGroup
	leaderCounts map[uint64]int
	voterCounts  map[uint64]int
	score        BalanceScore
}

// NewBalanceScorer validates groups and initializes their balance counts.
func NewBalanceScorer(groups []*Group) (*BalanceScorer, error) {
	scorer := &BalanceScorer{
		groups:       make(map[string]balanceGroup, len(groups)),
		leaderCounts: make(map[uint64]int),
		voterCounts:  make(map[uint64]int),
	}
	for _, group := range groups {
		if group == nil {
			return nil, errors.New("nil affinity group")
		}
		if _, exists := scorer.groups[group.ID]; exists {
			return nil, fmt.Errorf("duplicate affinity group ID %q", group.ID)
		}
		if group.BalancePolicy != "" && group.BalancePolicy != BalancePolicyAuto && group.BalancePolicy != BalancePolicyFixed {
			return nil, fmt.Errorf("group %q has invalid balance policy %q", group.ID, group.BalancePolicy)
		}

		if group.LeaderStoreID == 0 && len(group.VoterStoreIDs) == 0 {
			scorer.groups[group.ID] = balanceGroup{fixed: group.BalancePolicy == BalancePolicyFixed}
			continue
		}
		if group.LeaderStoreID == 0 || len(group.VoterStoreIDs) == 0 {
			return nil, fmt.Errorf("group %q must have both leader and voter stores", group.ID)
		}

		voters := make(map[uint64]struct{}, len(group.VoterStoreIDs))
		for _, storeID := range group.VoterStoreIDs {
			if storeID == 0 {
				return nil, fmt.Errorf("group %q has invalid voter store ID 0", group.ID)
			}
			if _, exists := voters[storeID]; exists {
				return nil, fmt.Errorf("group %q has duplicate voter store ID", group.ID)
			}
			voters[storeID] = struct{}{}
			scorer.voterCounts[storeID]++
		}
		if _, exists := voters[group.LeaderStoreID]; !exists {
			return nil, fmt.Errorf("group %q leader store ID is not a voter", group.ID)
		}
		scorer.leaderCounts[group.LeaderStoreID]++
		scorer.groups[group.ID] = balanceGroup{
			leaderStoreID: group.LeaderStoreID,
			voters:        voters,
			fixed:         group.BalancePolicy == BalancePolicyFixed,
		}
	}
	scorer.score = BalanceScore{
		Leader: sumSquares(scorer.leaderCounts),
		Voter:  sumSquares(scorer.voterCounts),
	}
	return scorer, nil
}

// Score returns the current score.
func (s *BalanceScorer) Score() BalanceScore {
	return s.score
}

// Evaluate returns the score after move when its shape is valid. The boolean
// is true only when the move is valid and strictly improves the current score.
func (s *BalanceScorer) Evaluate(move BalanceMove) (BalanceScore, bool) {
	group, exists := s.groups[move.GroupID]
	if !exists || group.fixed || len(group.voters) == 0 || move.LeaderStoreID == 0 {
		return s.score, false
	}

	next := s.score
	if move.FromStoreID == 0 && move.ToStoreID == 0 {
		if move.LeaderStoreID == group.leaderStoreID {
			return s.score, false
		}
		if _, isVoter := group.voters[move.LeaderStoreID]; !isVoter {
			return s.score, false
		}
	} else {
		if move.FromStoreID == 0 || move.ToStoreID == 0 {
			return s.score, false
		}
		if _, isVoter := group.voters[move.FromStoreID]; !isVoter {
			return s.score, false
		}
		if _, isVoter := group.voters[move.ToStoreID]; isVoter {
			return s.score, false
		}
		if move.LeaderStoreID != move.ToStoreID {
			if move.LeaderStoreID == move.FromStoreID {
				return s.score, false
			}
			if _, remainsVoter := group.voters[move.LeaderStoreID]; !remainsVoter {
				return s.score, false
			}
		}
		next.Voter += countMoveDelta(s.voterCounts, move.FromStoreID, move.ToStoreID)
	}

	if move.LeaderStoreID != group.leaderStoreID {
		next.Leader += countMoveDelta(s.leaderCounts, group.leaderStoreID, move.LeaderStoreID)
	}
	return next, next.Compare(s.score) < 0
}

// LeaderCount returns the number of groups led by storeID.
func (s *BalanceScorer) LeaderCount(storeID uint64) int {
	return s.leaderCounts[storeID]
}

// VoterCount returns the number of voter placements on storeID.
func (s *BalanceScorer) VoterCount(storeID uint64) int {
	return s.voterCounts[storeID]
}

func countMoveDelta(counts map[uint64]int, from, to uint64) int {
	fromCount, toCount := counts[from], counts[to]
	return 2 * (toCount - fromCount + 1)
}

func sumSquares(counts map[uint64]int) int {
	total := 0
	for _, count := range counts {
		total += count * count
	}
	return total
}

func normalizeVoters(voters []uint64) []uint64 {
	result := slices.Clone(voters)
	slices.Sort(result)
	return result
}
