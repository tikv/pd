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

// BalancePolicy controls the improvement behavior of an affinity group.
type BalancePolicy string

const (
	// BalancePolicyAuto allows the planner to improve the group's placement.
	BalancePolicyAuto BalancePolicy = "auto"
	// BalancePolicyFixed keeps the group's placement unchanged.
	BalancePolicyFixed BalancePolicy = "fixed"
)

// BalanceActionSet contains actions already validated by the affinity checker.
// The planner only selects from these actions and never evaluates placement
// rules itself.
type BalanceActionSet struct {
	// LeaderTargets contains stores that can become leader while voters stay
	// unchanged.
	LeaderTargets []uint64
	// VoterReplacements maps source voter to destination store and the leaders
	// allowed after that replacement.
	VoterReplacements map[uint64]map[uint64][]uint64
	// MoveBytes estimates the bytes moved by replacing one voter for the
	// whole group. It breaks table-score ties before global affinity counts.
	MoveBytes uint64
}

// BalancePlan is a target change produced by PlanBalance. It does not write
// storage or create an operator.
type BalancePlan struct {
	GroupID       string
	LeaderStoreID uint64
	VoterStoreIDs []uint64
	Changed       bool
}

// BalanceOptions supplies topology-independent inputs and checker-generated
// legal actions. A missing action set means that the group has no eligible
// balance action in this pass.
type BalanceOptions struct {
	Stores     []uint64
	ActionSets map[string]BalanceActionSet
	// GlobalLeaderCounts and GlobalVoterCounts contain affinity counts before
	// this pass. They only break ties after a strict table-score improvement
	// and an equal MoveBytes estimate.
	GlobalLeaderCounts map[uint64]int
	GlobalVoterCounts  map[uint64]int
}

// PlanBalance computes one deterministic pass over groups belonging to one
// table. It returns all group targets for callers that need a preview, but at
// most one returned plan has Changed set. Fixed groups remain unchanged and
// contribute to the balance counters.
func PlanBalance(groups []*Group, options BalanceOptions) ([]BalancePlan, error) {
	stores := sortedUnique(options.Stores)
	if len(stores) == 0 {
		return nil, errors.New("no stores available for affinity balance")
	}
	if stores[0] == 0 {
		return nil, errors.New("store ID must be positive")
	}

	seenIDs := make(map[string]struct{}, len(groups))
	ordered := slices.Clone(groups)
	for _, group := range ordered {
		if group == nil {
			return nil, errors.New("nil affinity group")
		}
		if group.BalancePolicy != "" && group.BalancePolicy != BalancePolicyAuto && group.BalancePolicy != BalancePolicyFixed {
			return nil, fmt.Errorf("group %q has invalid balance policy %q", group.ID, group.BalancePolicy)
		}
		if _, exists := seenIDs[group.ID]; exists {
			return nil, fmt.Errorf("duplicate affinity group ID %q", group.ID)
		}
		seenIDs[group.ID] = struct{}{}
	}
	slices.SortFunc(ordered, func(a, b *Group) int { return cmp.Compare(a.ID, b.ID) })

	leaderCounts, voterCounts := makeCounts(stores)
	for _, group := range ordered {
		if group.LeaderStoreID != 0 {
			leaderCounts[group.LeaderStoreID]++
		}
		for _, store := range group.VoterStoreIDs {
			voterCounts[store]++
		}
	}
	baseScore := calcBalanceScore(leaderCounts, voterCounts)
	globalBaseScore := balanceScore{
		leader: sumSquares(options.GlobalLeaderCounts),
		voter:  sumSquares(options.GlobalVoterCounts),
	}
	available := make(map[uint64]struct{}, len(stores))
	for _, store := range stores {
		available[store] = struct{}{}
	}
	plans := make([]BalancePlan, len(ordered))
	for i, group := range ordered {
		voters := normalizeVoters(group.VoterStoreIDs)
		if hasDuplicate(voters) {
			return nil, fmt.Errorf("group %q has duplicate voter store ID", group.ID)
		}
		plans[i] = makePlan(group, group.LeaderStoreID, voters, false)
	}
	var best *targetCandidate
	var bestGroup string
	// Search the whole table for a leader transfer before considering any
	// voter movement. Scores use constant-time deltas from the current load.
	for _, replace := range []bool{false, true} {
		for i, group := range ordered {
			oldVoters := plans[i].VoterStoreIDs
			if group.BalancePolicy == BalancePolicyFixed || len(oldVoters) == 0 ||
				!slices.Contains(oldVoters, group.LeaderStoreID) {
				continue
			}
			actions := options.ActionSets[group.ID]
			votersSet := make(map[uint64]struct{}, len(oldVoters))
			for _, store := range oldVoters {
				votersSet[store] = struct{}{}
			}
			evaluate := func(leader, from, to uint64) {
				score := baseScore
				if leader != group.LeaderStoreID {
					score.leader += 2 * (leaderCounts[leader] - leaderCounts[group.LeaderStoreID] + 1)
				}
				if replace {
					score.voter += 2 * (voterCounts[to] - voterCounts[from] + 1)
				}
				if compareScore(score, baseScore) >= 0 {
					return
				}
				candidate := target{leader: leader, voters: oldVoters, from: from, to: to}
				globalScore := scoreAfterGlobalMove(globalBaseScore, options.GlobalLeaderCounts, options.GlobalVoterCounts,
					group.LeaderStoreID, leader, from, to)
				moveBytes := uint64(0)
				if replace {
					moveBytes = actions.MoveBytes
				}
				preservesLeader := leader == group.LeaderStoreID
				order := 0
				if best != nil {
					order = compareScore(score, best.score)
					if order == 0 {
						order = cmp.Compare(moveBytes, best.moveBytes)
					}
					if order == 0 {
						order = compareScore(globalScore, best.globalScore)
					}
					if order == 0 && preservesLeader != best.preservesLeader {
						if preservesLeader {
							order = -1
						} else {
							order = 1
						}
					}
				}
				if best == nil || order < 0 || (order == 0 && targetLess(group.ID, candidate, bestGroup, best.target)) {
					best = &targetCandidate{
						target: candidate, score: score, globalScore: globalScore,
						moveBytes: moveBytes, preservesLeader: preservesLeader,
					}
					bestGroup = group.ID
				}
			}
			if !replace {
				for _, leader := range actions.LeaderTargets {
					if _, exists := votersSet[leader]; exists {
						evaluate(leader, 0, 0)
					}
				}
				continue
			}
			for _, from := range oldVoters {
				for to, leaders := range actions.VoterReplacements[from] {
					_, exists := available[to]
					_, alreadyVoter := votersSet[to]
					if !exists || to == 0 || alreadyVoter {
						continue
					}
					for _, leader := range leaders {
						_, staysVoter := votersSet[leader]
						if leader == to || (leader != from && staysVoter) {
							evaluate(leader, from, to)
						}
					}
				}
			}
		}
		if best != nil {
			break
		}
	}

	if best != nil {
		voters := best.voters
		if best.from != 0 {
			voters = replaceVoter(voters, best.from, best.to)
		}
		for i := range plans {
			if plans[i].GroupID != bestGroup {
				continue
			}
			plans[i] = BalancePlan{
				GroupID:       bestGroup,
				LeaderStoreID: best.leader,
				VoterStoreIDs: slices.Clone(voters),
				Changed:       true,
			}
			break
		}
	}
	return plans, nil
}

type target struct {
	leader   uint64
	voters   []uint64
	from, to uint64
}

type targetCandidate struct {
	target
	score           balanceScore
	globalScore     balanceScore
	moveBytes       uint64
	preservesLeader bool
}

type balanceScore struct{ leader, voter int }

func replaceVoter(voters []uint64, from, to uint64) []uint64 {
	result := slices.Clone(voters)
	for i, voter := range result {
		if voter == from {
			result[i] = to
			break
		}
	}
	slices.Sort(result)
	return result
}

func calcBalanceScore(leaders, voters map[uint64]int) balanceScore {
	return balanceScore{sumSquares(leaders), sumSquares(voters)}
}

func scoreAfterGlobalMove(base balanceScore, leaders, voters map[uint64]int, oldLeader, newLeader, from, to uint64) balanceScore {
	score := base
	if leaders != nil {
		score.leader += countMoveDelta(leaders, oldLeader, newLeader)
	}
	if voters != nil {
		score.voter += countMoveDelta(voters, from, to)
	}
	return score
}

func countMoveDelta(counts map[uint64]int, from, to uint64) int {
	if from == to {
		return 0
	}
	delta := 0
	if from != 0 {
		count := counts[from]
		delta += (count-1)*(count-1) - count*count
	}
	if to != 0 {
		count := counts[to]
		delta += (count+1)*(count+1) - count*count
	}
	return delta
}

func compareScore(a, b balanceScore) int {
	if order := cmp.Compare(a.leader, b.leader); order != 0 {
		return order
	}
	return cmp.Compare(a.voter, b.voter)
}

func targetLess(groupA string, a target, groupB string, b target) bool {
	if groupA != groupB {
		return groupA < groupB
	}
	if a.leader != b.leader {
		return a.leader < b.leader
	}
	if a.to != b.to {
		return a.to < b.to
	}
	return a.from < b.from
}

func makePlan(group *Group, leader uint64, voters []uint64, changed bool) BalancePlan {
	return BalancePlan{GroupID: group.ID, LeaderStoreID: leader, VoterStoreIDs: slices.Clone(voters), Changed: changed}
}

func normalizeVoters(voters []uint64) []uint64 {
	result := slices.Clone(voters)
	slices.Sort(result)
	return result
}

func sortedUnique(stores []uint64) []uint64 {
	result := normalizeVoters(stores)
	return slices.Compact(result)
}

func hasDuplicate(values []uint64) bool {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return true
		}
	}
	return false
}

func makeCounts(stores []uint64) (leaders, voters map[uint64]int) {
	l, v := make(map[uint64]int, len(stores)), make(map[uint64]int, len(stores))
	for _, store := range stores {
		l[store], v[store] = 0, 0
	}
	return l, v
}

func sumSquares(counts map[uint64]int) int {
	total := 0
	for _, count := range counts {
		total += count * count
	}
	return total
}
