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
	Stores       []uint64
	ReplicaCount int
	ActionSets   map[string]BalanceActionSet
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
	if options.ReplicaCount <= 0 {
		return nil, errors.New("replica count must be positive")
	}
	if options.ReplicaCount > len(stores) {
		return nil, fmt.Errorf("replica count %d exceeds available stores %d", options.ReplicaCount, len(stores))
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
	plans := make([]BalancePlan, 0, len(ordered))
	var best *targetCandidate
	var bestGroup string

	for _, group := range ordered {
		oldVoters := normalizeVoters(group.VoterStoreIDs)
		if hasDuplicate(oldVoters) {
			return nil, fmt.Errorf("group %q has duplicate voter store ID", group.ID)
		}
		plans = append(plans, makePlan(group, group.LeaderStoreID, oldVoters, false))
		if group.BalancePolicy == BalancePolicyFixed || len(oldVoters) != options.ReplicaCount {
			continue
		}
		actions, ok := options.ActionSets[group.ID]
		if !ok {
			continue
		}

		// Remove this group's current load before scoring its alternatives.
		applyCounts(leaderCounts, voterCounts, group.LeaderStoreID, oldVoters, 0, nil)
		candidateBase := calcBalanceScore(leaderCounts, voterCounts)
		evaluate := func(candidate target) {
			score := scoreAfter(candidateBase, leaderCounts, voterCounts, candidate)
			if compareScore(score, baseScore) >= 0 {
				return
			}
			candidate = target{leader: candidate.leader, voters: slices.Clone(candidate.voters)}
			if best == nil || compareScore(score, best.score) < 0 ||
				(compareScore(score, best.score) == 0 && targetLess(group.ID, candidate, bestGroup, best.target)) {
				best = &targetCandidate{target: candidate, score: score}
				bestGroup = group.ID
			}
		}
		for _, leader := range sortedUnique(actions.LeaderTargets) {
			if slices.Contains(oldVoters, leader) {
				evaluate(target{leader: leader, voters: oldVoters})
			}
		}
		for _, from := range oldVoters {
			for _, to := range sortedReplacementTargets(actions.VoterReplacements[from]) {
				if slices.Contains(oldVoters, to) || !slices.Contains(stores, to) {
					continue
				}
				voters := replaceVoter(oldVoters, from, to)
				for _, leader := range sortedUnique(actions.VoterReplacements[from][to]) {
					if slices.Contains(voters, leader) {
						evaluate(target{leader: leader, voters: voters})
					}
				}
			}
		}
		applyCounts(leaderCounts, voterCounts, 0, nil, group.LeaderStoreID, oldVoters)
	}

	if best != nil {
		for i := range plans {
			if plans[i].GroupID != bestGroup {
				continue
			}
			plans[i] = BalancePlan{
				GroupID:       bestGroup,
				LeaderStoreID: best.leader,
				VoterStoreIDs: slices.Clone(best.voters),
				Changed:       true,
			}
			break
		}
	}
	return plans, nil
}

type target struct {
	leader uint64
	voters []uint64
}

type targetCandidate struct {
	target
	score balanceScore
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

func sortedReplacementTargets(targets map[uint64][]uint64) []uint64 {
	result := make([]uint64, 0, len(targets))
	for target := range targets {
		result = append(result, target)
	}
	slices.Sort(result)
	return result
}

func scoreAfter(base balanceScore, leaders, voters map[uint64]int, candidate target) balanceScore {
	base.leader += 2*leaders[candidate.leader] + 1
	for _, store := range candidate.voters {
		base.voter += 2*voters[store] + 1
	}
	return base
}

func applyCounts(leaders, voters map[uint64]int, oldLeader uint64, oldVoters []uint64, newLeader uint64, newVoters []uint64) {
	if oldLeader != 0 {
		leaders[oldLeader]--
	}
	for _, store := range oldVoters {
		voters[store]--
	}
	if newLeader != 0 {
		leaders[newLeader]++
	}
	for _, store := range newVoters {
		voters[store]++
	}
}

func calcBalanceScore(leaders, voters map[uint64]int) balanceScore {
	return balanceScore{sumSquares(leaders), sumSquares(voters)}
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
	return slices.Compare(a.voters, b.voters) < 0
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
