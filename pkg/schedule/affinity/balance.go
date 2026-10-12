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

// BalancePlan is a target change produced by PlanBalance. It does not write
// storage or create an operator.
type BalancePlan struct {
	GroupID       string
	LeaderStoreID uint64
	VoterStoreIDs []uint64
	Changed       bool
}

// BalanceOptions supplies topology-independent inputs. ValidTarget is the
// integration point for placement rules and store state.
type BalanceOptions struct {
	Stores       []uint64
	ReplicaCount int
	ValidTarget  func(group *Group, leader uint64, voters []uint64) bool
}

// PlanBalance computes one deterministic pass over groups belonging to one
// table. Existing groups may transfer a leader or replace one voter. A group
// with no voters is initialized with ReplicaCount voters. Fixed groups remain
// unchanged but contribute to the balance counters.
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
	for _, group := range groups {
		if group == nil {
			return nil, errors.New("nil affinity group")
		}
		if group.BalancePolicy != "" && group.BalancePolicy != BalancePolicyAuto && group.BalancePolicy != BalancePolicyFixed {
			return nil, fmt.Errorf("group %q has invalid balance policy %q", group.ID, group.BalancePolicy)
		}
	}
	ordered := slices.Clone(groups)
	slices.SortFunc(ordered, func(a, b *Group) int { return stringsCompare(a.ID, b.ID) })
	leaderCounts, voterCounts := makeCounts(stores)
	for _, group := range ordered {
		if group.LeaderStoreID != 0 {
			leaderCounts[group.LeaderStoreID]++
		}
		for _, store := range group.VoterStoreIDs {
			voterCounts[store]++
		}
	}
	plans := make([]BalancePlan, 0, len(ordered))
	for _, group := range ordered {
		oldVoters := normalizeVoters(group.VoterStoreIDs)
		if hasDuplicate(oldVoters) {
			return nil, fmt.Errorf("group %q has duplicate voter store ID", group.ID)
		}
		if group.BalancePolicy == BalancePolicyFixed {
			plans = append(plans, makePlan(group, group.LeaderStoreID, oldVoters, false))
			continue
		}
		if len(oldVoters) > 0 && len(oldVoters) != options.ReplicaCount {
			return nil, fmt.Errorf("group %q has %d voters, want %d", group.ID, len(oldVoters), options.ReplicaCount)
		}
		oldLeader, oldScore := group.LeaderStoreID, calcBalanceScore(leaderCounts, voterCounts)
		oldTargetValid := oldLeader != 0 && slices.Contains(stores, oldLeader) && slices.Contains(oldVoters, oldLeader) && containsAll(stores, oldVoters)
		if oldTargetValid && options.ValidTarget != nil {
			oldTargetValid = options.ValidTarget(group, oldLeader, oldVoters)
		}
		var best *targetCandidate
		hasValidCandidate := false
		evaluate := func(candidate target) bool {
			if options.ValidTarget != nil && !options.ValidTarget(group, candidate.leader, candidate.voters) {
				return false
			}
			hasValidCandidate = true
			score := scoreAfter(leaderCounts, voterCounts, oldLeader, oldVoters, candidate.leader, candidate.voters)
			if oldTargetValid && len(oldVoters) > 0 && compareScore(score, oldScore) >= 0 {
				return false
			}
			if best == nil || compareScore(score, best.score) < 0 || (compareScore(score, best.score) == 0 && targetLess(candidate, best.target)) {
				best = &targetCandidate{target: target{leader: candidate.leader, voters: slices.Clone(candidate.voters)}, score: score}
			}
			return false
		}
		visitPreferredCandidateTargets(stores, oldVoters, options.ReplicaCount, voterCounts, evaluate)
		if options.ValidTarget != nil && !hasValidCandidate {
			visitAllCandidateTargets(stores, oldVoters, options.ReplicaCount, evaluate)
		}
		if best == nil {
			if !oldTargetValid {
				return nil, fmt.Errorf("unable to find valid balance target for group %q", group.ID)
			}
			plans = append(plans, makePlan(group, oldLeader, oldVoters, false))
			continue
		}
		applyCounts(leaderCounts, voterCounts, oldLeader, oldVoters, best.leader, best.voters)
		plans = append(plans, makePlan(group, best.leader, best.voters, best.leader != oldLeader || !slices.Equal(best.voters, oldVoters)))
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

func visitPreferredCandidateTargets(stores, oldVoters []uint64, replicaCount int, voterCounts map[uint64]int, visit func(target) bool) {
	if len(oldVoters) == 0 {
		for _, candidate := range initialCandidateTargets(stores, replicaCount, voterCounts) {
			if visit(candidate) {
				return
			}
		}
		return
	}
	if containsAll(stores, oldVoters) {
		for _, leader := range oldVoters {
			if visit(target{leader: leader, voters: oldVoters}) {
				return
			}
		}
		for i := range oldVoters {
			for _, store := range stores {
				if slices.Contains(oldVoters, store) {
					continue
				}
				voters := slices.Clone(oldVoters)
				voters[i] = store
				slices.Sort(voters)
				for _, leader := range voters {
					if visit(target{leader: leader, voters: voters}) {
						return
					}
				}
			}
		}
		return
	}
	visitPreferredReplacementTargets(stores, oldVoters, replicaCount, voterCounts, visit)
}

func visitAllCandidateTargets(stores, oldVoters []uint64, replicaCount int, visit func(target) bool) {
	if len(oldVoters) == 0 {
		forEachCombination(stores, replicaCount, func(voters []uint64) bool {
			for _, leader := range voters {
				if visit(target{leader: leader, voters: voters}) {
					return true
				}
			}
			return false
		})
		return
	}
	if containsAll(stores, oldVoters) {
		visitPreferredCandidateTargets(stores, oldVoters, replicaCount, nil, visit)
		return
	}
	available := make([]uint64, 0, len(oldVoters))
	for _, voter := range oldVoters {
		if slices.Contains(stores, voter) {
			available = append(available, voter)
		}
	}
	missing := replicaCount - len(available)
	candidates := make([]uint64, 0, len(stores)-len(available))
	for _, store := range stores {
		if !slices.Contains(available, store) {
			candidates = append(candidates, store)
		}
	}
	voters := make([]uint64, replicaCount)
	forEachCombination(candidates, missing, func(replacement []uint64) bool {
		copy(voters, available)
		copy(voters[len(available):], replacement)
		slices.Sort(voters)
		for _, leader := range voters {
			if visit(target{leader: leader, voters: voters}) {
				return true
			}
		}
		return false
	})
}

func visitPreferredReplacementTargets(stores, oldVoters []uint64, replicaCount int, voterCounts map[uint64]int, visit func(target) bool) {
	available := make([]uint64, 0, len(oldVoters))
	for _, voter := range oldVoters {
		if slices.Contains(stores, voter) {
			available = append(available, voter)
		}
	}
	candidates := make([]uint64, 0, len(stores)-len(available))
	for _, store := range stores {
		if !slices.Contains(available, store) {
			candidates = append(candidates, store)
		}
	}
	slices.SortFunc(candidates, func(a, b uint64) int {
		if voterCounts[a] != voterCounts[b] {
			return voterCounts[a] - voterCounts[b]
		}
		return compareUint64(a, b)
	})
	for _, leader := range stores {
		voters := make([]uint64, 0, replicaCount)
		voters = append(voters, available...)
		if !slices.Contains(available, leader) {
			voters = append(voters, leader)
		}
		for _, store := range candidates {
			if store == leader {
				continue
			}
			if len(voters) == replicaCount {
				break
			}
			voters = append(voters, store)
		}
		if len(voters) != replicaCount {
			continue
		}
		slices.Sort(voters)
		for _, candidateLeader := range voters {
			if visit(target{leader: candidateLeader, voters: voters}) {
				return
			}
		}
	}
}

func initialCandidateTargets(stores []uint64, replicaCount int, voterCounts map[uint64]int) []target {
	byVoterCount := slices.Clone(stores)
	slices.SortFunc(byVoterCount, func(a, b uint64) int {
		if voterCounts[a] < voterCounts[b] {
			return -1
		}
		if voterCounts[a] > voterCounts[b] {
			return 1
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	result := make([]target, 0, len(stores))
	for _, leader := range stores {
		voters := make([]uint64, 0, replicaCount)
		voters = append(voters, leader)
		for _, store := range byVoterCount {
			if len(voters) == replicaCount {
				break
			}
			if store == leader {
				continue
			}
			voters = append(voters, store)
			if len(voters) == replicaCount {
				break
			}
		}
		slices.Sort(voters)
		result = append(result, target{leader: leader, voters: voters})
	}
	return result
}

func forEachCombination(values []uint64, size int, visit func([]uint64) bool) bool {
	if size < 0 || size > len(values) {
		return false
	}
	chosen := make([]uint64, size)
	var walk func(int, int) bool
	walk = func(start, depth int) bool {
		if depth == size {
			return visit(chosen)
		}
		for i := start; i <= len(values)-(size-depth); i++ {
			chosen[depth] = values[i]
			if walk(i+1, depth+1) {
				return true
			}
		}
		return false
	}
	return walk(0, 0)
}

func containsAll(values, required []uint64) bool {
	for _, value := range required {
		if !slices.Contains(values, value) {
			return false
		}
	}
	return true
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func scoreAfter(leaders, voters map[uint64]int, oldLeader uint64, oldVoters []uint64, newLeader uint64, newVoters []uint64) balanceScore {
	l, v := cloneCounts(leaders), cloneCounts(voters)
	applyCounts(l, v, oldLeader, oldVoters, newLeader, newVoters)
	return balanceScore{sumSquares(l), sumSquares(v)}
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
	if a.leader != b.leader {
		if a.leader < b.leader {
			return -1
		}
		return 1
	}
	if a.voter < b.voter {
		return -1
	}
	if a.voter > b.voter {
		return 1
	}
	return 0
}
func targetLess(a, b target) bool {
	if a.leader != b.leader {
		return a.leader < b.leader
	}
	for i := range a.voters {
		if a.voters[i] != b.voters[i] {
			return a.voters[i] < b.voters[i]
		}
	}
	return false
}
func makePlan(group *Group, leader uint64, voters []uint64, changed bool) BalancePlan {
	return BalancePlan{group.ID, leader, slices.Clone(voters), changed}
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
	return len(slices.Compact(slices.Clone(values))) != len(values)
}
func makeCounts(stores []uint64) (leaders, voters map[uint64]int) {
	l, v := make(map[uint64]int, len(stores)), make(map[uint64]int, len(stores))
	for _, store := range stores {
		l[store], v[store] = 0, 0
	}
	return l, v
}
func cloneCounts(src map[uint64]int) map[uint64]int {
	dst := make(map[uint64]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func sumSquares(counts map[uint64]int) int {
	total := 0
	for _, count := range counts {
		total += count * count
	}
	return total
}
func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
