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
		// Remove this group's load once; candidate scores only add its new target.
		applyCounts(leaderCounts, voterCounts, oldLeader, oldVoters, 0, nil)
		baseScore := calcBalanceScore(leaderCounts, voterCounts)
		var best *targetCandidate
		hasValidCandidate := false
		evaluate := func(candidate target) {
			if options.ValidTarget != nil && !options.ValidTarget(group, candidate.leader, candidate.voters) {
				return
			}
			hasValidCandidate = true
			score := scoreAfter(baseScore, leaderCounts, voterCounts, candidate)
			if oldTargetValid && len(oldVoters) > 0 && compareScore(score, oldScore) >= 0 {
				return
			}
			if best == nil || compareScore(score, best.score) < 0 || (compareScore(score, best.score) == 0 && targetLess(candidate, best.target)) {
				best = &targetCandidate{target: target{leader: candidate.leader, voters: slices.Clone(candidate.voters)}, score: score}
			}
		}
		visitCandidateTargets(stores, oldVoters, options.ReplicaCount, voterCounts, false, evaluate)
		if options.ValidTarget != nil && !hasValidCandidate {
			visitCandidateTargets(stores, oldVoters, options.ReplicaCount, voterCounts, true, evaluate)
		}
		if best == nil {
			if !oldTargetValid {
				return nil, fmt.Errorf("unable to find valid balance target for group %q", group.ID)
			}
			applyCounts(leaderCounts, voterCounts, 0, nil, oldLeader, oldVoters)
			plans = append(plans, makePlan(group, oldLeader, oldVoters, false))
			continue
		}
		applyCounts(leaderCounts, voterCounts, 0, nil, best.leader, best.voters)
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

// visitCandidateTargets streams targets through a reused voter buffer. Healthy
// groups keep their voters or replace one; new and unavailable groups share
// the same selection path, retaining every surviving voter.
func visitCandidateTargets(stores, oldVoters []uint64, replicaCount int, voterCounts map[uint64]int, exhaustive bool, visit func(target)) {
	voters := make([]uint64, replicaCount)
	emit := func(selected []uint64) {
		copy(voters, selected)
		slices.Sort(voters)
		for _, leader := range voters {
			visit(target{leader: leader, voters: voters})
		}
	}
	available := make([]uint64, 0, replicaCount)
	for _, store := range oldVoters {
		if slices.Contains(stores, store) {
			available = append(available, store)
		}
	}
	if exhaustive && len(available) == replicaCount {
		forEachVoterCombination(stores, replicaCount, emit)
		return
	}
	if !exhaustive && len(available) == replicaCount {
		emit(oldVoters)
		for i := range oldVoters {
			for _, store := range stores {
				if slices.Contains(oldVoters, store) {
					continue
				}
				copy(voters, oldVoters)
				voters[i] = store
				emit(voters)
			}
		}
		return
	}
	candidates := make([]uint64, 0, len(stores)-len(available))
	for _, store := range stores {
		if !slices.Contains(available, store) {
			candidates = append(candidates, store)
		}
	}
	if exhaustive {
		forEachVoterCombination(candidates, replicaCount-len(available), func(replacement []uint64) {
			copy(voters, available)
			copy(voters[len(available):], replacement)
			emit(voters)
		})
		return
	}
	slices.SortFunc(candidates, func(a, b uint64) int {
		if order := cmp.Compare(voterCounts[a], voterCounts[b]); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
	// For each possible leader, fill missing voters from the least loaded stores.
	// This avoids combination enumeration unless ValidTarget rejects every set.
	for _, leader := range stores {
		copy(voters, available)
		n := len(available)
		if !slices.Contains(available, leader) {
			voters[n] = leader
			n++
		}
		for _, store := range candidates {
			if n == replicaCount {
				break
			}
			if store != leader {
				voters[n] = store
				n++
			}
		}
		emit(voters)
	}
}

// forEachVoterCombination visits each size-element combination without
// materializing all combinations at once.
func forEachVoterCombination(values []uint64, size int, visit func([]uint64)) {
	if size < 0 || size > len(values) {
		return
	}
	chosen := make([]uint64, size)
	var walk func(int, int)
	walk = func(start, depth int) {
		if depth == size {
			visit(chosen)
			return
		}
		for i := start; i <= len(values)-(size-depth); i++ {
			chosen[depth] = values[i]
			walk(i+1, depth+1)
		}
	}
	walk(0, 0)
}

func containsAll(values, required []uint64) bool {
	for _, value := range required {
		if !slices.Contains(values, value) {
			return false
		}
	}
	return true
}

func scoreAfter(base balanceScore, leaders, voters map[uint64]int, candidate target) balanceScore {
	// Adding one assignment changes a squared count by (n+1)^2-n^2 = 2n+1.
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
func targetLess(a, b target) bool {
	if a.leader != b.leader {
		return a.leader < b.leader
	}
	return slices.Compare(a.voters, b.voters) < 0
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
