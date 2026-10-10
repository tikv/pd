// Copyright 2026 TiKV Project Authors.
// Licensed under the Apache License, Version 2.0.
package affinity

import (
	"errors"
	"fmt"
	"slices"
)

// BalancePolicy controls the improvement behavior of an affinity group.
type BalancePolicy string

const (
	BalancePolicyAuto  BalancePolicy = "auto"
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
		if group.BalancePolicy == BalancePolicyFixed {
			plans = append(plans, makePlan(group, group.LeaderStoreID, oldVoters, false))
			continue
		}
		if len(oldVoters) > 0 && len(oldVoters) != options.ReplicaCount {
			return nil, fmt.Errorf("group %q has %d voters, want %d", group.ID, len(oldVoters), options.ReplicaCount)
		}
		oldLeader, oldScore := group.LeaderStoreID, calcBalanceScore(leaderCounts, voterCounts)
		oldTargetValid := (oldLeader == 0 || slices.Contains(stores, oldLeader)) && containsAll(stores, oldVoters)
		var best *targetCandidate
		for _, candidate := range candidateTargets(stores, oldVoters, options.ReplicaCount) {
			if options.ValidTarget != nil && !options.ValidTarget(group, candidate.leader, candidate.voters) {
				continue
			}
			score := scoreAfter(leaderCounts, voterCounts, oldLeader, oldVoters, candidate.leader, candidate.voters)
			if oldTargetValid && len(oldVoters) > 0 && compareScore(score, oldScore) >= 0 {
				continue
			}
			if best == nil || compareScore(score, best.score) < 0 || (compareScore(score, best.score) == 0 && targetLess(candidate, best.target)) {
				best = &targetCandidate{target: candidate, score: score}
			}
		}
		if best == nil {
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

func candidateTargets(stores, oldVoters []uint64, replicaCount int) []target {
	var voterSets [][]uint64
	if len(oldVoters) == 0 {
		voterSets = combinations(stores, replicaCount)
	} else {
		// Keep the current set as a candidate only while every peer is on an
		// available store. If a store was removed, at least one replacement is
		// required before the target can be selected.
		if containsAll(stores, oldVoters) {
			voterSets = append(voterSets, slices.Clone(oldVoters))
		}
		for i := range oldVoters {
			for _, store := range stores {
				if slices.Contains(oldVoters, store) {
					continue
				}
				voters := slices.Clone(oldVoters)
				voters[i] = store
				slices.Sort(voters)
				if !containsAll(stores, voters) {
					continue
				}
				voterSets = append(voterSets, voters)
			}
		}
	}
	var result []target
	for _, voters := range voterSets {
		for _, leader := range voters {
			result = append(result, target{leader: leader, voters: slices.Clone(voters)})
		}
	}
	return result
}

func containsAll(values, required []uint64) bool {
	for _, value := range required {
		if !slices.Contains(values, value) {
			return false
		}
	}
	return true
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
func makeCounts(stores []uint64) (map[uint64]int, map[uint64]int) {
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
func combinations(values []uint64, size int) [][]uint64 {
	result := make([][]uint64, 0)
	var visit func(int, []uint64)
	visit = func(start int, chosen []uint64) {
		if len(chosen) == size {
			result = append(result, slices.Clone(chosen))
			return
		}
		for i := start; i < len(values); i++ {
			visit(i+1, append(chosen, values[i]))
		}
	}
	visit(0, nil)
	return result
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
