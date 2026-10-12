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
	"context"
	"errors"
	"slices"
)

const maxManualBalancePairChecks = 100_000

// BalanceActionRebuilder builds legal actions for a virtual target using the
// original group as the placement reference. A false result means the table
// is not ready to plan.
type BalanceActionRebuilder func(context.Context, *Group, *Group) (BalanceActionSet, bool)

// PlanTableRebalance repeatedly applies strict table-wide balance steps and
// then considers coordinated pairs at a single-step local minimum. Pair
// planning is bounded to two groups and a fixed search budget; it never
// enumerates complete store assignments. Changed flags compare final targets
// with the original targets.
func PlanTableRebalance(ctx context.Context, groups []*Group, options BalanceOptions, rebuild BalanceActionRebuilder) ([]BalancePlan, error) {
	initialPlans, err := PlanBalance(groups, options)
	if err != nil {
		return nil, err
	}
	if len(initialPlans) == 0 {
		return initialPlans, nil
	}

	original := make(map[string]*Group, len(groups))
	current := make([]*Group, 0, len(groups))
	for _, group := range groups {
		copy := cloneBalanceGroup(group)
		original[copy.ID] = cloneBalanceGroup(group)
		current = append(current, copy)
	}
	slices.SortFunc(current, func(a, b *Group) int { return cmp.Compare(a.ID, b.ID) })
	actions := make(map[string]BalanceActionSet, len(options.ActionSets))
	for id, actionSet := range options.ActionSets {
		actions[id] = actionSet
	}
	globalLeaders := cloneBalanceCounts(options.GlobalLeaderCounts)
	globalVoters := cloneBalanceCounts(options.GlobalVoterCounts)
	pairChecksLeft := maxManualBalancePairChecks

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stepPlans, err := PlanBalance(current, BalanceOptions{
			Stores: options.Stores, ActionSets: actions,
			GlobalLeaderCounts: globalLeaders, GlobalVoterCounts: globalVoters,
		})
		if err != nil {
			return nil, err
		}
		var changed *BalancePlan
		for i := range stepPlans {
			if stepPlans[i].Changed {
				changed = &stepPlans[i]
				break
			}
		}
		if changed != nil {
			group := findVirtualGroup(current, changed.GroupID)
			if group == nil {
				return nil, nil
			}
			updateGlobalBalanceCounts(globalLeaders, globalVoters, group, changed.LeaderStoreID, changed.VoterStoreIDs)
			group = updateVirtualGroup(current, changed.GroupID, changed.LeaderStoreID, changed.VoterStoreIDs)
			ready, err := rebuildVirtualActions(ctx, original[changed.GroupID], group, rebuild, actions)
			if err != nil {
				return nil, err
			}
			if !ready {
				return nil, ctx.Err()
			}
			continue
		}

		pair, found, checks, err := findImprovingPair(ctx, current, options.Stores, actions,
			globalLeaders, globalVoters, pairChecksLeft)
		if err != nil {
			return nil, err
		}
		pairChecksLeft -= checks
		if !found {
			return finalBalancePlans(original, current), nil
		}
		first := findVirtualGroup(current, pair.first.GroupID)
		second := findVirtualGroup(current, pair.second.GroupID)
		if first == nil || second == nil {
			return nil, nil
		}
		updateGlobalBalanceCounts(globalLeaders, globalVoters, first, pair.first.target.leader, pair.first.target.voters)
		updateGlobalBalanceCounts(globalLeaders, globalVoters, second, pair.second.target.leader, pair.second.target.voters)
		first = updateVirtualGroup(current, pair.first.GroupID, pair.first.target.leader, pair.first.target.voters)
		second = updateVirtualGroup(current, pair.second.GroupID, pair.second.target.leader, pair.second.target.voters)
		ready, err := rebuildVirtualActions(ctx, original[pair.first.GroupID], first, rebuild, actions)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, ctx.Err()
		}
		ready, err = rebuildVirtualActions(ctx, original[pair.second.GroupID], second, rebuild, actions)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, ctx.Err()
		}
	}
}

func cloneBalanceGroup(group *Group) *Group {
	copy := *group
	copy.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
	return &copy
}

func updateVirtualGroup(groups []*Group, groupID string, leader uint64, voters []uint64) *Group {
	for _, group := range groups {
		if group.ID == groupID {
			group.LeaderStoreID = leader
			group.VoterStoreIDs = slices.Clone(voters)
			return group
		}
	}
	return nil
}

func findVirtualGroup(groups []*Group, groupID string) *Group {
	for _, group := range groups {
		if group.ID == groupID {
			return group
		}
	}
	return nil
}

func cloneBalanceCounts(counts map[uint64]int) map[uint64]int {
	if counts == nil {
		return nil
	}
	cloned := make(map[uint64]int, len(counts))
	for store, count := range counts {
		cloned[store] = count
	}
	return cloned
}

func updateGlobalBalanceCounts(leaders, voters map[uint64]int, group *Group, newLeader uint64, newVoters []uint64) {
	if leaders != nil && group.LeaderStoreID != newLeader {
		if group.LeaderStoreID != 0 {
			leaders[group.LeaderStoreID]--
		}
		if newLeader != 0 {
			leaders[newLeader]++
		}
	}
	if voters == nil {
		return
	}
	oldVoters := make(map[uint64]int, len(group.VoterStoreIDs))
	newVoterCounts := make(map[uint64]int, len(newVoters))
	for _, store := range group.VoterStoreIDs {
		oldVoters[store]++
	}
	for _, store := range newVoters {
		newVoterCounts[store]++
	}
	for store, count := range oldVoters {
		voters[store] += newVoterCounts[store] - count
	}
	for store, count := range newVoterCounts {
		if _, existed := oldVoters[store]; !existed {
			voters[store] += count
		}
	}
}

func rebuildVirtualActions(ctx context.Context, original, virtual *Group, rebuild BalanceActionRebuilder, actions map[string]BalanceActionSet) (bool, error) {
	if rebuild == nil {
		return false, errors.New("balance action rebuilder is required after changing a virtual target")
	}
	updated, ready := rebuild(ctx, cloneBalanceGroup(original), cloneBalanceGroup(virtual))
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !ready {
		return false, nil
	}
	actions[virtual.ID] = updated
	return true, nil
}

func finalBalancePlans(original map[string]*Group, current []*Group) []BalancePlan {
	plans := make([]BalancePlan, len(current))
	for i, group := range current {
		old := original[group.ID]
		voters := normalizeVoters(group.VoterStoreIDs)
		plans[i] = makePlan(group, group.LeaderStoreID, voters,
			group.LeaderStoreID != old.LeaderStoreID || !slices.Equal(voters, normalizeVoters(old.VoterStoreIDs)))
	}
	return plans
}

type manualBalanceAction struct {
	GroupID string
	target
	moveBytes uint64
}

type manualBalancePair struct {
	first, second manualBalanceAction
	score         balanceScore
	globalScore   balanceScore
	moveBytes     uint64
	preserves     int
}

func findImprovingPair(ctx context.Context, groups []*Group, stores []uint64, actionSets map[string]BalanceActionSet,
	globalLeaders, globalVoters map[uint64]int, maxChecks int) (manualBalancePair, bool, int, error) {
	if maxChecks <= 0 {
		return manualBalancePair{}, false, 0, nil
	}
	leaders, voters := countsForGroups(groups)
	baseScore := calcBalanceScore(leaders, voters)
	baseGlobalScore := balanceScore{leader: sumSquares(globalLeaders), voter: sumSquares(globalVoters)}
	candidates := make([][]manualBalanceAction, len(groups))
	candidateGroups := make([]int, 0, len(groups))
	for i, group := range groups {
		if group.BalancePolicy == BalancePolicyFixed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return manualBalancePair{}, false, 0, err
		}
		candidates[i] = manualBalanceActions(group, actionSets[group.ID], stores, leaders)
		if len(candidates[i]) > 0 {
			candidateGroups = append(candidateGroups, i)
		}
	}
	var best manualBalancePair
	found := false
	checks := 0
	for a := 0; a < len(candidateGroups); a++ {
		i := candidateGroups[a]
		for b := a + 1; b < len(candidateGroups); b++ {
			j := candidateGroups[b]
			for _, first := range candidates[i] {
				for _, second := range candidates[j] {
					if checks >= maxChecks {
						return best, found, checks, nil
					}
					checks++
					if checks&1023 == 0 {
						if err := ctx.Err(); err != nil {
							return manualBalancePair{}, false, checks, err
						}
					}
					score := scoreAfterPair(baseScore, leaders, voters, groups[i], first, groups[j], second)
					if compareScore(score, baseScore) >= 0 {
						continue
					}
					globalScore := scoreAfterGlobalPair(baseGlobalScore, globalLeaders, globalVoters,
						groups[i], first, groups[j], second)
					preserves := 0
					if first.target.leader == groups[i].LeaderStoreID {
						preserves++
					}
					if second.target.leader == groups[j].LeaderStoreID {
						preserves++
					}
					pair := manualBalancePair{
						first: first, second: second,
						score: score, globalScore: globalScore,
						moveBytes: first.moveBytes + second.moveBytes, preserves: preserves,
					}
					if !found || manualPairLess(pair, best) {
						best, found = pair, true
					}
				}
			}
		}
	}
	return best, found, checks, nil
}

func manualBalanceActions(group *Group, actions BalanceActionSet, stores []uint64, leaderCounts map[uint64]int) []manualBalanceAction {
	voters := normalizeVoters(group.VoterStoreIDs)
	votersSet := make(map[uint64]struct{}, len(voters))
	available := make(map[uint64]struct{}, len(stores))
	for _, store := range voters {
		votersSet[store] = struct{}{}
	}
	for _, store := range stores {
		available[store] = struct{}{}
	}
	seen := make(map[struct{ leader, from, to uint64 }]struct{})
	result := make([]manualBalanceAction, 0)
	add := func(leader, from, to uint64, targetVoters []uint64, moveBytes uint64) {
		if leader != group.LeaderStoreID && 2*(leaderCounts[leader]-leaderCounts[group.LeaderStoreID]+1) != 0 {
			return
		}
		key := struct{ leader, from, to uint64 }{leader, from, to}
		if _, exists := seen[key]; exists ||
			(leader == group.LeaderStoreID && from == 0 && to == 0) {
			return
		}
		seen[key] = struct{}{}
		result = append(result, manualBalanceAction{
			GroupID:   group.ID,
			target:    target{leader: leader, voters: slices.Clone(targetVoters), from: from, to: to},
			moveBytes: moveBytes,
		})
	}
	for _, leader := range actions.LeaderTargets {
		if _, valid := votersSet[leader]; valid {
			add(leader, 0, 0, voters, 0)
		}
	}
	for _, from := range voters {
		targets := actions.VoterReplacements[from]
		toStores := make([]uint64, 0, len(targets))
		for to := range targets {
			toStores = append(toStores, to)
		}
		slices.Sort(toStores)
		for _, to := range toStores {
			if _, valid := available[to]; !valid || to == 0 {
				continue
			}
			if _, alreadyVoter := votersSet[to]; alreadyVoter {
				continue
			}
			newVoters := replaceVoter(voters, from, to)
			leaders := slices.Clone(targets[to])
			slices.Sort(leaders)
			for _, leader := range leaders {
				_, staysVoter := votersSet[leader]
				if leader != to && (leader == from || !staysVoter) {
					continue
				}
				add(leader, from, to, newVoters, actions.MoveBytes)
			}
		}
	}
	slices.SortFunc(result, func(a, b manualBalanceAction) int {
		if order := cmp.Compare(a.target.leader, b.target.leader); order != 0 {
			return order
		}
		if order := cmp.Compare(a.target.to, b.target.to); order != 0 {
			return order
		}
		return cmp.Compare(a.target.from, b.target.from)
	})
	return result
}

func countsForGroups(groups []*Group) (map[uint64]int, map[uint64]int) {
	leaders, voters := make(map[uint64]int), make(map[uint64]int)
	for _, group := range groups {
		if group.LeaderStoreID != 0 {
			leaders[group.LeaderStoreID]++
		}
		for _, store := range group.VoterStoreIDs {
			voters[store]++
		}
	}
	return leaders, voters
}

func scoreAfterPair(base balanceScore, leaders, voters map[uint64]int, firstGroup *Group, first manualBalanceAction, secondGroup *Group, second manualBalanceAction) balanceScore {
	leaderDeltas, voterDeltas := make(map[uint64]int, 4), make(map[uint64]int, 4)
	addManualActionDeltas(leaderDeltas, voterDeltas, firstGroup, first)
	addManualActionDeltas(leaderDeltas, voterDeltas, secondGroup, second)
	base.leader = scoreWithDeltas(base.leader, leaders, leaderDeltas)
	base.voter = scoreWithDeltas(base.voter, voters, voterDeltas)
	return base
}

func addManualActionDeltas(leaderDeltas, voterDeltas map[uint64]int, group *Group, action manualBalanceAction) {
	if group.LeaderStoreID != action.target.leader {
		if group.LeaderStoreID != 0 {
			leaderDeltas[group.LeaderStoreID]--
		}
		if action.target.leader != 0 {
			leaderDeltas[action.target.leader]++
		}
	}
	if action.target.from != 0 {
		voterDeltas[action.target.from]--
		voterDeltas[action.target.to]++
	}
}

func scoreAfterGlobalPair(base balanceScore, leaders, voters map[uint64]int,
	firstGroup *Group, first manualBalanceAction, secondGroup *Group, second manualBalanceAction) balanceScore {
	leaderDeltas, voterDeltas := make(map[uint64]int, 4), make(map[uint64]int, 4)
	addManualActionDeltas(leaderDeltas, voterDeltas, firstGroup, first)
	addManualActionDeltas(leaderDeltas, voterDeltas, secondGroup, second)
	if leaders != nil {
		base.leader = scoreWithDeltas(base.leader, leaders, leaderDeltas)
	}
	if voters != nil {
		base.voter = scoreWithDeltas(base.voter, voters, voterDeltas)
	}
	return base
}

func scoreWithDeltas(base int, counts, deltas map[uint64]int) int {
	for store, delta := range deltas {
		count := counts[store]
		base += 2*count*delta + delta*delta
	}
	return base
}

func manualPairLess(a, b manualBalancePair) bool {
	if order := compareScore(a.score, b.score); order != 0 {
		return order < 0
	}
	if order := compareScore(a.globalScore, b.globalScore); order != 0 {
		return order < 0
	}
	if a.moveBytes != b.moveBytes {
		return a.moveBytes < b.moveBytes
	}
	if a.preserves != b.preserves {
		return a.preserves > b.preserves
	}
	if targetLess(a.first.GroupID, a.first.target, b.first.GroupID, b.first.target) {
		return true
	}
	if targetLess(b.first.GroupID, b.first.target, a.first.GroupID, a.first.target) {
		return false
	}
	return targetLess(a.second.GroupID, a.second.target, b.second.GroupID, b.second.target)
}
