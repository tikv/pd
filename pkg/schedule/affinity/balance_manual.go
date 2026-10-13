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

// BalanceActionRebuilder builds legal actions for a virtual target using the
// original group as the placement reference. A false result means the table
// is not ready to plan.
type BalanceActionRebuilder func(context.Context, *Group, *Group) (BalanceActionSet, bool)

// PlanTableRebalance repeatedly applies strict table-wide balance steps and
// then considers coordinated pairs at a single-step local minimum. Pair
// actions are generated lazily, so the planner does not retain all legal
// actions for every group at once. Changed flags compare final targets with
// the original targets.
func PlanTableRebalance(ctx context.Context, groups []*Group, options BalanceOptions, rebuild BalanceActionRebuilder) ([]BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	initialPlans, err := PlanBalance(groups, options)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
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
		if err := ctx.Err(); err != nil {
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

		pair, found, err := findImprovingPair(ctx, current, options.Stores, actions,
			globalLeaders, globalVoters)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !found {
			return finalBalancePlans(original, current), nil
		}
		first := findVirtualGroup(current, pair.first.GroupID)
		second := findVirtualGroup(current, pair.second.GroupID)
		if first == nil || second == nil {
			return nil, nil
		}
		updateGlobalBalanceCounts(globalLeaders, globalVoters, first, pair.first.leader, pair.first.voters)
		updateGlobalBalanceCounts(globalLeaders, globalVoters, second, pair.second.leader, pair.second.voters)
		first = updateVirtualGroup(current, pair.first.GroupID, pair.first.leader, pair.first.voters)
		second = updateVirtualGroup(current, pair.second.GroupID, pair.second.leader, pair.second.voters)
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
	globalLeaders, globalVoters map[uint64]int) (manualBalancePair, bool, error) {
	leaders, voters := countsForGroups(groups)
	baseScore := calcBalanceScore(leaders, voters)
	baseGlobalScore := balanceScore{leader: sumSquares(globalLeaders), voter: sumSquares(globalVoters)}
	actionGenerators := make([]manualBalanceActionGenerator, len(groups))
	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			return manualBalancePair{}, false, err
		}
		if group.BalancePolicy != BalancePolicyFixed {
			actionGenerators[i] = newManualBalanceActionGenerator(group, actionSets[group.ID], stores, leaders)
		}
	}
	var best manualBalancePair
	found := false
	for i, firstGroup := range groups {
		if firstGroup.BalancePolicy == BalancePolicyFixed {
			continue
		}
		for j := i + 1; j < len(groups); j++ {
			if err := ctx.Err(); err != nil {
				return manualBalancePair{}, false, err
			}
			secondGroup := groups[j]
			if secondGroup.BalancePolicy == BalancePolicyFixed {
				continue
			}
			var searchErr error
			actionGenerators[i].visit(func(first manualBalanceAction) bool {
				if searchErr != nil {
					return false
				}
				actionGenerators[j].visit(func(second manualBalanceAction) bool {
					if err := ctx.Err(); err != nil {
						searchErr = err
						return false
					}
					score := scoreAfterPair(baseScore, leaders, voters, firstGroup, first, secondGroup, second)
					if compareScore(score, baseScore) >= 0 {
						return true
					}
					globalScore := scoreAfterGlobalPair(baseGlobalScore, globalLeaders, globalVoters,
						firstGroup, first, secondGroup, second)
					preserves := 0
					if first.leader == firstGroup.LeaderStoreID {
						preserves++
					}
					if second.leader == secondGroup.LeaderStoreID {
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
					return true
				})
				return searchErr == nil
			})
			if searchErr != nil {
				return manualBalancePair{}, false, searchErr
			}
		}
	}
	return best, found, nil
}

type manualBalanceActionGenerator struct {
	group        *Group
	actions      BalanceActionSet
	voters       []uint64
	votersSet    map[uint64]struct{}
	stores       []uint64
	leaderCounts map[uint64]int
}

func newManualBalanceActionGenerator(group *Group, actions BalanceActionSet, stores []uint64,
	leaderCounts map[uint64]int) manualBalanceActionGenerator {
	voters := normalizeVoters(group.VoterStoreIDs)
	votersSet := make(map[uint64]struct{}, len(voters))
	for _, store := range voters {
		votersSet[store] = struct{}{}
	}
	return manualBalanceActionGenerator{
		group: group, actions: actions, voters: voters,
		votersSet: votersSet, stores: stores, leaderCounts: leaderCounts,
	}
}

func (g manualBalanceActionGenerator) visit(visit func(manualBalanceAction) bool) {
	group, actions := g.group, g.actions
	add := func(leader, from, to uint64, targetVoters []uint64, moveBytes uint64) bool {
		if leader != group.LeaderStoreID && 2*(g.leaderCounts[leader]-g.leaderCounts[group.LeaderStoreID]+1) != 0 {
			return true
		}
		if leader == group.LeaderStoreID && from == 0 && to == 0 {
			return true
		}
		return visit(manualBalanceAction{
			GroupID:   group.ID,
			target:    target{leader: leader, voters: slices.Clone(targetVoters), from: from, to: to},
			moveBytes: moveBytes,
		})
	}
	for _, leader := range actions.LeaderTargets {
		if _, valid := g.votersSet[leader]; valid {
			if !add(leader, 0, 0, g.voters, 0) {
				return
			}
		}
	}
	for _, from := range g.voters {
		targets := actions.VoterReplacements[from]
		toStores := make([]uint64, 0, len(targets))
		for to := range targets {
			toStores = append(toStores, to)
		}
		slices.Sort(toStores)
		for _, to := range toStores {
			if to == 0 || !slices.Contains(g.stores, to) {
				continue
			}
			if _, alreadyVoter := g.votersSet[to]; alreadyVoter {
				continue
			}
			newVoters := replaceVoter(g.voters, from, to)
			leaders := slices.Clone(targets[to])
			slices.Sort(leaders)
			for _, leader := range leaders {
				_, staysVoter := g.votersSet[leader]
				if leader != to && (leader == from || !staysVoter) {
					continue
				}
				if !add(leader, from, to, newVoters, actions.MoveBytes) {
					return
				}
			}
		}
	}
}

func countsForGroups(groups []*Group) (leaders map[uint64]int, voters map[uint64]int) {
	leaders, voters = make(map[uint64]int), make(map[uint64]int)
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
	if group.LeaderStoreID != action.leader {
		if group.LeaderStoreID != 0 {
			leaderDeltas[group.LeaderStoreID]--
		}
		if action.leader != 0 {
			leaderDeltas[action.leader]++
		}
	}
	if action.from != 0 {
		voterDeltas[action.from]--
		voterDeltas[action.to]++
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
