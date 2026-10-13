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

// PlanTableRebalance greedily assigns targets to auto groups in ID order.
// Fixed groups reserve their existing counts first. Each selected target is
// built only from checker-generated legal actions; virtual intermediate
// targets are not required to improve the score of the original table. The
// complete final table is published only when its score strictly improves.
func PlanTableRebalance(ctx context.Context, groups []*Group, options BalanceOptions, rebuild BalanceActionRebuilder) ([]BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := PlanBalance(groups, options); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stores := sortedUnique(options.Stores)
	original := make(map[string]*Group, len(groups))
	current := make([]*Group, 0, len(groups))
	for _, group := range groups {
		copy := cloneBalanceGroup(group)
		original[copy.ID] = cloneBalanceGroup(group)
		current = append(current, copy)
	}
	slices.SortFunc(current, func(a, b *Group) int { return cmp.Compare(a.ID, b.ID) })
	globalLeaders := cloneBalanceCounts(options.GlobalLeaderCounts)
	globalVoters := cloneBalanceCounts(options.GlobalVoterCounts)
	initialLeaders, initialVoters := countsForGroups(current)
	initialScore := calcBalanceScore(initialLeaders, initialVoters)
	leaders, voters := makeCounts(stores)
	for _, group := range current {
		if group.BalancePolicy == BalancePolicyFixed {
			addBalanceTarget(leaders, voters, group.LeaderStoreID, group.VoterStoreIDs)
		}
	}
	virtualStepBudget := min(max(256, len(current)*min(max(1, len(stores)), 64)), 1<<20)
	virtualSteps, actionChecks := 0, 0
	const maxManualBalanceActionChecks = 1 << 22

	for _, group := range current {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if group.BalancePolicy == BalancePolicyFixed {
			continue
		}
		old := original[group.ID]
		actions := options.ActionSets[group.ID]
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			best, err := bestManualBalanceAction(ctx, old, group, actions, stores, leaders, voters,
				globalLeaders, globalVoters, &actionChecks, maxManualBalanceActionChecks)
			if err != nil {
				return nil, err
			}
			if best == nil || sameBalanceTarget(group, best.target) {
				break
			}
			if virtualSteps >= virtualStepBudget {
				return nil, errors.New("manual balance virtual target budget exceeded")
			}
			group.LeaderStoreID = best.leader
			group.VoterStoreIDs = slices.Clone(best.voters)
			virtualSteps++
			updated, ready, err := rebuildVirtualActions(ctx, old, group, rebuild)
			if err != nil {
				return nil, err
			}
			if !ready {
				return nil, ErrBalanceNotReady
			}
			actions = updated
		}
		addBalanceTarget(leaders, voters, group.LeaderStoreID, group.VoterStoreIDs)
		updateGlobalBalanceCounts(globalLeaders, globalVoters, old, group.LeaderStoreID, group.VoterStoreIDs)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finalLeaders, finalVoters := countsForGroups(current)
	finalScore := calcBalanceScore(finalLeaders, finalVoters)
	if compareScore(finalScore, initialScore) >= 0 {
		for i := range current {
			current[i] = cloneBalanceGroup(original[current[i].ID])
		}
		return finalBalancePlans(original, current, initialScore, initialScore, options.ActionSets, false), nil
	}
	return finalBalancePlans(original, current, initialScore, finalScore, options.ActionSets, true), nil
}

func cloneBalanceGroup(group *Group) *Group {
	copy := *group
	copy.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
	return &copy
}

func sameBalanceTarget(group *Group, target target) bool {
	return group.LeaderStoreID == target.leader && slices.Equal(normalizeVoters(group.VoterStoreIDs), normalizeVoters(target.voters))
}

func addBalanceTarget(leaders, voters map[uint64]int, leader uint64, voterIDs []uint64) {
	if leader != 0 {
		leaders[leader]++
	}
	for _, storeID := range voterIDs {
		voters[storeID]++
	}
}

func bestManualBalanceAction(ctx context.Context, original, group *Group, actions BalanceActionSet,
	stores []uint64, leaders, voters, globalLeaders, globalVoters map[uint64]int,
	checks *int, maxChecks int) (*manualBalanceAction, error) {
	base := calcBalanceScore(leaders, voters)
	globalBase := balanceScore{leader: sumSquares(globalLeaders), voter: sumSquares(globalVoters)}
	var best *manualBalanceAction
	var bestScore, bestGlobal balanceScore
	var bestPreserves bool
	consider := func(candidate manualBalanceAction) bool {
		if err := ctx.Err(); err != nil {
			return false
		}
		(*checks)++
		if *checks > maxChecks {
			return false
		}
		score := scoreAfterAddTarget(base, leaders, voters, candidate)
		globalScore := scoreAfterAction(globalBase, globalLeaders, globalVoters, original, candidate)
		preserves := candidate.leader == original.LeaderStoreID
		if best == nil || manualActionLess(candidate, score, globalScore, preserves, *best, bestScore, bestGlobal, bestPreserves) {
			copy := candidate
			copy.voters = slices.Clone(candidate.voters)
			best = &copy
			bestScore, bestGlobal, bestPreserves = score, globalScore, preserves
		}
		return true
	}
	if !consider(manualBalanceAction{
		GroupID: group.ID,
		target:  target{leader: group.LeaderStoreID, voters: normalizeVoters(group.VoterStoreIDs)},
	}) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("manual balance action budget exceeded")
	}
	newManualBalanceActionGenerator(group, actions, stores).visit(consider)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if *checks > maxChecks {
		return nil, errors.New("manual balance action budget exceeded")
	}
	return best, nil
}

func scoreAfterAddTarget(base balanceScore, leaders, voters map[uint64]int, action manualBalanceAction) balanceScore {
	leaderDeltas, voterDeltas := make(map[uint64]int, 1), make(map[uint64]int, len(action.voters))
	if action.leader != 0 {
		leaderDeltas[action.leader]++
	}
	for _, storeID := range action.voters {
		voterDeltas[storeID]++
	}
	base.leader = scoreWithDeltas(base.leader, leaders, leaderDeltas)
	base.voter = scoreWithDeltas(base.voter, voters, voterDeltas)
	return base
}

func scoreAfterAction(base balanceScore, leaders, voters map[uint64]int, group *Group, action manualBalanceAction) balanceScore {
	leaderDeltas, voterDeltas := make(map[uint64]int, 2), make(map[uint64]int, 2)
	addManualActionDeltas(leaderDeltas, voterDeltas, group, action)
	if leaders != nil {
		base.leader = scoreWithDeltas(base.leader, leaders, leaderDeltas)
	}
	if voters != nil {
		base.voter = scoreWithDeltas(base.voter, voters, voterDeltas)
	}
	return base
}

func manualActionLess(candidate manualBalanceAction, score, globalScore balanceScore, preserves bool,
	best manualBalanceAction, bestScore, bestGlobal balanceScore, bestPreserves bool) bool {
	if order := compareScore(score, bestScore); order != 0 {
		return order < 0
	}
	if candidate.moveBytes != best.moveBytes {
		return candidate.moveBytes < best.moveBytes
	}
	if order := compareScore(globalScore, bestGlobal); order != 0 {
		return order < 0
	}
	if preserves != bestPreserves {
		return preserves
	}
	return targetLess(candidate.GroupID, candidate.target, best.GroupID, best.target)
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

func rebuildVirtualActions(ctx context.Context, original, virtual *Group, rebuild BalanceActionRebuilder) (BalanceActionSet, bool, error) {
	if rebuild == nil {
		return BalanceActionSet{}, false, errors.New("balance action rebuilder is required after changing a virtual target")
	}
	updated, ready := rebuild(ctx, cloneBalanceGroup(original), cloneBalanceGroup(virtual))
	if err := ctx.Err(); err != nil {
		return BalanceActionSet{}, false, err
	}
	return updated, ready, nil
}

func finalBalancePlans(original map[string]*Group, current []*Group, before, after balanceScore,
	actionSets map[string]BalanceActionSet, improved bool) []BalancePlan {
	plans := make([]BalancePlan, len(current))
	beforeScore, afterScore := &BalanceScore{Leader: before.leader, Voter: before.voter}, &BalanceScore{Leader: after.leader, Voter: after.voter}
	for i, group := range current {
		old := original[group.ID]
		voters := normalizeVoters(group.VoterStoreIDs)
		changed := group.LeaderStoreID != old.LeaderStoreID || !slices.Equal(voters, normalizeVoters(old.VoterStoreIDs))
		plans[i] = makePlan(group, group.LeaderStoreID, voters, changed)
		plans[i].OldLeaderStoreID = old.LeaderStoreID
		plans[i].OldVoterStoreIDs = normalizeVoters(old.VoterStoreIDs)
		plans[i].BeforeScore = beforeScore
		plans[i].AfterScore = afterScore
		plans[i].Status = "unchanged"
		if !improved {
			plans[i].Reason = "no strict table-score improvement"
			continue
		}
		if changed {
			plans[i].Status = "planned"
			moveCount := 0
			for _, storeID := range plans[i].OldVoterStoreIDs {
				if !slices.Contains(voters, storeID) {
					moveCount++
				}
			}
			plans[i].MoveBytes = actionSets[group.ID].MoveBytes * uint64(moveCount)
		}
	}
	return plans
}

// ErrBalanceBusy indicates another rebalance request currently owns the table.
var ErrBalanceBusy = errors.New("affinity rebalance is busy")

// ErrBalanceNotReady indicates the table or checker cannot safely rebalance yet.
var ErrBalanceNotReady = errors.New("affinity rebalance is not ready")

// ErrBalanceConflict indicates a planning dependency changed before submission.
var ErrBalanceConflict = errors.New("affinity rebalance plan conflict")

type manualBalanceAction struct {
	GroupID string
	target
	moveBytes uint64
}

type manualBalanceActionGenerator struct {
	group     *Group
	actions   BalanceActionSet
	voters    []uint64
	votersSet map[uint64]struct{}
	stores    []uint64
}

func newManualBalanceActionGenerator(group *Group, actions BalanceActionSet, stores []uint64) manualBalanceActionGenerator {
	voters := normalizeVoters(group.VoterStoreIDs)
	votersSet := make(map[uint64]struct{}, len(voters))
	for _, store := range voters {
		votersSet[store] = struct{}{}
	}
	return manualBalanceActionGenerator{
		group: group, actions: actions, voters: voters,
		votersSet: votersSet, stores: stores,
	}
}

func (g manualBalanceActionGenerator) visit(visit func(manualBalanceAction) bool) {
	group, actions := g.group, g.actions
	add := func(leader, from, to uint64, targetVoters []uint64, moveBytes uint64) bool {
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

func addManualActionDeltas(leaderDeltas, voterDeltas map[uint64]int, group *Group, action manualBalanceAction) {
	if group.LeaderStoreID != action.leader {
		if group.LeaderStoreID != 0 {
			leaderDeltas[group.LeaderStoreID]--
		}
		if action.leader != 0 {
			leaderDeltas[action.leader]++
		}
	}
	oldVoters := make(map[uint64]int, len(group.VoterStoreIDs))
	newVoters := make(map[uint64]int, len(action.voters))
	for _, storeID := range group.VoterStoreIDs {
		oldVoters[storeID]++
	}
	for _, storeID := range action.voters {
		newVoters[storeID]++
	}
	for storeID, count := range oldVoters {
		if delta := newVoters[storeID] - count; delta != 0 {
			voterDeltas[storeID] += delta
		}
	}
	for storeID, count := range newVoters {
		if _, existed := oldVoters[storeID]; !existed {
			voterDeltas[storeID] += count
		}
	}
}

func scoreWithDeltas(base int, counts, deltas map[uint64]int) int {
	for store, delta := range deltas {
		count := counts[store]
		base += 2*count*delta + delta*delta
	}
	return base
}
