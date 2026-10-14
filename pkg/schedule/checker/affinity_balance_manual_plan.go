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

package checker

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

const (
	manualBalancePlanStatusPlanned   = "planned"
	manualBalancePlanStatusUnchanged = "unchanged"
	manualBalancePlanStatusBlocked   = "blocked"
	manualBalancePlanStatusNotReady  = "not_ready"

	maxManualBalanceVirtualSteps = 4096
	maxManualBalanceCandidates   = 1 << 20
)

type manualBalanceCandidate struct {
	groupIndex      int
	target          affinity.Group
	score           affinity.BalanceScore
	global          affinity.BalanceScore
	moveBytes       uint64
	preservesLeader bool
}

type manualBalanceSearchResult struct {
	candidate       *manualBalanceCandidate
	temporarilyBusy bool
	blockedGroupID  string
	unreadyGroupID  string
	virtualBudget   bool
}

// planTableRebalance builds complete targets through checker-validated single
// moves. Rebuilding may worsen the original table while auto groups are
// reconstructed; only a strict final improvement is returned.
func (c *AffinityChecker) planTableRebalance(ctx context.Context, snapshot *affinity.BalanceSnapshot) ([]affinity.BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, affinity.ErrBalanceNotReady
	}

	original := cloneManualBalanceGroups(snapshot.Groups)
	if len(original) == 0 {
		return []affinity.BalancePlan{}, nil
	}
	originalScorer, err := newManualBalanceScorer(original)
	if err != nil {
		return nil, err
	}
	before := originalScorer.Score()
	unchanged := makeManualBalancePlans(original, original, nil, before, before, false)

	// A no-op plan is not evidence that the current table is ready. Check every
	// automatic group's complete current range before searching for changes.
	for _, actual := range original {
		if actual.BalancePolicy == affinity.BalancePolicyFixed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := cloneManualBalanceGroup(actual.Group)
		status, _, err := c.validateManualBalanceTarget(ctx, snapshot, actual, &target)
		if err != nil {
			return unchanged, err
		}
		if status == balanceAllowed {
			continue
		}
		reason := "current affinity group is not ready for manual balancing"
		planStatus := manualBalancePlanStatusNotReady
		if status == balanceTemporarilyBlocked {
			reason = "current affinity group is temporarily blocked"
			planStatus = manualBalancePlanStatusBlocked
		}
		markManualBalancePlanStatus(unchanged, actual.ID, planStatus, reason)
		return unchanged, affinity.ErrBalanceNotReady
	}

	// Fixed groups reserve their placements. Auto groups are restored one at a
	// time in ID order so later groups cannot bias an earlier target.
	rebuilt := cloneManualBalanceGroups(original)
	for _, group := range rebuilt {
		if group.BalancePolicy != affinity.BalancePolicyFixed {
			group.LeaderStoreID = 0
			group.VoterStoreIDs = nil
		}
	}
	moveBytes := make([]uint64, len(rebuilt))
	candidateChecks, virtualSteps := 0, 0
	for i, group := range original {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if group.BalancePolicy == affinity.BalancePolicyFixed {
			continue
		}
		rebuilt[i] = cloneManualBalanceGroupState(group)
		result, err := c.improveManualBalanceGroups(ctx, snapshot, original, rebuilt, i, moveBytes, &candidateChecks, &virtualSteps)
		if err != nil {
			if result.virtualBudget {
				return unchanged, err
			}
			return nil, err
		}
		if result.unreadyGroupID != "" {
			markManualBalancePlanStatus(unchanged, result.unreadyGroupID, manualBalancePlanStatusNotReady,
				"current affinity group is not ready for manual balancing")
			return unchanged, affinity.ErrBalanceNotReady
		}
		if result.temporarilyBusy {
			markManualBalancePlanStatus(unchanged, result.blockedGroupID, manualBalancePlanStatusBlocked,
				"an improving balance candidate is temporarily unavailable")
			return unchanged, affinity.ErrBalanceNotReady
		}
	}

	rebuiltScorer, err := newManualBalanceScorer(rebuilt)
	if err != nil {
		return nil, err
	}
	// Continue over the complete table after reconstruction. If reconstruction
	// did not improve the original table, restart from the original targets.
	current := cloneManualBalanceGroups(original)
	if rebuiltScorer.Score().Compare(before) < 0 {
		current = rebuilt
	} else {
		moveBytes = make([]uint64, len(current))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := c.improveManualBalanceGroups(ctx, snapshot, original, current, -1, moveBytes, &candidateChecks, &virtualSteps)
	if err != nil {
		if result.virtualBudget {
			return unchanged, err
		}
		return nil, err
	}
	scorer, err := newManualBalanceScorer(current)
	if err != nil {
		return nil, err
	}
	if result.unreadyGroupID != "" {
		plans := makeManualBalancePlans(original, current, moveBytes, before, scorer.Score(), false)
		markManualBalancePlanStatus(plans, result.unreadyGroupID, manualBalancePlanStatusNotReady,
			"current affinity group is not ready for manual balancing")
		return plans, affinity.ErrBalanceNotReady
	}
	if result.temporarilyBusy {
		plans := makeManualBalancePlans(original, current, moveBytes, before, scorer.Score(), false)
		markManualBalancePlanStatus(plans, result.blockedGroupID, manualBalancePlanStatusBlocked,
			"an improving balance candidate is temporarily unavailable")
		return plans, affinity.ErrBalanceNotReady
	}
	improved := scorer.Score().Compare(before) < 0
	return makeManualBalancePlans(original, current, moveBytes, before, scorer.Score(), improved), nil
}

func (c *AffinityChecker) improveManualBalanceGroups(ctx context.Context, snapshot *affinity.BalanceSnapshot,
	original, working []*affinity.GroupState, onlyGroup int, moveBytes []uint64, candidateChecks, virtualSteps *int,
) (manualBalanceSearchResult, error) {
	for {
		var result manualBalanceSearchResult
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if *virtualSteps >= maxManualBalanceVirtualSteps {
			result.virtualBudget = true
			return result, errors.New("manual affinity balance virtual step budget exceeded")
		}
		scorer, err := newManualBalanceScorer(working)
		if err != nil {
			return result, err
		}
		result, err = c.findManualBalanceCandidate(ctx, snapshot, original, working, scorer, false, onlyGroup, candidateChecks)
		if err != nil || result.unreadyGroupID != "" {
			return result, err
		}
		if result.candidate != nil {
			applyManualBalanceCandidate(working, moveBytes, *result.candidate)
			(*virtualSteps)++
			continue
		}
		if result.temporarilyBusy {
			return result, nil
		}

		result, err = c.findManualBalanceCandidate(ctx, snapshot, original, working, scorer, true, onlyGroup, candidateChecks)
		if err != nil || result.unreadyGroupID != "" {
			return result, err
		}
		if result.candidate != nil {
			applyManualBalanceCandidate(working, moveBytes, *result.candidate)
			(*virtualSteps)++
			continue
		}
		return result, nil
	}
}

func (c *AffinityChecker) findManualBalanceCandidate(ctx context.Context, snapshot *affinity.BalanceSnapshot,
	original, working []*affinity.GroupState, scorer *affinity.BalanceScorer, replace bool, onlyGroup int,
	candidateChecks *int) (manualBalanceSearchResult, error) {
	var result manualBalanceSearchResult
	candidateSnapshot := *snapshot
	if onlyGroup >= 0 {
		candidateSnapshot.Groups = []*affinity.GroupState{working[onlyGroup]}
	} else {
		candidateSnapshot.Groups = working
	}
	candidates := newBalanceCandidates(&candidateSnapshot, scorer, replace)
	globalLeaders, globalVoters := manualBalanceGlobalCounts(snapshot, original, working)
	baseGlobal := affinity.BalanceScore{
		Leader: manualBalanceCountScore(globalLeaders),
		Voter:  manualBalanceCountScore(globalVoters),
	}

	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		move, ok := candidates.next()
		if !ok {
			return result, nil
		}
		(*candidateChecks)++
		if *candidateChecks > maxManualBalanceCandidates {
			return result, errors.New("manual affinity balance candidate budget exceeded")
		}
		groupIndex := candidates.group
		if onlyGroup >= 0 {
			groupIndex = onlyGroup
		}
		if groupIndex < 0 || groupIndex >= len(working) || working[groupIndex].ID != move.GroupID {
			continue
		}
		nextScore, improves := scorer.Evaluate(move)
		if !improves {
			continue
		}
		if result.candidate != nil && nextScore.Compare(result.candidate.score) > 0 {
			continue
		}
		target := cloneManualBalanceGroup(working[groupIndex].Group)
		target.LeaderStoreID = move.LeaderStoreID
		if move.FromStoreID != 0 {
			from := slices.Index(target.VoterStoreIDs, move.FromStoreID)
			if from < 0 {
				continue
			}
			target.VoterStoreIDs[from] = move.ToStoreID
		}
		if sameManualBalanceTarget(working[groupIndex].Group, target) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		status, moveBytes, err := c.validateManualBalanceTarget(ctx, snapshot, original[groupIndex], &target)
		if err != nil {
			return result, err
		}
		switch status {
		case balanceRejected:
			continue
		case balanceTemporarilyBlocked:
			result.temporarilyBusy = true
			if result.blockedGroupID == "" {
				result.blockedGroupID = target.ID
			}
			continue
		case balanceUnready:
			result.unreadyGroupID = target.ID
			return result, nil
		}
		candidate := manualBalanceCandidate{
			groupIndex: groupIndex, target: target,
			score: nextScore, moveBytes: moveBytes,
			global: manualBalanceScoreAfterTarget(baseGlobal, globalLeaders, globalVoters,
				working[groupIndex].Group, target),
			preservesLeader: target.LeaderStoreID == original[groupIndex].LeaderStoreID,
		}
		if result.candidate == nil || manualBalanceCandidateLess(candidate, *result.candidate) {
			copy := candidate
			copy.target.VoterStoreIDs = slices.Clone(candidate.target.VoterStoreIDs)
			result.candidate = &copy
		}
	}
}

func manualBalanceCandidateLess(a, b manualBalanceCandidate) bool {
	if order := a.score.Compare(b.score); order != 0 {
		return order < 0
	}
	if a.moveBytes != b.moveBytes {
		return a.moveBytes < b.moveBytes
	}
	if order := a.global.Compare(b.global); order != 0 {
		return order < 0
	}
	if a.preservesLeader != b.preservesLeader {
		return a.preservesLeader
	}
	if a.target.ID != b.target.ID {
		return a.target.ID < b.target.ID
	}
	if a.target.LeaderStoreID != b.target.LeaderStoreID {
		return a.target.LeaderStoreID < b.target.LeaderStoreID
	}
	return slices.Compare(a.target.VoterStoreIDs, b.target.VoterStoreIDs) < 0
}

func applyManualBalanceCandidate(groups []*affinity.GroupState, moveBytes []uint64, candidate manualBalanceCandidate) {
	group := groups[candidate.groupIndex]
	group.LeaderStoreID = candidate.target.LeaderStoreID
	group.VoterStoreIDs = slices.Clone(candidate.target.VoterStoreIDs)
	moveBytes[candidate.groupIndex] = candidate.moveBytes
}

func manualBalanceGlobalCounts(snapshot *affinity.BalanceSnapshot, original, working []*affinity.GroupState) (leaders, voters map[uint64]int) {
	leaders = cloneManualBalanceCounts(snapshot.GlobalLeaderCounts)
	voters = cloneManualBalanceCounts(snapshot.GlobalVoterCounts)
	for i, old := range original {
		adjustManualBalanceGroupCounts(leaders, voters, old.Group, -1)
		adjustManualBalanceGroupCounts(leaders, voters, working[i].Group, 1)
	}
	return leaders, voters
}

func manualBalanceScoreAfterTarget(base affinity.BalanceScore, leaders, voters map[uint64]int, old, target affinity.Group) affinity.BalanceScore {
	return affinity.BalanceScore{
		Leader: base.Leader + manualBalanceCountDelta(leaders, []uint64{old.LeaderStoreID}, []uint64{target.LeaderStoreID}),
		Voter:  base.Voter + manualBalanceCountDelta(voters, old.VoterStoreIDs, target.VoterStoreIDs),
	}
}

func manualBalanceCountScore(counts map[uint64]int) int {
	score := 0
	for _, count := range counts {
		score += count * count
	}
	return score
}

func manualBalanceCountDelta(counts map[uint64]int, old, next []uint64) int {
	deltas := make(map[uint64]int, len(old)+len(next))
	for _, storeID := range old {
		if storeID != 0 {
			deltas[storeID]--
		}
	}
	for _, storeID := range next {
		if storeID != 0 {
			deltas[storeID]++
		}
	}
	delta := 0
	for storeID, countDelta := range deltas {
		count := counts[storeID]
		delta += 2*count*countDelta + countDelta*countDelta
	}
	return delta
}

func adjustManualBalanceGroupCounts(leaders, voters map[uint64]int, group affinity.Group, direction int) {
	if group.LeaderStoreID != 0 {
		leaders[group.LeaderStoreID] += direction
	}
	for _, storeID := range group.VoterStoreIDs {
		voters[storeID] += direction
	}
}

func cloneManualBalanceCounts(counts map[uint64]int) map[uint64]int {
	copy := make(map[uint64]int, len(counts))
	for storeID, count := range counts {
		copy[storeID] = count
	}
	return copy
}

func cloneManualBalanceGroups(groups []*affinity.GroupState) []*affinity.GroupState {
	result := make([]*affinity.GroupState, 0, len(groups))
	for _, group := range groups {
		result = append(result, cloneManualBalanceGroupState(group))
	}
	slices.SortFunc(result, func(a, b *affinity.GroupState) int { return cmp.Compare(a.ID, b.ID) })
	return result
}

func cloneManualBalanceGroupState(group *affinity.GroupState) *affinity.GroupState {
	copy := *group
	copy.Group = cloneManualBalanceGroup(group.Group)
	return &copy
}

func cloneManualBalanceGroup(group affinity.Group) affinity.Group {
	group.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
	return group
}

func newManualBalanceScorer(groups []*affinity.GroupState) (*affinity.BalanceScorer, error) {
	placements := make([]*affinity.Group, 0, len(groups))
	for _, group := range groups {
		copy := cloneManualBalanceGroup(group.Group)
		placements = append(placements, &copy)
	}
	return affinity.NewBalanceScorer(placements)
}

func sameManualBalanceTarget(a, b affinity.Group) bool {
	return a.LeaderStoreID == b.LeaderStoreID && slices.Equal(normalizeManualVoters(a.VoterStoreIDs), normalizeManualVoters(b.VoterStoreIDs))
}

func makeManualBalancePlans(original, targets []*affinity.GroupState, moveBytes []uint64,
	before, after affinity.BalanceScore, improved bool) []affinity.BalancePlan {
	plans := make([]affinity.BalancePlan, len(original))
	beforeScore := &affinity.BalanceScore{Leader: before.Leader, Voter: before.Voter}
	afterScore := &affinity.BalanceScore{Leader: after.Leader, Voter: after.Voter}
	for i, old := range original {
		current := targets[i]
		changed := !sameManualBalanceTarget(old.Group, current.Group)
		plan := affinity.BalancePlan{
			GroupID: old.ID, LeaderStoreID: current.LeaderStoreID,
			VoterStoreIDs: slices.Clone(current.VoterStoreIDs), Changed: changed,
			OldLeaderStoreID: old.LeaderStoreID, OldVoterStoreIDs: slices.Clone(old.VoterStoreIDs),
			BeforeScore: beforeScore, AfterScore: afterScore,
			Status: manualBalancePlanStatusUnchanged,
		}
		if !improved {
			plan.LeaderStoreID = old.LeaderStoreID
			plan.VoterStoreIDs = slices.Clone(old.VoterStoreIDs)
			plan.Changed = false
			plan.Reason = "no strict table-score improvement"
		} else if changed {
			plan.Status = manualBalancePlanStatusPlanned
			if i < len(moveBytes) {
				plan.MoveBytes = moveBytes[i]
			}
		}
		plans[i] = plan
	}
	return plans
}

func markManualBalancePlanStatus(plans []affinity.BalancePlan, groupID, status, reason string) {
	for i := range plans {
		if plans[i].GroupID == groupID {
			plans[i].Status, plans[i].Reason = status, reason
			return
		}
	}
}
