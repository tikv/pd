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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanTableRebalanceRebuildsChangedTargets(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p3", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p4", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p5", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}},
	}
	actions := make(map[string]BalanceActionSet, len(groups))
	for _, group := range groups {
		actions[group.ID] = BalanceActionSet{LeaderTargets: []uint64{2}}
	}
	var rebuilt []string
	plans, err := PlanTableRebalance(t.Context(), groups, BalanceOptions{
		Stores: []uint64{1, 2}, ActionSets: actions,
	}, func(_ context.Context, original, virtual *Group) (BalanceActionSet, bool) {
		require.Equal(t, uint64(1), original.LeaderStoreID)
		require.Equal(t, uint64(2), virtual.LeaderStoreID)
		rebuilt = append(rebuilt, virtual.ID)
		return BalanceActionSet{}, true
	})
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p3"}, rebuilt)
	require.Equal(t, []string{"p1", "p3"}, changedPlanIDs(plans))
	require.False(t, plans[0].Changed)
	require.Equal(t, uint64(2), plans[1].LeaderStoreID)
	require.Equal(t, uint64(2), plans[3].LeaderStoreID)
	require.False(t, plans[2].Changed)
}

func TestPlanTableRebalanceDoesNotPublishPairOnlyImprovement(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p3", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p4", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p5", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanTableRebalance(t.Context(), groups, BalanceOptions{
		Stores: []uint64{1, 2, 3},
		ActionSets: map[string]BalanceActionSet{
			"p0": {LeaderTargets: []uint64{2}},
			"p3": {LeaderTargets: []uint64{3}},
		},
	}, func(_ context.Context, _ *Group, _ *Group) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	})
	require.NoError(t, err)
	require.Empty(t, changedPlanIDs(plans))
	require.Len(t, plans, len(groups))
	for i, plan := range plans {
		require.False(t, plan.Changed)
		require.Equal(t, groups[i].LeaderStoreID, plan.LeaderStoreID)
		require.Equal(t, "unchanged", plan.Status)
		require.NotEmpty(t, plan.Reason)
	}
}

func TestPlanTableRebalanceGreedilyBalancesVoterTargets(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{2}},
		{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
	}
	plans, err := PlanTableRebalance(t.Context(), groups, BalanceOptions{
		Stores: []uint64{1, 2, 3},
		ActionSets: map[string]BalanceActionSet{
			"p0": {VoterReplacements: map[uint64]map[uint64][]uint64{1: {2: {2}}}},
			"p1": {VoterReplacements: map[uint64]map[uint64][]uint64{2: {3: {3}}}},
		},
	}, func(_ context.Context, _ *Group, _ *Group) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	})
	require.NoError(t, err)
	require.Equal(t, []string{"p0", "p1"}, changedPlanIDs(plans))
	byID := make(map[string]BalancePlan, len(plans))
	for _, plan := range plans {
		byID[plan.GroupID] = plan
	}
	require.Equal(t, uint64(2), byID["p0"].LeaderStoreID)
	require.Equal(t, []uint64{2}, byID["p0"].VoterStoreIDs)
	require.Equal(t, uint64(3), byID["p1"].LeaderStoreID)
	require.Equal(t, []uint64{3}, byID["p1"].VoterStoreIDs)
	require.False(t, byID["fixed"].Changed)
}

func TestUpdateGlobalBalanceCountsTracksVirtualTarget(t *testing.T) {
	leaders := map[uint64]int{1: 2, 2: 1}
	voters := map[uint64]int{1: 2, 2: 1, 3: 0}
	group := &Group{LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}

	updateGlobalBalanceCounts(leaders, voters, group, 2, []uint64{2, 3})

	require.Equal(t, map[uint64]int{1: 1, 2: 2}, leaders)
	require.Equal(t, map[uint64]int{1: 1, 2: 1, 3: 1}, voters)
}

func TestPlanTableRebalanceReturnsOriginalTargetsWithoutImprovement(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}},
	}
	plans, err := PlanTableRebalance(t.Context(), groups, BalanceOptions{
		Stores: []uint64{1, 2},
		ActionSets: map[string]BalanceActionSet{
			"p0": {LeaderTargets: []uint64{2}},
			"p1": {LeaderTargets: []uint64{1}},
		},
	}, func(_ context.Context, _ *Group, _ *Group) (BalanceActionSet, bool) {
		return BalanceActionSet{}, true
	})
	require.NoError(t, err)
	require.Len(t, plans, len(groups))
	require.Empty(t, changedPlanIDs(plans))
	for i, plan := range plans {
		require.Equal(t, groups[i].LeaderStoreID, plan.LeaderStoreID)
		require.Equal(t, groups[i].VoterStoreIDs, plan.VoterStoreIDs)
		require.Equal(t, groups[i].LeaderStoreID, plan.OldLeaderStoreID)
		require.Equal(t, groups[i].VoterStoreIDs, plan.OldVoterStoreIDs)
		require.Equal(t, plan.BeforeScore, plan.AfterScore)
		require.Equal(t, "unchanged", plan.Status)
		require.Equal(t, "no strict table-score improvement", plan.Reason)
	}
}

func TestManualBalanceGlobalScoreUsesCompleteVoterTarget(t *testing.T) {
	group := &Group{LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	action := manualBalanceAction{target: target{
		leader: 1, voters: []uint64{2, 3}, from: 2, to: 3,
	}}
	voterCounts := map[uint64]int{1: 3, 2: 2, 3: 0}
	base := balanceScore{voter: sumSquares(voterCounts)}

	score := scoreAfterAction(base, nil, voterCounts, group, action)

	require.Equal(t, 9, score.voter)
}

func changedPlanIDs(plans []BalancePlan) []string {
	ids := make([]string, 0)
	for _, plan := range plans {
		if plan.Changed {
			ids = append(ids, plan.GroupID)
		}
	}
	return ids
}
