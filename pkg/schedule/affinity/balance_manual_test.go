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
	require.Equal(t, []string{"p0", "p1"}, rebuilt)
	require.Equal(t, []string{"p0", "p1"}, changedPlanIDs(plans))
	require.Equal(t, uint64(2), plans[0].LeaderStoreID)
	require.Equal(t, uint64(2), plans[1].LeaderStoreID)
	require.False(t, plans[2].Changed)
}

func TestPlanTableRebalanceUsesCoordinatedLeaderPair(t *testing.T) {
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
	require.Equal(t, []string{"p0", "p3"}, changedPlanIDs(plans))
	require.Equal(t, uint64(2), plans[0].LeaderStoreID)
	require.Equal(t, uint64(3), plans[3].LeaderStoreID)
	require.Equal(t, balanceScore{leader: 12, voter: 108}, calcBalanceScore(
		map[uint64]int{1: 2, 2: 2, 3: 2}, map[uint64]int{1: 6, 2: 6, 3: 6},
	))
}

func TestPlanTableRebalanceUsesCoordinatedVoterPair(t *testing.T) {
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
	require.False(t, plans[0].Changed)
	require.Equal(t, uint64(2), plans[1].LeaderStoreID)
	require.Equal(t, []uint64{2}, plans[1].VoterStoreIDs)
	require.Equal(t, uint64(3), plans[2].LeaderStoreID)
	require.Equal(t, []uint64{3}, plans[2].VoterStoreIDs)
}

func TestUpdateGlobalBalanceCountsTracksVirtualTarget(t *testing.T) {
	leaders := map[uint64]int{1: 2, 2: 1}
	voters := map[uint64]int{1: 2, 2: 1, 3: 0}
	group := &Group{LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}

	updateGlobalBalanceCounts(leaders, voters, group, 2, []uint64{2, 3})

	require.Equal(t, map[uint64]int{1: 1, 2: 2}, leaders)
	require.Equal(t, map[uint64]int{1: 1, 2: 1, 3: 1}, voters)
}

func TestFindImprovingBalancePairUsesGlobalScoreForTies(t *testing.T) {
	groups := []*Group{
		{ID: "a0", LeaderStoreID: 4, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "a1", LeaderStoreID: 4, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "a2", LeaderStoreID: 4, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "a3", LeaderStoreID: 5, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "a4", LeaderStoreID: 5, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "a5", LeaderStoreID: 6, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z3", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z4", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
		{ID: "z5", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3, 4, 5, 6}},
	}
	pair, found, _, err := findImprovingPair(t.Context(), groups, []uint64{1, 2, 3, 4, 5, 6}, map[string]BalanceActionSet{
		"a0": {LeaderTargets: []uint64{5}},
		"a3": {LeaderTargets: []uint64{6}},
		"z0": {LeaderTargets: []uint64{2}},
		"z3": {LeaderTargets: []uint64{3}},
	}, map[uint64]int{1: 3, 2: 2, 3: 1, 4: 3, 5: 2, 6: 3}, nil, 100)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "z0", pair.first.GroupID)
	require.Equal(t, "z3", pair.second.GroupID)
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
