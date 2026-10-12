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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanBalanceTransfersLeader(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p3", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p4", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	leaders := make(map[uint64]int)
	for _, plan := range plans {
		leaders[plan.LeaderStoreID]++
	}
	requireBalancedCounts(t, leaders)
	require.True(t, anyPlanChanged(plans))
	for _, plan := range plans {
		require.Equal(t, []uint64{1, 2, 3}, plan.VoterStoreIDs)
	}
}

func TestPlanBalanceReplacesVoterAfterScaleOut(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	voters := make(map[uint64]int)
	for _, plan := range plans {
		for _, store := range plan.VoterStoreIDs {
			voters[store]++
		}
	}
	requireBalancedCounts(t, voters)
	require.Equal(t, 2, voters[4])
	require.Equal(t, 2, changedPlans(plans))
}

func TestPlanBalanceKeepsFixedAndFiltersInvalidTargets(t *testing.T) {
	groups := []*Group{
		{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "auto", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, leader uint64, voters []uint64) bool { return leader != 4 && !slices.Contains(voters, 4) },
	})
	require.NoError(t, err)
	byID := make(map[string]BalancePlan, len(plans))
	for _, plan := range plans {
		byID[plan.GroupID] = plan
	}
	require.False(t, byID["fixed"].Changed)
	require.Equal(t, uint64(1), byID["fixed"].LeaderStoreID)
	require.NotContains(t, byID["auto"].VoterStoreIDs, uint64(4))
}

func TestPlanBalanceKeepsMalformedFixedTarget(t *testing.T) {
	groups := []*Group{{
		ID: "fixed", BalancePolicy: BalancePolicyFixed,
		LeaderStoreID: 9, VoterStoreIDs: []uint64{10, 9},
	}}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	require.Equal(t, BalancePlan{
		GroupID: "fixed", LeaderStoreID: 9, VoterStoreIDs: []uint64{9, 10}, Changed: false,
	}, plans[0])
}

func TestPlanBalanceRejectsInvalidBalancePolicy(t *testing.T) {
	groups := []*Group{{ID: "p0", BalancePolicy: BalancePolicy("invalid")}}
	_, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1}, ReplicaCount: 1})
	require.EqualError(t, err, `group "p0" has invalid balance policy "invalid"`)
}

func TestPlanBalanceInitializesNewGroup(t *testing.T) {
	plans, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), plans[0].LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, plans[0].VoterStoreIDs)
	require.True(t, plans[0].Changed)
}

func TestPlanBalanceInitializesSingleReplica(t *testing.T) {
	plans, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{Stores: []uint64{1, 2}, ReplicaCount: 1})
	require.NoError(t, err)
	require.Len(t, plans[0].VoterStoreIDs, 1)
	require.Equal(t, plans[0].LeaderStoreID, plans[0].VoterStoreIDs[0])
}

func TestPlanBalanceFindsAlternativeInitialVoters(t *testing.T) {
	plans, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{
		Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, _ uint64, voters []uint64) bool { return !slices.Contains(voters, 2) },
	})
	require.NoError(t, err)
	require.NotContains(t, plans[0].VoterStoreIDs, uint64(2))
}

func TestPlanBalanceFindsAlternativeReplacementVoters(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 10, 11}}}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, _ uint64, voters []uint64) bool { return !slices.Contains(voters, 2) },
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 4}, plans[0].VoterStoreIDs)
}

func TestPlanBalanceFindsAlternativeVoterCombination(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3, 4, 5}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, _ uint64, voters []uint64) bool {
			return slices.Equal(voters, []uint64{1, 4, 5})
		},
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 4, 5}, plans[0].VoterStoreIDs)
}

func TestPlanBalanceInitializesOnLeastLoadedVoters(t *testing.T) {
	groups := []*Group{
		{ID: "fixed0", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "fixed1", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "fixed2", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 4}},
		{ID: "new"},
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	for _, plan := range plans {
		if plan.GroupID == "new" {
			require.Equal(t, uint64(4), plan.LeaderStoreID)
			require.Equal(t, []uint64{1, 3, 4}, plan.VoterStoreIDs)
		}
	}
}

func TestPlanBalanceRemovesOfflineStore(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 4, VoterStoreIDs: []uint64{1, 2, 4}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	for _, plan := range plans {
		require.NotEqual(t, uint64(4), plan.LeaderStoreID)
		require.NotContains(t, plan.VoterStoreIDs, uint64(4))
	}
	require.True(t, anyPlanChanged(plans))
}

func TestPlanBalanceReplacesMultipleOfflineVoters(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 5, 6}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	require.NotContains(t, plans[0].VoterStoreIDs, uint64(5))
	require.NotContains(t, plans[0].VoterStoreIDs, uint64(6))
	require.True(t, plans[0].Changed)
}

func TestPlanBalanceRejectsZeroStore(t *testing.T) {
	_, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{Stores: []uint64{0, 1}, ReplicaCount: 1})
	require.EqualError(t, err, "store ID must be positive")
}

func TestPlanBalanceRepairsUnsetLeader(t *testing.T) {
	groups := []*Group{{ID: "p0", VoterStoreIDs: []uint64{1, 2, 3}}}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	require.NotZero(t, plans[0].LeaderStoreID)
}

func TestPlanBalanceRepairsInvalidExistingTarget(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, leader uint64, _ []uint64) bool { return leader != 1 },
	})
	require.NoError(t, err)
	require.NotEqual(t, uint64(1), plans[0].LeaderStoreID)
}

func TestPlanBalanceRepairsLeaderOutsideVoters(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{2, 3, 4}}}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	require.Contains(t, plans[0].VoterStoreIDs, plans[0].LeaderStoreID)
	require.True(t, plans[0].Changed)
}

func TestPlanBalanceRejectsDuplicateVoters(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 1, 1}}}
	_, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.EqualError(t, err, `group "p0" has duplicate voter store ID`)
}

func TestPlanBalanceErrorsWhenInvalidTargetCannotBeReplaced(t *testing.T) {
	groups := []*Group{{ID: "p0", VoterStoreIDs: []uint64{1, 2, 3}}}
	_, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3}, ReplicaCount: 3,
		ValidTarget: func(_ *Group, _ uint64, _ []uint64) bool { return false },
	})
	require.EqualError(t, err, `unable to find valid balance target for group "p0"`)
}

func TestPreferredTargetsScaleWithStoreCount(t *testing.T) {
	stores := make([]uint64, 100)
	counts := make(map[uint64]int, len(stores))
	for i := range stores {
		stores[i] = uint64(i + 1)
	}
	for _, oldVoters := range [][]uint64{nil, {1, 101, 102}} {
		calls := 0
		visitCandidateTargets(stores, oldVoters, 3, counts, false, func(candidate target) {
			calls++
			require.Len(t, candidate.voters, 3)
			require.Contains(t, candidate.voters, candidate.leader)
			if len(oldVoters) > 0 {
				require.Contains(t, candidate.voters, uint64(1))
			}
		})
		require.Equal(t, 3*len(stores), calls)
	}
}

func TestPlanBalanceIsDeterministicAfterRestart(t *testing.T) {
	groups := []*Group{
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	options := BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3}
	first, err := PlanBalance(groups, options)
	require.NoError(t, err)
	second, err := PlanBalance(groups, options)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestPlanBalanceRejectsNilGroup(t *testing.T) {
	_, err := PlanBalance([]*Group{nil}, BalanceOptions{Stores: []uint64{1}, ReplicaCount: 1})
	require.EqualError(t, err, "nil affinity group")
}

func TestPlanBalanceRecomputesAfterStoreReturns(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	offline, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	returnedGroups := make([]*Group, len(offline))
	for i, plan := range offline {
		returnedGroups[i] = &Group{ID: plan.GroupID, LeaderStoreID: plan.LeaderStoreID, VoterStoreIDs: plan.VoterStoreIDs}
	}
	plans, err := PlanBalance(returnedGroups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	voters := make(map[uint64]int)
	for _, plan := range plans {
		for _, store := range plan.VoterStoreIDs {
			voters[store]++
		}
	}
	requireBalancedCounts(t, voters)
	require.Positive(t, voters[4])
}

func anyPlanChanged(plans []BalancePlan) bool {
	return changedPlans(plans) > 0
}

func changedPlans(plans []BalancePlan) int {
	changed := 0
	for _, plan := range plans {
		if plan.Changed {
			changed++
		}
	}
	return changed
}

func requireBalancedCounts(t *testing.T, counts map[uint64]int) {
	t.Helper()
	min, max := -1, -1
	for _, count := range counts {
		if min == -1 || count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}
	require.LessOrEqual(t, max-min, 1)
}
