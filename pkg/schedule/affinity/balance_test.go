// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package affinity

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanBalanceTransfersOneLeader(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p3", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p4", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3}, ReplicaCount: 3,
		ActionSets: allActionSets(groups, []uint64{1, 2, 3}),
	})
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
	require.Equal(t, "p0", changedPlan(plans).GroupID)
	require.Equal(t, uint64(2), changedPlan(plans).LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, changedPlan(plans).VoterStoreIDs)
}

func TestPlanBalanceReplacesOneVoter(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	actions := allActionSets(groups, []uint64{1, 2, 3, 4})
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3, ActionSets: actions})
	require.NoError(t, err)
	require.Equal(t, 1, changedPlans(plans))
	require.Contains(t, changedPlan(plans).VoterStoreIDs, uint64(4))
}

func TestPlanBalanceUsesOnlyCheckerActions(t *testing.T) {
	groups := []*Group{
		{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3,
		ActionSets: map[string]BalanceActionSet{
			"p0": {LeaderTargets: []uint64{1, 2, 3}, VoterReplacements: map[uint64]map[uint64][]uint64{
				3: {4: {1, 2, 3}},
			}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 4}, changedPlan(plans).VoterStoreIDs)
}

func TestPlanBalanceFixedGroupKeepsCanonicalVoters(t *testing.T) {
	groups := []*Group{{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 9, VoterStoreIDs: []uint64{10, 9}}}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 9, 10}, ReplicaCount: 2})
	require.NoError(t, err)
	require.Equal(t, BalancePlan{GroupID: "fixed", LeaderStoreID: 9, VoterStoreIDs: []uint64{9, 10}}, plans[0])
}

func TestPlanBalanceNoActionKeepsTarget(t *testing.T) {
	groups := []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: []uint64{1, 2, 3, 4}, ReplicaCount: 3})
	require.NoError(t, err)
	require.False(t, plans[0].Changed)
	require.Equal(t, []uint64{1, 2, 3}, plans[0].VoterStoreIDs)
}

func TestPlanBalanceSkipsUnseededGroup(t *testing.T) {
	plans, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	require.Equal(t, BalancePlan{GroupID: "p0"}, plans[0])
}

func TestPlanBalanceRejectsDuplicateGroupIDs(t *testing.T) {
	_, err := PlanBalance([]*Group{{ID: "p0"}, {ID: "p0"}}, BalanceOptions{Stores: []uint64{1}, ReplicaCount: 1})
	require.EqualError(t, err, `duplicate affinity group ID "p0"`)
}

func TestPlanBalanceRejectsInvalidPolicy(t *testing.T) {
	_, err := PlanBalance([]*Group{{ID: "p0", BalancePolicy: BalancePolicy("invalid")}}, BalanceOptions{Stores: []uint64{1}, ReplicaCount: 1})
	require.EqualError(t, err, `group "p0" has invalid balance policy "invalid"`)
}

func TestPlanBalanceIsDeterministic(t *testing.T) {
	groups := []*Group{
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	options := BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3, ActionSets: allActionSets(groups, []uint64{1, 2, 3})}
	first, err := PlanBalance(groups, options)
	require.NoError(t, err)
	second, err := PlanBalance(groups, options)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestPlanBalanceRejectsDuplicateVoters(t *testing.T) {
	_, err := PlanBalance([]*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 1, 2}}}, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.EqualError(t, err, `group "p0" has duplicate voter store ID`)
}

func allActionSets(groups []*Group, stores []uint64) map[string]BalanceActionSet {
	result := make(map[string]BalanceActionSet, len(groups))
	for _, group := range groups {
		voters := normalizeVoters(group.VoterStoreIDs)
		actions := BalanceActionSet{LeaderTargets: slices.Clone(voters), VoterReplacements: make(map[uint64]map[uint64][]uint64)}
		for _, from := range voters {
			actions.VoterReplacements[from] = make(map[uint64][]uint64)
			for _, to := range stores {
				if slices.Contains(voters, to) {
					continue
				}
				replaced := replaceVoter(voters, from, to)
				actions.VoterReplacements[from][to] = slices.Clone(replaced)
			}
		}
		result[group.ID] = actions
	}
	return result
}

func changedPlan(plans []BalancePlan) BalancePlan {
	for _, plan := range plans {
		if plan.Changed {
			return plan
		}
	}
	return BalancePlan{}
}

func changedPlans(plans []BalancePlan) int {
	count := 0
	for _, plan := range plans {
		if plan.Changed {
			count++
		}
	}
	return count
}
