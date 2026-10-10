package affinity

import (
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
		ValidTarget: func(_ *Group, leader uint64, voters []uint64) bool { return leader != 4 && !containsStore(voters, 4) },
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

func TestPlanBalanceInitializesNewGroup(t *testing.T) {
	plans, err := PlanBalance([]*Group{{ID: "p0"}}, BalanceOptions{Stores: []uint64{1, 2, 3}, ReplicaCount: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), plans[0].LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, plans[0].VoterStoreIDs)
	require.True(t, plans[0].Changed)
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
	require.Greater(t, voters[4], 0)
}

func containsStore(values []uint64, value uint64) bool {
	for _, current := range values {
		if current == value {
			return true
		}
	}
	return false
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
