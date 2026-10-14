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
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

type manualBalanceFixtureGroup struct {
	leader uint64
	voters []uint64
	fixed  bool
}

func newManualBalanceFixture(t *testing.T, storeCount uint64, groups []manualBalanceFixtureGroup) (*AffinityChecker, *affinity.Manager, *affinity.BalanceSnapshot) {
	t.Helper()
	tc, checker := newAffinityBalanceTestCluster(t, storeCount)
	manager := tc.GetAffinityManager()
	firstID := ""
	for i, group := range groups {
		id := fmt.Sprintf("_tidb_pt_100_p%d", i)
		if firstID == "" {
			firstID = id
		}
		policy := affinity.BalancePolicyAuto
		if group.fixed {
			policy = affinity.BalancePolicyFixed
		}
		start := string([]byte{byte('a' + 2*i)})
		end := string([]byte{byte('b' + 2*i)})
		createBalanceGroup(t, manager, id, start, end, group.leader, group.voters, policy)
		addBalanceRegion(t, tc, manager, uint64(i+1), start, end, group.leader, group.voters...)
	}
	snapshot := manager.GetBalanceSnapshot(firstID)
	require.NotNil(t, snapshot)
	return checker, manager, snapshot
}

func TestPlanTableRebalanceRebuildsMultipleGroupsAndPreservesFixedOrder(t *testing.T) {
	checker, _, snapshot := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}, fixed: true},
		{leader: 1, voters: []uint64{1, 2, 3}},
		{leader: 1, voters: []uint64{1, 2, 3}},
		{leader: 1, voters: []uint64{1, 2, 3}},
	})
	// The manager canonicalizes voters, while the planner must preserve the
	// fixed group's slice order exactly as supplied by its snapshot.
	snapshot.Groups[0].VoterStoreIDs = []uint64{3, 1, 2}

	plans, err := checker.planTableRebalance(t.Context(), snapshot)
	require.NoError(t, err)
	require.Len(t, plans, 4)
	require.Equal(t, []uint64{3, 1, 2}, plans[0].VoterStoreIDs)
	require.False(t, plans[0].Changed)
	changed := 0
	for _, plan := range plans {
		if plan.Changed {
			changed++
		}
	}
	require.GreaterOrEqual(t, changed, 2)
	require.Negative(t, plans[0].AfterScore.Compare(*plans[0].BeforeScore))
}

func TestPlanTableRebalanceKeepsOriginalWhenNoImprovementExists(t *testing.T) {
	checker, _, snapshot := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}},
	})
	original := slices.Clone(snapshot.Groups[0].VoterStoreIDs)

	plans, err := checker.planTableRebalance(t.Context(), snapshot)
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.False(t, plans[0].Changed)
	require.Equal(t, original, plans[0].VoterStoreIDs)
	require.Equal(t, "no strict table-score improvement", plans[0].Reason)
	require.Equal(t, plans[0].BeforeScore, plans[0].AfterScore)
}

func TestPlanTableRebalanceRefinesSixAutomaticGroups(t *testing.T) {
	checker, _, snapshot := newManualBalanceFixture(t, 6, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 3, 5}},
		{leader: 2, voters: []uint64{2, 4, 6}},
		{leader: 3, voters: []uint64{3, 5, 6}},
		{leader: 4, voters: []uint64{1, 2, 4}},
		{leader: 5, voters: []uint64{4, 5, 6}},
		{leader: 6, voters: []uint64{1, 2, 6}},
	})
	plans, err := checker.planTableRebalance(t.Context(), snapshot)
	require.NoError(t, err)
	require.Len(t, plans, 6)
	require.Equal(t, affinity.BalanceScore{Leader: 6, Voter: 56}, *plans[0].BeforeScore)
	require.Equal(t, affinity.BalanceScore{Leader: 6, Voter: 54}, *plans[0].AfterScore)
	require.Negative(t, plans[0].AfterScore.Compare(*plans[0].BeforeScore))
	changed := false
	for _, plan := range plans {
		changed = changed || plan.Changed
	}
	require.True(t, changed)
}

func TestPlanTableRebalanceReportsTemporaryCandidateBlock(t *testing.T) {
	checker, _, snapshot := newManualBalanceFixture(t, 4, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}},
		{leader: 2, voters: []uint64{1, 2, 3}, fixed: true},
	})
	checker.cluster.(*mockcluster.Cluster).SetStoreBusy(4, true)

	plans, err := checker.planTableRebalance(t.Context(), snapshot)
	require.ErrorIs(t, err, affinity.ErrBalanceNotReady)
	require.Len(t, plans, 2)
	require.Equal(t, manualBalancePlanStatusBlocked, plans[0].Status)
	require.NotEmpty(t, plans[0].Reason)
	require.False(t, plans[0].Changed)
}

func TestPlanTableRebalanceRejectsUnreadyCurrentGroup(t *testing.T) {
	checker, manager, snapshot := newManualBalanceFixture(t, 4, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}},
		{leader: 2, voters: []uint64{1, 2, 4}, fixed: true},
	})
	tc := checker.cluster.(*mockcluster.Cluster)
	region := tc.GetRegion(1)
	tc.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
	manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(1))

	plans, err := checker.planTableRebalance(t.Context(), snapshot)
	require.ErrorIs(t, err, affinity.ErrBalanceNotReady)
	require.Len(t, plans, 2)
	require.Equal(t, manualBalancePlanStatusNotReady, plans[0].Status)
	require.False(t, plans[0].Changed)
}

func TestPlanTableRebalanceReturnsCancellation(t *testing.T) {
	checker, _, snapshot := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}},
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	plans, err := checker.planTableRebalance(ctx, snapshot)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, plans)
}

func TestManualBalanceCandidateTieBreaks(t *testing.T) {
	base := manualBalanceCandidate{
		score:     affinity.BalanceScore{Leader: 6, Voter: 54},
		global:    affinity.BalanceScore{Leader: 20, Voter: 100},
		moveBytes: 10, preservesLeader: true,
	}
	for _, tc := range []struct {
		name   string
		change func(*manualBalanceCandidate, *manualBalanceCandidate)
	}{
		{"table score first", func(_ *manualBalanceCandidate, candidate *manualBalanceCandidate) {
			candidate.score.Voter += 2
			candidate.moveBytes = 0
		}},
		{"move bytes before global score", func(_ *manualBalanceCandidate, candidate *manualBalanceCandidate) {
			candidate.moveBytes++
			candidate.global.Voter = 0
		}},
		{"global score before leader preservation", func(better, candidate *manualBalanceCandidate) {
			better.preservesLeader = false
			candidate.global.Voter++
		}},
		{"preserve original leader", func(_ *manualBalanceCandidate, candidate *manualBalanceCandidate) {
			candidate.preservesLeader = false
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			better, other := base, base
			tc.change(&better, &other)
			require.True(t, manualBalanceCandidateLess(better, other))
			require.False(t, manualBalanceCandidateLess(other, better))
		})
	}
}
