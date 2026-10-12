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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
)

// Exercise the real checker entry point, persisted target change, operator
// generation and subsequent Region heartbeat, rather than just PlanBalance.
func TestAffinityBalanceLeaderConvergence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 10)
	}
	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	for i := uint64(1); i <= 3; i++ {
		start, end := []byte{byte(i)}, []byte{byte(i + 1)}
		id := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}[i-1]
		require.NoError(t, createAffinityGroupForTest(manager, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}, start, end))
		tc.AddLeaderRegion(i, 1, 2, 3)
		tc.PutRegion(tc.GetRegion(i).Clone(core.WithStartKey(start), core.WithEndKey(end)))
	}
	for i := uint64(1); i <= 3; i++ {
		checker.Check(tc.GetRegion(i))
	}
	checker.Check(tc.GetRegion(1))
	require.Eventually(t, func() bool {
		checker.Check(tc.GetRegion(1))
		return manager.GetAffinityGroupState("_tidb_pt_100_p1").LeaderStoreID != 1
	}, 3*time.Second, 10*time.Millisecond)
	target := manager.GetAffinityGroupState("_tidb_pt_100_p1")
	require.NotEqual(t, uint64(1), target.LeaderStoreID)
	ops := checker.Check(tc.GetRegion(1))
	require.Len(t, ops, 1, "a target change must produce a transfer operator, not merge using stale isAffinity")
	require.Equal(t, operator.OpLeader|operator.OpAffinity, ops[0].Kind())
	region := tc.GetRegion(1)
	peer := region.GetStorePeer(target.LeaderStoreID)
	require.NotNil(t, peer)
	tc.PutRegion(region.Clone(core.WithLeader(peer)))
	checker.Check(tc.GetRegion(1))
	require.Equal(t, affinity.PhaseStable, manager.GetAffinityGroupState(target.ID).Phase)
}

func TestAffinityBalanceActionsRequireAllRegions(t *testing.T) {
	first := affinity.BalanceActionSet{
		LeaderTargets: []uint64{1, 2, 3},
		VoterReplacements: map[uint64]map[uint64][]uint64{
			1: {4: {2, 3, 4}, 5: {2, 3}},
			2: {4: {1, 3}},
		},
	}
	second := affinity.BalanceActionSet{
		LeaderTargets:     []uint64{1, 3},
		VoterReplacements: map[uint64]map[uint64][]uint64{1: {4: {2, 4}}},
	}
	intersectBalanceActions(&first, second)
	require.Equal(t, []uint64{1, 3}, first.LeaderTargets)
	require.Equal(t, map[uint64]map[uint64][]uint64{1: {4: {2, 4}}}, first.VoterReplacements)
}
