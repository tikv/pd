package checker

import (
	"context"
	"testing"

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
