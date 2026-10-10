package affinity

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

func newBalanceRuntimeManager(t *testing.T) (*Manager, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewStorageWithMemoryBackend()
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	conf := mockconfig.NewTestOptions()
	labelerManager, err := labeler.NewRegionLabeler(ctx, store, time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, conf, labelerManager)
	require.NoError(t, err)
	return manager, cancel
}

func TestAutoBalanceGroupUsesVersionAndUpdatesOneGroup(t *testing.T) {
	manager, cancel := newBalanceRuntimeManager(t)
	defer cancel()
	groups := []GroupKeyRanges{
		{GroupID: "_tidb_pt_10_p1"},
		{GroupID: "_tidb_pt_10_p2"},
		{GroupID: "_tidb_pt_10_p3"},
	}
	require.NoError(t, manager.CreateAffinityGroups(groups))
	for _, group := range groups {
		_, err := manager.UpdateAffinityGroupPeers(group.GroupID, 1, []uint64{1, 2, 3})
		require.NoError(t, err)
	}
	old := manager.GetAffinityGroupState(groups[0].GroupID)
	plan, err := manager.AutoBalanceGroup(groups[0].GroupID)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.NotEqual(t, uint64(1), plan.LeaderStoreID)

	// A stale planner snapshot must not overwrite an intervening admin update.
	_, err = manager.UpdateAffinityGroupPeers(groups[0].GroupID, 3, []uint64{1, 2, 3})
	require.NoError(t, err)
	state, err := manager.updateAffinityGroupPeersWithBalanceVer(groups[0].GroupID, old.affinityVer, 2, []uint64{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.LeaderStoreID)
}
