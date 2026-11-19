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

package schedule

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPrepareCheckerRequiresAllRegions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		total         int
		cached        int
		collected     int
		storeReported int
		prepared      bool
	}{
		{name: "global 90 percent", total: 100, cached: 90, collected: 90, storeReported: 90},
		{name: "global 99 percent", total: 100, cached: 99, collected: 99, storeReported: 99},
		{name: "store reported 99 percent", total: 99, cached: 99, collected: 99, storeReported: 100},
		{name: "store cached 99 percent", total: 99, cached: 100, collected: 99, storeReported: 99},
		{name: "all collected", total: 100, cached: 100, collected: 100, storeReported: 100, prepared: true},
		{name: "exceeds upstream count", total: 99, cached: 100, collected: 100, storeReported: 100, prepared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			peer := &metapb.Peer{Id: 1, StoreId: 1}
			cluster.PutStore(core.NewStoreInfo(
				&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving},
				core.SetStoreStats(&pdpb.StoreStats{RegionCount: uint32(tc.storeReported)}),
			))
			for i := range tc.cached {
				source := core.Storage
				if i < tc.collected {
					source = core.Heartbeat
				}
				cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
					Id:       uint64(i + 1),
					StartKey: []byte(fmt.Sprintf("%03d", i)),
					EndKey:   []byte(fmt.Sprintf("%03d", i+1)),
					Peers:    []*metapb.Peer{peer},
				}, peer, core.SetSource(source)))
			}
			checker := newPrepareChecker(func() (int, error) { return tc.total, nil })
			// A failed count check must not clone and scan the Region trees.
			scanned := false
			point := "github.com/tikv/pd/pkg/core/regionPreparationScanBatch"
			re.NoError(failpoint.EnableCall(point, func(uint64) { scanned = true }))
			t.Cleanup(func() { re.NoError(failpoint.Disable(point)) })
			re.Equal(tc.prepared, checker.check(cluster))
			re.Equal(tc.prepared, checker.IsPrepared())
			re.Equal(tc.prepared, scanned)
		})
	}
}

func TestPrepareCheckerRechecksStoreCountsAfterScan(t *testing.T) {
	re := require.New(t)
	cluster := core.NewBasicCluster()
	store := core.NewStoreInfo(
		&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving},
		core.SetStoreStats(&pdpb.StoreStats{RegionCount: 1}),
	)
	cluster.PutStore(store)
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	cluster.PutRegion(core.NewRegionInfo(&metapb.Region{Id: 1, EndKey: []byte("b"), Peers: []*metapb.Peer{peer}}, peer, core.SetSource(core.Heartbeat)))
	checker := newPrepareChecker(func() (int, error) { return 1, nil })
	updated := false
	point := "github.com/tikv/pd/pkg/core/regionPreparationScanBatch"
	re.NoError(failpoint.EnableCall(point, func(uint64) {
		if !updated {
			// A store heartbeat reveals an uncollected Region during the scan.
			cluster.PutStore(store.Clone(core.SetStoreStats(&pdpb.StoreStats{RegionCount: 2})))
			updated = true
		}
	}))
	t.Cleanup(func() { re.NoError(failpoint.Disable(point)) })
	re.False(checker.check(cluster))
	re.True(updated)
	re.False(checker.IsPrepared())
	peer = &metapb.Peer{Id: 2, StoreId: 1}
	cluster.PutRegion(core.NewRegionInfo(&metapb.Region{Id: 2, StartKey: []byte("b"), Peers: []*metapb.Peer{peer}}, peer, core.SetSource(core.Heartbeat)))
	re.True(checker.check(cluster))
}

func TestPrepareCheckerRequiresRegionLeaders(t *testing.T) {
	for _, tc := range []struct {
		name           string
		missingLeader  *metapb.Peer
		recoverySource core.RegionSource
		total          int
	}{
		{name: "synced nil leader", recoverySource: core.Sync, total: 100},
		{name: "synced empty leader", missingLeader: &metapb.Peer{}, recoverySource: core.Sync, total: 100},
		{name: "heartbeat recovery", recoverySource: core.Heartbeat, total: 100},
		{name: "upstream count behind cache", recoverySource: core.Sync, total: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			for storeID := uint64(1); storeID <= 2; storeID++ {
				cluster.PutStore(core.NewStoreInfo(
					&metapb.Store{Id: storeID, NodeState: metapb.NodeState_Serving},
					core.SetStoreStats(&pdpb.StoreStats{RegionCount: 100}),
				))
			}
			for i := range 100 {
				peers := []*metapb.Peer{{Id: uint64(2*i + 1), StoreId: 1}, {Id: uint64(2*i + 2), StoreId: 2}}
				leader := tc.missingLeader
				if i < 90 {
					leader = peers[i/50]
				}
				cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
					Id:       uint64(i + 1),
					StartKey: fmt.Appendf(nil, "%03d", i),
					EndKey:   fmt.Appendf(nil, "%03d", i+1),
					Peers:    peers,
				}, leader, core.SetSource(core.Sync)))
			}
			// Metadata is complete, but the missing leaders skew the store counts.
			re.Equal(100, cluster.GetNotFromStorageRegionsCnt())
			re.Equal(50, cluster.GetStoreLeaderCount(1))
			re.Equal(40, cluster.GetStoreLeaderCount(2))
			checker := newPrepareChecker(func() (int, error) { return tc.total, nil })
			re.False(checker.check(cluster))
			re.False(checker.IsPrepared())

			for i := uint64(91); i <= 100; i++ {
				region := cluster.GetRegion(i)
				cluster.PutRegion(region.Clone(core.WithLeader(region.GetPeers()[1]), core.SetSource(tc.recoverySource)))
				re.Equal(i == 100, checker.check(cluster))
				re.Equal(i == 100, checker.IsPrepared())
			}
		})
	}
}

func TestPrepareCheckerRequiresRegionLeadersDuringMerge(t *testing.T) {
	re := require.New(t)
	cluster := core.NewBasicCluster()
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving}))
	regions := make([]*core.RegionInfo, 0, 3)
	for i := range 3 {
		peer := &metapb.Peer{Id: uint64(i + 1), StoreId: 1}
		var leader *metapb.Peer
		if i < 2 {
			leader = peer
		}
		region := core.NewRegionInfo(&metapb.Region{
			Id:          uint64(i + 1),
			StartKey:    []byte{byte(i)},
			EndKey:      []byte{byte(i + 1)},
			RegionEpoch: &metapb.RegionEpoch{Version: 1},
			Peers:       []*metapb.Peer{peer},
		}, leader, core.SetSource(core.Sync))
		cluster.PutRegion(region)
		regions = append(regions, region)
	}
	checker := newPrepareChecker(func() (int, error) { return cluster.GetTotalRegionCount(), nil })
	re.False(checker.check(cluster))
	merged := regions[0].Clone(core.WithEndKey([]byte{2}), core.WithIncVersion(), core.SetSource(core.Heartbeat))
	origin, overlaps, rangeChanged := cluster.SetRegion(merged)
	// Two old leader entries remain while the root tree has already shrunk to two regions.
	re.Equal(2, cluster.GetTotalRegionCount())
	re.Equal(2, cluster.GetStoreLeaderCount(1))
	re.False(checker.check(cluster))
	cluster.UpdateSubTree(merged, origin, overlaps, rangeChanged)
	re.False(checker.check(cluster))
	cluster.PutRegion(regions[2].Clone(core.WithLeader(regions[2].GetPeers()[0]), core.SetSource(core.Heartbeat)))
	re.True(checker.check(cluster))
}

func TestPrepareCheckerRefreshesSchedulerStatistics(t *testing.T) {
	schedulers.Register()
	for _, policy := range []string{"count", "size"} {
		t.Run(policy, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
			cluster.SetLeaderSchedulePolicy(policy)
			for id := uint64(1); id <= 2; id++ {
				cluster.AddLeaderStore(id, 0)
				cluster.PutStore(cluster.GetStore(id).Clone(core.SetNodeState(metapb.NodeState_Serving)))
			}
			for i := range 100 {
				peers := []*metapb.Peer{{Id: uint64(2*i + 1), StoreId: 1}, {Id: uint64(2*i + 2), StoreId: 2}}
				var leader *metapb.Peer
				if i < 80 {
					leader = peers[i/50]
				}
				cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
					Id: uint64(i + 1), StartKey: fmt.Appendf(nil, "%03d", i),
					EndKey: fmt.Appendf(nil, "%03d", i+1), Peers: peers,
				}, leader, core.SetSource(core.Sync), core.SetApproximateSize(1)))
			}
			cluster.UpdateAllStoreStatus()
			re.Equal(50, cluster.GetStore(1).GetLeaderCount())
			re.Equal(30, cluster.GetStore(2).GetLeaderCount())
			checker := newPrepareChecker(func() (int, error) { return 100, nil })
			re.False(checker.check(cluster.GetBasicCluster()))
			for id := uint64(81); id <= 100; id++ {
				region := cluster.GetRegion(id)
				cluster.PutRegion(region.Clone(core.WithLeader(region.GetPeers()[1]), core.SetSource(core.Sync)))
			}
			re.Equal(50, cluster.GetStoreLeaderCount(2))
			re.Equal(30, cluster.GetStore(2).GetLeaderCount())

			oc := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
			lb, err := schedulers.CreateScheduler(types.BalanceLeaderScheduler, oc, cluster.GetStorage(),
				schedulers.ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
			re.NoError(err)
			controller := schedulers.NewScheduleController(ctx, cluster, oc, lb)
			defer controller.Stop()
			// Establish that the stale snapshot can trigger real scheduling.
			re.NotEmpty(controller.Schedule(false))
			re.True(checker.check(cluster.GetBasicCluster()))
			re.Equal(50, cluster.GetStore(2).GetLeaderCount())
			re.Equal(int64(50), cluster.GetStore(2).GetLeaderSize())
			re.Empty(controller.Schedule(false))
		})
	}
}

func TestPrepareCheckerRequiresCurrentRegionSubtrees(t *testing.T) {
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}}
			for id := uint64(1); id <= 2; id++ {
				cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving},
					core.SetStoreStats(&pdpb.StoreStats{RegionCount: 3})))
			}
			regions := make([]*core.RegionInfo, 0, 3)
			for i, end := range []byte{1, 2, 4} {
				leader := peers[0]
				if i == 2 {
					leader = peers[1]
				}
				region := core.NewRegionInfo(&metapb.Region{
					Id: uint64(i + 1), StartKey: []byte{byte(i)}, EndKey: []byte{end},
					RegionEpoch: &metapb.RegionEpoch{Version: 1}, Peers: peers,
				}, leader, core.SetSource(core.Heartbeat))
				cluster.PutRegion(region)
				regions = append(regions, region)
			}
			updates := []*core.RegionInfo{
				regions[0].Clone(core.WithEndKey([]byte{2}), core.WithIncVersion(), core.SetSource(core.Heartbeat)),
				regions[2].Clone(core.WithEndKey([]byte{3}), core.WithIncVersion(), core.SetSource(core.Heartbeat)),
				core.NewRegionInfo(&metapb.Region{Id: 4, StartKey: []byte{3}, EndKey: []byte{4},
					RegionEpoch: &metapb.RegionEpoch{Version: 2}, Peers: peers}, peers[1], core.SetSource(core.Heartbeat)),
			}
			for _, region := range updates {
				_, err := cluster.CheckAndPutRootTree(core.ContextTODO(), region)
				re.NoError(err)
			}
			// Root IDs are 1/3/4 while the three leader-bearing subtree IDs are 1/2/3.
			re.Equal(3, cluster.GetTotalRegionCount())
			re.Equal(2, cluster.GetStoreLeaderCount(1))
			re.Equal(1, cluster.GetStoreLeaderCount(2))
			checker := newPrepareChecker(func() (int, error) { return cluster.GetTotalRegionCount(), nil })
			for i, index := range order {
				re.False(checker.check(cluster))
				cluster.CheckAndPutSubTree(updates[index])
				re.Equal(i == len(order)-1, checker.check(cluster))
			}
			re.Equal(1, cluster.GetStore(1).GetLeaderCount())
			re.Equal(2, cluster.GetStore(2).GetLeaderCount())
		})
	}
}

func TestPrepareCheckerCollectionTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster := core.NewBasicCluster()
		checker := newPrepareChecker(func() (int, error) { return 100, nil })
		re.False(checker.check(cluster))
		time.Sleep(CollectTimeout)
		re.False(checker.check(cluster))
		time.Sleep(time.Nanosecond)
		re.True(checker.check(cluster))
		re.True(checker.IsPrepared())
	})
}

func TestPrepareCheckerLeaderCollectionTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster := core.NewBasicCluster()
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving}))
		cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
			Id:    1,
			Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
		}, nil, core.SetSource(core.Sync)))
		checker := newPrepareChecker(func() (int, error) { return 1, nil })
		re.False(checker.check(cluster))
		time.Sleep(CollectTimeout)
		re.False(checker.check(cluster))
		time.Sleep(time.Nanosecond)
		re.True(checker.check(cluster))
		re.True(checker.IsPrepared())
	})
}
