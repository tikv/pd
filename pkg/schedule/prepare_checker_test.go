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

	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

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
	// Pooled timers cannot cross synctest bubble boundaries.
	deadlock.Opts.TimerPool = deadlock.TimerPoolDisabled
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
			cluster.UpdateAllStoreStatus()
			re.Equal(tc.prepared, checker.check(cluster))
			re.Equal(tc.prepared, checker.IsPrepared())
		})
	}
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
			cluster.UpdateAllStoreStatus()
			re.False(checker.check(cluster))
			re.False(checker.IsPrepared())

			for i := uint64(91); i <= 100; i++ {
				region := cluster.GetRegion(i)
				cluster.PutRegion(region.Clone(core.WithLeader(region.GetPeers()[1]), core.SetSource(tc.recoverySource)))
				cluster.UpdateAllStoreStatus()
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
	cluster.UpdateAllStoreStatus()
	re.False(checker.check(cluster))
	merged := regions[0].Clone(core.WithEndKey([]byte{2}), core.WithIncVersion(), core.SetSource(core.Heartbeat))
	origin, overlaps, rangeChanged := cluster.SetRegion(merged)
	// Two old leader entries remain while the root tree has already shrunk to two regions.
	re.Equal(2, cluster.GetTotalRegionCount())
	re.Equal(2, cluster.GetStoreLeaderCount(1))
	cluster.UpdateAllStoreStatus()
	re.False(checker.check(cluster))
	cluster.UpdateSubTree(merged, origin, overlaps, rangeChanged)
	cluster.UpdateAllStoreStatus()
	re.False(checker.check(cluster))
	cluster.PutRegion(regions[2].Clone(core.WithLeader(regions[2].GetPeers()[0]), core.SetSource(core.Heartbeat)))
	cluster.UpdateAllStoreStatus()
	re.True(checker.check(cluster))
}

func TestPrepareCheckerWaitsForSchedulerStatistics(t *testing.T) {
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
			re.False(checker.check(cluster.GetBasicCluster()))
			re.Equal(30, cluster.GetStore(2).GetLeaderCount(), "preparation must not publish statistics")
			cluster.UpdateAllStoreStatus()
			re.True(checker.check(cluster.GetBasicCluster()))
			re.Equal(50, cluster.GetStore(2).GetLeaderCount())
			re.Equal(int64(50), cluster.GetStore(2).GetLeaderSize())
			re.Empty(controller.Schedule(false))
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
		re.True(checker.AreStoresPrepared())
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
		re.True(checker.AreStoresPrepared())
	})
}

func TestPrepareCheckerStatisticsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster := core.NewBasicCluster()
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving}))
		cluster.PutRegion(core.NewTestRegionInfo(1, 1, nil, nil, core.SetSource(core.Heartbeat)))
		checker := newPrepareChecker(func() (int, error) { return 1, nil })
		re.False(checker.check(cluster))
		time.Sleep(CollectTimeout)
		re.False(checker.check(cluster))
		time.Sleep(time.Nanosecond)
		re.True(checker.check(cluster))
		re.True(checker.AreStoresPrepared())
		re.Zero(cluster.GetStore(1).GetLeaderCount(), "the timeout must not depend on a statistics refresh")
	})
}

func TestPrepareCheckerAllowsStatisticsLag(t *testing.T) {
	re := require.New(t)
	cluster := core.NewBasicCluster()
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving}))
	region := core.NewTestRegionInfo(1, 1, nil, nil, core.SetSource(core.Heartbeat), core.SetApproximateSize(10))
	cluster.PutRegion(region)
	cluster.UpdateAllStoreStatus()
	cluster.PutRegion(region.Clone(core.SetSource(core.Heartbeat), core.SetApproximateSize(20)))
	checker := newPrepareChecker(func() (int, error) { return 1, nil })
	re.True(checker.check(cluster))
	re.Equal(int64(10), cluster.GetStore(1).GetLeaderSize())
	re.Equal(int64(20), cluster.GetStoreLeaderRegionSize(1))
}
