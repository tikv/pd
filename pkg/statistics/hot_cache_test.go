// Copyright 2024 TiKV Project Authors.
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

package statistics

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestIsHot(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	for i := range utils.RWTypeLen {
		cluster := core.NewBasicCluster()
		cache := NewHotCache(ctx, cluster)
		region, err := buildRegion(cluster, i, 3, 60)
		re.NoError(err)
		loads := make([]float64, utils.RegionStatCount)
		loads[utils.RegionReadBytes] = 100000000
		loads[utils.RegionReadKeys] = 1000
		loads[utils.RegionReadQueryNum] = 1000
		stats := cache.CheckReadPeerSync(region, region.GetPeers(), loads, 60)
		cache.Update(stats[0], i)
		for range 100 {
			re.True(cache.IsRegionHot(region, 1))
		}
	}
}

func TestRegionHeartbeatUpdatesReadLeader(t *testing.T) {
	for _, tc := range []struct {
		name            string
		hotStores       []uint64
		removeOldLeader bool
		noLeader        bool
	}{
		{name: "both-leaders-hot", hotStores: []uint64{1, 2, 3}},
		{name: "only-old-leader-hot", hotStores: []uint64{1, 3}},
		{name: "only-new-leader-hot", hotStores: []uint64{2, 3}},
		{name: "old-leader-removed", hotStores: []uint64{1, 2, 3}, removeOldLeader: true},
		{name: "no-leader", hotStores: []uint64{1, 2, 3}, noLeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			region := newBenchmarkRegion(1, 3)
			for _, peer := range region.GetPeers() {
				cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
			}
			cache := NewHotCache(t.Context(), cluster)
			loads := make([]float64, utils.RegionStatCount)
			loads[utils.RegionReadBytes] = 1 << 20
			loads[utils.RegionReadKeys] = 1024
			loads[utils.RegionReadQueryNum] = 512
			for _, storeID := range tc.hotStores {
				re.True(cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
			before := cache.GetHotPeerStats(utils.Read, 0)
			for _, storeID := range tc.hotStores {
				re.Len(before[storeID], 1)
				re.Equal(storeID == 1, before[storeID][0].IsLeader())
			}

			leader := region.GetStorePeer(2)
			if tc.noLeader {
				leader = nil
			}
			region = region.Clone(core.WithLeader(leader))
			if tc.removeOldLeader {
				region = region.Clone(core.WithRemoveStorePeer(1))
			}
			cache.CheckRegionFlowAsync(region)
			after := cache.GetHotPeerStats(utils.Read, 0)
			for _, storeID := range tc.hotStores {
				oldItem := before[storeID][0]
				re.Equal(storeID == 1, oldItem.IsLeader(), "previously returned stats must stay unchanged")
				if tc.removeOldLeader && storeID == 1 {
					re.Empty(after[storeID])
					continue
				}
				re.Len(after[storeID], 1)
				item := after[storeID][0]
				re.Equal(storeID == leader.GetStoreId(), item.IsLeader())
				re.Equal(oldItem.Loads, item.Loads)
				re.Equal(oldItem.HotDegree, item.HotDegree)
				re.Equal(oldItem.AntiCount, item.AntiCount)
				re.Same(oldItem.rollingLoads[0], item.rollingLoads[0])
				if item.IsLeader() != oldItem.IsLeader() {
					re.True(item.IsNeedCoolDownTransferLeader(3, utils.Read))
				} else {
					re.Same(oldItem, item)
				}
			}
			for _, peer := range region.GetPeers() {
				if before[peer.GetStoreId()] == nil {
					re.Empty(after[peer.GetStoreId()], "region heartbeats must not create read statistics")
				}
			}

			cache.CheckRegionFlowAsync(region)
			re.Equal(after, cache.GetHotPeerStats(utils.Read, 0), "unchanged heartbeats must preserve cooldown")
			for _, storeID := range tc.hotStores {
				if region.GetStorePeer(storeID) == nil {
					continue
				}
				re.True(cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
			for storeID, stats := range cache.GetHotPeerStats(utils.Read, 0) {
				if len(stats) > 0 {
					re.Equal(after[storeID][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
				}
			}
		})
	}
}

func TestRegionHeartbeatUpdatesWriteLeader(t *testing.T) {
	re := require.New(t)
	cluster := core.NewBasicCluster()
	region := newBenchmarkRegion(1, 3)
	for _, peer := range region.GetPeers() {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
	}
	cache := NewHotCache(t.Context(), cluster)
	cache.CheckRegionFlowAsync(region)
	before := cache.GetHotPeerStats(utils.Write, 0)
	region = region.Clone(core.WithLeader(region.GetStorePeer(2)))
	cache.CheckRegionFlowAsync(region)
	after := cache.GetHotPeerStats(utils.Write, 0)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(before[storeID], 1)
		re.Len(after[storeID], 1)
		item := after[storeID][0]
		re.Equal(storeID == 2, item.IsLeader())
		re.True(item.IsNeedCoolDownTransferLeader(3, utils.Write))
		re.Equal(before[storeID][0].HotDegree, item.HotDegree, "the first write sample after a leader transfer must be skipped")
	}
	cache.CheckRegionFlowAsync(region)
	for storeID, stats := range cache.GetHotPeerStats(utils.Write, 0) {
		re.Len(stats, 1)
		re.Equal(after[storeID][0].HotDegree+1, stats[0].HotDegree)
		re.Equal(after[storeID][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
	}
}

func TestRegionFlowTasksMatchOriginalForAnyPeerCount(t *testing.T) {
	for _, peerCount := range []int{0, 1, 2, 3, 5, 9} {
		t.Run(fmt.Sprintf("%d-peers", peerCount), func(t *testing.T) {
			ctx := t.Context()
			previousRegion := newBenchmarkRegion(1, peerCount+1)
			newCache := func() *HotPeerCache {
				cluster := core.NewBasicCluster()
				for _, peer := range previousRegion.GetPeers() {
					cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
				}
				return NewHotPeerCache(ctx, cluster, utils.Write)
			}
			originalCache := newCache()
			compactCache := newCache()

			_, seedWrite := newOriginalRegionFlowTasks(previousRegion)
			seedWrite(originalCache)
			seedWrite(compactCache)
			originalStats := originalCache.GetHotPeerStats(0)
			compactStats := compactCache.GetHotPeerStats(0)
			require.Len(t, originalStats, peerCount+1)
			require.Len(t, compactStats, peerCount+1)
			for _, peer := range previousRegion.GetPeers() {
				require.NotEmpty(t, originalStats[peer.GetStoreId()])
				require.NotEmpty(t, compactStats[peer.GetStoreId()])
			}

			region := newBenchmarkRegion(1, peerCount)

			originalExpired, originalWrite := newOriginalRegionFlowTasks(region)
			compactExpired, compactWrite := newRegionFlowTasks(region)
			originalExpired(originalCache)
			originalWrite(originalCache)
			compactExpired(compactCache)
			compactWrite(compactCache)

			originalStats = originalCache.GetHotPeerStats(0)
			compactStats = compactCache.GetHotPeerStats(0)
			require.Equal(t, originalStats, compactStats)
			require.Equal(t, originalCache.storesOfRegion, compactCache.storesOfRegion)
			require.Equal(t, originalCache.regionsOfStore, compactCache.regionsOfStore)

			expiredStoreID := uint64(peerCount + 1)
			require.Empty(t, originalStats[expiredStoreID])
			require.Empty(t, compactStats[expiredStoreID])
			require.Nil(t, originalCache.getHotPeerStat(region.GetID(), expiredStoreID))
			require.Nil(t, compactCache.getHotPeerStat(region.GetID(), expiredStoreID))
			for _, peer := range region.GetPeers() {
				require.NotEmpty(t, originalStats[peer.GetStoreId()])
				require.NotEmpty(t, compactStats[peer.GetStoreId()])
			}
		})
	}
}

func BenchmarkPendingRegionHeartbeatTasks(b *testing.B) {
	for _, peerCount := range []int{1, 3, 5} {
		b.Run(fmt.Sprintf("%d-peers", peerCount), func(b *testing.B) {
			b.Run("retain-region-info", func(b *testing.B) {
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, newOriginalRegionFlowTasks)
			})
			b.Run("retain-region-meta", func(b *testing.B) {
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, newRegionFlowTasks)
			})
		})
	}
}

func runPendingRegionHeartbeatTaskBenchmark(
	b *testing.B,
	peerCount int,
	newTasks func(*core.RegionInfo) (func(*HotPeerCache), func(*HotPeerCache)),
) {
	const maxPendingHeartbeats = 16 * 1024
	pending := make([][3]func(*HotPeerCache), min(b.N, maxPendingHeartbeats))
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		region := newBenchmarkRegion(uint64(i+1), peerCount)
		checkExpiredTask, checkWritePeerTask := newTasks(region)
		pending[i%len(pending)] = [3]func(*HotPeerCache){
			checkExpiredTask,
			checkExpiredTask,
			checkWritePeerTask,
		}
	}
	b.StopTimer()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	retainedBytes := max(int64(after.HeapAlloc)-int64(before.HeapAlloc), 0)
	b.ReportMetric(float64(retainedBytes)/float64(len(pending)), "retained-B/heartbeat")
	b.ReportMetric(float64(retainedBytes)/float64(len(pending)*3), "retained-B/task")
	runtime.KeepAlive(pending)
}

func newOriginalRegionFlowTasks(region *core.RegionInfo) (checkExpiredTask, checkWritePeerTask func(*HotPeerCache)) {
	checkExpiredTask = func(cache *HotPeerCache) {
		expiredStats := cache.CollectExpiredItems(region)
		for _, stat := range expiredStats {
			cache.UpdateStat(stat)
		}
	}
	checkWritePeerTask = func(cache *HotPeerCache) {
		reportInterval := region.GetInterval()
		interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
		stats := cache.CheckPeerFlow(region, region.GetPeers(), region.GetWriteLoads(), interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
	return checkExpiredTask, checkWritePeerTask
}

func newBenchmarkRegion(regionID uint64, peerCount int) *core.RegionInfo {
	peers := make([]*metapb.Peer, peerCount)
	for i := range peerCount {
		peers[i] = &metapb.Peer{Id: regionID*10 + uint64(i+1), StoreId: uint64(i + 1)}
	}
	var leader *metapb.Peer
	if len(peers) > 0 {
		leader = peers[0]
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:          regionID,
			StartKey:    make([]byte, 32),
			EndKey:      make([]byte, 32),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       peers,
		},
		leader,
		core.SetWrittenBytes(1<<20),
		core.SetWrittenKeys(1024),
		core.SetReportInterval(0, utils.RegionHeartBeatReportInterval),
	)
}
