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
	"time"

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
			compactExpired, compactWrite := newRegionFlowTasks(region, previousRegion)
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
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, func(region *core.RegionInfo) (func(*HotPeerCache), func(*HotPeerCache)) {
					return newRegionFlowTasks(region, nil)
				})
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

func newWriteTransferTestRegion(id, writtenBytes, interval uint64) *core.RegionInfo {
	peers := make([]*metapb.Peer, 3)
	for i := range peers {
		peers[i] = &metapb.Peer{Id: id*10 + uint64(i) + 1, StoreId: (id+uint64(i))%5 + 1}
	}
	return core.NewRegionInfo(&metapb.Region{
		Id: id, Peers: peers,
		StartKey: []byte(fmt.Sprintf("%08d", id)),
		EndKey:   []byte(fmt.Sprintf("%08d", id+1)),
	}, peers[0], core.SetWrittenBytes(writtenBytes), core.SetReportInterval(0, interval))
}

func newWriteTransferTestCache(t *testing.T) (*HotCache, *core.BasicCluster) {
	t.Helper()
	cluster := core.NewBasicCluster()
	for id := uint64(1); id <= 5; id++ {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: id}))
	}
	return NewHotCache(t.Context(), cluster), cluster
}

func TestColdRegionLeaderTransferDoesNotRaiseWriteThreshold(t *testing.T) {
	for _, historicalBytes := range []uint64{0, 2 << 20} {
		t.Run(fmt.Sprintf("historical-bytes-%d", historicalBytes), func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			// Both heartbeat handlers submit statistics before publishing the region.
			report := func(region *core.RegionInfo) {
				cache.CheckRegionFlowWithOriginAsync(region, cluster.GetRegion(region.GetID()))
				cluster.PutRegion(region)
			}
			countSteadyPeers := func() int {
				count := 0
				for _, peers := range cache.GetHotPeerStats(utils.Write, 3) {
					for _, peer := range peers {
						if peer.RegionID >= 10000 {
							count++
						}
					}
				}
				return count
			}
			steady := make([]*core.RegionInfo, 32)
			for i := range steady {
				steady[i] = newWriteTransferTestRegion(uint64(10000+i), 2400*60, 60)
			}
			for range 12 {
				for _, region := range steady {
					report(region)
				}
			}
			re.Equal(96, countSteadyPeers())

			cold := make([]*core.RegionInfo, 128)
			for i := range cold {
				cold[i] = newWriteTransferTestRegion(uint64(i+1), 0, 60)
				report(cold[i])
			}
			re.Equal(96, countSteadyPeers())
			for _, region := range cold {
				// A returning leader reports writes accumulated before the idle period.
				report(region.Clone(core.WithLeader(region.GetPeers()[1]), core.SetWrittenBytes(historicalBytes), core.SetReportInterval(0, 500)))
			}
			thresholds := make(chan map[uint64]float64, 1)
			re.True(cache.CheckWriteAsync(func(writeCache *HotPeerCache) {
				// Let the usual threshold refresh occur before another cold report.
				for _, threshold := range writeCache.thresholdsOfStore {
					threshold.updatedTime = time.Now().Add(-2 * ThresholdsUpdateInterval)
				}
				rates := make(map[uint64]float64)
				for id := uint64(1); id <= 5; id++ {
					rates[id] = writeCache.calcHotThresholds(id)[utils.ByteDim]
				}
				thresholds <- rates
			}))
			rates := <-thresholds
			t.Logf("historical_bytes=%d thresholds=%v", historicalBytes, rates)
			for _, region := range steady {
				report(region)
			}
			re.Equal(96, countSteadyPeers(), "inactive regions must not cool continuously hot peers after a leader transfer")
			for _, rate := range rates {
				re.Equal(1024.0, rate)
			}
		})
	}
}

func TestWriteRegionLeaderTransferSample(t *testing.T) {
	for _, tc := range []struct {
		name                                                     string
		known, unknownLeader, cached, sameLeader, explicitOrigin bool
	}{
		{name: "cold-transfer", known: true},
		{name: "hot-transfer", known: true, cached: true},
		{name: "new-region"},
		{name: "unknown-leader", known: true, unknownLeader: true},
		{name: "unchanged-leader", known: true, sameLeader: true},
		{name: "cold-transfer-with-origin", known: true, explicitOrigin: true},
		{name: "hot-transfer-with-origin", known: true, cached: true, explicitOrigin: true},
		{name: "new-region-with-origin", explicitOrigin: true},
		{name: "unknown-leader-with-origin", known: true, unknownLeader: true, explicitOrigin: true},
		{name: "unchanged-leader-with-origin", known: true, sameLeader: true, explicitOrigin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			report := func(region *core.RegionInfo) {
				if tc.explicitOrigin {
					cache.CheckRegionFlowWithOriginAsync(region, cluster.GetRegion(region.GetID()))
				} else {
					cache.CheckRegionFlowAsync(region)
				}
			}
			region := newWriteTransferTestRegion(1, 8000*60, 60)
			if tc.known {
				origin := region
				if tc.unknownLeader {
					origin = origin.Clone(core.WithLeader(nil))
				}
				cluster.PutRegion(origin)
			}
			if tc.cached {
				for range 3 {
					report(region)
				}
			}
			old := cache.GetHotPeerStat(utils.Write, region.GetID(), region.GetPeers()[0].GetStoreId())
			oldDegree := 0
			if old != nil {
				oldDegree = old.HotDegree
			}

			// Publish the new leader before the queued sample runs. Transfer
			// detection must use the heartbeat's original metadata snapshot.
			entered, release := make(chan struct{}), make(chan struct{})
			re.True(cache.CheckWriteAsync(func(*HotPeerCache) {
				close(entered)
				select {
				case <-release:
				case <-t.Context().Done():
				}
			}))
			<-entered
			moved := region
			if !tc.sameLeader {
				moved = moved.Clone(core.WithLeader(region.GetPeers()[1]))
			}
			moved = moved.Clone(core.SetWrittenBytes(2<<20), core.SetReportInterval(0, 500))
			report(moved)
			cluster.PutRegion(moved)
			close(release)

			skipped := tc.known && !tc.unknownLeader && !tc.sameLeader
			for _, peer := range moved.GetPeers() {
				stat := cache.GetHotPeerStat(utils.Write, moved.GetID(), peer.GetStoreId())
				switch {
				case skipped && !tc.cached:
					re.Nil(stat, "a stale first sample must not create a cache entry")
				case skipped:
					re.NotNil(stat)
					re.Equal(oldDegree, stat.HotDegree)
					re.Equal(8000.0, stat.GetLoad(utils.ByteDim))
					re.Equal(peer.GetStoreId() == moved.GetLeader().GetStoreId(), stat.IsLeader())
				default:
					re.NotNil(stat, "valid new hot regions must still be admitted")
				}
			}
			fresh := moved.Clone(core.SetWrittenBytes(8000*60), core.SetReportInterval(500, 560))
			report(fresh)
			cluster.PutRegion(fresh)
			stat := cache.GetHotPeerStat(utils.Write, fresh.GetID(), fresh.GetLeader().GetStoreId())
			re.NotNil(stat)
			if tc.cached {
				re.Equal(oldDegree+1, stat.HotDegree, "the second sample must not be skipped again")
			}
			for range 3 {
				report(fresh)
			}
			re.Len(cache.GetHotPeerStats(utils.Write, 3), 3, "fresh sustained writes must become schedulable")
		})
	}
}
