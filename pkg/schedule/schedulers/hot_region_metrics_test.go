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

package schedulers

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestHotRegionTopAgainstFullSort(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100 {
		var top hotRegionTop
		best := make(map[uint64]hotRegionEntry)
		for range 1000 {
			e := hotRegionEntry{regionID: uint64(rng.IntN(100)), storeID: uint64(rng.IntN(10)), load: float64(rng.IntN(100))}
			top.add(e)
			if old, ok := best[e.regionID]; e.load > 0 && (!ok || e.hotter(old)) {
				best[e.regionID] = e
			}
		}
		expected := make([]hotRegionEntry, 0, len(best))
		for _, e := range best {
			expected = append(expected, e)
		}
		slices.SortFunc(expected, func(a, b hotRegionEntry) int {
			if a.hotter(b) {
				return -1
			}
			if b.hotter(a) {
				return 1
			}
			return 0
		})
		require.Equal(t, expected[:hotRegionTopN], top.entries[:top.count])
	}
	var top hotRegionTop
	for _, load := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		top.add(hotRegionEntry{load: load})
	}
	require.Zero(t, top.count)
}

func TestCollectHotRegionMetrics(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	// PD API-service mode uses handlers; scheduling service uses controllers.
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	for id := uint64(1); id <= 20; id++ {
		tc.AddLeaderRegionWithWriteInfo(id, 1, 1<<20, 1<<16, id*1000, 60, []uint64{2}, 5)
	}
	stats := tc.HotCache.GetHotPeerStats(utils.Write, 1)
	c.CollectHotRegionMetrics(utils.Write, stats)
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].GetMetric(), 20)
	for _, metric := range families[0].GetMetric() {
		labels := make(map[string]string)
		for _, label := range metric.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		require.Equal(t, "1", labels["store"])
		switch labels["type"] {
		case "write-leader":
			require.Equal(t, "query", labels["dim"])
		case "write-peer":
			require.Equal(t, "byte", labels["dim"])
		default:
			t.Fatalf("unexpected type: %s", labels["type"])
		}
	}
	// Config replacement removes the old dimension rather than leaving old labels.
	scheduler.conf.Lock()
	scheduler.conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	scheduler.conf.Unlock()
	c.CollectHotRegionMetrics(utils.Write, stats)
	topHotRegionMetrics.RLock()
	snapshot := topHotRegionMetrics.metrics[utils.Write]
	topHotRegionMetrics.RUnlock()
	require.Len(t, snapshot, 20)
	families, err = registry.Gather()
	require.NoError(t, err)
	for _, metric := range families[0].GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "dim" {
				require.NotEqual(t, "query", label.GetValue())
			}
		}
	}
	// No new heartbeat samples: collection never changes hot degree.
	before := stats[1][0].HotDegree
	c.CollectHotRegionMetrics(utils.Write, stats)
	require.Equal(t, before, stats[1][0].HotDegree)
	// No hot peers clears the entire previous snapshot.
	c.CollectHotRegionMetrics(utils.Write, nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	// An unavailable store cannot be represented by stale hot-cache entries.
	tc.PutStore(tc.GetStore(1).Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
	top := selectHotRegions(tc, stats, utils.QueryDim, true)
	require.Zero(t, top.count)
	// Normal controller path, then removal of the scheduler.
	delete(c.schedulerHandlers, scheduler.GetName())
	c.schedulers[scheduler.GetName()] = &ScheduleController{Scheduler: scheduler}
	c.CollectHotRegionMetrics(utils.Write, stats)
	delete(c.schedulers, scheduler.GetName())
	c.CollectHotRegionMetrics(utils.Write, stats)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func TestHotRegionMetricsSustainedAndRead(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.SetHotRegionCacheHitsThreshold(3)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	// A single hot heartbeat does not enter the persistent-hot snapshot.
	tc.AddLeaderRegionWithWriteInfo(1, 1, 1<<20, 1<<16, 1000, 60, []uint64{2}, 1)
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 1<<20, 1<<16, 1000, 60, []uint64{2}, 5)
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write))
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].GetMetric(), 2)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 0, 0, 0, 60, []uint64{2}, 10)
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write))
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	// Read ranks by CPU, including follower peers, taking the hottest replica.
	stats := map[uint64][]*statistics.HotPeerStat{
		1: {{RegionID: 1, Loads: []float64{100, 0, 100, 1}}, {RegionID: 2, Loads: []float64{1, 0, 1, 10}}},
		2: {{RegionID: 1, Loads: []float64{100, 0, 100, 20}}},
	}
	c.CollectHotRegionMetrics(utils.Read, stats)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families[0].GetMetric(), 2)
	top := selectHotRegions(tc, stats, utils.CPUDim, false)
	require.Equal(t, hotRegionEntry{regionID: 1, storeID: 2, load: 20}, top.entries[0])
	tc.PutStore(tc.GetStore(2).Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Hour))))
	top = selectHotRegions(tc, stats, utils.CPUDim, false)
	require.Equal(t, uint64(2), top.entries[0].regionID)
	ResetHotRegionMetrics()
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func TestHotRegionMetricPriorities(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	for _, test := range []struct{ version, read, leader, peer string }{
		{"4.0.0", "byte", "key", "byte"},
		{"8.5.6", "query", "query", "byte"},
		{"9.0.0", "cpu", "query", "byte"},
	} {
		tc.SetClusterVersion(semver.New(test.version))
		require.Equal(t, test.read, scheduler.getPriorities(tc, readPeer)[0])
		require.Equal(t, test.leader, scheduler.getPriorities(tc, writeLeader)[0])
		require.Equal(t, test.peer, scheduler.getPriorities(tc, writePeer)[0])
	}
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.HotScheduleWithQuery))
	scheduler.conf.Lock()
	scheduler.conf.ReadPriorities = []string{utils.KeyPriority, utils.BytePriority}
	scheduler.conf.Unlock()
	require.Equal(t, utils.KeyPriority, scheduler.getPriorities(tc, readPeer)[0])
}

func TestHotRegionCollectorConcurrent(t *testing.T) {
	collector := newHotRegionCollector()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				collector.set(utils.Read, []prometheus.Metric{prometheus.MustNewConstMetric(collector.desc, prometheus.GaugeValue, 1, "read-peer", "cpu", "1", "1")})
				_, err := registry.Gather()
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
}

func BenchmarkSelectHotRegions(b *testing.B) {
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.AddRegionStore(1, 10000)
	peers := make([]*statistics.HotPeerStat, 10000)
	for i := range peers {
		peers[i] = &statistics.HotPeerStat{RegionID: uint64(i + 1), Loads: []float64{float64((i * 7919) % 10000)}}
	}
	stats := map[uint64][]*statistics.HotPeerStat{1: peers}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		selectHotRegions(tc, stats, utils.ByteDim, false)
	}
}
