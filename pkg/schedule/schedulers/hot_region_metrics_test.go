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
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
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
			if old, ok := best[e.regionID]; !ok || e.hotter(old) {
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
	for _, load := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		top.add(hotRegionEntry{load: load})
	}
	require.Zero(t, top.count)
}

func TestHotRegionTopZeroLoads(t *testing.T) {
	for _, positive := range []bool{false, true} {
		t.Run(strconv.FormatBool(positive), func(t *testing.T) {
			var top hotRegionTop
			for id := uint64(12); id > 0; id-- {
				load := float64(0)
				if positive && id >= 10 {
					load = float64(id)
				}
				top.add(hotRegionEntry{regionID: id, storeID: 2, load: load})
			}
			// Equal zero-load replicas still select the smaller Store ID.
			top.add(hotRegionEntry{regionID: 1, storeID: 1})
			expected := make([]hotRegionEntry, 0, hotRegionTopN)
			if positive {
				for id := uint64(12); id >= 10; id-- {
					expected = append(expected, hotRegionEntry{regionID: id, storeID: 2, load: float64(id)})
				}
			}
			expected = append(expected, hotRegionEntry{regionID: 1, storeID: 1})
			for id := uint64(2); len(expected) < hotRegionTopN; id++ {
				expected = append(expected, hotRegionEntry{regionID: id, storeID: 2})
			}
			require.Equal(t, expected, top.entries[:top.count])
		})
	}
}

func TestHotRegionMetricsZeroFirstDimension(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 12)
	tc.AddRegionStore(2, 12)
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	// These sustained-hot read peers qualify on bytes/query, while the
	// effective first dimension (CPU) is zero for every peer.
	for id := uint64(1); id <= 12; id++ {
		tc.AddRegionWithReadInfo(id, 1, 1<<20, 1<<16, 60000, 10, []uint64{2}, 5)
	}
	stats := tc.GetHotPeerStats(utils.Read)
	require.Len(t, stats[1], 12)
	for _, peer := range stats[1] {
		require.Positive(t, peer.GetLoad(utils.ByteDim))
		require.Zero(t, peer.GetLoad(utils.CPUDim))
	}
	c.CollectHotRegionMetrics(utils.Read, stats, map[uint64]statistics.StoreKindLoads{
		1: {utils.StoreReadCPU: 100},
		2: {utils.StoreReadCPU: 100},
	})
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 3)
	for _, family := range families {
		require.Len(t, family.GetMetric(), hotRegionTopN)
		for _, metric := range family.GetMetric() {
			if family.GetName() == "pd_hotspot_top_region_hot_duration_seconds" {
				require.Equal(t, 50.0, metric.GetGauge().GetValue())
			} else {
				require.Zero(t, metric.GetGauge().GetValue())
			}
		}
	}
	// Region 12 was outside the CPU Top 10. Its history still advances in the
	// hot cache and is retained when a priority change brings it onto the list.
	tc.AddRegionWithReadInfo(12, 1, 1<<30, 1<<16, 60000, 10, []uint64{2}, 5)
	scheduler.conf.Lock()
	scheduler.conf.ReadPriorities = []string{utils.BytePriority, utils.QueryPriority}
	scheduler.conf.Unlock()
	c.CollectHotRegionMetrics(utils.Read, tc.GetHotPeerStats(utils.Read), nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() != "pd_hotspot_top_region_hot_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "region" && label.GetValue() == "12" {
					require.Equal(t, 100.0, metric.GetGauge().GetValue())
					found = true
				}
			}
		}
	}
	require.True(t, found)
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
	backend := storage.NewStorageWithMemoryBackend()
	scheduler.conf.init(scheduler.GetName(), backend, scheduler.conf)
	c := NewController(context.Background(), tc, backend, oc)
	// PD API-service mode uses handlers; scheduling service uses controllers.
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	for id := uint64(1); id <= 20; id++ {
		tc.AddLeaderRegionWithWriteInfo(id, 1, (21-id)<<20, id<<16, id*1000, 60, []uint64{2}, 5)
	}
	stats := tc.HotCache.GetHotPeerStats(utils.Write, 1)
	c.CollectHotRegionMetrics(utils.Write, stats, nil)
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	for _, family := range families {
		require.Len(t, family.GetMetric(), 20)
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			require.Equal(t, "1", labels["store"])
			id, err := strconv.ParseUint(labels["region"], 10, 64)
			require.NoError(t, err)
			switch labels["type"] {
			case "write-leader":
				require.Equal(t, "query", labels["dim"])
				require.Greater(t, id, uint64(10))
			case "write-peer":
				require.Equal(t, "byte", labels["dim"])
				require.LessOrEqual(t, id, uint64(10))
			default:
				t.Fatalf("unexpected type: %s", labels["type"])
			}
		}
	}
	// Updating both write priorities changes their rankings and ratio denominators
	// independently, and replaces samples carrying the previous dimensions.
	response := httptest.NewRecorder()
	scheduler.conf.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/config", strings.NewReader(
		`{"write-leader-priorities":["byte","query"],"write-peer-priorities":["key","byte"]}`)))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	storeLoads := map[uint64]statistics.StoreKindLoads{
		1: {utils.StoreWriteBytes: 100 << 20, utils.StoreWriteKeys: 100 << 16},
	}
	c.CollectHotRegionMetrics(utils.Write, stats, storeLoads)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 3)
	for _, family := range families {
		require.Len(t, family.GetMetric(), 20)
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			id, err := strconv.ParseUint(labels["region"], 10, 64)
			require.NoError(t, err)
			require.Equal(t, "1", labels["store"])
			var load, total float64
			switch labels["type"] {
			case "write-leader":
				require.Equal(t, "byte", labels["dim"])
				require.LessOrEqual(t, id, uint64(10))
				load, total = math.Round(float64((21-id)<<20)/60), 100<<20
			case "write-peer":
				require.Equal(t, "key", labels["dim"])
				require.Greater(t, id, uint64(10))
				load, total = math.Round(float64(id<<16)/60), 100<<16
			default:
				t.Fatalf("unexpected type: %s", labels["type"])
			}
			if family.GetName() == "pd_hotspot_top_region_hot_duration_seconds" {
				load = 300
			} else if family.GetName() == "pd_hotspot_top_region_store_load_ratio" {
				load /= total
			}
			require.InDelta(t, load, metric.GetGauge().GetValue(), 1e-9)
		}
	}
	// Matching write priorities can share each leader's load read.
	peersTop, leadersTop := selectHotRegions(tc, stats, utils.KeyDim, utils.KeyDim)
	require.Equal(t, hotRegionTopN, peersTop.count)
	require.Equal(t, peersTop, leadersTop)
	// No new heartbeat samples: collection never changes hot degree.
	before := stats[1][0].HotDegree
	c.CollectHotRegionMetrics(utils.Write, stats, nil)
	require.Equal(t, before, stats[1][0].HotDegree)
	// No hot peers clears the entire previous snapshot.
	c.CollectHotRegionMetrics(utils.Write, nil, nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	// An unavailable store cannot be represented by stale hot-cache entries.
	tc.PutStore(tc.GetStore(1).Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
	_, top := selectHotRegions(tc, stats, -1, utils.QueryDim)
	require.Zero(t, top.count)
	// Normal controller path, then removal of the scheduler.
	delete(c.schedulerHandlers, scheduler.GetName())
	c.schedulers[scheduler.GetName()] = &ScheduleController{Scheduler: scheduler}
	c.CollectHotRegionMetrics(utils.Write, stats, nil)
	delete(c.schedulers, scheduler.GetName())
	c.CollectHotRegionMetrics(utils.Write, stats, nil)
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
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write), nil)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 1<<20, 1<<16, 1000, 60, []uint64{2}, 5)
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write), nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	require.Len(t, families[0].GetMetric(), 2)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 0, 0, 0, 60, []uint64{2}, 10)
	c.CollectHotRegionMetrics(utils.Write, tc.GetHotPeerStats(utils.Write), nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	// Read ranks by CPU, including follower peers, taking the hottest replica.
	stats := map[uint64][]*statistics.HotPeerStat{
		1: {{RegionID: 1, Loads: []float64{100, 0, 100, 1}}, {RegionID: 2, Loads: []float64{1, 0, 1, 10}}},
		2: {{RegionID: 1, Loads: []float64{100, 0, 100, 20}}},
	}
	c.CollectHotRegionMetrics(utils.Read, stats, nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families[0].GetMetric(), 2)
	top, _ := selectHotRegions(tc, stats, utils.CPUDim, -1)
	require.Equal(t, hotRegionEntry{regionID: 1, storeID: 2, load: 20, peer: stats[2][0]}, top.entries[0])
	tc.PutStore(tc.GetStore(2).Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Hour))))
	top, _ = selectHotRegions(tc, stats, utils.CPUDim, -1)
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

func TestHotRegionMetricsPriorityChange(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 11)
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	backend := storage.NewStorageWithMemoryBackend()
	scheduler.conf.init(scheduler.GetName(), backend, scheduler.conf)
	c := NewController(context.Background(), tc, backend, oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	stats := map[uint64][]*statistics.HotPeerStat{1: {}}
	for id := uint64(1); id <= 11; id++ {
		// CPU and byte loads select different sets of ten Regions.
		stats[1] = append(stats[1], &statistics.HotPeerStat{
			RegionID: id, Loads: []float64{float64(id), 0, float64(2 * id), float64(12 - id)},
		})
	}
	storeLoads := map[uint64]statistics.StoreKindLoads{
		1: {utils.StoreReadBytes: 1000, utils.StoreReadQuery: 10000, utils.StoreReadCPU: 100},
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	for _, test := range []struct {
		name, config, version, dim string
		excluded                   uint64
		total                      float64
	}{
		{name: "default", dim: "cpu", excluded: 11, total: 100},
		{name: "update", config: `{"read-priorities":["byte","cpu"]}`, dim: "byte", excluded: 1, total: 1000},
		{name: "restore", config: `{"read-priorities":["cpu","byte"]}`, dim: "cpu", excluded: 11, total: 100},
		{name: "version fallback", version: "8.5.6", dim: "query", excluded: 1, total: 10000},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.config != "" {
				// Use the same configuration endpoint as pd-ctl.
				response := httptest.NewRecorder()
				scheduler.conf.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/config", strings.NewReader(test.config)))
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			}
			if test.version != "" {
				tc.SetClusterVersion(semver.New(test.version))
			}
			c.CollectHotRegionMetrics(utils.Read, stats, storeLoads)
			families, err := registry.Gather()
			require.NoError(t, err)
			require.Len(t, families, 2)
			for _, family := range families {
				require.Len(t, family.GetMetric(), hotRegionTopN)
				for _, metric := range family.GetMetric() {
					labels := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					require.Equal(t, test.dim, labels["dim"])
					require.Equal(t, "read-peer", labels["type"])
					require.Equal(t, "1", labels["store"])
					id, err := strconv.ParseUint(labels["region"], 10, 64)
					require.NoError(t, err)
					require.NotEqual(t, test.excluded, id)
					load := float64(id)
					switch test.dim {
					case "cpu":
						load = float64(12 - id)
					case "query":
						load = float64(2 * id)
					}
					if family.GetName() == "pd_hotspot_top_region_store_load_ratio" {
						load /= test.total
					}
					require.InDelta(t, load, metric.GetGauge().GetValue(), 1e-9)
				}
			}
		})
	}
}

func TestHotRegionStoreLoadRatio(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 1)
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	stats := map[uint64][]*statistics.HotPeerStat{
		1: {{RegionID: 10, Loads: []float64{0, 0, 0, 25}}},
	}
	storeLoads := map[uint64]statistics.StoreKindLoads{
		1: {utils.StoreReadCPU: 100},
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	c.CollectHotRegionMetrics(utils.Read, stats, storeLoads)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	var ratioFound bool
	for _, family := range families {
		if family.GetName() == "pd_hotspot_top_region_store_load_ratio" {
			ratioFound = true
			require.Len(t, family.GetMetric(), 1)
			require.InDelta(t, 0.25, family.GetMetric()[0].GetGauge().GetValue(), 1e-9)
		}
	}
	require.True(t, ratioFound)

	// A missing or zero total Store load leaves the ratio absent instead of
	// reporting a fabricated zero or infinity.
	storeLoads[1] = statistics.StoreKindLoads{}
	c.CollectHotRegionMetrics(utils.Read, stats, storeLoads)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "pd_hotspot_top_region_load", families[0].GetName())

	for _, test := range []struct {
		typeValue resourceType
		dim       int
		want      utils.StoreLoadKind
	}{
		{readPeer, utils.ByteDim, utils.StoreReadBytes},
		{writeLeader, utils.QueryDim, utils.StoreWriteQuery},
		{writePeer, utils.KeyDim, utils.StoreWriteKeys},
		{readPeer, utils.CPUDim, utils.StoreReadCPU},
	} {
		kind, ok := storeLoadKind(test.typeValue, test.dim, tc.GetStore(1))
		require.True(t, ok)
		require.Equal(t, test.want, kind)
	}
	tc.SetStoreLabel(1, map[string]string{core.EngineKey: core.EngineTiFlash})
	kind, ok := storeLoadKind(writePeer, utils.ByteDim, tc.GetStore(1))
	require.True(t, ok)
	require.Equal(t, utils.StoreRegionsWriteBytes, kind)
	kind, ok = storeLoadKind(writeLeader, utils.KeyDim, tc.GetStore(1))
	require.True(t, ok)
	require.Equal(t, utils.StoreRegionsWriteKeys, kind)
	_, ok = storeLoadKind(writePeer, utils.CPUDim, tc.GetStore(1))
	require.False(t, ok)
}

func TestHotRegionCollectorConcurrent(t *testing.T) {
	collector := newHotRegionCollector()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				collector.set(utils.Read, []prometheus.Metric{prometheus.MustNewConstMetric(collector.loadDesc, prometheus.GaugeValue, 1, "read-peer", "cpu", "1", "1")})
				_, err := registry.Gather()
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
}

type hotRegionMetricsBlockingCluster struct {
	*mockcluster.Cluster
	entered, release chan struct{}
	once             sync.Once
}

func (c *hotRegionMetricsBlockingCluster) GetStore(id uint64) *core.StoreInfo {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.Cluster.GetStore(id)
}

func TestHotRegionMetricsRemovalDuringCollection(t *testing.T) {
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 1)
	cluster := &hotRegionMetricsBlockingCluster{
		Cluster: tc, entered: make(chan struct{}), release: make(chan struct{}),
	}
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	backend := storage.NewStorageWithMemoryBackend()
	scheduler.conf.init(scheduler.GetName(), backend, scheduler.conf)
	c := NewController(context.Background(), cluster, backend, oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	collected := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(cluster.release) })
	t.Cleanup(func() {
		unblock()
		<-collected
	})
	go func() {
		defer close(collected)
		c.CollectHotRegionMetrics(utils.Read, map[uint64][]*statistics.HotPeerStat{
			1: {{RegionID: 1, Loads: []float64{0, 0, 0, 10}}},
		}, nil)
	}()
	<-cluster.entered
	removed := make(chan error, 1)
	go func() { removed <- c.RemoveSchedulerHandler(scheduler.GetName()) }()
	select {
	case err := <-removed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		unblock()
		<-collected
		require.NoError(t, <-removed)
		t.Fatal("scheduler removal waited for the hot Region scan")
	}
	unblock()
	<-collected
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families, "collection must not republish a removed scheduler's metrics")
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
		_, _ = selectHotRegions(tc, stats, utils.ByteDim, -1)
	}
}
