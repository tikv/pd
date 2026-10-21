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
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
)

func prepareHotRegionStoreMetricsTest(t *testing.T) (*mockcluster.Cluster, *Controller, map[uint64]statistics.StoreKindLoads, *prometheus.Registry) {
	t.Helper()
	ResetHotRegionMetrics()
	t.Cleanup(ResetHotRegionMetrics)
	cancel, _, tc, oc := prepareSchedulersTest()
	t.Cleanup(cancel)
	tc.SetClusterVersion(semver.New("9.0.0"))
	storeLoads := make(map[uint64]statistics.StoreKindLoads)
	for id := uint64(1); id <= 2; id++ {
		tc.AddRegionStore(id, 1)
		tc.AddRegionWithReadInfo(id, id, 1<<20, 1<<16, 1000, 10, nil, 5)
		tc.AddLeaderRegionWithWriteInfo(id, id, 1<<20, 1<<16, 1000, 60, nil, 5)
		storeLoads[id] = statistics.StoreKindLoads{
			utils.StoreReadCPU: 100, utils.StoreWriteBytes: 100 << 20, utils.StoreWriteQuery: 1000,
		}
	}
	scheduler := newHotScheduler(oc, initHotRegionScheduleConfig())
	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)
	c.schedulerHandlers[scheduler.GetName()] = scheduler
	for _, rw := range []utils.RWType{utils.Read, utils.Write} {
		c.CollectHotRegionMetrics(rw, tc.GetHotPeerStats(rw), storeLoads)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(topHotRegionMetrics)
	requireHotRegionMetricStores(t, registry, "1", "2")
	return tc, c, storeLoads, registry
}

func requireHotRegionMetricStores(t *testing.T, registry *prometheus.Registry, stores ...string) {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	if len(stores) == 0 {
		require.Empty(t, families)
		return
	}
	require.Len(t, families, 3, "load, Store share and hot duration must all be present")
	var expected []string
	for _, store := range stores {
		for _, typ := range []string{"read-peer", "write-peer", "write-leader"} {
			expected = append(expected, store+"/"+typ)
		}
	}
	for _, family := range families {
		var actual []string
		for _, metric := range family.GetMetric() {
			var store, typ string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "store":
					store = label.GetValue()
				case "type":
					typ = label.GetValue()
				}
			}
			actual = append(actual, store+"/"+typ)
		}
		require.ElementsMatch(t, expected, actual, family.GetName())
	}
}

func TestHotRegionMetricsDeleteStore(t *testing.T) {
	_, _, _, registry := prepareHotRegionStoreMetricsTest(t)
	// Collect copies slice headers before releasing the collector lock. Keep
	// those slices to verify that Store cleanup cannot mutate a running scrape.
	topHotRegionMetrics.RLock()
	snapshots := topHotRegionMetrics.metrics
	readBefore := slices.Clone(snapshots[utils.Read])
	writeBefore := slices.Clone(snapshots[utils.Write])
	topHotRegionMetrics.RUnlock()
	DeleteStoreMetrics("1")
	requireHotRegionMetricStores(t, registry, "2")
	require.Equal(t, readBefore, snapshots[utils.Read])
	require.Equal(t, writeBefore, snapshots[utils.Write])
	DeleteStoreMetrics("1")
	DeleteStoreMetrics("99")
	requireHotRegionMetricStores(t, registry, "2")
	DeleteStoreMetrics("2")
	requireHotRegionMetricStores(t, registry)
}

type hotRegionMetricsStoreRemovalCluster struct {
	*mockcluster.Cluster
	entered, release chan struct{}
	calls            int
}

func (c *hotRegionMetricsStoreRemovalCluster) GetStore(id uint64) *core.StoreInfo {
	store := c.Cluster.GetStore(id)
	if id == 1 {
		c.calls++
		// The first lookup selects the Store; the second builds the selected
		// peer's metrics. Return the captured Store after cleanup to exercise
		// publication of a scan that has already accepted the removed Store.
		if c.calls == 2 {
			close(c.entered)
			<-c.release
		}
	}
	return store
}

func TestHotRegionMetricsStoreRemovalDuringCollection(t *testing.T) {
	for _, fused := range []bool{false, true} {
		for _, rw := range []utils.RWType{utils.Read, utils.Write} {
			for _, tombstone := range []bool{false, true} {
				t.Run(strconv.FormatBool(fused)+"/"+rw.String()+"/tombstone="+strconv.FormatBool(tombstone), func(t *testing.T) {
					tc, c, storeLoads, registry := prepareHotRegionStoreMetricsTest(t)
					cluster := &hotRegionMetricsStoreRemovalCluster{
						Cluster: tc, entered: make(chan struct{}), release: make(chan struct{}),
					}
					c.cluster = cluster
					collected := make(chan struct{})
					unblock := sync.OnceFunc(func() { close(cluster.release) })
					t.Cleanup(func() {
						unblock()
						<-collected
					})
					go func() {
						defer close(collected)
						stats := tc.GetHotPeerStats(rw)
						if fused {
							c.CollectHotRegionMetricsAndStoreStats(rw, tc.GetStores(), stats, storeLoads)
						} else {
							c.CollectHotRegionMetrics(rw, stats, storeLoads)
						}
					}()
					<-cluster.entered
					if tombstone {
						tc.PutStore(tc.GetStore(1).Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
					} else {
						tc.DeleteStore(tc.GetStore(1))
					}
					DeleteStoreMetrics("1")
					unblock()
					<-collected
					requireHotRegionMetricStores(t, registry, "2")
					// A later refresh must still publish surviving Stores normally.
					for _, nextRW := range []utils.RWType{utils.Read, utils.Write} {
						c.CollectHotRegionMetrics(nextRW, tc.GetHotPeerStats(nextRW), storeLoads)
					}
					requireHotRegionMetricStores(t, registry, "2")
				})
			}
		}
	}
}
