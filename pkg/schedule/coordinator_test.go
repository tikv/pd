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
	"sync"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestHotRegionsConcurrentWithMetrics(t *testing.T) {
	cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	const storeCount = 16
	for id := uint64(1); id <= storeCount; id++ {
		cluster.AddRegionStore(id, 1)
		cluster.HotStat.GetOrCreateRollingStoreStats(id).Set(&pdpb.StoreStats{
			Interval:     &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10},
			BytesRead:    id * 1000,
			BytesWritten: id * 2000,
		})
	}
	coordinator := NewCoordinator(t.Context(), cluster, nil)
	t.Cleanup(coordinator.Stop)
	t.Cleanup(ResetHotSpotMetrics)

	// Hotspot API calls must keep independent Store snapshots while the
	// periodic metrics collector refreshes its reusable buffer.
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		for range 100 {
			coordinator.CollectHotSpotMetrics()
		}
	})
	for _, rw := range []utils.RWType{utils.Read, utils.Write, utils.Read, utils.Write} {
		wg.Go(func() {
			<-start
			for range 100 {
				infos := coordinator.GetHotRegionsByType(rw)
				assert.Len(t, infos.AsPeer, storeCount)
				for id := uint64(1); id <= storeCount; id++ {
					if stat := infos.AsPeer[id]; assert.NotNil(t, stat) {
						expected := float64(id * 100)
						if rw == utils.Write {
							expected *= 2
						}
						assert.Equal(t, expected, stat.StoreByteRate)
					}
				}
			}
		})
	}
	close(start)
	wg.Wait()
}

type hotMetricsTestCluster struct {
	*mockcluster.Cluster
	stats [utils.RWTypeLen]map[uint64][]*statistics.HotPeerStat
	calls [utils.RWTypeLen]int
}

func (c *hotMetricsTestCluster) GetHotPeerStats(rw utils.RWType) map[uint64][]*statistics.HotPeerStat {
	c.calls[rw]++
	return c.stats[rw]
}

func TestCollectHotSpotMetricsUsesOneSnapshot(t *testing.T) {
	t.Cleanup(ResetHotSpotMetrics)
	schedulers.Register()
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetClusterVersion(semver.New("9.0.0"))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	for id := uint64(1); id <= 20; id++ {
		tc.AddLeaderRegionWithWriteInfo(id, 1, id<<20, id<<16, 60000, 60, []uint64{2}, 5)
		tc.AddRegionWithReadInfo(id+20, 1, id<<20, id<<16, 60000, 10, []uint64{2}, 5)
	}
	cluster := &hotMetricsTestCluster{Cluster: tc}
	for _, rw := range []utils.RWType{utils.Read, utils.Write} {
		cluster.stats[rw] = tc.GetHotPeerStats(rw)
	}
	coordinator := NewCoordinator(t.Context(), cluster, nil)
	t.Cleanup(coordinator.Stop)
	scheduler, err := schedulers.CreateScheduler(types.BalanceHotRegionScheduler, coordinator.opController, tc.GetStorage(), schedulers.ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
	require.NoError(t, err)
	require.NoError(t, coordinator.schedulers.AddSchedulerHandler(scheduler))
	coordinator.CollectHotSpotMetrics()
	require.Equal(t, [utils.RWTypeLen]int{1, 1}, cluster.calls)
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() == "pd_hotspot_top_region_load" {
			require.Len(t, family.GetMetric(), 30)
			found = true
		}
	}
	require.True(t, found, "the periodic path must publish all three Top 10 lists")
}
