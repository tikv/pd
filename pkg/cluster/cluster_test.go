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

package cluster

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

type hotCacheBenchmarkCluster struct {
	*mockcluster.Cluster
	coordinator *schedule.Coordinator
}

func newHotCacheBenchmarkCluster(ctx context.Context) *hotCacheBenchmarkCluster {
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	return &hotCacheBenchmarkCluster{
		Cluster:     cluster,
		coordinator: schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true)),
	}
}

func (c *hotCacheBenchmarkCluster) GetHotStat() *statistics.HotStat {
	return c.HotStat
}

func (*hotCacheBenchmarkCluster) GetRegionStats() *statistics.RegionStatistics {
	return nil
}

func (*hotCacheBenchmarkCluster) GetLabelStats() *statistics.LabelStatistics {
	return nil
}

func (c *hotCacheBenchmarkCluster) GetCoordinator() *schedule.Coordinator {
	return c.coordinator
}

func BenchmarkHandleStatsAsync(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	cluster := newHotCacheBenchmarkCluster(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		HandleStatsAsync(cluster, newHotCacheBenchmarkRegion(uint64(i+1)))
	}
	cluster.GetHotStat().GetHotPeerStats(utils.Write, 0)
	cluster.GetHotStat().GetHotPeerStats(utils.Read, 0)
}

func newHotCacheBenchmarkRegion(regionID uint64) *core.RegionInfo {
	peers := []*metapb.Peer{
		{Id: regionID*10 + 1, StoreId: 1},
		{Id: regionID*10 + 2, StoreId: 2},
		{Id: regionID*10 + 3, StoreId: 3},
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:          regionID,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       peers,
		},
		peers[0],
		core.SetReportInterval(0, utils.RegionHeartBeatReportInterval),
	)
}
