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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestRegionHeartbeatRefreshesReadLeaderBeforeStoreHeartbeat(t *testing.T) {
	re := require.New(t)
	_, cluster, _ := newTestSchedulingServiceForSplit(t)
	region := newAffinitySplitTestRegion().Clone(core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
	for range 5 {
		for _, peer := range region.GetPeers() {
			readBytes := peer.GetStoreId() * (1 << 20) * utils.StoreHeartBeatReportInterval
			re.NoError(cluster.HandleStoreHeartbeat(&schedulingpb.StoreHeartbeatRequest{Stats: &pdpb.StoreStats{
				StoreId:     peer.GetStoreId(),
				Capacity:    100,
				Available:   50,
				RegionCount: 1,
				Interval:    &pdpb.TimeInterval{EndTimestamp: utils.StoreHeartBeatReportInterval},
				PeerStats:   []*pdpb.PeerStat{{RegionId: region.GetID(), ReadBytes: readBytes, ReadKeys: 10240}},
			}}))
		}
	}
	before := cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		re.Len(before[peer.GetStoreId()], 1)
	}

	// A region heartbeat publishes the transfer before another store sample arrives.
	moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), moved))
	re.Equal(moved.GetLeader(), cluster.GetRegion(region.GetID()).GetLeader())
	after := cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(after[storeID], 1)
		stat := after[storeID][0]
		re.Equal(storeID == moved.GetLeader().GetStoreId(), stat.IsLeader())
		re.Equal(float64(storeID)*(1<<20), stat.GetLoad(utils.ByteDim))
		re.Equal(before[storeID][0].GetLoads(), stat.GetLoads())
		re.Equal(before[storeID][0].HotDegree, stat.HotDegree)
		single := cluster.hotStat.GetHotPeerStat(utils.Read, region.GetID(), storeID)
		re.NotNil(single)
		re.Equal(stat.IsLeader(), single.IsLeader())
	}
	summary := statistics.CollectHotPeerInfos(cluster.GetStores(), after)
	re.Zero(summary.AsLeader[region.GetLeader().GetStoreId()].Count)
	re.Equal(1, summary.AsLeader[moved.GetLeader().GetStoreId()].Count)
	re.Equal(float64(2<<20), summary.AsLeader[moved.GetLeader().GetStoreId()].TotalBytesRate)
}
