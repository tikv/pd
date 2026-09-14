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

package utils

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/pkg/v3/report"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"

	heartbeatconfig "github.com/tikv/pd/tools/pd-heartbeat-bench/config"
)

func TestRegionUpdateRatiosUseTotalRegionCount(t *testing.T) {
	const regionCount = 1_000
	rs := NewRegions(
		regionCount,
		3,
		10,
		&pdpb.RequestHeader{ClusterId: 1},
		WithRegionSize(96*units.MiB),
		WithRegionKeys(960_000),
		WithRandomSeed(42),
	)
	initialIntervalEnds := make([]uint64, regionCount)
	for i, region := range rs.Regions {
		initialIntervalEnds[i] = region.GetInterval().GetEndTimestamp()
	}

	cfg := &heartbeatconfig.Config{
		HotStoreCount:     10,
		ReportRatio:       0.10,
		LeaderUpdateRatio: 0.03,
		EpochUpdateRatio:  0.02,
		SpaceUpdateRatio:  0.04,
		FlowUpdateRatio:   0.05,
	}
	rs.Update(heartbeatconfig.NewOptions(cfg))

	require.Len(t, rs.ReportedRegions(), 100)
	require.Len(t, rs.updateLeader, 30)
	require.Len(t, rs.updateEpoch, 20)
	require.Len(t, rs.updateSpace, 40)
	require.Len(t, rs.updateFlow, 50)

	reportedIndexes := make(map[int]struct{}, 100)
	for _, index := range rs.reportOrder[:100] {
		reportedIndexes[index] = struct{}{}
		require.Equal(t, initialIntervalEnds[index], rs.Regions[index].GetInterval().GetEndTimestamp())
	}
	for _, indexes := range [][]int{rs.updateLeader, rs.updateEpoch, rs.updateSpace, rs.updateFlow} {
		for _, index := range indexes {
			require.Contains(t, reportedIndexes, index)
		}
	}
	for _, index := range rs.reportOrder[100:] {
		require.Equal(t, initialIntervalEnds[index], rs.Regions[index].GetInterval().GetEndTimestamp())
	}

	reportTime := time.Unix(int64(initialIntervalEnds[0]+regionReportInterval), 0)
	rs.PrepareReportIntervals(reportTime)
	for index := range reportedIndexes {
		require.Equal(t, initialIntervalEnds[index], rs.Regions[index].GetInterval().GetStartTimestamp())
		require.Equal(t, uint64(reportTime.Unix()), rs.Regions[index].GetInterval().GetEndTimestamp())
	}
	for _, index := range rs.reportOrder[100:] {
		require.Equal(t, initialIntervalEnds[index], rs.Regions[index].GetInterval().GetEndTimestamp())
	}

	firstReportedIDs := regionIDs(rs.ReportedRegions())
	rs.Update(heartbeatconfig.NewOptions(cfg))
	require.Equal(t, firstReportedIDs, regionIDs(rs.ReportedRegions()))
	for index := range reportedIndexes {
		require.Equal(t, uint64(reportTime.Unix()), rs.Regions[index].GetInterval().GetEndTimestamp())
	}
}

func TestPrepareInitialReportIntervalsUsesRoundStart(t *testing.T) {
	rs := NewRegions(3, 1, 1, &pdpb.RequestHeader{})
	reportTime := time.Unix(1_000, 0)
	rs.PrepareReportIntervals(reportTime)

	for _, region := range rs.Regions {
		require.Equal(t, uint64(940), region.GetInterval().GetStartTimestamp())
		require.Equal(t, uint64(1_000), region.GetInterval().GetEndTimestamp())
	}
}

func TestHotReadFlowCoversRegionReportInterval(t *testing.T) {
	rs := NewRegions(1, 1, 1, &pdpb.RequestHeader{}, WithRandomSeed(42))
	cfg := &heartbeatconfig.Config{
		HotStoreCount:   1,
		ReportRatio:     1,
		FlowUpdateRatio: 1,
	}
	rs.Update(heartbeatconfig.NewOptions(cfg))
	region := rs.ReportedRegions()[0]

	require.GreaterOrEqual(t, region.GetBytesRead(), uint64(hotByteUnit*regionReportInterval))
	require.GreaterOrEqual(t, region.GetKeysRead(), uint64(hotKeysUint*regionReportInterval))
	require.GreaterOrEqual(t, region.GetQueryStats().GetGet(), uint64(hotQueryUnit*regionReportInterval))
}

type cancelingRegionHeartbeatClient struct {
	grpc.ClientStream
	cancel context.CancelFunc
	sends  int
}

func (c *cancelingRegionHeartbeatClient) Send(*pdpb.RegionHeartbeatRequest) error {
	c.sends++
	c.cancel()
	return nil
}

func (*cancelingRegionHeartbeatClient) Recv() (*pdpb.RegionHeartbeatResponse, error) {
	return nil, io.EOF
}

func TestHandleRegionHeartbeatChecksCancellationBeforeEachSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &cancelingRegionHeartbeatClient{cancel: cancel}
	rep := report.NewReport("%.4f")
	statsCh := rep.Stats()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	(&Regions{}).HandleRegionHeartbeat(ctx, wg, client, 1, []*pdpb.RegionHeartbeatRequest{{}, {}}, rep)
	wg.Wait()
	close(rep.Results())
	stats := <-statsCh

	require.Equal(t, 1, client.sends)
	require.Len(t, stats.Lats, 1)
}

func TestGroupReportedRegionsByLeader(t *testing.T) {
	rs := NewRegions(100, 3, 10, &pdpb.RequestHeader{}, WithRandomSeed(7))
	cfg := &heartbeatconfig.Config{ReportRatio: 0.2}
	rs.Update(heartbeatconfig.NewOptions(cfg))

	groups := rs.GroupReportedRegionsByLeader(10)
	total := 0
	for storeID := 1; storeID <= 10; storeID++ {
		for _, region := range groups[storeID] {
			require.Equal(t, uint64(storeID), region.GetLeader().GetStoreId())
			total++
		}
	}
	require.Equal(t, 20, total)
}

func regionIDs(regions []*pdpb.RegionHeartbeatRequest) []uint64 {
	ids := make([]uint64, 0, len(regions))
	for _, region := range regions {
		ids = append(ids, region.GetRegion().GetId())
	}
	return ids
}
