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
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

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
		require.Greater(t, rs.Regions[index].GetInterval().GetEndTimestamp(), initialIntervalEnds[index])
	}
	for _, indexes := range [][]int{rs.updateLeader, rs.updateEpoch, rs.updateSpace, rs.updateFlow} {
		for _, index := range indexes {
			require.Contains(t, reportedIndexes, index)
		}
	}
	for _, index := range rs.reportOrder[100:] {
		require.Equal(t, initialIntervalEnds[index], rs.Regions[index].GetInterval().GetEndTimestamp())
	}

	firstReportedIDs := regionIDs(rs.ReportedRegions())
	rs.Update(heartbeatconfig.NewOptions(cfg))
	require.Equal(t, firstReportedIDs, regionIDs(rs.ReportedRegions()))
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
