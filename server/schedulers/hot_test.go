// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/kv"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/statistics"
)

var _ = Suite(&testHotWriteRegionSchedulerSuite{})

type testHotWriteRegionSchedulerSuite struct{}

func (s *testHotWriteRegionSchedulerSuite) TestSchedule(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := mockoption.NewScheduleOptions()
	newTestReplication(opt, 3, "zone", "host")
	tc := mockcluster.NewCluster(opt)
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	opt.HotRegionCacheHitsThreshold = 0

	// Add stores 1, 2, 3, 4, 5, 6  with region counts 3, 2, 2, 2, 0, 0.

	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.SetStoreDown(7)

	//| store_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |       7.5MB      |
	//|    2     |       4.5MB      |
	//|    3     |       4.5MB      |
	//|    4     |        6MB       |
	//|    5     |        0MB       |
	//|    6     |        0MB       |
	tc.UpdateStorageWrittenBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	// Region 1, 2 and 3 are hot regions.
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 3, 4)
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 4)

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	op := hb.Schedule(tc)[0]
	switch op.Len() {
	case 1:
		// balance by leader selected
		testutil.CheckTransferLeaderFrom(c, op, operator.OpHotRegion, 1)
	case 4:
		// balance by peer selected
		if op.RegionID() == 2 {
			// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(c, op, operator.OpHotRegion, 1)
		} else {
			// peer in store 1 of the region 1,3 can only transfer to store 6
			testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 6)
		}
	default:
		c.Fatalf("wrong op: %v", op)
	}

	// hot region scheduler is restricted by `hot-region-schedule-limit`.
	opt.HotRegionScheduleLimit = 0
	c.Assert(hb.Schedule(tc), HasLen, 0)
	opt.HotRegionScheduleLimit = mockoption.NewScheduleOptions().HotRegionScheduleLimit

	// hot region scheduler is restricted by schedule limit.
	opt.LeaderScheduleLimit = 0
	for i := 0; i < 20; i++ {
		op := hb.Schedule(tc)[0]
		c.Assert(op.Len(), Equals, 4)
		if op.RegionID() == 2 {
			// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(c, op, operator.OpHotRegion, 1)
		} else {
			// peer in store 1 of the region 1,3 can only transfer to store 6
			testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 6)
		}
	}
	opt.LeaderScheduleLimit = mockoption.NewScheduleOptions().LeaderScheduleLimit

	// hot region scheduler is not affect by `balance-region-schedule-limit`.
	opt.RegionScheduleLimit = 0
	c.Assert(hb.Schedule(tc), HasLen, 1)
	// Always produce operator
	c.Assert(hb.Schedule(tc), HasLen, 1)
	c.Assert(hb.Schedule(tc), HasLen, 1)

	//| store_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |        6MB       |
	//|    2     |        5MB       |
	//|    3     |        6MB       |
	//|    4     |        3MB       |
	//|    5     |        0MB       |
	//|    6     |        3MB       |
	tc.UpdateStorageWrittenBytes(1, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 3*MB*statistics.StoreHeartBeatReportInterval)

	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        2       |       3        |      512KB    |
	//|     3     |       6      |        1       |       4        |      512KB    |
	//|     4     |       5      |        6       |       4        |      512KB    |
	//|     5     |       3      |        4       |       5        |      512KB    |
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(3, 6, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 4)
	tc.AddLeaderRegionWithWriteInfo(4, 5, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 6, 4)
	tc.AddLeaderRegionWithWriteInfo(5, 3, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 4, 5)

	// 6 possible operator.
	// Assuming different operators have the same possibility,
	// if code has bug, at most 6/7 possibility to success,
	// test 30 times, possibility of success < 0.1%.
	// Cannot transfer leader because store 2 and store 3 are hot.
	// Source store is 1 or 3.
	//   Region 1 and 2 are the same, cannot move peer to store 5 due to the label.
	//   Region 3 can only move peer to store 5.
	//   Region 5 can only move peer to store 6.
	opt.LeaderScheduleLimit = 0
	for i := 0; i < 30; i++ {
		op := hb.Schedule(tc)[0]
		switch op.RegionID() {
		case 1, 2:
			if op.Len() == 3 {
				testutil.CheckTransferPeer(c, op, operator.OpHotRegion, 3, 6)
			} else if op.Len() == 4 {
				testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 6)
			} else {
				c.Fatalf("wrong operator: %v", op)
			}
		case 3:
			testutil.CheckTransferPeer(c, op, operator.OpHotRegion, 1, 5)
		case 5:
			testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 3, 6)
		default:
			c.Fatalf("wrong operator: %v", op)
		}
	}

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
}

var _ = Suite(&testHotReadRegionSchedulerSuite{})

type testHotReadRegionSchedulerSuite struct{}

func (s *testHotReadRegionSchedulerSuite) TestSchedule(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	opt.HotRegionCacheHitsThreshold = 0

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	//| store_id | read_bytes_rate |
	//|----------|-----------------|
	//|    1     |     7.5MB       |
	//|    2     |     4.5MB       |
	//|    3     |     4.5MB       |
	//|    4     |       6MB       |
	//|    5     |       0MB       |
	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 0)

	//| region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	//|-----------|--------------|----------------|----------------|--------------------|
	//|     1     |       1      |        2       |       3        |        512KB       |
	//|     2     |       2      |        1       |       3        |        512KB       |
	//|     3     |       1      |        2       |       3        |        512KB       |
	//|     11    |       1      |        2       |       3        |         24KB       |
	// Region 1, 2 and 3 are hot regions.
	tc.AddLeaderRegionWithReadInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(2, 2, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 3)
	tc.AddLeaderRegionWithReadInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	// lower than hot read flow rate, but higher than write flow rate
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)

	c.Assert(tc.IsRegionHot(tc.GetRegion(1)), IsTrue)
	c.Assert(tc.IsRegionHot(tc.GetRegion(11)), IsFalse)
	// check randomly pick hot region
	r := tc.RandHotRegionFromStore(2, statistics.ReadFlow)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(2))
	// check hot items
	stats := tc.HotCache.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats), Equals, 2)
	for _, ss := range stats {
		for _, s := range ss {
			c.Assert(s.BytesRate, Equals, 512.0*KB)
		}
	}

	// Will transfer a hot region leader from store 1 to store 3.
	// bytes_rate[store 1] * 0.9 > bytes_rate[store 3] + region_bytes_rate
	// hot_region_count[store 3] < hot_regin_count[store 2]
	testutil.CheckTransferLeader(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 3)
	// assume handle the operator
	tc.AddLeaderRegionWithReadInfo(3, 3, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 2)
	// After transfer a hot region leader from store 1 to store 3
	// the three region leader will be evenly distributed in three stores

	//| store_id | read_bytes_rate |
	//|----------|-----------------|
	//|    1     |       6MB       |
	//|    2     |       5MB       |
	//|    3     |       6MB       |
	//|    4     |       3MB       |
	//|    5     |       3MB       |
	tc.UpdateStorageReadBytes(1, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 3*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 3*MB*statistics.StoreHeartBeatReportInterval)

	//| region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	//|-----------|--------------|----------------|----------------|--------------------|
	//|     1     |       1      |        2       |       3        |        512KB       |
	//|     2     |       2      |        1       |       3        |        512KB       |
	//|     3     |       3      |        2       |       1        |        512KB       |
	//|     4     |       1      |        2       |       3        |        512KB       |
	//|     5     |       4      |        2       |       5        |        512KB       |
	//|     11    |       1      |        2       |       3        |         24KB       |
	tc.AddLeaderRegionWithReadInfo(4, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(5, 4, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 5)

	// We will move leader peer of region 1 from 1 to 5
	// Store 1 will be selected as source store (max rate, count > store 4 count).
	// When trying to transfer leader:
	//   Store 2 and store 3 are also hot, failed.
	// Trying to move leader peer:
	//   Store 5 is selected as destination because of less hot region count.
	testutil.CheckTransferPeerWithLeaderTransfer(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 5)

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
}

var _ = Suite(&testHotCacheSuite{})

type testHotCacheSuite struct{}

func (s *testHotCacheSuite) TestUpdateCache(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	// Report store read bytes.
	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 0)

	/// For read flow
	tc.AddLeaderRegionWithReadInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(2, 2, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 3)
	tc.AddLeaderRegionWithReadInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	// lower than hot read flow rate, but higher than write flow rate
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	opt.HotRegionCacheHitsThreshold = 0
	stats := tc.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 1)
	c.Assert(len(stats[3]), Equals, 0)

	tc.AddLeaderRegionWithReadInfo(3, 2, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	stats = tc.RegionStats(statistics.ReadFlow)

	c.Assert(len(stats[1]), Equals, 1)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 0)

	// For write flow
	tc.UpdateStorageWrittenBytes(1, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 3*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.AddLeaderRegionWithWriteInfo(4, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(5, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(6, 1, 12*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)

	stats = tc.RegionStats(statistics.WriteFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 2)

	tc.AddLeaderRegionWithWriteInfo(5, 1, 512*KB*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 5)
	stats = tc.RegionStats(statistics.WriteFlow)

	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 1)
	c.Assert(len(stats[5]), Equals, 1)
}
