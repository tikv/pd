// Copyright 2019 TiKV Project Authors.
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
	"encoding/hex"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/versioninfo"
)

func init() {
	schedulePeerPr = 1.0
}

var _ = Suite(&testHotWriteRegionSchedulerSuite{})
var _ = Suite(&testHotSchedulerSuite{})

type testHotSchedulerSuite struct{}

func (s *testHotSchedulerSuite) TestGCPendingOpInfos(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	for id := uint64(1); id <= 10; id++ {
		tc.PutStoreWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(HotRegionType, schedule.NewOperatorController(ctx, tc, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigJSONDecoder([]byte("null")))
	c.Assert(err, IsNil)
	hb := sche.(*hotScheduler)

	nilOp := func(region *core.RegionInfo, ty opType) *operator.Operator {
		return nil
	}
	notDoneOp := func(region *core.RegionInfo, ty opType) *operator.Operator {
		var op *operator.Operator
		var err error
		switch ty {
		case movePeer:
			op, err = operator.CreateMovePeerOperator("move-peer-test", tc, region, operator.OpAdmin, 2, &metapb.Peer{Id: region.GetID()*10000 + 1, StoreId: 4})
		case transferLeader:
			op, err = operator.CreateTransferLeaderOperator("transfer-leader-test", tc, region, 1, 2, operator.OpAdmin)
		}
		c.Assert(err, IsNil)
		c.Assert(op, NotNil)
		return op
	}
	doneOp := func(region *core.RegionInfo, ty opType) *operator.Operator {
		op := notDoneOp(region, ty)
		op.Cancel()
		return op
	}
	shouldRemoveOp := func(region *core.RegionInfo, ty opType) *operator.Operator {
		op := doneOp(region, ty)
		operator.SetOperatorStatusReachTime(op, operator.CREATED, time.Now().Add(-3*statistics.StoreHeartBeatReportInterval*time.Second))
		return op
	}
	opCreaters := [4]func(region *core.RegionInfo, ty opType) *operator.Operator{nilOp, shouldRemoveOp, notDoneOp, doneOp}

	for i := 0; i < len(opCreaters); i++ {
		for j := 0; j < len(opCreaters); j++ {
			regionID := uint64(i*len(opCreaters) + j + 1)
			region := newTestRegion(regionID)
			hb.regionPendings[regionID] = [2]*operator.Operator{
				movePeer:       opCreaters[i](region, movePeer),
				transferLeader: opCreaters[j](region, transferLeader),
			}
		}
	}

	hb.gcRegionPendings()

	for i := 0; i < len(opCreaters); i++ {
		for j := 0; j < len(opCreaters); j++ {
			regionID := uint64(i*len(opCreaters) + j + 1)
			if i < 2 && j < 2 {
				c.Assert(hb.regionPendings, Not(HasKey), regionID)
			} else if i < 2 {
				c.Assert(hb.regionPendings, HasKey, regionID)
				c.Assert(hb.regionPendings[regionID][movePeer], IsNil)
				c.Assert(hb.regionPendings[regionID][transferLeader], NotNil)
			} else if j < 2 {
				c.Assert(hb.regionPendings, HasKey, regionID)
				c.Assert(hb.regionPendings[regionID][movePeer], NotNil)
				c.Assert(hb.regionPendings[regionID][transferLeader], IsNil)
			} else {
				c.Assert(hb.regionPendings, HasKey, regionID)
				c.Assert(hb.regionPendings[regionID][movePeer], NotNil)
				c.Assert(hb.regionPendings[regionID][transferLeader], NotNil)
			}
		}
	}
}

func newTestRegion(id uint64) *core.RegionInfo {
	peers := []*metapb.Peer{{Id: id*100 + 1, StoreId: 1}, {Id: id*100 + 2, StoreId: 2}, {Id: id*100 + 3, StoreId: 3}}
	return core.NewRegionInfo(&metapb.Region{Id: id, Peers: peers}, peers[0])
}

type testHotWriteRegionSchedulerSuite struct{}

func (s *testHotWriteRegionSchedulerSuite) TestByteRateOnly(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.SetHotRegionCacheHitsThreshold(0)

	s.checkByteRateOnly(c, tc, opt, hb)
	tc.SetEnablePlacementRules(true)
	s.checkByteRateOnly(c, tc, opt, hb)
}

func (s *testHotWriteRegionSchedulerSuite) checkByteRateOnly(c *C, tc *mockcluster.Cluster, opt *config.PersistOptions, hb schedule.Scheduler) {
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
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{1, 3, 4}, 512 * KB, 0},
		{3, []uint64{1, 2, 4}, 512 * KB, 0},
	})

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	op := hb.Schedule(tc)[0]
	hb.(*hotScheduler).clearPendingInfluence()
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
	tc.SetHotRegionScheduleLimit(0)
	c.Assert(hb.Schedule(tc), HasLen, 0)
	hb.(*hotScheduler).clearPendingInfluence()
	tc.SetHotRegionScheduleLimit(int(config.NewTestOptions().GetScheduleConfig().HotRegionScheduleLimit))

	// hot region scheduler is restricted by schedule limit.
	tc.SetLeaderScheduleLimit(0)
	for i := 0; i < 20; i++ {
		op := hb.Schedule(tc)[0]
		hb.(*hotScheduler).clearPendingInfluence()
		c.Assert(op.Len(), Equals, 4)
		if op.RegionID() == 2 {
			// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(c, op, operator.OpHotRegion, 1)
		} else {
			// peer in store 1 of the region 1,3 can only transfer to store 6
			testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 6)
		}
	}
	tc.SetLeaderScheduleLimit(int(config.NewTestOptions().GetScheduleConfig().LeaderScheduleLimit))

	// hot region scheduler is not affect by `balance-region-schedule-limit`.
	tc.SetRegionScheduleLimit(0)
	c.Assert(hb.Schedule(tc), HasLen, 1)
	hb.(*hotScheduler).clearPendingInfluence()
	// Always produce operator
	c.Assert(hb.Schedule(tc), HasLen, 1)
	hb.(*hotScheduler).clearPendingInfluence()
	c.Assert(hb.Schedule(tc), HasLen, 1)
	hb.(*hotScheduler).clearPendingInfluence()

	//| store_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |        6MB       |
	//|    2     |        5MB       |
	//|    3     |        6MB       |
	//|    4     |        3.1MB     |
	//|    5     |        0MB       |
	//|    6     |        3MB       |
	tc.UpdateStorageWrittenBytes(1, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3.1*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 3*MB*statistics.StoreHeartBeatReportInterval)

	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        2       |       3        |      512KB    |
	//|     3     |       6      |        1       |       4        |      512KB    |
	//|     4     |       5      |        6       |       4        |      512KB    |
	//|     5     |       3      |        4       |       5        |      512KB    |
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{1, 2, 3}, 512 * KB, 0},
		{3, []uint64{6, 1, 4}, 512 * KB, 0},
		{4, []uint64{5, 6, 4}, 512 * KB, 0},
		{5, []uint64{3, 4, 5}, 512 * KB, 0},
	})

	// 6 possible operator.
	// Assuming different operators have the same possibility,
	// if code has bug, at most 6/7 possibility to success,
	// test 30 times, possibility of success < 0.1%.
	// Cannot transfer leader because store 2 and store 3 are hot.
	// Source store is 1 or 3.
	//   Region 1 and 2 are the same, cannot move peer to store 5 due to the label.
	//   Region 3 can only move peer to store 5.
	//   Region 5 can only move peer to store 6.
	tc.SetLeaderScheduleLimit(0)
	for i := 0; i < 30; i++ {
		op := hb.Schedule(tc)[0]
		hb.(*hotScheduler).clearPendingInfluence()
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
	hb.(*hotScheduler).clearPendingInfluence()
}

func (s *testHotWriteRegionSchedulerSuite) TestWithKeyRate(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.8*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*MB*statistics.StoreHeartBeatReportInterval, 9*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 8.9*MB*statistics.StoreHeartBeatReportInterval, 9.2*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{3, []uint64{2, 4, 3}, 0.05 * MB, 0.1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
		testutil.CheckTransferPeer(c, op, operator.OpHotRegion, 1, 4)
		// store byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
		// store key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

		op = hb.Schedule(tc)[0]
		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
		testutil.CheckTransferPeer(c, op, operator.OpHotRegion, 3, 5)
		// store byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
		// store key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

		// byteDecRatio <= 0.95
		// op = hb.Schedule(tc)[0]
		// FIXME: cover this case
		// testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 5)
		// store byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
		// store key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
	}
}

func (s *testHotWriteRegionSchedulerSuite) TestUnhealthyStore(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 0*MB*statistics.StoreHeartBeatReportInterval, 0*MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 0.5 * MB},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{3, []uint64{3, 2, 1}, 0.5 * MB, 0.5 * MB},
	})

	intervals := []time.Duration{
		9 * time.Second,
		10 * time.Second,
		19 * time.Second,
		20 * time.Second,
		9 * time.Minute,
		10 * time.Minute,
		29 * time.Minute,
		30 * time.Minute,
	}
	// test dst
	for _, interval := range intervals {
		tc.SetStoreLastHeartbeatInterval(4, interval)
		hb.(*hotScheduler).clearPendingInfluence()
		hb.Schedule(tc)
		// no panic
	}
}

func (s *testHotWriteRegionSchedulerSuite) TestLeader(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{6, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB},
		{7, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		testutil.CheckTransferLeaderFrom(c, op, operator.OpHotRegion, 2)

		c.Assert(hb.Schedule(tc), HasLen, 0)
	}
}

func (s *testHotWriteRegionSchedulerSuite) TestWithPendingInfluence(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	for i := 0; i < 2; i++ {
		// 0: byte rate
		// 1: key rate
		tc := mockcluster.NewCluster(opt)
		tc.SetHotRegionCacheHitsThreshold(0)
		tc.SetLeaderScheduleLimit(0)
		tc.DisableFeature(versioninfo.JointConsensus)
		tc.AddRegionStore(1, 20)
		tc.AddRegionStore(2, 20)
		tc.AddRegionStore(3, 20)
		tc.AddRegionStore(4, 20)

		updateStore := tc.UpdateStorageWrittenBytes // byte rate
		if i == 1 {                                 // key rate
			updateStore = tc.UpdateStorageWrittenKeys
		}
		updateStore(1, 8*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(2, 6*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(3, 6*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(4, 4*MB*statistics.StoreHeartBeatReportInterval)

		if i == 0 { // byte rate
			addRegionInfo(tc, write, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 512 * KB, 0},
				{2, []uint64{1, 2, 3}, 512 * KB, 0},
				{3, []uint64{1, 2, 3}, 512 * KB, 0},
				{4, []uint64{1, 2, 3}, 512 * KB, 0},
				{5, []uint64{1, 2, 3}, 512 * KB, 0},
				{6, []uint64{1, 2, 3}, 512 * KB, 0},
			})
		} else if i == 1 { // key rate
			addRegionInfo(tc, write, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 0, 512 * KB},
				{2, []uint64{1, 2, 3}, 0, 512 * KB},
				{3, []uint64{1, 2, 3}, 0, 512 * KB},
				{4, []uint64{1, 2, 3}, 0, 512 * KB},
				{5, []uint64{1, 2, 3}, 0, 512 * KB},
				{6, []uint64{1, 2, 3}, 0, 512 * KB},
			})
		}

		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()
			cnt := 0
		testLoop:
			for j := 0; j < 1000; j++ {
				c.Assert(cnt, LessEqual, 5)
				emptyCnt := 0
				ops := hb.Schedule(tc)
				for len(ops) == 0 {
					emptyCnt++
					if emptyCnt >= 10 {
						break testLoop
					}
					ops = hb.Schedule(tc)
				}
				op := ops[0]
				switch op.Len() {
				case 1:
					// balance by leader selected
					testutil.CheckTransferLeaderFrom(c, op, operator.OpHotRegion, 1)
				case 4:
					// balance by peer selected
					testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 4)
					cnt++
					if cnt == 3 {
						c.Assert(op.Cancel(), IsTrue)
					}
				default:
					c.Fatalf("wrong op: %v", op)
				}
			}
			c.Assert(cnt, Equals, 4)
		}
	}
}

func (s *testHotWriteRegionSchedulerSuite) TestWithRuleEnabled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetEnablePlacementRules(true)
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.SetHotRegionCacheHitsThreshold(0)
	key, err := hex.DecodeString("")
	c.Assert(err, IsNil)
	err = tc.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "leader",
		Index:    1,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "ID",
				Op:     placement.In,
				Values: []string{"2", "1"},
			},
		},
		StartKey: key,
		EndKey:   key,
	})
	c.Assert(err, IsNil)
	err = tc.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "voter",
		Index:    2,
		Override: false,
		Role:     placement.Voter,
		Count:    2,
		StartKey: key,
		EndKey:   key,
	})
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{6, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{7, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// The targetID should always be 1 as leader is only allowed to be placed in store1 or store2 by placement rule
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 2, 1)
		c.Assert(hb.Schedule(tc), HasLen, 0)
	}
}

var _ = Suite(&testHotReadRegionSchedulerSuite{})

type testHotReadRegionSchedulerSuite struct{}

func (s *testHotReadRegionSchedulerSuite) TestByteRateOnly(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.SetHotRegionCacheHitsThreshold(0)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	//| store_id | read_bytes_rate |
	//|----------|-----------------|
	//|    1     |     7.5MB       |
	//|    2     |     4.9MB       |
	//|    3     |     3.7MB       |
	//|    4     |       6MB       |
	//|    5     |       0MB       |
	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.9*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 3.7*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 0)

	//| region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	//|-----------|--------------|----------------|----------------|--------------------|
	//|     1     |       1      |        2       |       3        |        512KB       |
	//|     2     |       2      |        1       |       3        |        512KB       |
	//|     3     |       1      |        2       |       3        |        512KB       |
	//|     11    |       1      |        2       |       3        |          7KB       |
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{2, 1, 3}, 512 * KB, 0},
		{3, []uint64{1, 2, 3}, 512 * KB, 0},
		{11, []uint64{1, 2, 3}, 7 * KB, 0},
	})

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
			c.Assert(s.ByteRate, Equals, 512.0*KB)
		}
	}

	testutil.CheckTransferLeader(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 3)
	hb.(*hotScheduler).clearPendingInfluence()
	// assume handle the operator
	tc.AddLeaderRegionWithReadInfo(3, 3, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{1, 2})
	// After transfer a hot region leader from store 1 to store 3
	// the three region leader will be evenly distributed in three stores

	//| store_id | read_bytes_rate |
	//|----------|-----------------|
	//|    1     |       6MB       |
	//|    2     |       5.5MB     |
	//|    3     |       5.5MB     |
	//|    4     |       3.4MB     |
	//|    5     |       3MB       |
	tc.UpdateStorageReadBytes(1, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 5.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 5.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 3.4*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 3*MB*statistics.StoreHeartBeatReportInterval)

	//| region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	//|-----------|--------------|----------------|----------------|--------------------|
	//|     1     |       1      |        2       |       3        |        512KB       |
	//|     2     |       2      |        1       |       3        |        512KB       |
	//|     3     |       3      |        2       |       1        |        512KB       |
	//|     4     |       1      |        2       |       3        |        512KB       |
	//|     5     |       4      |        2       |       5        |        512KB       |
	//|     11    |       1      |        2       |       3        |         24KB       |
	addRegionInfo(tc, read, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 512 * KB, 0},
		{5, []uint64{4, 2, 5}, 512 * KB, 0},
	})

	// We will move leader peer of region 1 from 1 to 5
	testutil.CheckTransferPeerWithLeaderTransfer(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 5)
	hb.(*hotScheduler).clearPendingInfluence()

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
	hb.(*hotScheduler).clearPendingInfluence()
}

func (s *testHotReadRegionSchedulerSuite) TestWithKeyRate(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageReadStats(1, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.8*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 9*MB*statistics.StoreHeartBeatReportInterval, 9*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(5, 8.9*MB*statistics.StoreHeartBeatReportInterval, 9.2*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB},
		{2, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB},
		{3, []uint64{3, 4, 5}, 0.05 * MB, 0.1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 1, 4)
		// store byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
		// store key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

		op = hb.Schedule(tc)[0]
		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 3, 5)
		// store byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
		// store key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

		// byteDecRatio <= 0.95
		// FIXME: cover this case
		// op = hb.Schedule(tc)[0]
		// testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 5)
		// store byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
		// store key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
	}
}

func (s *testHotReadRegionSchedulerSuite) TestWithPendingInfluence(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	// For test
	hb.(*hotScheduler).conf.GreatDecRatio = 0.99
	hb.(*hotScheduler).conf.MinorDecRatio = 1
	hb.(*hotScheduler).conf.DstToleranceRatio = 1

	for i := 0; i < 2; i++ {
		// 0: byte rate
		// 1: key rate
		tc := mockcluster.NewCluster(opt)
		tc.SetHotRegionCacheHitsThreshold(0)
		tc.DisableFeature(versioninfo.JointConsensus)
		tc.AddRegionStore(1, 20)
		tc.AddRegionStore(2, 20)
		tc.AddRegionStore(3, 20)
		tc.AddRegionStore(4, 20)

		updateStore := tc.UpdateStorageReadBytes // byte rate
		if i == 1 {                              // key rate
			updateStore = tc.UpdateStorageReadKeys
		}
		updateStore(1, 7.1*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(2, 6.1*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(3, 6*MB*statistics.StoreHeartBeatReportInterval)
		updateStore(4, 5*MB*statistics.StoreHeartBeatReportInterval)

		if i == 0 { // byte rate
			addRegionInfo(tc, read, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 512 * KB, 0},
				{2, []uint64{1, 2, 3}, 512 * KB, 0},
				{3, []uint64{1, 2, 3}, 512 * KB, 0},
				{4, []uint64{1, 2, 3}, 512 * KB, 0},
				{5, []uint64{2, 1, 3}, 512 * KB, 0},
				{6, []uint64{2, 1, 3}, 512 * KB, 0},
				{7, []uint64{3, 2, 1}, 512 * KB, 0},
				{8, []uint64{3, 2, 1}, 512 * KB, 0},
			})
		} else if i == 1 { // key rate
			addRegionInfo(tc, read, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 0, 512 * KB},
				{2, []uint64{1, 2, 3}, 0, 512 * KB},
				{3, []uint64{1, 2, 3}, 0, 512 * KB},
				{4, []uint64{1, 2, 3}, 0, 512 * KB},
				{5, []uint64{2, 1, 3}, 0, 512 * KB},
				{6, []uint64{2, 1, 3}, 0, 512 * KB},
				{7, []uint64{3, 2, 1}, 0, 512 * KB},
				{8, []uint64{3, 2, 1}, 0, 512 * KB},
			})
		}

		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()

			op1 := hb.Schedule(tc)[0]
			testutil.CheckTransferLeader(c, op1, operator.OpLeader, 1, 3)
			// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

			op2 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeerWithLeaderTransfer(c, op2, operator.OpHotRegion, 1, 4)
			// store byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

			ops := hb.Schedule(tc)
			c.Logf("%v", ops)
			c.Assert(ops, HasLen, 0)
		}
		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()

			op1 := hb.Schedule(tc)[0]
			testutil.CheckTransferLeader(c, op1, operator.OpLeader, 1, 3)
			// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

			op2 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeerWithLeaderTransfer(c, op2, operator.OpHotRegion, 1, 4)
			// store bytekey rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)
			c.Assert(op2.Cancel(), IsTrue)
			// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

			op2 = hb.Schedule(tc)[0]
			testutil.CheckTransferPeerWithLeaderTransfer(c, op2, operator.OpHotRegion, 1, 4)
			// store byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

			c.Assert(op1.Cancel(), IsTrue)
			// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

			op3 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeerWithLeaderTransfer(c, op3, operator.OpHotRegion, 1, 4)
			// store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

			ops := hb.Schedule(tc)
			c.Assert(ops, HasLen, 0)
		}
	}
}

var _ = Suite(&testHotCacheSuite{})

type testHotCacheSuite struct{}

func (s *testHotCacheSuite) TestUpdateCache(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)

	/// For read flow
	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{2, 1, 3}, 512 * KB, 0},
		{3, []uint64{1, 2, 3}, 20 * KB, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{11, []uint64{1, 2, 3}, 7 * KB, 0},
	})
	stats := tc.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 1)
	c.Assert(len(stats[3]), Equals, 0)

	addRegionInfo(tc, read, []testRegionInfo{
		{3, []uint64{2, 1, 3}, 20 * KB, 0},
		{11, []uint64{1, 2, 3}, 7 * KB, 0},
	})
	stats = tc.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats[1]), Equals, 1)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 0)

	addRegionInfo(tc, write, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 512 * KB, 0},
		{5, []uint64{1, 2, 3}, 20 * KB, 0},
		{6, []uint64{1, 2, 3}, 0.8 * KB, 0},
	})
	stats = tc.RegionStats(statistics.WriteFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 2)

	addRegionInfo(tc, write, []testRegionInfo{
		{5, []uint64{1, 2, 5}, 20 * KB, 0},
	})
	stats = tc.RegionStats(statistics.WriteFlow)

	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 1)
	c.Assert(len(stats[5]), Equals, 1)
}

func (s *testHotCacheSuite) TestKeyThresholds(c *C) {
	opt := config.NewTestOptions()
	{ // only a few regions
		tc := mockcluster.NewCluster(opt)
		tc.SetHotRegionCacheHitsThreshold(0)
		addRegionInfo(tc, read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 1},
			{2, []uint64{1, 2, 3}, 0, 1 * KB},
		})
		stats := tc.RegionStats(statistics.ReadFlow)
		c.Assert(stats[1], HasLen, 1)
		addRegionInfo(tc, write, []testRegionInfo{
			{3, []uint64{4, 5, 6}, 0, 1},
			{4, []uint64{4, 5, 6}, 0, 1 * KB},
		})
		stats = tc.RegionStats(statistics.WriteFlow)
		c.Assert(stats[4], HasLen, 1)
		c.Assert(stats[5], HasLen, 1)
		c.Assert(stats[6], HasLen, 1)
	}
	{ // many regions
		tc := mockcluster.NewCluster(opt)
		regions := []testRegionInfo{}
		for i := 1; i <= 1000; i += 2 {
			regions = append(regions, testRegionInfo{
				id:      uint64(i),
				peers:   []uint64{1, 2, 3},
				keyRate: 100 * KB,
			})
			regions = append(regions, testRegionInfo{
				id:      uint64(i + 1),
				peers:   []uint64{1, 2, 3},
				keyRate: 10 * KB,
			})
		}

		{ // read
			addRegionInfo(tc, read, regions)
			stats := tc.RegionStats(statistics.ReadFlow)
			c.Assert(len(stats[1]), Greater, 500)

			// for AntiCount
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			stats = tc.RegionStats(statistics.ReadFlow)
			c.Assert(len(stats[1]), Equals, 500)
		}
		{ // write
			addRegionInfo(tc, write, regions)
			stats := tc.RegionStats(statistics.WriteFlow)
			c.Assert(len(stats[1]), Greater, 500)
			c.Assert(len(stats[2]), Greater, 500)
			c.Assert(len(stats[3]), Greater, 500)

			// for AntiCount
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			stats = tc.RegionStats(statistics.WriteFlow)
			c.Assert(len(stats[1]), Equals, 500)
			c.Assert(len(stats[2]), Equals, 500)
			c.Assert(len(stats[3]), Equals, 500)
		}
	}
}

func (s *testHotCacheSuite) TestByteAndKey(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	regions := []testRegionInfo{}
	for i := 1; i <= 500; i++ {
		regions = append(regions, testRegionInfo{
			id:       uint64(i),
			peers:    []uint64{1, 2, 3},
			byteRate: 100 * KB,
			keyRate:  100 * KB,
		})
	}
	{ // read
		addRegionInfo(tc, read, regions)
		stats := tc.RegionStats(statistics.ReadFlow)
		c.Assert(len(stats[1]), Equals, 500)

		addRegionInfo(tc, read, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB},
		})
		stats = tc.RegionStats(statistics.ReadFlow)
		c.Assert(len(stats[1]), Equals, 503)
	}
	{ // write
		addRegionInfo(tc, write, regions)
		stats := tc.RegionStats(statistics.WriteFlow)
		c.Assert(len(stats[1]), Equals, 500)
		c.Assert(len(stats[2]), Equals, 500)
		c.Assert(len(stats[3]), Equals, 500)
		addRegionInfo(tc, write, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB},
		})
		stats = tc.RegionStats(statistics.WriteFlow)
		c.Assert(len(stats[1]), Equals, 503)
		c.Assert(len(stats[2]), Equals, 503)
		c.Assert(len(stats[3]), Equals, 503)
	}
}

type testRegionInfo struct {
	id uint64
	// the storeID list for the peers, the leader is stored in the first store
	peers    []uint64
	byteRate float64
	keyRate  float64
}

func addRegionInfo(tc *mockcluster.Cluster, rwTy rwType, regions []testRegionInfo) {
	addFunc := tc.AddLeaderRegionWithReadInfo
	if rwTy == write {
		addFunc = tc.AddLeaderRegionWithWriteInfo
	}
	for _, r := range regions {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*statistics.RegionHeartBeatReportInterval),
			uint64(r.keyRate*statistics.RegionHeartBeatReportInterval),
			statistics.RegionHeartBeatReportInterval,
			r.peers[1:],
		)
	}
}
