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
	"math"
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
	schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		cfg := initHotRegionScheduleConfig()
		return newHotWriteScheduler(opController, cfg), nil
	})
	schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotReadScheduler(opController, initHotRegionScheduleConfig()), nil
	})
}

func newHotReadScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{read}
	return ret
}

func newHotWriteScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{write}
	return ret
}

type testHotSchedulerSuite struct{}
type testInfluenceSerialSuite struct{}
type testHotCacheSuite struct{}
type testHotReadRegionSchedulerSuite struct{}

var _ = Suite(&testHotWriteRegionSchedulerSuite{})
var _ = Suite(&testHotSchedulerSuite{})
var _ = Suite(&testHotReadRegionSchedulerSuite{})
var _ = Suite(&testHotCacheSuite{})
var _ = SerialSuites(&testInfluenceSerialSuite{})

func (s *testHotSchedulerSuite) TestGCPendingOpInfos(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	for id := uint64(1); id <= 10; id++ {
		tc.PutStoreWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(HotRegionType, schedule.NewOperatorController(ctx, tc, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigJSONDecoder([]byte("null")))
	c.Assert(err, IsNil)
	hb := sche.(*hotScheduler)

	notDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
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
		op.Start()
		operator.SetOperatorStatusReachTime(op, operator.CREATED, time.Now().Add(-5*statistics.StoreHeartBeatReportInterval*time.Second))
		operator.SetOperatorStatusReachTime(op, operator.STARTED, time.Now().Add((-5*statistics.StoreHeartBeatReportInterval+1)*time.Second))
		return newPendingInfluence(op, 2, 4, Influence{}, hb.conf.GetStoreStatZombieDuration())
	}
	justDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := notDoneOpInfluence(region, ty)
		infl.op.Cancel()
		return infl
	}
	shouldRemoveOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := justDoneOpInfluence(region, ty)
		operator.SetOperatorStatusReachTime(infl.op, operator.CANCELED, time.Now().Add(-3*statistics.StoreHeartBeatReportInterval*time.Second))
		return infl
	}
	opInfluenceCreators := [3]func(region *core.RegionInfo, ty opType) *pendingInfluence{shouldRemoveOpInfluence, notDoneOpInfluence, justDoneOpInfluence}

	typs := []opType{movePeer, transferLeader}

	for i, creator := range opInfluenceCreators {
		for j, typ := range typs {
			regionID := uint64(i*len(typs) + j + 1)
			region := newTestRegion(regionID)
			hb.regionPendings[regionID] = creator(region, typ)
		}
	}

	hb.summaryPendingInfluence() // Calling this function will GC.

	for i := range opInfluenceCreators {
		for j, typ := range typs {
			regionID := uint64(i*len(typs) + j + 1)
			if i < 1 { // shouldRemoveOpInfluence
				c.Assert(hb.regionPendings, Not(HasKey), regionID)
			} else { // notDoneOpInfluence, justDoneOpInfluence
				c.Assert(hb.regionPendings, HasKey, regionID)
				kind := hb.regionPendings[regionID].op.Kind()
				switch typ {
				case transferLeader:
					c.Assert(kind&operator.OpLeader != 0, IsTrue)
					c.Assert(kind&operator.OpRegion == 0, IsTrue)
				case movePeer:
					c.Assert(kind&operator.OpLeader == 0, IsTrue)
					c.Assert(kind&operator.OpRegion != 0, IsTrue)
				}
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

	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.SetHotRegionCacheHitsThreshold(0)

	s.checkByteRateOnly(c, tc, hb)
	tc.SetEnablePlacementRules(true)
	s.checkByteRateOnly(c, tc, hb)
}

func (s *testHotWriteRegionSchedulerSuite) checkByteRateOnly(c *C, tc *mockcluster.Cluster, hb schedule.Scheduler) {
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
		{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{2, []uint64{1, 3, 4}, 512 * KB, 0, 0},
		{3, []uint64{1, 2, 4}, 512 * KB, 0, 0},
	})
	c.Assert(len(hb.Schedule(tc)) == 0, IsFalse)
	hb.(*hotScheduler).clearPendingInfluence()

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is larger than other stores.
	for i := 0; i < 20; i++ {
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
	}

	// hot region scheduler is restricted by `hot-region-schedule-limit`.
	tc.SetHotRegionScheduleLimit(0)
	c.Assert(hb.IsScheduleAllowed(tc), IsFalse)
	hb.(*hotScheduler).clearPendingInfluence()
	tc.SetHotRegionScheduleLimit(int(config.NewTestOptions().GetScheduleConfig().HotRegionScheduleLimit))

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
		{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{2, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{3, []uint64{6, 1, 4}, 512 * KB, 0, 0},
		{4, []uint64{5, 6, 4}, 512 * KB, 0, 0},
		{5, []uint64{3, 4, 5}, 512 * KB, 0, 0},
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

func (s *testHotWriteRegionSchedulerSuite) TestByteRateOnlyWithTiFlash(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetHotRegionCacheHitsThreshold(0)
	c.Assert(tc.RuleManager.SetRules([]*placement.Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           placement.Voter,
			Count:          3,
			LocationLabels: []string{"zone", "host"},
		},
		{
			GroupID:        "tiflash",
			ID:             "tiflash",
			Role:           placement.Learner,
			Count:          1,
			LocationLabels: []string{"zone", "host"},
			LabelConstraints: []placement.LabelConstraint{
				{
					Key:    core.EngineKey,
					Op:     placement.In,
					Values: []string{core.EngineTiFlash},
				},
			},
		},
	}), IsNil)
	sche, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb := sche.(*hotScheduler)

	// Add TiKV stores 1, 2, 3, 4, 5, 6, 7(Down) with region counts 3, 3, 2, 2, 0, 0, 0.
	// Add TiFlash stores 8, 9, 10, 11 with region counts 3, 1, 1, 0.
	storeCount := uint64(11)
	aliveTiKVStartID := uint64(1)
	aliveTiKVLastID := uint64(6)
	aliveTiFlashStartID := uint64(8)
	aliveTiFlashLastID := uint64(11)
	downStoreID := uint64(7)
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 3, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.AddLabelsStore(8, 3, map[string]string{"zone": "z1", "host": "h8", "engine": "tiflash"})
	tc.AddLabelsStore(9, 1, map[string]string{"zone": "z2", "host": "h9", "engine": "tiflash"})
	tc.AddLabelsStore(10, 1, map[string]string{"zone": "z5", "host": "h10", "engine": "tiflash"})
	tc.AddLabelsStore(11, 0, map[string]string{"zone": "z3", "host": "h11", "engine": "tiflash"})
	tc.SetStoreDown(downStoreID)
	for i := uint64(1); i <= storeCount; i++ {
		if i != downStoreID {
			tc.UpdateStorageWrittenBytes(i, 0)
		}
	}
	//| region_id | leader_store | follower_store | follower_store | learner_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|---------------|
	//|     1     |       1      |        2       |       3        |       8       |      512 KB   |
	//|     2     |       1      |        3       |       4        |       8       |      512 KB   |
	//|     3     |       1      |        2       |       4        |       9       |      512 KB   |
	//|     4     |       2      |                |                |      10       |      100 B    |
	// Region 1, 2 and 3 are hot regions.
	testRegions := []testRegionInfo{
		{1, []uint64{1, 2, 3, 8}, 512 * KB, 5 * KB, 3000},
		{2, []uint64{1, 3, 4, 8}, 512 * KB, 5 * KB, 3000},
		{3, []uint64{1, 2, 4, 9}, 512 * KB, 5 * KB, 3000},
		{4, []uint64{2, 10}, 100, 1, 1},
	}
	addRegionInfo(tc, write, testRegions)
	regionBytesSum := 0.0
	regionKeysSum := 0.0
	regionQuerySum := 0.0
	hotRegionBytesSum := 0.0
	hotRegionKeysSum := 0.0
	hotRegionQuerySum := 0.0
	for _, r := range testRegions {
		regionBytesSum += r.byteRate
		regionKeysSum += r.keyRate
		regionQuerySum += r.queryRate
	}
	for _, r := range testRegions[0:3] {
		hotRegionBytesSum += r.byteRate
		hotRegionKeysSum += r.keyRate
		hotRegionQuerySum += r.queryRate
	}
	// Will transfer a hot learner from store 8, because the total count of peers
	// which is hot for store 8 is larger than other TiFlash stores.
	for i := 0; i < 20; i++ {
		hb.clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		switch op.Len() {
		case 1:
			// balance by leader selected
			testutil.CheckTransferLeaderFrom(c, op, operator.OpHotRegion, 1)
		case 2:
			// balance by peer selected
			testutil.CheckTransferLearner(c, op, operator.OpHotRegion, 8, 10)
		default:
			c.Fatalf("wrong op: %v", op)
		}
	}
	// Disable for TiFlash
	hb.conf.SetEnableForTiFlash(false)
	for i := 0; i < 20; i++ {
		hb.clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		testutil.CheckTransferLeaderFrom(c, op, operator.OpHotRegion, 1)
	}
	//| store_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |       7.5MB      |
	//|    2     |       4.5MB      |
	//|    3     |       4.5MB      |
	//|    4     |        6MB       |
	//|    5     |        0MB(Evict)|
	//|    6     |        0MB       |
	//|    7     |        n/a (Down)|
	//|    8     |        n/a       | <- TiFlash is always 0.
	//|    9     |        n/a       |
	//|   10     |        n/a       |
	//|   11     |        n/a       |
	storesBytes := map[uint64]uint64{
		1: 7.5 * MB * statistics.StoreHeartBeatReportInterval,
		2: 4.5 * MB * statistics.StoreHeartBeatReportInterval,
		3: 4.5 * MB * statistics.StoreHeartBeatReportInterval,
		4: 6 * MB * statistics.StoreHeartBeatReportInterval,
	}
	tc.SetStoreEvictLeader(5, true)
	tikvBytesSum, tikvKeysSum, tikvQuerySum := 0.0, 0.0, 0.0
	for i := aliveTiKVStartID; i <= aliveTiKVLastID; i++ {
		tikvBytesSum += float64(storesBytes[i]) / 10
		tikvKeysSum += float64(storesBytes[i]/100) / 10
		tikvQuerySum += float64(storesBytes[i]/100) / 10
	}
	for i := uint64(1); i <= storeCount; i++ {
		if i != downStoreID {
			tc.UpdateStorageWrittenBytes(i, storesBytes[i])
		}
	}
	{ // Check the load expect
		aliveTiKVCount := float64(aliveTiKVLastID - aliveTiKVStartID + 1)
		allowLeaderTiKVCount := aliveTiKVCount - 1 // store 5 with evict leader
		aliveTiFlashCount := float64(aliveTiFlashLastID - aliveTiFlashStartID + 1)
		tc.ObserveRegionsStats()
		c.Assert(len(hb.Schedule(tc)) == 0, IsFalse)
		c.Assert(
			loadsEqual(
				hb.stLoadInfos[writeLeader][1].LoadPred.Expect.Loads,
				[]float64{hotRegionBytesSum / allowLeaderTiKVCount, hotRegionKeysSum / allowLeaderTiKVCount, tikvQuerySum / allowLeaderTiKVCount}),
			IsTrue)
		c.Assert(tikvQuerySum != hotRegionQuerySum, IsTrue)
		c.Assert(
			loadsEqual(
				hb.stLoadInfos[writePeer][1].LoadPred.Expect.Loads,
				[]float64{tikvBytesSum / aliveTiKVCount, tikvKeysSum / aliveTiKVCount, 0}),
			IsTrue)
		c.Assert(
			loadsEqual(
				hb.stLoadInfos[writePeer][8].LoadPred.Expect.Loads,
				[]float64{regionBytesSum / aliveTiFlashCount, regionKeysSum / aliveTiFlashCount, 0}),
			IsTrue)
		// check IsTraceRegionFlow == false
		pdServerCfg := tc.GetOpts().GetPDServerConfig()
		pdServerCfg.FlowRoundByDigit = 8
		tc.GetOpts().SetPDServerConfig(pdServerCfg)
		hb.clearPendingInfluence()
		c.Assert(len(hb.Schedule(tc)) == 0, IsFalse)
		c.Assert(
			loadsEqual(
				hb.stLoadInfos[writePeer][8].LoadPred.Expect.Loads,
				[]float64{hotRegionBytesSum / aliveTiFlashCount, hotRegionKeysSum / aliveTiFlashCount, 0}),
			IsTrue)
		// revert
		pdServerCfg.FlowRoundByDigit = 3
		tc.GetOpts().SetPDServerConfig(pdServerCfg)
	}
	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is larger than other stores.
	for i := 0; i < 20; i++ {
		hb.clearPendingInfluence()
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
	}
}

func (s *testHotWriteRegionSchedulerSuite) TestWithQuery(c *C) {
	originValue := schedulePeerPr
	defer func() {
		schedulePeerPr = originValue
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{QueryPriority, BytePriority}

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWriteQuery(1, 11000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(2, 10000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(3, 9000*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 500, 0, 500},
		{2, []uint64{1, 2, 3}, 500, 0, 500},
		{3, []uint64{2, 1, 3}, 500, 0, 500},
	})
	schedulePeerPr = 0.0
	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 1, 3)
	}
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

	tc := mockcluster.NewCluster(ctx, opt)
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
		{1, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB, 0},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB, 0},
		{3, []uint64{2, 4, 3}, 0.05 * MB, 0.1 * MB, 0},
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

	tc := mockcluster.NewCluster(ctx, opt)
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
		{1, []uint64{1, 2, 3}, 0.5 * MB, 0.5 * MB, 0},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB, 0},
		{3, []uint64{3, 2, 1}, 0.5 * MB, 0.5 * MB, 0},
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

func (s *testHotWriteRegionSchedulerSuite) TestCheckHot(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 10.5*MB*statistics.StoreHeartBeatReportInterval, 10.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9.5*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 90, 0.5 * MB, 0},       // no hot
		{1, []uint64{2, 1, 3}, 90, 0.5 * MB, 0},       // no hot
		{2, []uint64{3, 2, 1}, 0.5 * MB, 0.5 * MB, 0}, // byteDecRatio is greater than greatDecRatio
	})

	c.Check(hb.Schedule(tc), HasLen, 0)
}

func (s *testHotWriteRegionSchedulerSuite) TestLeader(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc := mockcluster.NewCluster(ctx, opt)
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

	// store1 has 2 peer as leader
	// store2 has 3 peer as leader
	// store3 has 2 peer as leader
	// If transfer leader from store2 to store1 or store3, it will keep on looping, which introduces a lot of unnecessary scheduling
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{6, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB, 0},
		{7, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB, 0},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		c.Assert(hb.Schedule(tc), HasLen, 0)
	}

	addRegionInfo(tc, write, []testRegionInfo{
		{8, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
	})

	// store1 has 2 peer as leader
	// store2 has 4 peer as leader
	// store3 has 2 peer as leader
	// We expect to transfer leader from store2 to store1 or store3
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
	old := pendingAmpFactor
	pendingAmpFactor = 0.0
	defer func() {
		pendingAmpFactor = old
	}()
	for i := 0; i < 2; i++ {
		// 0: byte rate
		// 1: key rate
		tc := mockcluster.NewCluster(ctx, opt)
		tc.SetHotRegionCacheHitsThreshold(0)
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
				{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{2, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{3, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{4, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{5, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{6, []uint64{1, 2, 3}, 512 * KB, 0, 0},
			})
		} else if i == 1 { // key rate
			addRegionInfo(tc, write, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{2, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{3, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{4, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{5, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{6, []uint64{1, 2, 3}, 0, 512 * KB, 0},
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
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetEnablePlacementRules(true)
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.SetHotRegionCacheHitsThreshold(0)
	key, err := hex.DecodeString("")
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

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

	tc.UpdateStorageWrittenBytes(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{6, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
		{7, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB, 0},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// The targetID should always be 1 as leader is only allowed to be placed in store1 or store2 by placement rule
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 2, 1)
		c.Assert(hb.Schedule(tc), HasLen, 0)
	}
}

func (s *testHotReadRegionSchedulerSuite) TestByteRateOnly(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.ReadPriorities = []string{BytePriority, KeyPriority}
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
		{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{2, []uint64{2, 1, 3}, 512 * KB, 0, 0},
		{3, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * KB, 0, 0},
	})

	c.Assert(tc.IsRegionHot(tc.GetRegion(1)), IsTrue)
	c.Assert(tc.IsRegionHot(tc.GetRegion(11)), IsFalse)
	// check randomly pick hot region
	r := tc.HotRegionsFromStore(2, statistics.ReadFlow)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// check hot items
	stats := tc.HotCache.RegionStats(statistics.ReadFlow, 0)
	c.Assert(stats, HasLen, 3)
	for _, ss := range stats {
		for _, s := range ss {
			c.Assert(s.GetLoad(statistics.RegionReadBytes), Equals, 512.0*KB)
		}
	}

	op := hb.Schedule(tc)[0]

	// move leader from store 1 to store 5
	// it is better than transfer leader from store 1 to store 3
	testutil.CheckTransferPeerWithLeaderTransfer(c, op, operator.OpHotRegion, 1, 5)
	hb.(*hotScheduler).clearPendingInfluence()

	// assume handle the transfer leader operator rather than move leader
	tc.AddRegionWithReadInfo(3, 3, 512*KB*statistics.ReadReportInterval, 0, 0, statistics.ReadReportInterval, []uint64{1, 2})
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
		{4, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{5, []uint64{4, 2, 5}, 512 * KB, 0, 0},
	})

	// We will move leader peer of region 1 from 1 to 5
	testutil.CheckTransferPeerWithLeaderTransfer(c, hb.Schedule(tc)[0], operator.OpHotRegion|operator.OpLeader, 1, 5)
	hb.(*hotScheduler).clearPendingInfluence()

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
	hb.(*hotScheduler).clearPendingInfluence()
}

func (s *testHotReadRegionSchedulerSuite) TestWithQuery(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageReadQuery(1, 10500*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(2, 10000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(3, 9000*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0, 0, 500},
		{2, []uint64{2, 1, 3}, 0, 0, 500},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		testutil.CheckTransferLeader(c, op, operator.OpHotRegion, 1, 3)
	}
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
	hb.(*hotScheduler).conf.ReadPriorities = []string{BytePriority, KeyPriority}

	tc := mockcluster.NewCluster(ctx, opt)
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
		{1, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB, 0},
		{2, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB, 0},
		{3, []uint64{3, 4, 5}, 0.05 * MB, 0.1 * MB, 0},
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
	hb.(*hotScheduler).conf.ReadPriorities = []string{BytePriority, KeyPriority}
	old := pendingAmpFactor
	pendingAmpFactor = 0.0
	defer func() {
		pendingAmpFactor = old
	}()
	for i := 0; i < 2; i++ {
		// 0: byte rate
		// 1: key rate
		tc := mockcluster.NewCluster(ctx, opt)
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
				{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{2, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{3, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{4, []uint64{1, 2, 3}, 512 * KB, 0, 0},
				{5, []uint64{2, 1, 3}, 512 * KB, 0, 0},
				{6, []uint64{2, 1, 3}, 512 * KB, 0, 0},
				{7, []uint64{3, 2, 1}, 512 * KB, 0, 0},
				{8, []uint64{3, 2, 1}, 512 * KB, 0, 0},
			})
		} else if i == 1 { // key rate
			addRegionInfo(tc, read, []testRegionInfo{
				{1, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{2, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{3, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{4, []uint64{1, 2, 3}, 0, 512 * KB, 0},
				{5, []uint64{2, 1, 3}, 0, 512 * KB, 0},
				{6, []uint64{2, 1, 3}, 0, 512 * KB, 0},
				{7, []uint64{3, 2, 1}, 0, 512 * KB, 0},
				{8, []uint64{3, 2, 1}, 0, 512 * KB, 0},
			})
		}

		// Before schedule, store byte/key rate: 7.1 | 6.1 | 6 | 5
		// Min and max from storeLoadPred. They will be generated in the comparison of current and future.
		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()

			op1 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op1, operator.OpHotRegion, 1, 4)
			// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

			pendingAmpFactor = old
			ops := hb.Schedule(tc)
			c.Assert(ops, HasLen, 0)
			pendingAmpFactor = 0.0

			op2 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op2, operator.OpHotRegion, 1, 4)
			// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

			ops = hb.Schedule(tc)
			c.Logf("%v", ops)
			c.Assert(ops, HasLen, 0)
		}

		// Before schedule, store byte/key rate: 7.1 | 6.1 | 6 | 5
		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()

			op1 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op1, operator.OpHotRegion, 1, 4)
			// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

			op2 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op2, operator.OpHotRegion, 1, 4)
			// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)
			c.Assert(op2.Cancel(), IsTrue)

			op2 = hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op2, operator.OpHotRegion, 1, 4)
			// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

			c.Assert(op1.Cancel(), IsTrue)
			// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

			op3 := hb.Schedule(tc)[0]
			testutil.CheckTransferPeer(c, op3, operator.OpHotRegion, 1, 4)
			// store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

			ops := hb.Schedule(tc)
			c.Assert(ops, HasLen, 0)
		}
	}
}

func (s *testHotCacheSuite) TestUpdateCache(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)

	/// For read flow
	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{2, []uint64{2, 1, 3}, 512 * KB, 0, 0},
		{3, []uint64{1, 2, 3}, 20 * KB, 0, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{11, []uint64{1, 2, 3}, 7 * KB, 0, 0},
	})
	stats := tc.RegionStats(statistics.ReadFlow, 0)
	c.Assert(len(stats[1]), Equals, 3)
	c.Assert(len(stats[2]), Equals, 3)
	c.Assert(len(stats[3]), Equals, 3)

	addRegionInfo(tc, read, []testRegionInfo{
		{3, []uint64{2, 1, 3}, 20 * KB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * KB, 0, 0},
	})
	stats = tc.RegionStats(statistics.ReadFlow, 0)
	c.Assert(len(stats[1]), Equals, 3)
	c.Assert(len(stats[2]), Equals, 3)
	c.Assert(len(stats[3]), Equals, 3)

	addRegionInfo(tc, write, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 512 * KB, 0, 0},
		{5, []uint64{1, 2, 3}, 20 * KB, 0, 0},
		{6, []uint64{1, 2, 3}, 0.8 * KB, 0, 0},
	})
	stats = tc.RegionStats(statistics.WriteFlow, 0)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 2)

	addRegionInfo(tc, write, []testRegionInfo{
		{5, []uint64{1, 2, 5}, 20 * KB, 0, 0},
	})
	stats = tc.RegionStats(statistics.WriteFlow, 0)

	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 1)
	c.Assert(len(stats[5]), Equals, 1)

	// For leader read flow
	addRegionLeaderReadInfo(tc, []testRegionInfo{
		{21, []uint64{4, 5, 6}, 512 * KB, 0, 0},
		{22, []uint64{5, 4, 6}, 512 * KB, 0, 0},
		{23, []uint64{4, 5, 6}, 20 * KB, 0, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{31, []uint64{4, 5, 6}, 7 * KB, 0, 0},
	})
	stats = tc.RegionStats(statistics.ReadFlow, 0)
	c.Assert(len(stats[4]), Equals, 2)
	c.Assert(len(stats[5]), Equals, 1)
	c.Assert(len(stats[6]), Equals, 0)
}

func (s *testHotCacheSuite) TestKeyThresholds(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	{ // only a few regions
		tc := mockcluster.NewCluster(ctx, opt)
		tc.SetHotRegionCacheHitsThreshold(0)
		addRegionInfo(tc, read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 1, 0},
			{2, []uint64{1, 2, 3}, 0, 1 * KB, 0},
		})
		stats := tc.RegionStats(statistics.ReadFlow, 0)
		c.Assert(stats[1], HasLen, 1)
		addRegionInfo(tc, write, []testRegionInfo{
			{3, []uint64{4, 5, 6}, 0, 1, 0},
			{4, []uint64{4, 5, 6}, 0, 1 * KB, 0},
		})
		stats = tc.RegionStats(statistics.WriteFlow, 0)
		c.Assert(stats[4], HasLen, 1)
		c.Assert(stats[5], HasLen, 1)
		c.Assert(stats[6], HasLen, 1)
	}
	{ // many regions
		tc := mockcluster.NewCluster(ctx, opt)
		regions := []testRegionInfo{}
		for i := 1; i <= 1000; i += 2 {
			regions = append(regions,
				testRegionInfo{
					id:      uint64(i),
					peers:   []uint64{1, 2, 3},
					keyRate: 100 * KB,
				},
				testRegionInfo{
					id:      uint64(i + 1),
					peers:   []uint64{1, 2, 3},
					keyRate: 10 * KB,
				},
			)
		}

		{ // read
			addRegionInfo(tc, read, regions)
			stats := tc.RegionStats(statistics.ReadFlow, 0)
			c.Assert(len(stats[1]), Greater, 500)

			// for AntiCount
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			addRegionInfo(tc, read, regions)
			stats = tc.RegionStats(statistics.ReadFlow, 0)
			c.Assert(len(stats[1]), Equals, 500)
		}
		{ // write
			addRegionInfo(tc, write, regions)
			stats := tc.RegionStats(statistics.WriteFlow, 0)
			c.Assert(len(stats[1]), Greater, 500)
			c.Assert(len(stats[2]), Greater, 500)
			c.Assert(len(stats[3]), Greater, 500)

			// for AntiCount
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			addRegionInfo(tc, write, regions)
			stats = tc.RegionStats(statistics.WriteFlow, 0)
			c.Assert(len(stats[1]), Equals, 500)
			c.Assert(len(stats[2]), Equals, 500)
			c.Assert(len(stats[3]), Equals, 500)
		}
	}
}

func (s *testHotCacheSuite) TestByteAndKey(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
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
		stats := tc.RegionStats(statistics.ReadFlow, 0)
		c.Assert(len(stats[1]), Equals, 500)

		addRegionInfo(tc, read, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB, 0},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB, 0},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB, 0},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB, 0},
		})
		stats = tc.RegionStats(statistics.ReadFlow, 0)
		c.Assert(len(stats[1]), Equals, 503)
	}
	{ // write
		addRegionInfo(tc, write, regions)
		stats := tc.RegionStats(statistics.WriteFlow, 0)
		c.Assert(len(stats[1]), Equals, 500)
		c.Assert(len(stats[2]), Equals, 500)
		c.Assert(len(stats[3]), Equals, 500)
		addRegionInfo(tc, write, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB, 0},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB, 0},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB, 0},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB, 0},
		})
		stats = tc.RegionStats(statistics.WriteFlow, 0)
		c.Assert(len(stats[1]), Equals, 503)
		c.Assert(len(stats[2]), Equals, 503)
		c.Assert(len(stats[3]), Equals, 503)
	}
}

type testRegionInfo struct {
	id uint64
	// the storeID list for the peers, the leader is stored in the first store
	peers     []uint64
	byteRate  float64
	keyRate   float64
	queryRate float64
}

func addRegionInfo(tc *mockcluster.Cluster, rwTy rwType, regions []testRegionInfo) {
	addFunc := tc.AddRegionWithReadInfo
	if rwTy == write {
		addFunc = tc.AddLeaderRegionWithWriteInfo
	}
	reportIntervalSecs := statistics.WriteReportInterval
	if rwTy == read {
		reportIntervalSecs = statistics.ReadReportInterval
	}
	for _, r := range regions {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*float64(reportIntervalSecs)),
			uint64(r.keyRate*float64(reportIntervalSecs)),
			uint64(r.queryRate*float64(reportIntervalSecs)),
			uint64(reportIntervalSecs),
			r.peers[1:],
		)
	}
}

func addRegionLeaderReadInfo(tc *mockcluster.Cluster, regions []testRegionInfo) {
	addFunc := tc.AddRegionLeaderWithReadInfo
	reportIntervalSecs := statistics.ReadReportInterval
	for _, r := range regions {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*float64(reportIntervalSecs)),
			uint64(r.keyRate*float64(reportIntervalSecs)),
			uint64(r.queryRate*float64(reportIntervalSecs)),
			uint64(reportIntervalSecs),
			r.peers[1:],
		)
	}
}

func (s *testHotCacheSuite) TestCheckRegionFlow(c *C) {
	testcases := []struct {
		kind                      rwType
		onlyLeader                bool
		DegreeAfterTransferLeader int
	}{
		{
			kind:                      write,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 3,
		},
		{
			kind:                      read,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 4,
		},
		{
			kind:                      read,
			onlyLeader:                true,
			DegreeAfterTransferLeader: 1,
		},
	}

	for _, testcase := range testcases {
		ctx, cancel := context.WithCancel(context.Background())
		opt := config.NewTestOptions()
		tc := mockcluster.NewCluster(ctx, opt)
		tc.SetMaxReplicas(3)
		tc.SetLocationLabels([]string{"zone", "host"})
		tc.DisableFeature(versioninfo.JointConsensus)
		sche, err := schedule.CreateScheduler(HotRegionType, schedule.NewOperatorController(ctx, tc, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigJSONDecoder([]byte("null")))
		c.Assert(err, IsNil)
		hb := sche.(*hotScheduler)
		heartbeat := tc.AddLeaderRegionWithWriteInfo
		if testcase.kind == read {
			if testcase.onlyLeader {
				heartbeat = tc.AddRegionLeaderWithReadInfo
			} else {
				heartbeat = tc.AddRegionWithReadInfo
			}
		}
		tc.AddRegionStore(2, 20)
		tc.UpdateStorageReadStats(2, 9.5*MB*statistics.StoreHeartBeatReportInterval, 9.5*MB*statistics.StoreHeartBeatReportInterval)
		reportInterval := uint64(statistics.WriteReportInterval)
		if testcase.kind == read {
			reportInterval = uint64(statistics.ReadReportInterval)
		}
		// hot degree increase
		heartbeat(1, 1, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
		heartbeat(1, 1, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
		items := heartbeat(1, 1, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
		c.Check(len(items), Greater, 0)
		for _, item := range items {
			c.Check(item.HotDegree, Equals, 3)
		}
		// transfer leader
		items = heartbeat(1, 2, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{1, 3}, 1)
		for _, item := range items {
			if item.StoreID == 2 {
				c.Check(item.HotDegree, Equals, testcase.DegreeAfterTransferLeader)
			}
		}

		if testcase.DegreeAfterTransferLeader >= 3 {
			// try schedule
			hb.prepareForBalance(testcase.kind, tc)
			leaderSolver := newBalanceSolver(hb, tc, testcase.kind, transferLeader)
			leaderSolver.cur = &solution{srcDetail: hb.stLoadInfos[toResourceType(testcase.kind, transferLeader)][2]}
			c.Check(leaderSolver.filterHotPeers(), HasLen, 0) // skip schedule
			threshold := tc.GetHotRegionCacheHitsThreshold()
			tc.SetHotRegionCacheHitsThreshold(0)
			c.Check(leaderSolver.filterHotPeers(), HasLen, 1)
			tc.SetHotRegionCacheHitsThreshold(threshold)
		}

		// move peer: add peer and remove peer
		items = heartbeat(1, 2, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{1, 3, 4}, 1)
		c.Check(len(items), Greater, 0)
		for _, item := range items {
			c.Check(item.HotDegree, Equals, testcase.DegreeAfterTransferLeader+1)
		}
		items = heartbeat(1, 2, 512*KB*reportInterval, 0, 0, reportInterval, []uint64{1, 4}, 1)
		c.Check(len(items), Greater, 0)
		for _, item := range items {
			if item.StoreID == 3 {
				c.Check(item.IsNeedDelete(), IsTrue)
				continue
			}
			c.Check(item.HotDegree, Equals, testcase.DegreeAfterTransferLeader+2)
		}
		cancel()
	}
}

func (s *testHotCacheSuite) TestCheckRegionFlowWithDifferentThreshold(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	// some peers are hot, and some are cold #3198

	rate := uint64(512 * KB)
	for i := 0; i < statistics.TopNN; i++ {
		for j := 0; j < statistics.DefaultAotSize; j++ {
			tc.AddLeaderRegionWithWriteInfo(uint64(i+100), 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3}, 1)
		}
	}
	items := tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3}, 1)
	c.Check(items[0].GetThresholds()[0], Equals, float64(rate)*statistics.HotThresholdRatio)
	// Threshold of store 1,2,3 is 409.6 KB and others are 1 KB
	// Make the hot threshold of some store is high and the others are low
	rate = 10 * KB
	tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3, 4}, 1)
	items = tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{3, 4}, 1)
	for _, item := range items {
		if item.StoreID < 4 {
			c.Check(item.IsNeedDelete(), IsTrue)
		} else {
			c.Check(item.IsNeedDelete(), IsFalse)
		}
	}
}

func (s *testHotCacheSuite) TestSortHotPeer(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicas(3)
	tc.DisableFeature(versioninfo.JointConsensus)
	sche, err := schedule.CreateScheduler(HotRegionType, schedule.NewOperatorController(ctx, tc, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigJSONDecoder([]byte("null")))
	c.Assert(err, IsNil)
	hb := sche.(*hotScheduler)
	leaderSolver := newBalanceSolver(hb, tc, read, transferLeader)
	hotPeers := []*statistics.HotPeerStat{{
		RegionID: 1,
		Loads: []float64{
			statistics.RegionReadQuery: 10,
			statistics.RegionReadBytes: 1,
		},
	}, {
		RegionID: 2,
		Loads: []float64{
			statistics.RegionReadQuery: 1,
			statistics.RegionReadBytes: 10,
		},
	}, {
		RegionID: 3,
		Loads: []float64{
			statistics.RegionReadQuery: 5,
			statistics.RegionReadBytes: 6,
		},
	}}

	u := leaderSolver.sortHotPeers(hotPeers, 1)
	checkSortResult(c, []uint64{1}, u)

	u = leaderSolver.sortHotPeers(hotPeers, 2)
	checkSortResult(c, []uint64{1, 2}, u)
}

func checkSortResult(c *C, regions []uint64, hotPeers map[*statistics.HotPeerStat]struct{}) {
	c.Assert(len(regions), Equals, len(hotPeers))
	for _, region := range regions {
		in := false
		for hotPeer := range hotPeers {
			if hotPeer.RegionID == region {
				in = true
				break
			}
		}
		c.Assert(in, IsTrue)
	}
}

func (s *testInfluenceSerialSuite) TestInfluenceByRWType(c *C) {
	originValue := schedulePeerPr
	defer func() {
		schedulePeerPr = originValue
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.UpdateStorageWrittenStats(1, 99*MB*statistics.StoreHeartBeatReportInterval, 99*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 50*MB*statistics.StoreHeartBeatReportInterval, 98*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 2*MB*statistics.StoreHeartBeatReportInterval, 2*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB, 0},
	})
	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB, 0},
	})
	// must move peer
	schedulePeerPr = 1.0
	// must move peer from 1 to 4
	op := hb.Schedule(tc)[0]
	c.Assert(op, NotNil)
	hb.(*hotScheduler).summaryPendingInfluence()
	stInfos := hb.(*hotScheduler).stInfos
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteKeys], -0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteBytes], -0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionWriteKeys], 0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionWriteBytes], 0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadKeys], -0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadBytes], -0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionReadKeys], 0.5*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionReadBytes], 0.5*MB), IsTrue)

	// consider pending amp, there are nine regions or more.
	for i := 2; i < 13; i++ {
		addRegionInfo(tc, write, []testRegionInfo{
			{uint64(i), []uint64{1, 2, 3}, 0.7 * MB, 0.7 * MB, 0},
		})
	}

	// must transfer leader
	schedulePeerPr = 0
	// must transfer leader from 1 to 3
	op = hb.Schedule(tc)[0]
	c.Assert(op, NotNil)
	hb.(*hotScheduler).summaryPendingInfluence()
	stInfos = hb.(*hotScheduler).stInfos
	// assert read/write influence is the sum of write peer and write leader
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteKeys], -1.2*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteBytes], -1.2*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionWriteKeys], 0.7*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionWriteBytes], 0.7*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadKeys], -1.2*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadBytes], -1.2*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionReadKeys], 0.7*MB), IsTrue)
	c.Assert(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionReadBytes], 0.7*MB), IsTrue)
}

func nearlyAbout(f1, f2 float64) bool {
	if f1-f2 < 0.1*KB || f2-f1 < 0.1*KB {
		return true
	}
	return false
}

func loadsEqual(loads1, loads2 []float64) bool {
	if len(loads1) != statistics.DimLen || len(loads2) != statistics.DimLen {
		return false
	}
	for i, load := range loads1 {
		if math.Abs(load-loads2[i]) > 0.01 {
			return false
		}
	}
	return true
}

func (s *testHotSchedulerSuite) TestHotReadPeerSchedule(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	for id := uint64(1); id <= 6; id++ {
		tc.PutStoreWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, tc, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigJSONDecoder([]byte("null")))
	c.Assert(err, IsNil)
	hb := sche.(*hotScheduler)
	hb.conf.ReadPriorities = []string{BytePriority, KeyPriority}

	tc.UpdateStorageReadStats(1, 20*MB, 20*MB)
	tc.UpdateStorageReadStats(2, 19*MB, 19*MB)
	tc.UpdateStorageReadStats(3, 19*MB, 19*MB)
	tc.UpdateStorageReadStats(4, 0*MB, 0*MB)
	tc.AddRegionWithPeerReadInfo(1, 3, 1, uint64(0.9*KB*float64(10)), uint64(0.9*KB*float64(10)), 10, []uint64{1, 2}, 3)
	op := hb.Schedule(tc)[0]
	testutil.CheckTransferPeer(c, op, operator.OpHotRegion, 1, 4)
}

func (s *testHotSchedulerSuite) TestHotScheduleWithPriority(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1.05)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1.05)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10*MB*statistics.StoreHeartBeatReportInterval, 9*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	// must transfer peer
	schedulePeerPr = 1.0
	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * MB, 1 * MB, 0},
		{6, []uint64{4, 2, 3}, 1 * MB, 2 * MB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{BytePriority, KeyPriority}
	ops := hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpHotRegion, 1, 5)
	hb.(*hotScheduler).clearPendingInfluence()
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{KeyPriority, BytePriority}
	ops = hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpHotRegion, 4, 5)
	hb.(*hotScheduler).clearPendingInfluence()

	// assert read priority schedule
	hb, err = schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc.UpdateStorageReadStats(5, 10*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 10*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(1, 10*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 1*MB*statistics.StoreHeartBeatReportInterval, 7*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 7*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * MB, 2 * MB, 0},
	})
	hb.(*hotScheduler).conf.ReadPriorities = []string{BytePriority, KeyPriority}
	ops = hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferLeader(c, ops[0], operator.OpHotRegion, 1, 2)
	hb.(*hotScheduler).clearPendingInfluence()
	hb.(*hotScheduler).conf.ReadPriorities = []string{KeyPriority, BytePriority}
	ops = hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferLeader(c, ops[0], operator.OpHotRegion, 1, 3)

	hb, err = schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.StrictPickingStore = false
	// assert loose store picking
	tc.UpdateStorageWrittenStats(1, 10*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{BytePriority, KeyPriority}
	ops = hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpHotRegion, 1, 5)
	hb.(*hotScheduler).clearPendingInfluence()

	tc.UpdateStorageWrittenStats(1, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*MB*statistics.StoreHeartBeatReportInterval, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{KeyPriority, BytePriority}
	ops = hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpHotRegion, 4, 5)
	hb.(*hotScheduler).clearPendingInfluence()
}

func (s *testHotSchedulerSuite) TestHotWriteLeaderScheduleWithPriority(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.UpdateStorageWrittenStats(1, 31*MB*statistics.StoreHeartBeatReportInterval, 31*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*MB*statistics.StoreHeartBeatReportInterval, 1*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 1*MB*statistics.StoreHeartBeatReportInterval, 10*MB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 10 * MB, 10 * MB, 0},
		{2, []uint64{1, 2, 3}, 10 * MB, 10 * MB, 0},
		{3, []uint64{1, 2, 3}, 10 * MB, 10 * MB, 0},
		{4, []uint64{2, 1, 3}, 10 * MB, 0 * MB, 0},
		{5, []uint64{3, 2, 1}, 0 * MB, 10 * MB, 0},
	})
	old1, old2 := schedulePeerPr, pendingAmpFactor
	schedulePeerPr, pendingAmpFactor = 0.0, 0.0
	defer func() {
		schedulePeerPr, pendingAmpFactor = old1, old2
	}()
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{KeyPriority, BytePriority}
	ops := hb.Schedule(tc)
	c.Assert(len(ops), Equals, 1)
	testutil.CheckTransferLeader(c, ops[0], operator.OpHotRegion, 1, 2)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{BytePriority, KeyPriority}
	ops = hb.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferLeader(c, ops[0], operator.OpHotRegion, 1, 3)
}

func (s *testHotSchedulerSuite) TestCompatibility(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	tc := mockcluster.NewCluster(ctx, opt)
	// default
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{"error", BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{QueryPriority, BytePriority, KeyPriority}
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// low version
	tc.DisableFeature(versioninfo.HotScheduleWithQuery)
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config byte and key
	hb.(*hotScheduler).conf.ReadPriorities = []string{KeyPriority, BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{BytePriority, KeyPriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{KeyPriority, BytePriority}
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
	})
	// config query in low version
	hb.(*hotScheduler).conf.ReadPriorities = []string{QueryPriority, BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{QueryPriority, BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{QueryPriority, BytePriority}
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error", "error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{QueryPriority, BytePriority, KeyPriority}
	checkPriority(c, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
}

func checkPriority(c *C, hb *hotScheduler, tc *mockcluster.Cluster, dims [3][2]int) {
	readSolver := newBalanceSolver(hb, tc, read, transferLeader)
	writeLeaderSolver := newBalanceSolver(hb, tc, write, transferLeader)
	writePeerSolver := newBalanceSolver(hb, tc, write, movePeer)
	c.Assert(readSolver.firstPriority, Equals, dims[0][0])
	c.Assert(readSolver.secondPriority, Equals, dims[0][1])
	c.Assert(writeLeaderSolver.firstPriority, Equals, dims[1][0])
	c.Assert(writeLeaderSolver.secondPriority, Equals, dims[1][1])
	c.Assert(writePeerSolver.firstPriority, Equals, dims[2][0])
	c.Assert(writePeerSolver.secondPriority, Equals, dims[2][1])
}
