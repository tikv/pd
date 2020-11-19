// Copyright 2016 TiKV Project Authors.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/versioninfo"
)

const (
	KB = 1024
	MB = 1024 * KB
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testShuffleLeaderSuite{})

type testShuffleLeaderSuite struct{}

func (s *testShuffleLeaderSuite) TestShuffle(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	sl, err := schedule.CreateScheduler(ShuffleLeaderType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ShuffleLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(tc), IsNil)

	// Add stores 1,2,3,4
	tc.AddLeaderStore(1, 6)
	tc.AddLeaderStore(2, 7)
	tc.AddLeaderStore(3, 8)
	tc.AddLeaderStore(4, 9)
	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1, 2, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4, 1)
	tc.AddLeaderRegion(3, 3, 4, 1, 2)
	tc.AddLeaderRegion(4, 4, 1, 2, 3)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(tc)
		c.Assert(op, NotNil)
		c.Assert(op[0].Kind(), Equals, operator.OpLeader|operator.OpAdmin)
	}
}

var _ = Suite(&testRejectLeaderSuite{})

type testRejectLeaderSuite struct{}

func (s *testRejectLeaderSuite) TestRejectLeader(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := config.NewTestOptions()
	opts.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	tc := mockcluster.NewCluster(opts)

	// Add 3 stores 1,2,3.
	tc.AddLabelsStore(1, 1, map[string]string{"noleader": "true"})
	tc.UpdateLeaderCount(1, 1)
	tc.AddLeaderStore(2, 10)
	tc.AddLeaderStore(3, 0)
	// Add 2 regions with leader on 1 and 2.
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 1, 3)

	// The label scheduler transfers leader out of store1.
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sl, err := schedule.CreateScheduler(LabelType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(LabelType, []string{"", ""}))
	c.Assert(err, IsNil)
	op := sl.Schedule(tc)
	testutil.CheckTransferLeaderFrom(c, op[0], operator.OpLeader, 1)

	// If store3 is disconnected, transfer leader to store 2.
	tc.SetStoreDisconnect(3)
	op = sl.Schedule(tc)
	testutil.CheckTransferLeader(c, op[0], operator.OpLeader, 1, 2)

	// As store3 is disconnected, store1 rejects leader. Balancer will not create
	// any operators.
	bs, err := schedule.CreateScheduler(BalanceLeaderType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	op = bs.Schedule(tc)
	c.Assert(op, IsNil)

	// Can't evict leader from store2, neither.
	el, err := schedule.CreateScheduler(EvictLeaderType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"2"}))
	c.Assert(err, IsNil)
	op = el.Schedule(tc)
	c.Assert(op, IsNil)

	// If the peer on store3 is pending, not transfer to store3 neither.
	tc.SetStoreUp(3)
	region := tc.Regions.GetRegion(1)
	for _, p := range region.GetPeers() {
		if p.GetStoreId() == 3 {
			region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), p)))
			break
		}
	}
	tc.Regions.AddRegion(region)
	op = sl.Schedule(tc)
	testutil.CheckTransferLeader(c, op[0], operator.OpLeader, 1, 2)
}

var _ = Suite(&testShuffleHotRegionSchedulerSuite{})

type testShuffleHotRegionSchedulerSuite struct{}

func (s *testShuffleHotRegionSchedulerSuite) TestBalance(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	hb, err := schedule.CreateScheduler(ShuffleHotRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder("shuffle-hot-region", []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkBalance(c, tc, opt, hb)
	tc.SetEnablePlacementRules(true)
	s.checkBalance(c, tc, opt, hb)
}

func (s *testShuffleHotRegionSchedulerSuite) checkBalance(c *C, tc *mockcluster.Cluster, opt *config.PersistOptions, hb schedule.Scheduler) {
	// Add stores 1, 2, 3, 4, 5, 6  with hot peer counts 3, 2, 2, 2, 0, 0.
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z5", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z4", "host": "h6"})

	// Report store written bytes.
	tc.UpdateStorageWrittenBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{3, 4})
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 4})
	tc.SetHotRegionCacheHitsThreshold(0)

	// try to get an operator
	var op []*operator.Operator
	for i := 0; i < 100; i++ {
		op = hb.Schedule(tc)
		if op != nil {
			break
		}
	}
	c.Assert(op, NotNil)
	c.Assert(op[0].Step(1).(operator.PromoteLearner).ToStore, Equals, op[0].Step(op[0].Len()-1).(operator.TransferLeader).ToStore)
	c.Assert(op[0].Step(1).(operator.PromoteLearner).ToStore, Not(Equals), 6)
}

var _ = Suite(&testHotRegionSchedulerSuite{})

type testHotRegionSchedulerSuite struct{}

func (s *testHotRegionSchedulerSuite) TestAbnormalReplica(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetLeaderScheduleLimit(0)
	hb, err := schedule.CreateScheduler(HotReadRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)

	// Report store read bytes.
	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)

	tc.AddLeaderRegionWithReadInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2})
	tc.AddLeaderRegionWithReadInfo(2, 2, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{1, 3})
	tc.AddLeaderRegionWithReadInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.SetHotRegionCacheHitsThreshold(0)
	c.Assert(tc.IsRegionHot(tc.GetRegion(1)), IsTrue)
	c.Assert(hb.Schedule(tc), IsNil)
}

var _ = Suite(&testEvictLeaderSuite{})

type testEvictLeaderSuite struct{}

func (s *testEvictLeaderSuite) TestEvictLeader(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions 1, 2, 3 with leaders in stores 1, 2, 3
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 2, 1)
	tc.AddLeaderRegion(3, 3, 1)

	sl, err := schedule.CreateScheduler(EvictLeaderType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"1"}))
	c.Assert(err, IsNil)
	c.Assert(sl.IsScheduleAllowed(tc), IsTrue)
	op := sl.Schedule(tc)
	testutil.CheckTransferLeader(c, op[0], operator.OpLeader, 1, 2)
}

var _ = Suite(&testShuffleRegionSuite{})

type testShuffleRegionSuite struct{}

func (s *testShuffleRegionSuite) TestShuffle(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	sl, err := schedule.CreateScheduler(ShuffleRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ShuffleRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	c.Assert(sl.IsScheduleAllowed(tc), IsTrue)
	c.Assert(sl.Schedule(tc), IsNil)

	// Add stores 1, 2, 3, 4
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 7)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 9)
	// Add regions 1, 2, 3, 4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 3, 4)
	tc.AddLeaderRegion(3, 3, 4, 1)
	tc.AddLeaderRegion(4, 4, 1, 2)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(tc)
		c.Assert(op, NotNil)
		c.Assert(op[0].Kind(), Equals, operator.OpRegion|operator.OpAdmin)
	}
}

func (s *testShuffleRegionSuite) TestRole(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)

	// update rule to 1leader+1follower+1learner
	tc.SetEnablePlacementRules(true)
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd",
		ID:      "default",
		Role:    placement.Voter,
		Count:   2,
	})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd",
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
	})

	// Add stores 1, 2, 3, 4
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 7)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 9)

	// Put a region with 1leader + 1follower + 1learner
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(&metapb.Region{
		Id:          1,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}, peers[0])
	tc.PutRegion(region)

	sl, err := schedule.CreateScheduler(ShuffleRegionType, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ShuffleRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	conf := sl.(*shuffleRegionScheduler).conf
	conf.Roles = []string{"follower"}
	ops := sl.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpKind(0), 2, 4) // transfer follower
	conf.Roles = []string{"learner"}
	ops = sl.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferLearner(c, ops[0], operator.OpRegion, 3, 4) // transfer learner
}

var _ = Suite(&testSpecialUseSuite{})

type testSpecialUseSuite struct{}

func (s *testSpecialUseSuite) TestSpecialUseHotRegion(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	storage := core.NewStorage(kv.NewMemoryKV())
	cd := schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""})
	bs, err := schedule.CreateScheduler(BalanceRegionType, oc, storage, cd)
	c.Assert(err, IsNil)
	hs, err := schedule.CreateScheduler(HotWriteRegionType, oc, storage, cd)
	c.Assert(err, IsNil)

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 4)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)
	tc.AddLeaderRegion(4, 1, 2, 3)
	tc.AddLeaderRegion(5, 1, 2, 3)

	// balance region without label
	ops := bs.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to store 4 and 5 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "hotRegion"})
	tc.AddLabelsStore(5, 0, map[string]string{"specialUse": "reserved"})
	ops = bs.Schedule(tc)
	c.Assert(ops, HasLen, 0)

	// can only move peer to 4
	tc.UpdateStorageWrittenBytes(1, 60*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 0)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(4, 2, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{1, 3})
	tc.AddLeaderRegionWithWriteInfo(5, 3, 512*KB*statistics.RegionHeartBeatReportInterval, 0, statistics.RegionHeartBeatReportInterval, []uint64{1, 2})
	ops = hs.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpHotRegion, 1, 4)
}

func (s *testSpecialUseSuite) TestSpecialUseReserved(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	storage := core.NewStorage(kv.NewMemoryKV())
	cd := schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""})
	bs, err := schedule.CreateScheduler(BalanceRegionType, oc, storage, cd)
	c.Assert(err, IsNil)

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 4)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 0)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)
	tc.AddLeaderRegion(4, 1, 2, 3)
	tc.AddLeaderRegion(5, 1, 2, 3)

	// balance region without label
	ops := bs.Schedule(tc)
	c.Assert(ops, HasLen, 1)
	testutil.CheckTransferPeer(c, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to store 4 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "reserved"})
	ops = bs.Schedule(tc)
	c.Assert(ops, HasLen, 0)
}

var _ = Suite(&testBalanceLeaderSchedulerWithRuleEnabledSuite{})

type testBalanceLeaderSchedulerWithRuleEnabledSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *config.PersistOptions
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.opt)
	s.tc.SetEnablePlacementRules(true)
	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) TestBalanceLeaderWithConflictRule(c *C) {
	// Stores:     1    2    3
	// Leaders:    1    0    0
	// Region1:    L    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderRegion(1, 1, 2, 3)
	s.tc.SetStoreLabel(1, map[string]string{
		"host": "a",
	})
	s.tc.SetStoreLabel(2, map[string]string{
		"host": "b",
	})
	s.tc.SetStoreLabel(3, map[string]string{
		"host": "c",
	})

	// Stores:     1    2    3
	// Leaders:    16   0    0
	// Region1:    L    F    F
	s.tc.UpdateLeaderCount(1, 16)
	testcases := []struct {
		name     string
		rule     placement.Rule
		schedule bool
	}{
		{
			name: "default Rule",
			rule: placement.Rule{
				GroupID:        "pd",
				ID:             "default",
				Index:          1,
				StartKey:       []byte(""),
				EndKey:         []byte(""),
				Role:           placement.Voter,
				Count:          3,
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
		{
			name: "single store allowed to be placed leader",
			rule: placement.Rule{
				GroupID:  "pd",
				ID:       "default",
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: false,
		},
		{
			name: "2 store allowed to be placed leader",
			rule: placement.Rule{
				GroupID:  "pd",
				ID:       "default",
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a", "b"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
	}

	for _, testcase := range testcases {
		c.Logf(testcase.name)
		c.Check(s.tc.SetRule(&testcase.rule), IsNil)
		if testcase.schedule {
			c.Check(len(s.schedule()), Equals, 1)
		} else {
			c.Check(s.schedule(), IsNil)
		}
	}
}
