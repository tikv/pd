// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"math"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testBalanceSuite{})

type testBalanceSuite struct{}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	regionSize     int64
	expectedResult bool
	kind           core.SchedulePolicy
}

func (s *testBalanceSuite) TestShouldBalance(c *C) {
	tests := []testBalanceSpeedCase{
		// all store capacity is 1024MB
		// size = count * 10

		// target size is zero
		{2, 0, 1, true, core.BySize},
		{2, 0, 10, false, core.BySize},
		// all in high space stage
		{10, 5, 1, true, core.BySize},
		{10, 5, 20, false, core.BySize},
		{10, 10, 1, false, core.BySize},
		{10, 10, 20, false, core.BySize},
		// all in transition stage
		{70, 50, 1, true, core.BySize},
		{70, 50, 50, false, core.BySize},
		{70, 70, 1, false, core.BySize},
		// all in low space stage
		{90, 80, 1, true, core.BySize},
		{90, 80, 50, false, core.BySize},
		{90, 90, 1, false, core.BySize},
		// one in high space stage, other in transition stage
		{65, 55, 5, true, core.BySize},
		{65, 50, 50, false, core.BySize},
		// one in transition space stage, other in low space stage
		{80, 70, 5, true, core.BySize},
		{80, 70, 50, false, core.BySize},

		// default leader tolerant ratio is 5, when schedule by count
		// target size is zero
		{2, 0, 1, false, core.ByCount},
		{2, 0, 10, false, core.ByCount},
		// all in high space stage
		{10, 5, 1, true, core.ByCount},
		{10, 5, 20, true, core.ByCount},
		{10, 6, 20, false, core.ByCount},
		{10, 10, 1, false, core.ByCount},
		{10, 10, 20, false, core.ByCount},
		// all in transition stage
		{70, 50, 1, true, core.ByCount},
		{70, 50, 50, true, core.ByCount},
		{70, 70, 1, false, core.ByCount},
		// all in low space stage
		{90, 80, 1, true, core.ByCount},
		{90, 80, 50, true, core.ByCount},
		{90, 90, 1, false, core.ByCount},
		// one in high space stage, other in transition stage
		{65, 55, 5, true, core.ByCount},
		{65, 50, 50, true, core.ByCount},
		// one in transition space stage, other in low space stage
		{80, 70, 5, true, core.ByCount},
		{80, 70, 50, true, core.ByCount},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetTolerantSizeRatio(2.5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	// create a region to control average region size.
	tc.AddLeaderRegion(1, 1, 2)

	for _, t := range tests {
		tc.AddLeaderStore(1, int(t.sourceCount))
		tc.AddLeaderStore(2, int(t.targetCount))
		source := tc.GetStore(1)
		target := tc.GetStore(2)
		region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
		tc.PutRegion(region)
		tc.SetLeaderSchedulePolicy(t.kind.String())
		kind := core.NewScheduleKind(core.LeaderKind, t.kind)
		shouldBalance, _, _ := shouldBalance(tc, source, target, region, kind, oc.GetOpInfluence(tc), "")
		c.Assert(shouldBalance, Equals, t.expectedResult)
	}

	for _, t := range tests {
		if t.kind.String() == core.BySize.String() {
			tc.AddRegionStore(1, int(t.sourceCount))
			tc.AddRegionStore(2, int(t.targetCount))
			source := tc.GetStore(1)
			target := tc.GetStore(2)
			region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
			tc.PutRegion(region)
			kind := core.NewScheduleKind(core.RegionKind, t.kind)
			shouldBalance, _, _ := shouldBalance(tc, source, target, region, kind, oc.GetOpInfluence(tc), "")
			c.Assert(shouldBalance, Equals, t.expectedResult)
		}
	}
}

func (s *testBalanceSuite) TestBalanceLimit(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddLeaderStore(1, 10)
	tc.AddLeaderStore(2, 20)
	tc.AddLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(200.0/3.0)))

	tc.SetStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(50.0/2.0)))
}

func (s *testBalanceSuite) TestTolerantRatio(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// create a region to control average region size.
	c.Assert(tc.AddLeaderRegion(1, 1, 2), NotNil)
	regionSize := int64(96 * KB)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(regionSize))

	tc.SetTolerantSizeRatio(0)
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.ByCount}), Equals, int64(leaderTolerantSizeRatio))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Policy: core.ByCount}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Policy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))

	tc.SetTolerantSizeRatio(10)
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.ByCount}), Equals, int64(tc.GetScheduleConfig().TolerantSizeRatio))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Policy: core.ByCount}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Policy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *config.PersistOptions
}

func (s *testBalanceLeaderSchedulerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.opt)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceLeaderSchedulerSuite) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLimit(c *C) {
	s.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 0)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	s.tc.UpdateLeaderCount(1, 16)
	c.Check(s.schedule(), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 7)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 10)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(4, 16)
	c.Check(s.schedule(), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLeaderSchedulePolicy(c *C) {
	// Stores:          1       2       3       4
	// Leader Count:    10      10      10      10
	// Leader Size :    10000   100    	100    	100
	// Region1:         L       F       F       F
	s.tc.AddLeaderStore(1, 10, 10000)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Assert(s.tc.GetScheduleConfig().LeaderSchedulePolicy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), IsNil)
	s.tc.SetLeaderSchedulePolicy(core.BySize.String())
	c.Check(s.schedule(), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLeaderTolerantRatio(c *C) {
	s.tc.SetTolerantSizeRatio(2.5)
	// test schedule leader by count, with tolerantSizeRatio=2.5
	// Stores:          1       2       3       4
	// Leader Count:    14->15  10      10      10
	// Leader Size :    100     100     100     100
	// Region1:         L       F       F       F
	s.tc.AddLeaderStore(1, 14, 100)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Assert(s.tc.GetScheduleConfig().LeaderSchedulePolicy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), IsNil)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 14)
	s.tc.AddLeaderStore(1, 15, 100)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 15)
	c.Check(s.schedule(), NotNil)
	s.tc.SetTolerantSizeRatio(6) // (15-10)<6
	c.Check(s.schedule(), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestScheduleWithOpInfluence(c *C) {
	s.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   14
	// Region1:    F    F    F    L
	s.tc.AddLeaderStore(1, 7)
	s.tc.AddLeaderStore(2, 8)
	s.tc.AddLeaderStore(3, 9)
	s.tc.AddLeaderStore(4, 14)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	op := s.schedule()[0]
	c.Check(op, NotNil)
	s.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader difference is not greater than 5.
	c.Assert(s.tc.GetScheduleConfig().LeaderSchedulePolicy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), NotNil)
	s.tc.SetLeaderSchedulePolicy(core.BySize.String())
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 8)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 13)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestTransferLeaderOut(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   12
	s.tc.AddLeaderStore(1, 7)
	s.tc.AddLeaderStore(2, 8)
	s.tc.AddLeaderStore(3, 9)
	s.tc.AddLeaderStore(4, 12)
	s.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		s.tc.AddLeaderRegion(i, 4, 1, 2, 3)
	}

	// balance leader: 4->1, 4->1, 4->2
	regions := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for i := 0; i < 20; i++ {
		if len(s.schedule()) == 0 {
			continue
		}
		if op := s.schedule()[0]; op != nil {
			if _, ok := regions[op.RegionID()]; !ok {
				s.oc.SetOperator(op)
				regions[op.RegionID()] = struct{}{}
				tr := op.Step(0).(operator.TransferLeader)
				c.Assert(tr.FromStore, Equals, uint64(4))
				targets[tr.ToStore]--
			}
		}
	}
	c.Assert(regions, HasLen, 3)
	for _, count := range targets {
		c.Assert(count, Equals, uint64(0))
	}
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceFilter(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)

	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we should consider it
	// because it still provides services
	s.tc.SetStoreOffline(4)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.SetStoreDown(1)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.SetStoreBusy(2, true)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 3)

	// Test disconnectFilter.
	// If store 3 is disconnected, no operator can be created.
	s.tc.SetStoreDisconnect(3)
	c.Assert(s.schedule(), HasLen, 0)
}

func (s *testBalanceLeaderSchedulerSuite) TestLeaderWeight(c *C) {
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	s.tc.SetTolerantSizeRatio(2.5)
	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 1, 4)
	s.tc.UpdateLeaderCount(4, 30)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 1, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalancePolicy(c *C) {
	// Stores:       1    2     3    4
	// LeaderCount: 20   66     6   20
	// LeaderSize:  66   20    20    6
	s.tc.AddLeaderStore(1, 20, 60)
	s.tc.AddLeaderStore(2, 66, 20)
	s.tc.AddLeaderStore(3, 6, 20)
	s.tc.AddLeaderStore(4, 20, 1)
	s.tc.AddLeaderRegion(1, 2, 1, 3, 4)
	s.tc.AddLeaderRegion(2, 1, 2, 3, 4)
	s.tc.SetLeaderSchedulePolicy("count")
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 2, 3)
	s.tc.SetLeaderSchedulePolicy("size")
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 1, 4)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceSelector(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderRegion(1, 4, 2, 3)
	s.tc.AddLeaderRegion(2, 3, 1, 2)
	// store4 has max leader score, store1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.UpdateLeaderCount(2, 14)
	s.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderRegion(1, 3, 2, 4)
	s.tc.AddLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. Now source and target are store3 and store2.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 3, 2)

	// Stores:     1    2    3    4
	// Leaders:    9    10   10   11
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.AddLeaderRegion(1, 4, 2, 3)
	s.tc.AddLeaderRegion(2, 1, 2, 3)
	// The cluster is balanced.
	c.Assert(s.schedule(), IsNil)
	c.Assert(s.schedule(), IsNil)

	// store3's leader drops:
	// Stores:     1    2    3    4
	// Leaders:    11   13   0    16
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(1, 11)
	s.tc.AddLeaderStore(2, 13)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 16)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpKind(0), 4, 3)
}

var _ = Suite(&testBalanceLeaderRangeSchedulerSuite{})

type testBalanceLeaderRangeSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *schedule.OperatorController
}

func (s *testBalanceLeaderRangeSchedulerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	opt := config.NewTestOptions()
	s.tc = mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
}

func (s *testBalanceLeaderRangeSchedulerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceLeaderRangeSchedulerSuite) TestSingleRangeBalance(c *C) {
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F

	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	ops := lb.Schedule(s.tc)
	c.Assert(ops, NotNil)
	c.Assert(ops, HasLen, 1)
	c.Assert(ops[0].Counters, HasLen, 3)
	c.Assert(ops[0].FinishedCounters, HasLen, 2)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"h", "n"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"b", "f"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "a"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"g", ""}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "f"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"b", ""}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
}

func (s *testBalanceLeaderRangeSchedulerSuite) TestMultiRangeBalance(c *C) {
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F

	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "g", "o", "t"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc)[0].RegionID(), Equals, uint64(1))
	s.tc.RemoveRegion(s.tc.GetRegion(1))
	s.tc.AddLeaderRegionWithRange(2, "p", "r", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc)[0].RegionID(), Equals, uint64(2))
	s.tc.RemoveRegion(s.tc.GetRegion(2))
	s.tc.AddLeaderRegionWithRange(3, "u", "w", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
	s.tc.RemoveRegion(s.tc.GetRegion(3))
	s.tc.AddLeaderRegionWithRange(4, "", "", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), IsNil)
}

var _ = Suite(&testBalanceRegionSchedulerSuite{})

type testBalanceRegionSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testBalanceRegionSchedulerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testBalanceRegionSchedulerSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testBalanceRegionSchedulerSuite) TestBalance(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	opt.SetMaxReplicas(1)

	// Add stores 1,2,3,4.
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 8)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 16)
	// Add region 1 with leader in store 4.
	tc.AddLeaderRegion(1, 4)
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpKind(0), 4, 1)

	// Test stateFilter.
	tc.SetStoreOffline(1)
	tc.UpdateRegionCount(2, 6)

	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpKind(0), 4, 2)
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(tc), IsNil)

	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(tc), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplica3(c, tc, opt, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica3(c, tc, opt, sb)
}

func (s *testBalanceRegionSchedulerSuite) checkReplica3(c *C, tc *mockcluster.Cluster, opt *config.PersistOptions, sb schedule.Scheduler) {
	// Store 1 has the largest region score, so the balance scheduler tries to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores.
	c.Assert(sb.Schedule(tc), IsNil)

	// Store 4 has smaller region score than store 2.
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.AddLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// Store 7 has smaller region score with store 6.
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 7)

	// If store 7 is not available, will choose store 6.
	tc.SetStoreDown(7)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// Take down 4,5,6,7
	tc.SetStoreDown(4)
	tc.SetStoreDown(5)
	tc.SetStoreDown(6)
	tc.SetStoreDown(7)
	tc.SetStoreDown(8)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.AddLabelsStore(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	c.Assert(sb.Schedule(tc), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(5)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplica5(c, tc, opt, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica5(c, tc, opt, sb)
}

func (s *testBalanceRegionSchedulerSuite) checkReplica5(c *C, tc *mockcluster.Cluster, opt *config.PersistOptions, sb schedule.Scheduler) {
	tc.AddLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// Store 7 has larger region score and same distinct score with store 6.
	tc.AddLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// Store 1 has smaller region score and higher distinct score.
	tc.AddLeaderRegion(1, 2, 3, 4, 5, 6)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.AddLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.AddLeaderRegion(1, 2, 3, 11, 12, 13)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 11, 6)
}

// TestBalance2 for corner case 1:
// 11 regions distributed across 5 stores.
//| region_id | leader_store | follower_store | follower_store |
//|-----------|--------------|----------------|----------------|
//|     1     |       1      |        2       |       3        |
//|     2     |       1      |        2       |       3        |
//|     3     |       1      |        2       |       3        |
//|     4     |       1      |        2       |       3        |
//|     5     |       1      |        2       |       3        |
//|     6     |       1      |        2       |       3        |
//|     7     |       1      |        2       |       4        |
//|     8     |       1      |        2       |       4        |
//|     9     |       1      |        2       |       4        |
//|    10     |       1      |        4       |       5        |
//|    11     |       1      |        4       |       5        |
// and the space of last store 5 if very small, about 5 * regionSize
// the source region is more likely distributed in store[1, 2, 3].
func (s *testBalanceRegionSchedulerSuite) TestBalance1(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetTolerantSizeRatio(1)
	tc.SetRegionScheduleLimit(1)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	source := core.NewRegionInfo(
		&metapb.Region{
			Id:       1,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
			Peers: []*metapb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
			},
		},
		&metapb.Peer{Id: 101, StoreId: 1},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	target := core.NewRegionInfo(
		&metapb.Region{
			Id:       2,
			StartKey: []byte("a"),
			EndKey:   []byte("t"),
			Peers: []*metapb.Peer{
				{Id: 103, StoreId: 1},
				{Id: 104, StoreId: 4},
				{Id: 105, StoreId: 5},
			},
		},
		&metapb.Peer{Id: 104, StoreId: 4},
		core.SetApproximateSize(200),
		core.SetApproximateKeys(200),
	)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 11)
	tc.AddRegionStore(2, 9)
	tc.AddRegionStore(3, 6)
	tc.AddRegionStore(4, 5)
	tc.AddRegionStore(5, 2)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)

	// add two merge operator to let the count of opRegion to 2.
	ops, err := operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	c.Assert(err, IsNil)
	oc.SetOperator(ops[0])
	oc.SetOperator(ops[1])

	c.Assert(sb.IsScheduleAllowed(tc), IsTrue)
	c.Assert(sb.Schedule(tc)[0], NotNil)
	// if the space of store 5 is normal, we can balance region to store 5
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// the used size of store 5 reach (highSpace, lowSpace)
	origin := tc.GetStore(5)
	stats := origin.GetStoreStats()
	stats.Capacity = 50
	stats.Available = 28
	stats.UsedSize = 20
	store5 := origin.Clone(core.SetStoreStats(stats))
	tc.PutStore(store5)

	// the scheduler first picks store 1 as source store,
	// and store 5 as target store, but cannot pass `shouldBalance`.
	// Then it will try store 4.
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func (s *testBalanceRegionSchedulerSuite) TestStoreWeight(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.UpdateStoreRegionWeight(1, 0.5)
	tc.UpdateStoreRegionWeight(2, 0.9)
	tc.UpdateStoreRegionWeight(3, 1.0)
	tc.UpdateStoreRegionWeight(4, 2.0)

	tc.AddLeaderRegion(1, 1)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)

	tc.UpdateRegionCount(4, 30)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 3)
}

func (s *testBalanceRegionSchedulerSuite) TestReplacePendingRegion(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplacePendingRegion(c, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplacePendingRegion(c, tc, sb)
}

func (s *testBalanceRegionSchedulerSuite) TestOpInfluence(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := schedule.NewOperatorController(s.ctx, tc, stream)
	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)
	// Add stores 1,2,3,4.
	tc.AddRegionStoreWithLeader(1, 2)
	tc.AddRegionStoreWithLeader(2, 8)
	tc.AddRegionStoreWithLeader(3, 8)
	tc.AddRegionStoreWithLeader(4, 16, 8)

	// add 8 leader regions to store 4 and move them to store 3
	// ensure store score without operator influence : store 4 > store 3
	// and store score with operator influence : store 3 > store 4
	for i := 1; i <= 8; i++ {
		id, _ := tc.Alloc()
		origin := tc.AddLeaderRegion(id, 4)
		newPeer := &metapb.Peer{StoreId: 3, Role: metapb.PeerRole_Voter}
		op, _ := operator.CreateMovePeerOperator("balance-region", tc, origin, operator.OpKind(0), 4, newPeer)
		c.Assert(op, NotNil)
		oc.AddOperator(op)
	}
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpKind(0), 3, 1)
}

func (s *testBalanceRegionSchedulerSuite) checkReplacePendingRegion(c *C, tc *mockcluster.Cluster, sb schedule.Scheduler) {
	// Store 1 has the largest region score, so the balance scheduler try to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 7, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})
	// Store 4 has smaller region score than store 1 and more better place than store 2.
	tc.AddLabelsStore(4, 10, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// set pending peer
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 2, 1, 3)
	region := tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(1)}))
	tc.PutRegion(region)

	c.Assert(sb.Schedule(tc)[0].RegionID(), Equals, uint64(3))
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func (s *testBalanceRegionSchedulerSuite) TestShouldNotBalance(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	region := tc.MockRegionInfo(1, 0, []uint64{2, 3, 4}, nil, nil)
	tc.PutRegion(region)
	operators := sb.Schedule(tc)
	if operators != nil {
		c.Assert(len(operators), Equals, 0)
	} else {
		c.Assert(operators, IsNil)
	}
}

var _ = Suite(&testRandomMergeSchedulerSuite{})

type testRandomMergeSchedulerSuite struct{}

func (s *testRandomMergeSchedulerSuite) TestMerge(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMergeScheduleLimit(1)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, true /* need to run */)
	oc := schedule.NewOperatorController(ctx, tc, stream)

	mb, err := schedule.CreateScheduler(RandomMergeType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(RandomMergeType, []string{"", ""}))
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 4)
	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 1)
	tc.AddLeaderRegion(3, 1)
	tc.AddLeaderRegion(4, 1)

	c.Assert(mb.IsScheduleAllowed(tc), IsTrue)
	ops := mb.Schedule(tc)
	c.Assert(ops, HasLen, 0) // regions are not fully replicated

	tc.SetMaxReplicas(1)
	ops = mb.Schedule(tc)
	c.Assert(ops, HasLen, 2)
	c.Assert(ops[0].Kind()&operator.OpMerge, Not(Equals), 0)
	c.Assert(ops[1].Kind()&operator.OpMerge, Not(Equals), 0)

	oc.AddWaitingOperator(ops...)
	c.Assert(mb.IsScheduleAllowed(tc), IsFalse)
}

var _ = Suite(&testScatterRangeLeaderSuite{})

type testScatterRangeLeaderSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testScatterRangeLeaderSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testScatterRangeLeaderSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testScatterRangeLeaderSuite) TestBalance(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetTolerantSizeRatio(2.5)
	// Add stores 1,2,3,4,5.
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 0)
	var (
		id      uint64
		regions []*metapb.Region
	)
	for i := 0; i < 50; i++ {
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: 1},
			{Id: id + 2, StoreId: 2},
			{Id: id + 3, StoreId: 3},
		}
		regions = append(regions, &metapb.Region{
			Id:       id + 4,
			Peers:    peers,
			StartKey: []byte(fmt.Sprintf("s_%02d", i)),
			EndKey:   []byte(fmt.Sprintf("s_%02d", i+1)),
		})
		id += 4
	}
	// empty case
	regions[49].EndKey = []byte("")
	for _, meta := range regions {
		leader := rand.Intn(4) % 3
		regionInfo := core.NewRegionInfo(
			meta,
			meta.Peers[leader],
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Regions.SetRegion(regionInfo)
	}
	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		c.Assert(err, IsNil)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	c.Assert(err, IsNil)
	limit := 0
	for {
		if limit > 100 {
			break
		}
		ops := hb.Schedule(tc)
		if ops == nil {
			limit++
			continue
		}
		schedule.ApplyOperator(tc, ops[0])
	}
	for i := 1; i <= 5; i++ {
		leaderCount := tc.Regions.GetStoreLeaderCount(uint64(i))
		c.Check(leaderCount, LessEqual, 12)
		regionCount := tc.Regions.GetStoreRegionCount(uint64(i))
		c.Check(regionCount, LessEqual, 32)
	}
}

func (s *testScatterRangeLeaderSuite) TestConcurrencyUpdateConfig(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	sche := hb.(*scatterRangeScheduler)
	c.Assert(err, IsNil)
	ch := make(chan struct{})
	args := []string{"test", "s_00", "s_99"}
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
			}
			sche.config.BuildWithArgs(args)
			c.Assert(sche.config.Persist(), IsNil)
		}
	}()
	for i := 0; i < 1000; i++ {
		sche.Schedule(tc)
	}
	ch <- struct{}{}
}

func (s *testScatterRangeLeaderSuite) TestBalanceWhenRegionNotHeartbeat(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add stores 1,2,3.
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	var (
		id      uint64
		regions []*metapb.Region
	)
	for i := 0; i < 10; i++ {
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: 1},
			{Id: id + 2, StoreId: 2},
			{Id: id + 3, StoreId: 3},
		}
		regions = append(regions, &metapb.Region{
			Id:       id + 4,
			Peers:    peers,
			StartKey: []byte(fmt.Sprintf("s_%02d", i)),
			EndKey:   []byte(fmt.Sprintf("s_%02d", i+1)),
		})
		id += 4
	}
	// empty case
	regions[9].EndKey = []byte("")

	// To simulate server prepared,
	// store 1 contains 8 leader region peers and leaders of 2 regions are unknown yet.
	for _, meta := range regions {
		var leader *metapb.Peer
		if meta.Id < 8 {
			leader = meta.Peers[0]
		}
		regionInfo := core.NewRegionInfo(
			meta,
			leader,
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Regions.SetRegion(regionInfo)
	}

	for i := 1; i <= 3; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_09", "t"}))
	c.Assert(err, IsNil)

	limit := 0
	for {
		if limit > 100 {
			break
		}
		ops := hb.Schedule(tc)
		if ops == nil {
			limit++
			continue
		}
		schedule.ApplyOperator(tc, ops[0])
	}
}
