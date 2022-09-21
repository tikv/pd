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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testBalanceSuite{})

type testBalanceSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testBalanceSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testBalanceSuite) TearDownTest(c *C) {
	s.cancel()
}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	regionSize     int64
	expectedResult bool
	kind           core.SchedulePolicy
}

func (s *testBalanceSuite) TestInfluenceAmp(c *C) {
	R := int64(96)
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	influence := oc.GetOpInfluence(tc)
	influence.GetStoreInfluence(1).RegionSize = R
	influence.GetStoreInfluence(2).RegionSize = -R
	tc.SetTolerantSizeRatio(1)

	// It will schedule if the diff region count is greater than the sum
	// of TolerantSizeRatio and influenceAmp*2.
	tc.AddRegionStore(1, int(100+influenceAmp+3))
	tc.AddRegionStore(2, int(100-influenceAmp))
	tc.AddLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(R))
	tc.PutRegion(region)
	plan := newBalancePlan(kind, tc, influence)
	plan.source, plan.target, plan.region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
	c.Assert(plan.shouldBalance(""), IsTrue)

	// It will not schedule if the diff region count is greater than the sum
	// of TolerantSizeRatio and influenceAmp*2.
	tc.AddRegionStore(1, int(100+influenceAmp+2))
	plan.source = tc.GetStore(1)
	c.Assert(plan.shouldBalance(""), IsFalse)
	c.Assert(plan.sourceScore-plan.targetScore, Less, float64(1))
}

func (s *testBalanceSuite) TestShouldBalance(c *C) {
	// store size = 100GiB
	// region size = 96MiB
	const R = 96
	tests := []testBalanceSpeedCase{
		// target size is zero
		{2, 0, R / 10, true, core.BySize},
		{2, 0, R, false, core.BySize},
		// all in high space stage
		{10, 5, R / 10, true, core.BySize},
		{10, 5, 2 * R, false, core.BySize},
		{10, 10, R / 10, false, core.BySize},
		{10, 10, 2 * R, false, core.BySize},
		// all in transition stage
		{700, 680, R / 10, true, core.BySize},
		{700, 680, 5 * R, false, core.BySize},
		{700, 700, R / 10, false, core.BySize},
		// all in low space stage
		{900, 890, R / 10, true, core.BySize},
		{900, 890, 5 * R, false, core.BySize},
		{900, 900, R / 10, false, core.BySize},
		// one in high space stage, other in transition stage
		{650, 550, R, true, core.BySize},
		{650, 500, 50 * R, false, core.BySize},
		// one in transition space stage, other in low space stage
		{800, 700, R, true, core.BySize},
		{800, 700, 50 * R, false, core.BySize},

		// default leader tolerant ratio is 5, when schedule by count
		// target size is zero
		{2, 0, R / 10, false, core.ByCount},
		{2, 0, R, false, core.ByCount},
		// all in high space stage
		{10, 5, R / 10, true, core.ByCount},
		{10, 5, 2 * R, true, core.ByCount},
		{10, 6, 2 * R, false, core.ByCount},
		{10, 10, R / 10, false, core.ByCount},
		{10, 10, 2 * R, false, core.ByCount},
		// all in transition stage
		{70, 50, R / 10, true, core.ByCount},
		{70, 50, 5 * R, true, core.ByCount},
		{70, 70, R / 10, false, core.ByCount},
		// all in low space stage
		{90, 80, R / 10, true, core.ByCount},
		{90, 80, 5 * R, true, core.ByCount},
		{90, 90, R / 10, false, core.ByCount},
		// one in high space stage, other in transition stage
		{65, 55, R / 2, true, core.ByCount},
		{65, 50, 5 * R, true, core.ByCount},
		// one in transition space stage, other in low space stage
		{80, 70, R / 2, true, core.ByCount},
		{80, 70, 5 * R, true, core.ByCount},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetTolerantSizeRatio(2.5)
	tc.SetRegionScoreFormulaVersion("v1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	// create a region to control average region size.
	tc.AddLeaderRegion(1, 1, 2)

	for _, t := range tests {
		tc.AddLeaderStore(1, int(t.sourceCount))
		tc.AddLeaderStore(2, int(t.targetCount))
		region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
		tc.PutRegion(region)
		tc.SetLeaderSchedulePolicy(t.kind.String())
		kind := core.NewScheduleKind(core.LeaderKind, t.kind)
		plan := newBalancePlan(kind, tc, oc.GetOpInfluence(tc))
		plan.source, plan.target, plan.region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
		c.Assert(plan.shouldBalance(""), Equals, t.expectedResult)
	}

	for _, t := range tests {
		if t.kind.String() == core.BySize.String() {
			tc.AddRegionStore(1, int(t.sourceCount))
			tc.AddRegionStore(2, int(t.targetCount))
			region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
			tc.PutRegion(region)
			kind := core.NewScheduleKind(core.RegionKind, t.kind)
			plan := newBalancePlan(kind, tc, oc.GetOpInfluence(tc))
			plan.source, plan.target, plan.region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
			c.Assert(plan.shouldBalance(""), Equals, t.expectedResult)
		}
	}
}

func (s *testBalanceSuite) TestTolerantRatio(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	// create a region to control average region size.
	c.Assert(tc.AddLeaderRegion(1, 1, 2), NotNil)
	regionSize := int64(96 * KB)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(regionSize))

	tbl := []struct {
		ratio                  float64
		kind                   core.ScheduleKind
		expectTolerantResource func(core.ScheduleKind) int64
	}{
		{0, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.ByCount}, func(k core.ScheduleKind) int64 {
			return int64(leaderTolerantSizeRatio)
		}},
		{0, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.BySize}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{0, core.ScheduleKind{Resource: core.RegionKind, Policy: core.ByCount}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{0, core.ScheduleKind{Resource: core.RegionKind, Policy: core.BySize}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.ByCount}, func(k core.ScheduleKind) int64 {
			return int64(tc.GetScheduleConfig().TolerantSizeRatio)
		}},
		{10, core.ScheduleKind{Resource: core.LeaderKind, Policy: core.BySize}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, core.ScheduleKind{Resource: core.RegionKind, Policy: core.ByCount}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, core.ScheduleKind{Resource: core.RegionKind, Policy: core.BySize}, func(k core.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
	}
	for i, t := range tbl {
		tc.SetTolerantSizeRatio(t.ratio)
		plan := newBalancePlan(t.kind, tc, operator.OpInfluence{})
		plan.region = region
		c.Assert(plan.getTolerantResource(), Equals, t.expectTolerantResource(t.kind), Commentf("case #%d", i+1))
	}
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
	s.tc = mockcluster.NewCluster(s.ctx, s.opt)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
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
	c.Assert(s.schedule(), HasLen, 0)

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	s.tc.UpdateLeaderCount(1, 16)
	c.Assert(len(s.schedule()), Greater, 0)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 7)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 10)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Assert(s.schedule(), HasLen, 0)

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
	s.tc.AddLeaderStore(1, 10, 10000*MB)
	s.tc.AddLeaderStore(2, 10, 100*MB)
	s.tc.AddLeaderStore(3, 10, 100*MB)
	s.tc.AddLeaderStore(4, 10, 100*MB)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Assert(s.tc.GetScheduleConfig().LeaderSchedulePolicy, Equals, core.ByCount.String()) // default by count
	c.Assert(s.schedule(), HasLen, 0)
	s.tc.SetLeaderSchedulePolicy(core.BySize.String())
	c.Assert(len(s.schedule()), Greater, 0)
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
	c.Assert(s.schedule(), HasLen, 0)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 14)
	s.tc.AddLeaderStore(1, 15, 100)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 15)
	c.Check(s.schedule(), NotNil)
	s.tc.SetTolerantSizeRatio(6) // (15-10)<6
	c.Assert(s.schedule(), HasLen, 0)
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
	c.Assert(s.schedule(), HasLen, 0)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 8)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 13)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Assert(s.schedule(), HasLen, 0)
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
	s.tc.AddLeaderStore(1, 20, 600*MB)
	s.tc.AddLeaderStore(2, 66, 200*MB)
	s.tc.AddLeaderStore(3, 6, 20*MB)
	s.tc.AddLeaderStore(4, 20, 1*MB)
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
	c.Assert(s.schedule(), HasLen, 0)
	c.Assert(s.schedule(), HasLen, 0)

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
	s.tc = mockcluster.NewCluster(s.ctx, opt)
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
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	ops := lb.Schedule(s.tc)
	c.Assert(ops, NotNil)
	c.Assert(ops, HasLen, 1)
	c.Assert(ops[0].Counters, HasLen, 2)
	c.Assert(ops[0].FinishedCounters, HasLen, 3)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"h", "n"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"b", "f"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "a"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"g", ""}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "f"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"b", ""}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
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
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", "g", "o", "t"}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc)[0].RegionID(), Equals, uint64(1))
	s.tc.RemoveRegion(s.tc.GetRegion(1))
	s.tc.AddLeaderRegionWithRange(2, "p", "r", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc)[0].RegionID(), Equals, uint64(2))
	s.tc.RemoveRegion(s.tc.GetRegion(2))
	s.tc.AddLeaderRegionWithRange(3, "u", "w", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
	s.tc.RemoveRegion(s.tc.GetRegion(3))
	s.tc.AddLeaderRegionWithRange(4, "", "", 1, 2, 3, 4)
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 0)
}

func (s *testBalanceLeaderRangeSchedulerSuite) TestBatchBalance(c *C) {
	s.tc.AddLeaderStore(1, 100)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 100)
	s.tc.AddLeaderStore(5, 100)
	s.tc.AddLeaderStore(6, 0)

	s.tc.AddLeaderRegionWithRange(uint64(102), "102a", "102z", 1, 2, 3)
	s.tc.AddLeaderRegionWithRange(uint64(103), "103a", "103z", 4, 5, 6)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	c.Assert(lb.Schedule(s.tc), HasLen, 2)
	for i := 1; i <= 50; i++ {
		s.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 1, 2, 3)
	}
	for i := 51; i <= 100; i++ {
		s.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 4, 5, 6)
	}
	s.tc.AddLeaderRegionWithRange(uint64(101), "101a", "101z", 5, 4, 3)
	ops := lb.Schedule(s.tc)
	c.Assert(ops, HasLen, 4)
	regions := make(map[uint64]struct{})
	for _, op := range ops {
		regions[op.RegionID()] = struct{}{}
	}
	c.Assert(regions, HasLen, 4)
}

func (s *testBalanceLeaderRangeSchedulerSuite) TestReSortStores(c *C) {
	s.tc.AddLeaderStore(1, 104)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 100)
	s.tc.AddLeaderStore(5, 100)
	s.tc.AddLeaderStore(6, 0)
	stores := s.tc.Stores.GetStores()

	deltaMap := make(map[uint64]int64)
	less := func(i, j int) bool {
		iOp := deltaMap[stores[i].GetID()]
		jOp := deltaMap[stores[j].GetID()]
		return stores[i].LeaderScore(0, iOp) > stores[j].LeaderScore(0, jOp)
	}

	sort.Slice(stores, less)
	storeIndexMap := map[uint64]int{}
	for i := 0; i < len(stores); i++ {
		storeIndexMap[stores[i].GetID()] = i
	}
	c.Assert(stores[0].GetID(), Equals, uint64(1))
	c.Assert(storeIndexMap[uint64(1)], Equals, 0)
	deltaMap[1] = -1

	resortStores(stores, storeIndexMap, storeIndexMap[uint64(1)], less)
	c.Assert(stores[0].GetID(), Equals, uint64(1))
	c.Assert(storeIndexMap[uint64(1)], Equals, 0)
	deltaMap[1] = -4
	resortStores(stores, storeIndexMap, storeIndexMap[uint64(1)], less)
	c.Assert(stores[2].GetID(), Equals, uint64(1))
	c.Assert(storeIndexMap[uint64(1)], Equals, 2)
	topID := stores[0].GetID()
	deltaMap[topID] = -1
	resortStores(stores, storeIndexMap, storeIndexMap[topID], less)
	c.Assert(stores[1].GetID(), Equals, uint64(1))
	c.Assert(storeIndexMap[uint64(1)], Equals, 1)
	c.Assert(stores[2].GetID(), Equals, topID)
	c.Assert(storeIndexMap[topID], Equals, 2)

	bottomID := stores[5].GetID()
	deltaMap[bottomID] = 4
	resortStores(stores, storeIndexMap, storeIndexMap[bottomID], less)
	c.Assert(stores[3].GetID(), Equals, bottomID)
	c.Assert(storeIndexMap[bottomID], Equals, 3)
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
	// TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
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
	c.Assert(sb.Schedule(tc), HasLen, 0)

	opt.SetMaxReplicas(1)
	c.Assert(len(sb.Schedule(tc)), Greater, 0)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	opt := config.NewTestOptions()
	// TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplica3(c, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica3(c, tc, sb)
}

func (s *testBalanceRegionSchedulerSuite) checkReplica3(c *C, tc *mockcluster.Cluster, sb schedule.Scheduler) {
	// Store 1 has the largest region score, so the balance scheduler tries to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores.
	c.Assert(sb.Schedule(tc), HasLen, 0)

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
	c.Assert(sb.Schedule(tc), HasLen, 0)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	opt := config.NewTestOptions()
	// TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetMaxReplicas(5)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplica5(c, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica5(c, tc, sb)
}

func (s *testBalanceRegionSchedulerSuite) checkReplica5(c *C, tc *mockcluster.Cluster, sb schedule.Scheduler) {
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
// | region_id | leader_store | follower_store | follower_store |
// |-----------|--------------|----------------|----------------|
// |     1     |       1      |        2       |       3        |
// |     2     |       1      |        2       |       3        |
// |     3     |       1      |        2       |       3        |
// |     4     |       1      |        2       |       3        |
// |     5     |       1      |        2       |       3        |
// |     6     |       1      |        2       |       3        |
// |     7     |       1      |        2       |       4        |
// |     8     |       1      |        2       |       4        |
// |     9     |       1      |        2       |       4        |
// |    10     |       1      |        4       |       5        |
// |    11     |       1      |        4       |       5        |
// and the space of last store 5 if very small, about 5 * regionSize
// the source region is more likely distributed in store[1, 2, 3].
func (s *testBalanceRegionSchedulerSuite) TestBalance1(c *C) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetTolerantSizeRatio(1)
	tc.SetRegionScheduleLimit(1)
	tc.SetRegionScoreFormulaVersion("v1")
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
				{Id: 105, StoreId: 3},
			},
		},
		&metapb.Peer{Id: 104, StoreId: 4},
		core.SetApproximateSize(200),
		core.SetApproximateKeys(200),
	)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
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
	stats.Available = 20
	stats.UsedSize = 28
	store5 := origin.Clone(core.SetStoreStats(stats))
	tc.PutStore(store5)
	// remove op influence
	oc.RemoveOperator(ops[1])
	oc.RemoveOperator(ops[0])
	// the scheduler first picks store 1 as source store,
	// and store 5 as target store, but cannot pass `shouldBalance`.
	// Then it will try store 4.
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func (s *testBalanceRegionSchedulerSuite) TestStoreWeight(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	// TODO: enable placementrules
	tc.SetPlacementRuleEnabled(false)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
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
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)

	s.checkReplacePendingRegion(c, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplacePendingRegion(c, tc, sb)
}

func (s *testBalanceRegionSchedulerSuite) TestOpInfluence(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	// TODO: enable placementrules
	tc.SetEnablePlacementRules(false)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := schedule.NewOperatorController(s.ctx, tc, stream)
	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
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
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpKind(0), 2, 1)
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
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	region := tc.MockRegionInfo(1, 0, []uint64{2, 3, 4}, nil, nil)
	tc.PutRegion(region)
	operators := sb.Schedule(tc)
	if operators != nil {
		c.Assert(operators, HasLen, 0)
	} else {
		c.Assert(operators, IsNil)
	}
}

func (s *testBalanceRegionSchedulerSuite) TestEmptyRegion(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	sb, err := schedule.CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	c.Assert(err, IsNil)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 9)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       5,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
			Peers: []*metapb.Peer{
				{Id: 6, StoreId: 1},
				{Id: 7, StoreId: 3},
				{Id: 8, StoreId: 4},
			},
		},
		&metapb.Peer{Id: 7, StoreId: 3},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	tc.PutRegion(region)
	operators := sb.Schedule(tc)
	c.Assert(operators, NotNil)

	for i := uint64(10); i < 60; i++ {
		tc.PutRegionStores(i, 1, 3, 4)
	}
	operators = sb.Schedule(tc)
	c.Assert(operators, HasLen, 0)
}

var _ = Suite(&testRandomMergeSchedulerSuite{})

type testRandomMergeSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testRandomMergeSchedulerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testRandomMergeSchedulerSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testRandomMergeSchedulerSuite) TestMerge(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	// TODO: enable palcementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetMergeScheduleLimit(1)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, true /* need to run */)
	oc := schedule.NewOperatorController(ctx, tc, stream)

	mb, err := schedule.CreateScheduler(RandomMergeType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(RandomMergeType, []string{"", ""}))
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

var _ = Suite(&testScatterRangeSuite{})

type testScatterRangeSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testScatterRangeSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testScatterRangeSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testScatterRangeSuite) TestBalance(c *C) {
	opt := config.NewTestOptions()
	// TODO: enable palcementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	// range cluster use a special tolerant ratio, cluster opt take no impact
	tc.SetTolerantSizeRatio(10000)
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
	// empty region case
	regions[49].EndKey = []byte("")
	for _, meta := range regions {
		leader := rand.Intn(4) % 3
		regionInfo := core.NewRegionInfo(
			meta,
			meta.Peers[leader],
			core.SetApproximateKeys(1),
			core.SetApproximateSize(1),
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

	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	c.Assert(err, IsNil)

	scheduleAndApplyOperator(tc, hb, 100)
	for i := 1; i <= 5; i++ {
		leaderCount := tc.Regions.GetStoreLeaderCount(uint64(i))
		c.Check(leaderCount, LessEqual, 12)
		regionCount := tc.Regions.GetStoreRegionCount(uint64(i))
		c.Check(regionCount, LessEqual, 32)
	}
}

func (s *testScatterRangeSuite) TestBalanceLeaderLimit(c *C) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
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

	// test not allow schedule leader
	tc.SetLeaderScheduleLimit(0)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	c.Assert(err, IsNil)

	scheduleAndApplyOperator(tc, hb, 100)
	maxLeaderCount := 0
	minLeaderCount := 99
	for i := 1; i <= 5; i++ {
		leaderCount := tc.Regions.GetStoreLeaderCount(uint64(i))
		if leaderCount < minLeaderCount {
			minLeaderCount = leaderCount
		}
		if leaderCount > maxLeaderCount {
			maxLeaderCount = leaderCount
		}
		regionCount := tc.Regions.GetStoreRegionCount(uint64(i))
		c.Check(regionCount, LessEqual, 32)
	}
	c.Check(maxLeaderCount-minLeaderCount, Greater, 10)
}

func (s *testScatterRangeSuite) TestConcurrencyUpdateConfig(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
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

func (s *testScatterRangeSuite) TestBalanceWhenRegionNotHeartbeat(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
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
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_09", "t"}))
	c.Assert(err, IsNil)

	scheduleAndApplyOperator(tc, hb, 100)
}

// scheduleAndApplyOperator will try to schedule for `count` times and apply the operator if the operator is created.
func scheduleAndApplyOperator(tc *mockcluster.Cluster, hb schedule.Scheduler, count int) {
	limit := 0
	for {
		if limit > count {
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
