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
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	regionSize     int64
	expectedResult bool
	kind           constant.SchedulePolicy
}

func TestInfluenceAmp(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re := require.New(t)

	R := int64(96)
	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)

	influence := oc.GetOpInfluence(tc.GetBasicCluster())
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
	basePlan := plan.NewBalanceSchedulerPlan()
	solver := newSolver(basePlan, kind, tc, influence)
	solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
	re.True(solver.shouldBalance(""))

	// It will not schedule if the diff region count is greater than the sum
	// of TolerantSizeRatio and influenceAmp*2.
	tc.AddRegionStore(1, int(100+influenceAmp+2))
	solver.Source = tc.GetStore(1)
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
	re.False(solver.shouldBalance(""))
	re.Less(solver.sourceScore-solver.targetScore, float64(1))
}

func TestShouldBalance(t *testing.T) {
	// store size = 100GiB
	// region size = 96MiB
	re := require.New(t)

	const R = 96
	testCases := []testBalanceSpeedCase{
		// target size is zero
		{2, 0, R / 10, true, constant.BySize},
		{2, 0, R, false, constant.BySize},
		// all in high space stage
		{10, 5, R / 10, true, constant.BySize},
		{10, 5, 2 * R, false, constant.BySize},
		{10, 10, R / 10, false, constant.BySize},
		{10, 10, 2 * R, false, constant.BySize},
		// all in transition stage
		{700, 680, R / 10, true, constant.BySize},
		{700, 680, 5 * R, false, constant.BySize},
		{700, 700, R / 10, false, constant.BySize},
		// all in low space stage
		{900, 890, R / 10, true, constant.BySize},
		{900, 890, 5 * R, false, constant.BySize},
		{900, 900, R / 10, false, constant.BySize},
		// one in high space stage, other in transition stage
		{650, 550, R, true, constant.BySize},
		{650, 500, 50 * R, false, constant.BySize},
		// one in transition space stage, other in low space stage
		{800, 700, R, true, constant.BySize},
		{800, 700, 50 * R, false, constant.BySize},

		// default leader tolerant ratio is 5, when schedule by count
		// target size is zero
		{2, 0, R / 10, false, constant.ByCount},
		{2, 0, R, false, constant.ByCount},
		// all in high space stage
		{10, 5, R / 10, true, constant.ByCount},
		{10, 5, 2 * R, true, constant.ByCount},
		{10, 6, 2 * R, false, constant.ByCount},
		{10, 10, R / 10, false, constant.ByCount},
		{10, 10, 2 * R, false, constant.ByCount},
		// all in transition stage
		{70, 50, R / 10, true, constant.ByCount},
		{70, 50, 5 * R, true, constant.ByCount},
		{70, 70, R / 10, false, constant.ByCount},
		// all in low space stage
		{90, 80, R / 10, true, constant.ByCount},
		{90, 80, 5 * R, true, constant.ByCount},
		{90, 90, R / 10, false, constant.ByCount},
		// one in high space stage, other in transition stage
		{65, 55, R / 2, true, constant.ByCount},
		{65, 50, 5 * R, true, constant.ByCount},
		// one in transition space stage, other in low space stage
		{80, 70, R / 2, true, constant.ByCount},
		{80, 70, 5 * R, true, constant.ByCount},
	}

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetTolerantSizeRatio(2.5)
	tc.SetRegionScoreFormulaVersion("v1")
	// create a region to control average region size.
	tc.AddLeaderRegion(1, 1, 2)

	for _, testCase := range testCases {
		tc.AddLeaderStore(1, int(testCase.sourceCount))
		tc.AddLeaderStore(2, int(testCase.targetCount))
		region := tc.GetRegion(1).Clone(core.SetApproximateSize(testCase.regionSize))
		tc.PutRegion(region)
		tc.SetLeaderSchedulePolicy(testCase.kind.String())
		kind := constant.NewScheduleKind(constant.LeaderKind, testCase.kind)
		basePlan := plan.NewBalanceSchedulerPlan()
		solver := newSolver(basePlan, kind, tc, oc.GetOpInfluence(tc.GetBasicCluster()))
		solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
		solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
		re.Equal(testCase.expectedResult, solver.shouldBalance(""))
	}

	for _, testCase := range testCases {
		if testCase.kind.String() == constant.BySize.String() {
			tc.AddRegionStore(1, int(testCase.sourceCount))
			tc.AddRegionStore(2, int(testCase.targetCount))
			region := tc.GetRegion(1).Clone(core.SetApproximateSize(testCase.regionSize))
			tc.PutRegion(region)
			kind := constant.NewScheduleKind(constant.RegionKind, testCase.kind)
			basePlan := plan.NewBalanceSchedulerPlan()
			solver := newSolver(basePlan, kind, tc, oc.GetOpInfluence(tc.GetBasicCluster()))
			solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
			solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
			re.Equal(testCase.expectedResult, solver.shouldBalance(""))
		}
	}
}

func TestTolerantRatio(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	// create a region to control average region size.
	re.NotNil(tc.AddLeaderRegion(1, 1, 2))
	regionSize := int64(96)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(regionSize))

	tbl := []struct {
		ratio                  float64
		kind                   constant.ScheduleKind
		expectTolerantResource func(constant.ScheduleKind) int64
	}{
		{0, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.ByCount}, func(k constant.ScheduleKind) int64 {
			return int64(leaderTolerantSizeRatio)
		}},
		{0, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.BySize}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{0, constant.ScheduleKind{Resource: constant.RegionKind, Policy: constant.ByCount}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{0, constant.ScheduleKind{Resource: constant.RegionKind, Policy: constant.BySize}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.ByCount}, func(k constant.ScheduleKind) int64 {
			return int64(tc.GetScheduleConfig().TolerantSizeRatio)
		}},
		{10, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.BySize}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, constant.ScheduleKind{Resource: constant.RegionKind, Policy: constant.ByCount}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
		{10, constant.ScheduleKind{Resource: constant.RegionKind, Policy: constant.BySize}, func(k constant.ScheduleKind) int64 {
			return int64(adjustTolerantRatio(tc, k) * float64(regionSize))
		}},
	}
	for _, t := range tbl {
		tc.SetTolerantSizeRatio(t.ratio)
		basePlan := plan.NewBalanceSchedulerPlan()
		solver := newSolver(basePlan, t.kind, tc, operator.OpInfluence{})
		solver.Region = region

		sourceScore := t.expectTolerantResource(t.kind)
		targetScore := solver.getTolerantResource()
		re.Equal(sourceScore, targetScore)
	}
}

type balanceLeaderSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     Scheduler
	oc     *operator.Controller
	conf   config.SchedulerConfigProvider
}

func TestBalanceLeaderSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceLeaderSchedulerTestSuite))
}

func (suite *balanceLeaderSchedulerTestSuite) SetupTest() {
	re := suite.Require()
	suite.cancel, suite.conf, suite.tc, suite.oc = prepareSchedulersTest()
	lb, err := CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	suite.lb = lb
}

func (suite *balanceLeaderSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceLeaderSchedulerTestSuite) schedule() []*operator.Operator {
	ops, _ := suite.lb.Schedule(suite.tc, false)
	return ops
}

func (suite *balanceLeaderSchedulerTestSuite) dryRun() []plan.Plan {
	_, plans := suite.lb.Schedule(suite.tc, true)
	return plans
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLimit() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 0)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Empty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	suite.tc.UpdateLeaderCount(1, 16)
	re.NotEmpty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(1, 7)
	suite.tc.UpdateLeaderCount(2, 8)
	suite.tc.UpdateLeaderCount(3, 9)
	suite.tc.UpdateLeaderCount(4, 10)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	re.Empty(suite.schedule())
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(3, plans[0].GetStep())
	re.Equal(plan.StatusStoreScoreDisallowed, int(plans[0].GetStatus().StatusCode))

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(4, 16)
	re.NotEmpty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLeaderSchedulePolicy() {
	re := suite.Require()
	// Stores:          1       2       3       4
	// Leader Count:    10      10      10      10
	// Leader Size :    10000   100    	100    	100
	// Region1:         L       F       F       F
	suite.tc.AddLeaderStore(1, 10, 10000*units.MiB)
	suite.tc.AddLeaderStore(2, 10, 100*units.MiB)
	suite.tc.AddLeaderStore(3, 10, 100*units.MiB)
	suite.tc.AddLeaderStore(4, 10, 100*units.MiB)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.Empty(suite.schedule())
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(3, plans[0].GetStep())
	re.Equal(plan.StatusStoreScoreDisallowed, int(plans[0].GetStatus().StatusCode))

	suite.tc.SetLeaderSchedulePolicy(constant.BySize.String())
	re.NotEmpty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLeaderTolerantRatio() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// test schedule leader by count, with tolerantSizeRatio=2.5
	// Stores:          1       2       3       4
	// Leader Count:    14->15  10      10      10
	// Leader Size :    100     100     100     100
	// Region1:         L       F       F       F
	suite.tc.AddLeaderStore(1, 14, 100)
	suite.tc.AddLeaderStore(2, 10, 100)
	suite.tc.AddLeaderStore(3, 10, 100)
	suite.tc.AddLeaderStore(4, 10, 100)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.Empty(suite.schedule())
	re.Equal(14, suite.tc.GetStore(1).GetLeaderCount())
	suite.tc.AddLeaderStore(1, 15, 100)
	re.Equal(15, suite.tc.GetStore(1).GetLeaderCount())
	re.NotEmpty(suite.schedule())
	suite.tc.SetTolerantSizeRatio(6) // (15-10)<6
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestScheduleWithOpInfluence() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   14
	// Region1:    F    F    F    L
	suite.tc.AddLeaderStore(1, 7)
	suite.tc.AddLeaderStore(2, 8)
	suite.tc.AddLeaderStore(3, 9)
	suite.tc.AddLeaderStore(4, 14)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	op := suite.schedule()[0]
	re.NotNil(op)
	suite.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader difference is not greater than 5.
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.NotEmpty(suite.schedule())
	suite.tc.SetLeaderSchedulePolicy(constant.BySize.String())
	re.Empty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(1, 8)
	suite.tc.UpdateLeaderCount(2, 8)
	suite.tc.UpdateLeaderCount(3, 9)
	suite.tc.UpdateLeaderCount(4, 13)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestTransferLeaderOut() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   12
	suite.tc.AddLeaderStore(1, 7)
	suite.tc.AddLeaderStore(2, 8)
	suite.tc.AddLeaderStore(3, 9)
	suite.tc.AddLeaderStore(4, 12)
	suite.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		suite.tc.AddLeaderRegion(i, 4, 1, 2, 3)
	}

	// balance leader: 4->1, 4->1, 4->2
	regions := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for i := 0; i < 20; i++ {
		if len(suite.schedule()) == 0 {
			continue
		}
		if op := suite.schedule()[0]; op != nil {
			if _, ok := regions[op.RegionID()]; !ok {
				suite.oc.SetOperator(op)
				regions[op.RegionID()] = struct{}{}
				tr := op.Step(0).(operator.TransferLeader)
				re.Equal(uint64(4), tr.FromStore)
				targets[tr.ToStore]--
			}
		}
	}
	re.Len(regions, 3)
	for _, count := range targets {
		re.Zero(count)
	}
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceFilter() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderStore(3, 3)
	suite.tc.AddLeaderStore(4, 16)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)

	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we should consider it
	// because it still provides services
	suite.tc.SetStoreOffline(4)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	suite.tc.SetStoreDown(1)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 2)
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(0, plans[0].GetStep())
	re.Equal(plan.StatusStoreDown, int(plans[0].GetStatus().StatusCode))
	re.Equal(uint64(1), plans[0].GetResource(0))

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	suite.tc.SetStoreBusy(2, true)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 3)

	// Test disconnectFilter.
	// If store 3 is disconnected, no operator can be created.
	suite.tc.SetStoreDisconnect(3)
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestLeaderWeight() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	suite.tc.SetTolerantSizeRatio(2.5)
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 4)
	suite.tc.UpdateLeaderCount(4, 30)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 3)
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalancePolicy() {
	re := suite.Require()
	// Stores:       1    2     3    4
	// LeaderCount: 20   66     6   20
	// LeaderSize:  66   20    20    6
	suite.tc.AddLeaderStore(1, 20, 600*units.MiB)
	suite.tc.AddLeaderStore(2, 66, 200*units.MiB)
	suite.tc.AddLeaderStore(3, 6, 20*units.MiB)
	suite.tc.AddLeaderStore(4, 20, 1*units.MiB)
	suite.tc.AddLeaderRegion(1, 2, 1, 3, 4)
	suite.tc.AddLeaderRegion(2, 1, 2, 3, 4)
	suite.tc.SetLeaderSchedulePolicy("count")
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 2, 3)
	suite.tc.SetLeaderSchedulePolicy("size")
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 4)
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceSelector() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderStore(3, 3)
	suite.tc.AddLeaderStore(4, 16)
	suite.tc.AddLeaderRegion(1, 4, 2, 3)
	suite.tc.AddLeaderRegion(2, 3, 1, 2)
	// store4 has max leader score, store1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	suite.tc.UpdateLeaderCount(2, 14)
	suite.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderRegion(1, 3, 2, 4)
	suite.tc.AddLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. Now source and target are store3 and store2.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 3, 2)

	// Stores:     1    2    3    4
	// Leaders:    9    10   10   11
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.AddLeaderRegion(1, 4, 2, 3)
	suite.tc.AddLeaderRegion(2, 1, 2, 3)
	// The cluster is balanced.
	re.Empty(suite.schedule())

	// store3's leader drops:
	// Stores:     1    2    3    4
	// Leaders:    11   13   0    16
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	suite.tc.AddLeaderStore(1, 11)
	suite.tc.AddLeaderStore(2, 13)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 16)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 3)
}

type balanceLeaderRangeSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *operator.Controller
}

func TestBalanceLeaderRangeSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceLeaderRangeSchedulerTestSuite))
}

func (suite *balanceLeaderRangeSchedulerTestSuite) SetupTest() {
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestSingleRangeBalance() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.NotEmpty(ops)
	re.Len(ops, 1)
	re.Len(ops[0].Counters, 1)
	re.Len(ops[0].FinishedCounters, 1)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"h", "n"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"b", "f"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", "a"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"g", ""}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", "f"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"b", ""}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestMultiRangeBalance() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", "g", "o", "t"}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.Equal(uint64(1), ops[0].RegionID())
	r := suite.tc.GetRegion(1)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)
	suite.tc.AddLeaderRegionWithRange(2, "p", "r", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Equal(uint64(2), ops[0].RegionID())
	r = suite.tc.GetRegion(2)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)

	suite.tc.AddLeaderRegionWithRange(3, "u", "w", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	r = suite.tc.GetRegion(3)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)
	suite.tc.AddLeaderRegionWithRange(4, "", "", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestBatchBalance() {
	re := suite.Require()
	suite.tc.AddLeaderStore(1, 100)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 100)
	suite.tc.AddLeaderStore(5, 100)
	suite.tc.AddLeaderStore(6, 0)

	suite.tc.AddLeaderRegionWithRange(uint64(102), "102a", "102z", 1, 2, 3)
	suite.tc.AddLeaderRegionWithRange(uint64(103), "103a", "103z", 4, 5, 6)
	lb, err := CreateScheduler(BalanceLeaderType, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.Len(ops, 2)
	for i := 1; i <= 50; i++ {
		suite.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 1, 2, 3)
	}
	for i := 51; i <= 100; i++ {
		suite.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 4, 5, 6)
	}
	suite.tc.AddLeaderRegionWithRange(uint64(101), "101a", "101z", 5, 4, 3)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Len(ops, 4)
	regions := make(map[uint64]struct{})
	for _, op := range ops {
		regions[op.RegionID()] = struct{}{}
	}
	re.Len(regions, 4)
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestReSortStores() {
	re := suite.Require()
	suite.tc.AddLeaderStore(1, 104)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 100)
	suite.tc.AddLeaderStore(5, 100)
	suite.tc.AddLeaderStore(6, 0)
	stores := suite.tc.Stores.GetStores()
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetID() < stores[j].GetID()
	})

	deltaMap := make(map[uint64]int64)
	getScore := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(0, deltaMap[store.GetID()])
	}
	candidateStores := make([]*core.StoreInfo, 0)
	// order by score desc.
	cs := newCandidateStores(append(candidateStores, stores...), false, getScore)
	// in candidate,the order stores:1(104),5(100),4(100),6,3,2
	// store 4 should in pos 2
	re.Equal(2, cs.binarySearch(stores[3]))

	// store 1 should in pos 0
	store1 := stores[0]
	re.Zero(cs.binarySearch(store1))
	deltaMap[store1.GetID()] = -1 // store 1
	cs.resortStoreWithPos(0)
	// store 1 should still in pos 0.
	re.Equal(uint64(1), cs.stores[0].GetID())
	curIndex := cs.binarySearch(store1)
	re.Zero(curIndex)
	deltaMap[1] = -4
	// store 1 update the scores to 104-4=100
	// the order stores should be:5(100),4(100),1(100),6,3,2
	cs.resortStoreWithPos(curIndex)
	re.Equal(uint64(1), cs.stores[2].GetID())
	re.Equal(2, cs.binarySearch(store1))
	// the top store is : 5(100)
	topStore := cs.stores[0]
	topStorePos := cs.binarySearch(topStore)
	deltaMap[topStore.GetID()] = -1
	cs.resortStoreWithPos(topStorePos)

	// after recorder, the order stores should be: 4(100),1(100),5(99),6,3,2
	re.Equal(uint64(1), cs.stores[1].GetID())
	re.Equal(1, cs.binarySearch(store1))
	re.Equal(topStore.GetID(), cs.stores[2].GetID())
	re.Equal(2, cs.binarySearch(topStore))

	bottomStore := cs.stores[5]
	deltaMap[bottomStore.GetID()] = 4
	cs.resortStoreWithPos(5)

	// the order stores should be: 4(100),1(100),5(99),2(5),6,3
	re.Equal(bottomStore.GetID(), cs.stores[3].GetID())
	re.Equal(3, cs.binarySearch(bottomStore))
}

func TestBalanceRegionSchedule1(t *testing.T) {
	re := require.New(t)
	checkBalanceRegionSchedule1(re, false /* disable placement rules */)
	checkBalanceRegionSchedule1(re, true /* enable placement rules */)
}

func checkBalanceRegionSchedule1(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	// Add stores 1,2,3,4.
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 8)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 16)
	// Add region 1 with leader in store 4.
	tc.AddLeaderRegion(1, 4)
	ops, _ := sb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpKind(0), 4, 1)

	// Test stateFilter.
	tc.SetStoreOffline(1)
	tc.UpdateRegionCount(2, 6)

	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpKind(0), 4, 2)
	tc.SetStoreUp(1)
	// test region replicate not match
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	ops, plans := sb.Schedule(tc, true)
	re.Len(plans, 101)
	re.Empty(ops)
	if enablePlacementRules {
		re.Equal(plan.StatusRegionNotMatchRule, int(plans[1].GetStatus().StatusCode))
	} else {
		re.Equal(plan.StatusRegionNotReplicated, int(plans[1].GetStatus().StatusCode))
	}

	tc.SetStoreOffline(1)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	ops, plans = sb.Schedule(tc, true)
	re.NotEmpty(ops)
	re.Len(plans, 4)
	re.True(plans[0].GetStatus().IsOK())
}

func TestBalanceRegionReplicas3(t *testing.T) {
	re := require.New(t)
	checkReplica3(re, false /* disable placement rules */)
	checkReplica3(re, true /* enable placement rules */)
}

func checkReplica3(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)

	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	// Store 1 has the largest region score, so the balance scheduler tries to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores.
	ops, _ := sb.Schedule(tc, false)
	re.Empty(ops)

	// Store 4 has smaller region score than store 2.
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	ops, _ = sb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.AddLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 6)

	// Store 7 has smaller region score with store 6.
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 7)

	// If store 7 is not available, will choose store 6.
	tc.SetStoreDown(7)
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 6)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 6)

	// Take down 4,5,6,7
	tc.SetStoreDown(4)
	tc.SetStoreDown(5)
	tc.SetStoreDown(6)
	tc.SetStoreDown(7)
	tc.SetStoreDown(8)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.AddLabelsStore(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	ops, _ = sb.Schedule(tc, false)
	re.Empty(ops)
}

func TestBalanceRegionReplicas5(t *testing.T) {
	re := require.New(t)
	checkReplica5(re, false /* disable placement rules */)
	checkReplica5(re, true /* enable placement rules */)
}

func checkReplica5(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 5)

	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	tc.AddLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	ops, _ := sb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 5, 6)

	// Store 7 has larger region score and same distinct score with store 6.
	tc.AddLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 5, 6)

	// Store 1 has smaller region score and higher distinct score.
	tc.AddLeaderRegion(1, 2, 3, 4, 5, 6)
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.AddLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.AddLeaderRegion(1, 2, 3, 11, 12, 13)
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 11, 6)
}

// TestBalanceRegionSchedule2 for corner case 1:
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
func TestBalanceRegionSchedule2(t *testing.T) {
	re := require.New(t)
	checkBalanceRegionSchedule2(re, false /* disable placement rules */)
	checkBalanceRegionSchedule2(re, true /* enable placement rules */)
}

func checkBalanceRegionSchedule2(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	tc.SetTolerantSizeRatio(1)
	tc.SetRegionScheduleLimit(1)
	tc.SetRegionScoreFormulaVersion("v1")

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

	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)

	tc.AddRegionStore(1, 11)
	tc.AddRegionStore(2, 9)
	tc.AddRegionStore(3, 6)
	tc.AddRegionStore(4, 5)
	tc.AddRegionStore(5, 2)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)

	// add two merge operator to let the count of opRegion to 2.
	ops, err := operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	re.NoError(err)
	oc.SetOperator(ops[0])
	oc.SetOperator(ops[1])
	re.True(sb.IsScheduleAllowed(tc))
	ops1, _ := sb.Schedule(tc, false)
	op := ops1[0]
	re.NotNil(op)
	// if the space of store 5 is normal, we can balance region to store 5
	ops1, _ = sb.Schedule(tc, false)
	op = ops1[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 5)

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
	ops1, _ = sb.Schedule(tc, false)
	op = ops1[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 4)
}

func TestBalanceRegionStoreWeight(t *testing.T) {
	re := require.New(t)
	checkBalanceRegionStoreWeight(re, false /* disable placement rules */)
	checkBalanceRegionStoreWeight(re, true /* enable placement rules */)
}

func checkBalanceRegionStoreWeight(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.UpdateStoreRegionWeight(1, 0.5)
	tc.UpdateStoreRegionWeight(2, 0.9)
	tc.UpdateStoreRegionWeight(3, 1.0)
	tc.UpdateStoreRegionWeight(4, 2.0)

	tc.AddLeaderRegion(1, 1)
	ops, _ := sb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 4)

	tc.UpdateRegionCount(4, 30)
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 3)
}

func TestBalanceRegionOpInfluence(t *testing.T) {
	re := require.New(t)
	checkBalanceRegionOpInfluence(re, false /* disable placement rules */)
	checkBalanceRegionOpInfluence(re, true /* enable placement rules */)
}

func checkBalanceRegionOpInfluence(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest(false /* no need to run stream*/)
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
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
		re.NotNil(op)
		oc.AddOperator(op)
	}
	ops, _ := sb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpKind(0), 2, 1)
}

func TestBalanceRegionReplacePendingRegion(t *testing.T) {
	re := require.New(t)
	checkReplacePendingRegion(re, false /* disable placement rules */)
	checkReplacePendingRegion(re, true /* enable placement rules */)
}

func checkReplacePendingRegion(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
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

	ops, _ := sb.Schedule(tc, false)
	op := ops[0]
	re.Equal(uint64(3), op.RegionID())
	ops, _ = sb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 1, 4)
}

func TestBalanceRegionShouldNotBalance(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	region := tc.MockRegionInfo(1, 0, []uint64{2, 3, 4}, nil, nil)
	tc.PutRegion(region)
	operators, _ := sb.Schedule(tc, false)
	re.Empty(operators)
}

func TestBalanceRegionEmptyRegion(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	sb, err := CreateScheduler(BalanceRegionType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(BalanceRegionType, []string{"", ""}))
	re.NoError(err)
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
	operators, _ := sb.Schedule(tc, false)
	re.NotEmpty(operators)

	for i := uint64(10); i < 111; i++ {
		tc.PutRegionStores(i, 1, 3, 4)
	}
	operators, _ = sb.Schedule(tc, false)
	re.Empty(operators)
}

func TestRandomMergeSchedule(t *testing.T) {
	re := require.New(t)
	checkRandomMergeSchedule(re, false /* disable placement rules */)
	checkRandomMergeSchedule(re, true /* enable placement rules */)
}

func checkRandomMergeSchedule(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest(true /* need to run stream*/)
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	tc.SetMergeScheduleLimit(1)

	mb, err := CreateScheduler(RandomMergeType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(RandomMergeType, []string{"", ""}))
	re.NoError(err)

	tc.AddRegionStore(1, 4)
	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 1)
	tc.AddLeaderRegion(3, 1)
	tc.AddLeaderRegion(4, 1)

	re.True(mb.IsScheduleAllowed(tc))
	ops, _ := mb.Schedule(tc, false)
	re.Empty(ops) // regions are not fully replicated

	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	ops, _ = mb.Schedule(tc, false)
	re.Len(ops, 2)
	re.NotZero(ops[0].Kind() & operator.OpMerge)
	re.NotZero(ops[1].Kind() & operator.OpMerge)

	oc.AddWaitingOperator(ops...)
	re.False(mb.IsScheduleAllowed(tc))
}

func TestScatterRangeBalance(t *testing.T) {
	re := require.New(t)
	checkScatterRangeBalance(re, false /* disable placement rules */)
	checkScatterRangeBalance(re, true /* enable placement rules */)
}

func checkScatterRangeBalance(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
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
		origin, overlaps, rangeChanged := tc.SetRegion(regionInfo)
		tc.UpdateSubTree(regionInfo, origin, overlaps, rangeChanged)
	}
	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		re.NoError(err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	hb, err := CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	re.NoError(err)

	scheduleAndApplyOperator(tc, hb, 100)
	for i := 1; i <= 5; i++ {
		leaderCount := tc.GetStoreLeaderCount(uint64(i))
		re.LessOrEqual(leaderCount, 12)
		regionCount = tc.GetStoreRegionCount(uint64(i))
		re.LessOrEqual(regionCount, 32)
	}
}

func TestBalanceLeaderLimit(t *testing.T) {
	re := require.New(t)
	checkBalanceLeaderLimit(re, false /* disable placement rules */)
	checkBalanceLeaderLimit(re, true /* enable placement rules */)
}

func checkBalanceLeaderLimit(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
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

		origin, overlaps, rangeChanged := tc.SetRegion(regionInfo)
		tc.UpdateSubTree(regionInfo, origin, overlaps, rangeChanged)
	}

	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		re.NoError(err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	// test not allow schedule leader
	tc.SetLeaderScheduleLimit(0)
	hb, err := CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	re.NoError(err)

	scheduleAndApplyOperator(tc, hb, 100)
	maxLeaderCount := 0
	minLeaderCount := 99
	for i := 1; i <= 5; i++ {
		leaderCount := tc.GetStoreLeaderCount(uint64(i))
		if leaderCount < minLeaderCount {
			minLeaderCount = leaderCount
		}
		if leaderCount > maxLeaderCount {
			maxLeaderCount = leaderCount
		}
		regionCount = tc.GetStoreRegionCount(uint64(i))
		re.LessOrEqual(regionCount, 32)
	}
	re.Greater(maxLeaderCount-minLeaderCount, 10)
}

func TestConcurrencyUpdateConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	sche := hb.(*scatterRangeScheduler)
	re.NoError(err)
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
			re.NoError(sche.config.Persist())
		}
	}()
	for i := 0; i < 1000; i++ {
		sche.Schedule(tc, false)
	}
	ch <- struct{}{}
}

func TestBalanceWhenRegionNotHeartbeat(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
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

		origin, overlaps, rangeChanged := tc.SetRegion(regionInfo)
		tc.UpdateSubTree(regionInfo, origin, overlaps, rangeChanged)
	}

	for i := 1; i <= 3; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	hb, err := CreateScheduler(ScatterRangeType, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_09", "t"}))
	re.NoError(err)

	scheduleAndApplyOperator(tc, hb, 100)
}

// scheduleAndApplyOperator will try to schedule for `count` times and apply the operator if the operator is created.
func scheduleAndApplyOperator(tc *mockcluster.Cluster, hb Scheduler, count int) {
	limit := 0
	for {
		if limit > count {
			break
		}
		ops, _ := hb.Schedule(tc, false)
		if ops == nil {
			limit++
			continue
		}
		operator.ApplyOperator(tc, ops[0])
	}
}
