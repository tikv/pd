// Copyright 2022 TiKV Project Authors.
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

package plan

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
)

type balanceSchedulerPlanAnalyzeTestSuite struct {
	suite.Suite

	stores  []*core.StoreInfo
	regions []*core.RegionInfo
	check   func(map[uint64]Status, map[uint64]*Status) bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestBalanceSchedulerPlanAnalyzerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceSchedulerPlanAnalyzeTestSuite))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.check = func(output map[uint64]Status, expects map[uint64]*Status) bool {
		for id, Status := range expects {
			outputStatus, ok := output[id]
			if !ok {
				return false
			}
			if outputStatus != *Status {
				return false
			}
		}
		return true
	}
	suite.stores = []*core.StoreInfo{
		core.NewStoreInfo(
			&metapb.Store{
				Id: 1,
			},
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id: 2,
			},
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id: 3,
			},
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id: 4,
			},
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id: 5,
			},
		),
	}
	suite.regions = []*core.RegionInfo{
		core.NewRegionInfo(
			&metapb.Region{
				Id: 1,
			},
			&metapb.Peer{
				Id:      1,
				StoreId: 1,
			},
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id: 2,
			},
			&metapb.Peer{
				Id:      2,
				StoreId: 2,
			},
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id: 3,
			},
			&metapb.Peer{
				Id:      3,
				StoreId: 3,
			},
		),
	}
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TearDownSuite() {
	suite.cancel()
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult1() {
	re := suite.Require()
	plans := make([]Plan, 0)
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 2, Target: suite.stores[0], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 2, Target: suite.stores[1], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 2, Target: suite.stores[2], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 2, Target: suite.stores[3], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 2, Target: suite.stores[4], Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 2, Target: suite.stores[0], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 2, Target: suite.stores[1], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 2, Target: suite.stores[2], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 2, Target: suite.stores[3], Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 2, Target: suite.stores[4], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 2, Target: suite.stores[0], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 2, Target: suite.stores[1], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 2, Target: suite.stores[2], Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 2, Target: suite.stores[3], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 2, Target: suite.stores[4], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 2, Target: suite.stores[0], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 2, Target: suite.stores[1], Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 2, Target: suite.stores[2], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 2, Target: suite.stores[3], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 2, Target: suite.stores[4], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 2, Target: suite.stores[0], Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 2, Target: suite.stores[1], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 2, Target: suite.stores[2], Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 2, Target: suite.stores[3], Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 2, Target: suite.stores[4], Status: NewStatus(StatusStoreScoreDisallowed)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	re.NoError(err)
	re.True(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusStoreNotMatchRule),
			2: NewStatus(StatusStoreNotMatchRule),
			3: NewStatus(StatusStoreNotMatchRule),
			4: NewStatus(StatusStoreNotMatchRule),
			5: NewStatus(StatusStoreNotMatchRule),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult2() {
	re := suite.Require()
	plans := make([]Plan, 0)
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Step: 0, Status: NewStatus(StatusStoreDown)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	re.NoError(err)
	re.False(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusStoreDown),
			2: NewStatus(StatusStoreDown),
			3: NewStatus(StatusStoreDown),
			4: NewStatus(StatusStoreDown),
			5: NewStatus(StatusStoreDown),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult3() {
	re := suite.Require()
	plans := make([]Plan, 0)
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Region: suite.regions[1], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Region: suite.regions[1], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	re.NoError(err)
	re.False(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusRegionNotMatchRule),
			2: NewStatus(StatusRegionNotMatchRule),
			3: NewStatus(StatusRegionNotMatchRule),
			4: NewStatus(StatusRegionNotMatchRule),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult4() {
	re := suite.Require()
	plans := make([]Plan, 0)
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 0, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[0], Step: 2, Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[1], Step: 2, Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[2], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[3], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[4], Step: 2, Status: NewStatus(StatusStoreDown)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[0], Step: 2, Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[1], Step: 2, Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[2], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[3], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[4], Step: 2, Status: NewStatus(StatusStoreDown)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	re.NoError(err)
	re.False(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusStoreAlreadyHasPeer),
			2: NewStatus(StatusStoreAlreadyHasPeer),
			3: NewStatus(StatusStoreNotMatchRule),
			4: NewStatus(StatusStoreNotMatchRule),
			5: NewStatus(StatusStoreDown),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult5() {
	re := suite.Require()
	plans := make([]Plan, 0)
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[4], Step: 0, Status: NewStatus(StatusStoreRemoveLimitThrottled)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[3], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[2], Region: suite.regions[0], Step: 1, Status: NewStatus(StatusRegionNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[0], Step: 2, Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[1], Step: 2, Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[2], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[1], Target: suite.stores[3], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[0], Step: 2, Status: NewStatus(StatusStoreAlreadyHasPeer)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[1], Step: 3, Status: NewStatus(StatusStoreScoreDisallowed)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[2], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	plans = append(plans, &BalanceSchedulerPlan{Source: suite.stores[0], Target: suite.stores[3], Step: 2, Status: NewStatus(StatusStoreNotMatchRule)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	re.NoError(err)
	re.False(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusStoreAlreadyHasPeer),
			2: NewStatus(StatusStoreAlreadyHasPeer),
			3: NewStatus(StatusStoreNotMatchRule),
			4: NewStatus(StatusStoreNotMatchRule),
			5: NewStatus(StatusStoreRemoveLimitThrottled),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult6() {
	re := suite.Require()
	basePlan := NewBalanceSchedulerPlan()
	collector := NewCollector(basePlan)
	collector.Collect(SetResourceWithStep(suite.stores[0], 2), SetStatus(NewStatus(StatusStoreDown)))
	collector.Collect(SetResourceWithStep(suite.stores[1], 2), SetStatus(NewStatus(StatusStoreDown)))
	collector.Collect(SetResourceWithStep(suite.stores[2], 2), SetStatus(NewStatus(StatusStoreDown)))
	collector.Collect(SetResourceWithStep(suite.stores[3], 2), SetStatus(NewStatus(StatusStoreDown)))
	collector.Collect(SetResourceWithStep(suite.stores[4], 2), SetStatus(NewStatus(StatusStoreDown)))
	basePlan.Source = suite.stores[0]
	basePlan.Step++
	collector.Collect(SetResource(suite.regions[0]), SetStatus(NewStatus(StatusRegionNoLeader)))
	statuses, isNormal, err := BalancePlanSummary(collector.GetPlans())
	re.NoError(err)
	re.False(isNormal)
	re.True(suite.check(statuses,
		map[uint64]*Status{
			1: NewStatus(StatusStoreDown),
			2: NewStatus(StatusStoreDown),
			3: NewStatus(StatusStoreDown),
			4: NewStatus(StatusStoreDown),
			5: NewStatus(StatusStoreDown),
		}))
}
