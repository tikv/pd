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

package schedulers

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/plan"
)

type balanceSchedulerPlanAnalyzeTestSuite struct {
	suite.Suite

	stores  []*core.StoreInfo
	regions []*core.RegionInfo
	check   func(map[uint64]plan.Status, map[uint64]*plan.Status) bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestBalanceSchedulerPlanAnalyzerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceSchedulerPlanAnalyzeTestSuite))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.check = func(output map[uint64]plan.Status, expects map[uint64]*plan.Status) bool {
		for id, status := range expects {
			outputStatus, ok := output[id]
			if !ok {
				return false
			}
			if outputStatus != *status {
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
	plans := make([]plan.Plan, 0)
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 2, target: suite.stores[0], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 2, target: suite.stores[1], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 2, target: suite.stores[2], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 2, target: suite.stores[3], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 2, target: suite.stores[4], status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 2, target: suite.stores[0], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 2, target: suite.stores[1], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 2, target: suite.stores[2], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 2, target: suite.stores[3], status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 2, target: suite.stores[4], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 2, target: suite.stores[0], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 2, target: suite.stores[1], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 2, target: suite.stores[2], status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 2, target: suite.stores[3], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 2, target: suite.stores[4], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 2, target: suite.stores[0], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 2, target: suite.stores[1], status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 2, target: suite.stores[2], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 2, target: suite.stores[3], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 2, target: suite.stores[4], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 2, target: suite.stores[0], status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 2, target: suite.stores[1], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 2, target: suite.stores[2], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 2, target: suite.stores[3], status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 2, target: suite.stores[4], status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	suite.NoError(err)
	suite.True(isNormal)
	suite.True(suite.check(statuses,
		map[uint64]*plan.Status{
			1: plan.NewStatus(plan.StatusStoreNotMatchRule),
			2: plan.NewStatus(plan.StatusStoreNotMatchRule),
			3: plan.NewStatus(plan.StatusStoreNotMatchRule),
			4: plan.NewStatus(plan.StatusStoreNotMatchRule),
			5: plan.NewStatus(plan.StatusStoreNotMatchRule),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult2() {
	plans := make([]plan.Plan, 0)
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	suite.NoError(err)
	suite.False(isNormal)
	suite.True(suite.check(statuses,
		map[uint64]*plan.Status{
			1: plan.NewStatus(plan.StatusStoreDown),
			2: plan.NewStatus(plan.StatusStoreDown),
			3: plan.NewStatus(plan.StatusStoreDown),
			4: plan.NewStatus(plan.StatusStoreDown),
			5: plan.NewStatus(plan.StatusStoreDown),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult3() {
	plans := make([]plan.Plan, 0)
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], region: suite.regions[1], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], region: suite.regions[1], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	suite.NoError(err)
	suite.False(isNormal)
	suite.True(suite.check(statuses,
		map[uint64]*plan.Status{
			1: plan.NewStatus(plan.StatusRegionNotMatchRule),
			2: plan.NewStatus(plan.StatusRegionNotMatchRule),
			3: plan.NewStatus(plan.StatusRegionNotMatchRule),
			4: plan.NewStatus(plan.StatusRegionNotMatchRule),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult4() {
	plans := make([]plan.Plan, 0)
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[0], step: 2, status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[1], step: 2, status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[2], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[3], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[4], step: 2, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[0], step: 2, status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[1], step: 2, status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[2], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[3], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[4], step: 2, status: plan.NewStatus(plan.StatusStoreDown)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	suite.NoError(err)
	suite.False(isNormal)
	suite.True(suite.check(statuses,
		map[uint64]*plan.Status{
			1: plan.NewStatus(plan.StatusStoreAlreadyHasPeer),
			2: plan.NewStatus(plan.StatusStoreAlreadyHasPeer),
			3: plan.NewStatus(plan.StatusStoreNotMatchRule),
			4: plan.NewStatus(plan.StatusStoreNotMatchRule),
			5: plan.NewStatus(plan.StatusStoreDown),
		}))
}

func (suite *balanceSchedulerPlanAnalyzeTestSuite) TestAnalyzerResult5() {
	plans := make([]plan.Plan, 0)
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[4], step: 0, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[3], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[2], region: suite.regions[0], step: 1, status: plan.NewStatus(plan.StatusRegionNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[0], step: 2, status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[1], step: 2, status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[2], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[3], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[1], target: suite.stores[4], step: 2, status: plan.NewStatus(plan.StatusStoreDown)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[0], step: 2, status: plan.NewStatus(plan.StatusStoreAlreadyHasPeer)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[1], step: 3, status: plan.NewStatus(plan.StatusStoreScoreDisallowed)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[2], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[3], step: 2, status: plan.NewStatus(plan.StatusStoreNotMatchRule)})
	plans = append(plans, &balanceSchedulerPlan{source: suite.stores[0], target: suite.stores[4], step: 2, status: plan.NewStatus(plan.StatusStoreDown)})
	statuses, isNormal, err := BalancePlanSummary(plans)
	suite.NoError(err)
	suite.False(isNormal)
	suite.True(suite.check(statuses,
		map[uint64]*plan.Status{
			1: plan.NewStatus(plan.StatusStoreAlreadyHasPeer),
			2: plan.NewStatus(plan.StatusStoreAlreadyHasPeer),
			3: plan.NewStatus(plan.StatusStoreNotMatchRule),
			4: plan.NewStatus(plan.StatusStoreNotMatchRule),
			5: plan.NewStatus(plan.StatusStoreDown),
		}))
}
