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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
)

func TestBalanceRegionRetriesAfterFinalIsolationFailure(t *testing.T) {
	for _, joint := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-joint", true: "joint"}[joint], func(t *testing.T) {
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(true)
			tc.SetEnableUseJointConsensus(joint)
			tc.SetTolerantSizeRatio(1)
			tc.SetRegionScoreFormulaVersion("v1")
			for i, host := range []string{"a", "b", "a", "a", "c"} {
				count := 1
				switch i {
				case 0:
					count = 100
				case 3:
					count = 0
				}
				tc.AddLabelsStore(uint64(i+1), count, map[string]string{"host": host})
			}
			tc.SetStoreEvictLeader(3, true)
			tc.SetStoreEvictLeader(4, true)
			rm := tc.GetRuleManager()
			leader := rm.GetRule("pd", "default").Clone()
			leader.Role, leader.Count = placement.Leader, 1
			require.NoError(t, rm.SetRule(leader))
			voters := leader.Clone()
			voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
			voters.LocationLabels, voters.IsolationLevel = []string{"host"}, "host"
			require.NoError(t, rm.SetRule(voters))
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			fit := rm.FitRegionWithoutCache(tc, region)
			require.True(t, fit.IsSatisfied())
			guard := filter.NewPlacementSafeguard("review", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(1), fit)
			for _, id := range []uint64{4, 5} {
				require.True(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(id)).IsOK())
			}
			_, err := operator.CreateMovePeerOperator("bad", tc, region, operator.OpRegion, 1, &metapb.Peer{StoreId: 4})
			require.ErrorIs(t, err, operator.ErrTargetIsolation)
			good, err := operator.CreateMovePeerOperator("good", tc, region, operator.OpRegion, 1, &metapb.Peer{StoreId: 5})
			require.NoError(t, err)
			require.NotNil(t, good)

			scheduler := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}).(*balanceRegionScheduler)
			basePlan := plan.NewBalanceSchedulerPlan()
			solve := newSolver(basePlan, constant.NewScheduleKind(constant.RegionKind, constant.BySize), tc, oc.GetOpInfluence(tc.GetBasicCluster()))
			solve.Source, solve.Region, solve.fit = tc.GetStore(1), region, fit
			solve.sourceScore = solve.sourceStoreScore(scheduler.GetName())
			for _, id := range []uint64{4, 5} {
				solve.Target = tc.GetStore(id)
				solve.targetScore = solve.targetStoreScore(scheduler.GetName())
				require.True(t, solve.shouldBalance(scheduler.GetName()))
			}

			require.NotNil(t, scheduler.transferPeer(solve, nil, []*core.StoreInfo{tc.GetStore(5)}, nil))
			solve.Step = 2
			collector := plan.NewCollector(basePlan)
			// transferPeer traverses its destinations from the end of this list.
			op := scheduler.transferPeer(solve, collector, []*core.StoreInfo{tc.GetStore(5), tc.GetStore(4)}, nil)
			require.NotNil(t, op, "the scheduler must try store 5 after store 4 fails final isolation validation")
			require.Equal(t, 3, solve.Step)
			plans := collector.GetPlans()
			require.Len(t, plans, 2)
			require.True(t, plans[0].GetStatus().IsOK())
			require.Equal(t, uint64(5), plans[0].(*plan.BalanceSchedulerPlan).Target.GetID())
			require.Equal(t, uint64(4), plans[1].(*plan.BalanceSchedulerPlan).Target.GetID())
			targetLeader := region.GetLeader().GetStoreId()
			added, removed := false, false
			for i := range op.Len() {
				switch step := op.Step(i).(type) {
				case operator.TransferLeader:
					targetLeader = step.ToStore
				case operator.AddLearner:
					require.Equal(t, uint64(5), step.ToStore)
					added = true
				case operator.RemovePeer:
					require.Equal(t, uint64(1), step.FromStore)
					removed = true
				}
			}
			require.True(t, added)
			require.True(t, removed)
			target := region.Clone(core.WithReplacePeerStore(1, 5))
			target = target.Clone(core.WithLeader(target.GetStorePeer(targetLeader)))
			after := rm.FitRegionWithoutCache(tc, target)
			require.True(t, after.IsSatisfied())
			for _, rf := range after.RuleFits {
				require.True(t, rf.IsIsolationSatisfied())
			}
			solve.Step = 2
			require.Nil(t, scheduler.transferPeer(solve, nil, []*core.StoreInfo{tc.GetStore(4)}, nil))
			require.Equal(t, 2, solve.Step)
		})
	}
}
