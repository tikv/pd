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
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
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
		{0, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.ByCount}, func(constant.ScheduleKind) int64 {
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
		{10, constant.ScheduleKind{Resource: constant.LeaderKind, Policy: constant.ByCount}, func(constant.ScheduleKind) int64 {
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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

	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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

	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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

	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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

func TestConcurrencyUpdateConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(types.ScatterRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ScatterRangeScheduler, []string{"s_00", "s_50", "t"}))
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
			sche.config.buildWithArgs(args)
			re.NoError(sche.config.persist())
		}
	}()
	for range 1000 {
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
	for i := range 10 {
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

	hb, err := CreateScheduler(types.ScatterRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ScatterRangeScheduler, []string{"s_00", "s_09", "t"}))
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
