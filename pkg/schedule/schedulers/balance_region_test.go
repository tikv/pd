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
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
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

// TestBalanceRegionOrdinaryMoveNotBlockedByCandidateSize guards against
// double-counting the candidate region's size on top of tolerantResource in
// targetStoreScore. tolerantResource already represents roughly one average
// region's worth of margin, so a candidate whose size sits at or below that
// margin must not additionally raise the bar the target has to clear. Here
// the source holds three ordinary, equal-sized regions and the target is
// empty: moving one region is a clear improvement (192 vs 96 after the move)
// and must still be scheduled.
func TestBalanceRegionOrdinaryMoveNotBlockedByCandidateSize(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re := require.New(t)

	tc.SetTolerantSizeRatio(1)
	tc.SetRegionScoreFormulaVersion("v1")

	tc.AddRegionStore(1, 3, 288)
	tc.AddRegionStore(2, 0, 0)
	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(96))
	tc.PutRegion(region)

	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)
	influence := oc.GetOpInfluence(tc.GetBasicCluster())
	basePlan := plan.NewBalanceSchedulerPlan()
	solver := newSolver(basePlan, kind, tc, influence)
	solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
	re.True(solver.shouldBalance(""))
}

// TestBalanceRegionLargeCandidateDoesNotOvershoot guards against overshoot
// when the candidate region is much larger than the tolerant margin.
// sourceStoreScore and targetStoreScore both apply the same
// max(tolerantResource, candidateSize) delta, so a candidate that would leave
// the target heavier than the source after the move is correctly rejected:
// tolerantResource=10, candidate=100, source=150, target=0 project to
// source=50, target=100 after the move, which must not be scheduled even
// though comparing raw pre-move sizes alone would allow it.
func TestBalanceRegionLargeCandidateDoesNotOvershoot(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re := require.New(t)

	tc.SetTolerantSizeRatio(0.1)
	tc.SetRegionScoreFormulaVersion("v1")

	tc.AddRegionStore(1, 1, 150)
	tc.AddRegionStore(2, 0, 0)
	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(100))
	tc.PutRegion(region)

	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)
	influence := oc.GetOpInfluence(tc.GetBasicCluster())
	basePlan := plan.NewBalanceSchedulerPlan()
	solver := newSolver(basePlan, kind, tc, influence)
	solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
	re.False(solver.shouldBalance(""))
}

// TestSingleRegionOnLargeEmptyDiskDoesNotMigrate verifies that when a store's
// disk is mostly empty except for a single small region (e.g. 6TiB capacity
// with only a 10MiB region), balance-region does NOT move that region to an
// otherwise-identical, entirely empty peer store. Moving it would not fix any
// real imbalance (10MiB vs 6TiB capacity is a negligible utilization
// difference either way) and would just relocate the same "which store holds
// the only real data" state onto a different empty store — inviting the kind
// of pointless churn reported in #11135, since every other empty store looks
// equally attractive as a target on the next scheduling pass.
func TestSingleRegionOnLargeEmptyDiskDoesNotMigrate(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re := require.New(t)

	const (
		capacity     = 6 * units.TiB
		regionSizeMB = 10 // MiB, matches core.StoreInfo.GetRegionSize()'s unit
	)

	mkStore := func(id uint64, usedMB int64) *core.StoreInfo {
		usedBytes := uint64(usedMB) * units.MiB
		stats := &pdpb.StoreStats{
			Capacity:  capacity,
			UsedSize:  usedBytes,
			Available: capacity - usedBytes,
		}
		return core.NewStoreInfo(
			&metapb.Store{Id: id, State: metapb.StoreState_Up},
			core.SetStoreStats(stats),
			core.SetRegionCount(10),
			core.SetRegionSize(usedMB),
			core.SetLastHeartbeatTS(time.Now()),
		)
	}

	// storeA holds the cluster's only non-empty region; storeB is otherwise
	// identical (same capacity, same region count) but has no data at all,
	// and does not already hold a peer of region 1 — a real Schedule() run
	// would still consider it a legitimate, unfiltered candidate target.
	tc.PutStore(mkStore(1, regionSizeMB))
	tc.PutStore(mkStore(2, 0))

	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(regionSizeMB))
	tc.PutRegion(region)

	// Mirror the real-world setup that motivated this test: each store already
	// holds 10 regions (region *count* is balanced), but region 1 above is the
	// only one with any data — the other 19 are freshly-split, empty regions.
	var nextID uint64 = 2
	for range 9 {
		tc.AddLeaderRegion(nextID, 1)
		empty := tc.GetRegion(nextID).Clone(core.SetApproximateSize(0))
		tc.PutRegion(empty)
		nextID++
	}
	for range 10 {
		tc.AddLeaderRegion(nextID, 2)
		empty := tc.GetRegion(nextID).Clone(core.SetApproximateSize(0))
		tc.PutRegion(empty)
		nextID++
	}

	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)
	influence := oc.GetOpInfluence(tc.GetBasicCluster())
	basePlan := plan.NewBalanceSchedulerPlan()
	solver := newSolver(basePlan, kind, tc, influence)
	solver.Source, solver.Target, solver.Region = tc.GetStore(1), tc.GetStore(2), tc.GetRegion(1)
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(""), solver.targetStoreScore("")
	re.False(solver.shouldBalance(""))
}

// TestBalanceRegionRealScheduleDoesNotMoveSoleRegion drives the actual
// Schedule() entry point (not the solver methods directly) through the
// #11135 churn scenario: one store holds only the cluster's one real region,
// the other holds enough empty regions to dilute GetAverageRegionSize() well
// below the candidate's size. On pre-PR code this deterministically produces
// an operator — the real region is its store's only candidate, so region
// selection can't dodge it by picking an empty region instead — while the
// symmetric getRegionScoreDelta() must refuse it here. Checked in both
// directions so the result isn't an artifact of store iteration/sort order.
func TestBalanceRegionRealScheduleDoesNotMoveSoleRegion(t *testing.T) {
	for _, realOnStore1 := range []bool{true, false} {
		cancel, _, tc, oc := prepareSchedulersTest(false)
		re := require.New(t)

		tc.SetTolerantSizeRatio(1)
		tc.SetRegionScoreFormulaVersion("v1")
		tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
		tc.SetEnablePlacementRules(false)
		tc.SetMaxReplicasWithLabel(false, 1)
		sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
		re.NoError(err)

		realStore, emptyStore := uint64(1), uint64(2)
		if !realOnStore1 {
			realStore, emptyStore = 2, 1
		}
		tc.AddRegionStore(realStore, 1, 96)
		tc.AddRegionStore(emptyStore, 9, 0)
		tc.AddLeaderRegion(1, realStore)
		tc.PutRegion(tc.GetRegion(1).Clone(core.SetApproximateSize(96)))
		var nextID uint64 = 2
		for range 9 {
			tc.AddLeaderRegion(nextID, emptyStore)
			tc.PutRegion(tc.GetRegion(nextID).Clone(core.SetApproximateSize(0)))
			nextID++
		}

		for range 10 {
			ops, _ := sb.Schedule(tc, false)
			re.Empty(ops)
		}
		cancel()
	}
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
		solver := newSolver(basePlan, t.kind, tc, &operator.OpInfluence{})
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

// checkBalanceRegionSchedule1 checks source selection, peer movement and replica mismatch diagnostics.
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
	re.True(op.NeedStoreHealthCheck())

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
	// One store-score plan and two rejected batches of ten region candidates.
	re.Len(plans, 21)
	re.Empty(ops)
	status := plan.StatusRegionNotReplicated
	if enablePlacementRules {
		status = plan.StatusRegionNotMatchRule
	}
	for _, candidatePlan := range plans[1:] {
		re.Equal(status, int(candidatePlan.GetStatus().StatusCode))
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
		id, _, err := tc.Alloc(1)
		re.NoError(err)
		origin := tc.AddLeaderRegion(id, 4)
		newPeer := &metapb.Peer{StoreId: 3, Role: metapb.PeerRole_Voter}
		op, err := operator.CreateMovePeerOperator("balance-region", tc, origin, operator.OpKind(0), 4, newPeer)
		re.NoError(err)
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

// TestBalanceRegionUnhealthySource guards against balance-region excluding an
// unhealthy store as a rebalance source: a move-peer operator populates its
// new learner from the region's other (healthy) replicas and only removes
// the old peer afterward, so it never needs the old store to be reachable.
// See the review discussion on tikv/pd#11146.
func TestBalanceRegionUnhealthySource(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sb, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
	re.NoError(err)

	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(3, 1)
	tc.AddRegionStore(4, 0)
	tc.AddLeaderRegion(1, 2, 1, 3)
	// Store 1 has missed heartbeats for 11 minutes (Unhealthy), but that
	// doesn't stop balance-region from moving its peer to empty store 4.
	tc.SetStoreLastHeartbeatInterval(1, 11*time.Minute)

	ops, _ := sb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpKind(0), 1, 4)
}

// TestBalanceRegionEmptyRegion checks small keyed regions can move while empty regions and balanced stores stay put.
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
		core.SetApproximateKeys(0),
	)
	tc.PutRegion(region)

	tc.KeyRangeManager.Append([]keyutil.KeyRange{keyutil.NewKeyRange("a", "b")})
	operators, _ := sb.Schedule(tc, false)
	re.Empty(operators)
	tc.KeyRangeManager.Delete([]keyutil.KeyRange{keyutil.NewKeyRange("a", "b")})

	operators, _ = sb.Schedule(tc, false)
	re.NotEmpty(operators)

	// reject the empty regions if the cluster has more regions.
	for i := uint64(10); i < 111; i++ {
		tc.PutRegionStores(i, 1, 3, 4)
		tc.PutRegion(tc.GetRegion(i).Clone(core.SetApproximateSize(1)))
	}
	operators, _ = sb.Schedule(tc, false)
	re.Empty(operators)

	// A small region with keys can still move to a store with fewer regions.
	tc.UpdateStoreRegionSize(1, 104)
	tc.UpdateStoreRegionSize(2, 101)
	tc.UpdateStoreRegionSize(3, 103)
	tc.UpdateStoreRegionSize(4, 103)
	region = region.Clone(core.SetApproximateKeys(2000))
	tc.PutRegion(region)
	operators, _ = sb.Schedule(tc, false)
	re.Len(operators, 1)
	re.Equal(region.GetID(), operators[0].RegionID())
	operatorutil.CheckTransferPeer(re, operators[0], operator.OpKind(0), 1, 2)

	// Once the move finishes, the remaining size difference is within tolerance.
	region = region.Clone(core.WithRemoveStorePeer(1), core.WithAddPeer(&metapb.Peer{Id: 9, StoreId: 2}))
	tc.PutRegion(region)
	tc.UpdateStoreRegionSize(1, 103)
	tc.UpdateStoreRegionSize(2, 102)
	operators, _ = sb.Schedule(tc, false)
	re.Empty(operators)
}

// TestBalanceSmallRegionsAfterScaleOut checks small keyed peers converge after adding stores.
func TestBalanceSmallRegionsAfterScaleOut(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest(false)
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(false)
	tc.SetMaxReplicasWithLabel(false, 3)

	const regionCount = 500
	for id := uint64(1); id <= 3; id++ {
		tc.AddRegionStore(id, 0)
	}
	for id := uint64(1); id <= regionCount; id++ {
		region := tc.AddLeaderRegion(id, 1, 2, 3)
		tc.PutRegion(region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(2000)))
	}
	for id := uint64(1); id <= 3; id++ {
		tc.UpdateStoreStatus(id)
	}
	// Scale out only after every small region has peers on the original stores.
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 0)
	scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
	re.NoError(err)

	var idle, moves int
	for range regionCount * 4 {
		ops, _ := scheduler.Schedule(tc, false)
		if len(ops) == 0 {
			idle++
			if idle == 10 {
				break
			}
			continue
		}
		idle = 0
		operator.ApplyOperator(tc, ops[0])
		moves++
	}
	re.Equal(10, idle, "balancing should converge instead of moving peers indefinitely")
	var counts []int
	for id := uint64(1); id <= 5; id++ {
		count := tc.GetStoreRegionCount(id)
		counts = append(counts, count)
	}
	t.Logf("peer counts after 3-to-5 scale-out: %v; completed moves: %d", counts, moves)
	for _, count := range counts {
		re.InDelta(regionCount*3/5, count, 5)
	}
}

// TestBalanceRegionRejectedFollowerDoesNotBlockLeader checks hot or unmovable followers do not hide movable leaders.
func TestBalanceRegionRejectedFollowerDoesNotBlockLeader(t *testing.T) {
	for _, sizes := range []struct {
		rejected, movable int64
	}{{1, 1}, {1, 100}, {100, 1}, {100, 100}} {
		for _, rejection := range []string{"hot", "no target"} {
			for _, retryLimit := range []int{1, 10} {
				for _, placementRules := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/retries=%d/placement=%t/rejected-size=%d/movable-size=%d", rejection, retryLimit, placementRules, sizes.rejected, sizes.movable), func(t *testing.T) {
						re := require.New(t)
						cancel, _, tc, oc := prepareSchedulersTest(false)
						defer cancel()
						tc.SetEnablePlacementRules(placementRules)
						tc.SetMaxReplicasWithLabel(placementRules, 3)
						for id := uint64(1); id <= 5; id++ {
							tc.AddRegionStore(id, 0)
						}
						for id := uint64(1); id <= 4; id++ {
							region := tc.AddLeaderRegion(id, 1, 2, 3)
							tc.PutRegion(region.Clone(core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
						}
						movable := tc.GetRegion(1).Clone(core.SetApproximateSize(sizes.movable), core.SetApproximateKeys(2000))
						tc.PutRegion(movable)
						tc.AddLeaderRegionWithWriteInfo(5, 4, 1<<30, 100000, 0, 60, []uint64{1, 3}, 5)
						rejected := tc.GetRegion(5).Clone(core.SetApproximateSize(sizes.rejected), core.SetApproximateKeys(2000))
						tc.PutRegion(rejected)
						// Keep the cluster beyond the empty-region initialization exception.
						for id := uint64(10); id < 110; id++ {
							region := tc.AddLeaderRegion(id, 2, 3, 4)
							tc.PutRegion(region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(0)))
						}
						for id := uint64(1); id <= 5; id++ {
							tc.UpdateStoreStatus(id)
						}
						tc.SetStoreBusy(2, true)
						tc.SetStoreBusy(3, true)
						scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
							ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
						re.NoError(err)
						sb := scheduler.(*balanceRegionScheduler)
						cluster := &balanceRegionSampler{Cluster: tc, leaders: []*core.RegionInfo{movable}}

						// First prove that the rejected follower is eligible and has a target.
						tc.SetHotRegionCacheHitsThreshold(100)
						re.False(tc.IsRegionHot(rejected))
						ops, _ := sb.Schedule(cluster, false)
						re.Len(ops, 1)
						re.Equal(rejected.GetID(), ops[0].RegionID())
						operatorutil.CheckTransferPeer(re, ops[0], 0, 1, 5)

						if rejection == "hot" {
							tc.SetHotRegionCacheHitsThreshold(3)
							re.True(tc.IsRegionHot(rejected))
						} else {
							// Store 5 already holds a peer of the rejected region.
							rejected = rejected.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 9999, StoreId: 5}))
							tc.PutRegion(rejected)
							tc.UpdateStoreStatus(3)
							tc.UpdateStoreStatus(5)
							tc.SetStoreBusy(3, true)
							// Another eligible target is already within balance tolerance.
							// Only store 5 would improve the score for this region.
							tc.AddRegionStore(6, 0)
							tc.UpdateStoreRegionSize(6, tc.GetStoreRegionSize(1)-1)
						}
						// Include a source whose retry quota has already attenuated to one.
						sb.limits[1] = retryLimit
						ops, _ = sb.Schedule(cluster, false)
						re.Len(ops, 1)
						re.Equal(movable.GetID(), ops[0].RegionID())
						operatorutil.CheckTransferPeer(re, ops[0], 0, 1, 5)
					})
				}
			}
		}
	}
}

type balanceRegionSampler struct {
	*mockcluster.Cluster
	followers      []*core.RegionInfo
	leaders        []*core.RegionInfo
	batches        [][]*core.RegionInfo
	calls, samples int
	regionChecks   int
}

func (c *balanceRegionSampler) IsRegionHot(region *core.RegionInfo) bool {
	c.regionChecks++
	return c.Cluster.IsRegionHot(region)
}

func (c *balanceRegionSampler) RandLeaderRegions(id uint64, ranges []keyutil.KeyRange) []*core.RegionInfo {
	if id == 1 && c.leaders != nil {
		return c.leaders
	}
	return c.Cluster.RandLeaderRegions(id, ranges)
}

// RandFollowerRegions supplies controlled follower batches or counts real samples.
func (c *balanceRegionSampler) RandFollowerRegions(id uint64, ranges []keyutil.KeyRange) []*core.RegionInfo {
	if id != 1 {
		return c.Cluster.RandFollowerRegions(id, ranges)
	}
	regions := c.followers
	if len(c.batches) > 0 {
		regions = c.batches[min(c.calls, len(c.batches)-1)]
	} else if regions == nil {
		regions = c.Cluster.RandFollowerRegions(id, ranges)
	}
	c.calls++
	c.samples += len(regions)
	return regions
}

// TestBalanceRegionSampledFollowerFallback checks that late rejection preserves the rest of a sampled batch.
func TestBalanceRegionSampledFollowerFallback(t *testing.T) {
	for _, sizes := range []struct {
		rejected, movable int64
	}{{1, 1}, {1, 100}, {100, 1}, {100, 100}} {
		for _, rejection := range []string{"hot", "no target"} {
			for _, retryLimit := range []int{1, 10} {
				for _, placementRules := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/retries=%d/placement=%t/rejected-size=%d/movable-size=%d", rejection, retryLimit, placementRules, sizes.rejected, sizes.movable), func(t *testing.T) {
						re := require.New(t)
						cancel, _, tc, oc := prepareSchedulersTest(false)
						defer cancel()
						tc.SetEnablePlacementRules(placementRules)
						tc.SetMaxReplicasWithLabel(placementRules, 3)
						tc.SetHotRegionCacheHitsThreshold(3)
						for id := uint64(1); id <= 5; id++ {
							tc.AddRegionStore(id, 0)
						}
						for id := uint64(1); id <= 4; id++ {
							region := tc.AddLeaderRegion(id, 2, 1, 3)
							tc.PutRegion(region.Clone(core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
						}
						tc.AddLeaderRegionWithWriteInfo(5, 4, 1<<30, 100000, 0, 60, []uint64{1, 3}, 5)
						rejected := tc.GetRegion(5).Clone(core.SetApproximateSize(sizes.rejected), core.SetApproximateKeys(2000))
						tc.PutRegion(rejected)
						re.True(tc.IsRegionHot(rejected))
						for id := uint64(10); id < 110; id++ {
							region := tc.AddLeaderRegion(id, 2, 3, 4)
							tc.PutRegion(region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(0)))
						}
						movable := tc.GetRegion(1).Clone(core.SetApproximateSize(sizes.movable), core.SetApproximateKeys(2000))
						tc.PutRegion(movable)
						for id := uint64(1); id <= 5; id++ {
							tc.UpdateStoreStatus(id)
						}
						tc.UpdateStoreRegionWeight(2, 1000)
						tc.UpdateStoreRegionWeight(3, 1000)
						cluster := &balanceRegionSampler{Cluster: tc, followers: []*core.RegionInfo{movable}}
						scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
							ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
						re.NoError(err)
						// Prove that the follower behind the rejected candidate can move.
						ops, _ := scheduler.Schedule(cluster, false)
						re.Len(ops, 1)
						re.Equal(movable.GetID(), ops[0].RegionID())
						operatorutil.CheckTransferPeer(re, ops[0], 0, 1, 5)
						if rejection == "no target" {
							tc.SetHotRegionCacheHitsThreshold(100)
							rejected = rejected.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 9999, StoreId: 5}))
							tc.PutRegion(rejected)
							tc.UpdateStoreStatus(3)
							tc.UpdateStoreStatus(5)
							for _, id := range []uint64{2, 3} {
								// Exhaust receive capacity without preventing the leader from sending.
								tc.SetStoreLimit(id, storelimit.AddPeer, 0.00006)
								tc.ResetStoreLimit(id, storelimit.AddPeer, 0.000001)
								re.True(tc.GetStore(id).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
								re.False(tc.GetStore(id).IsAvailable(storelimit.AddPeer, constant.Medium))
							}
							re.False(tc.IsRegionHot(rejected))
							// An eligible but balanced target must not hide the movable follower.
							tc.AddRegionStore(6, 0)
							tc.UpdateStoreRegionSize(6, tc.GetStoreRegionSize(1)-1)
						}
						cluster.followers = nil
						for range 5 {
							cluster.followers = append(cluster.followers, rejected, movable)
						}
						cluster.calls, cluster.samples = 0, 0
						scheduler.(*balanceRegionScheduler).limits[1] = retryLimit
						ops, _ = scheduler.Schedule(cluster, false)
						re.Len(ops, 1)
						re.Equal(movable.GetID(), ops[0].RegionID())
						operatorutil.CheckTransferPeer(re, ops[0], 0, 1, 5)
						re.Equal(1, cluster.calls)
					})
				}
			}
		}
	}
}

// TestBalanceRegionFilteredFollowerResampling checks that a missed follower gets another sampling opportunity within the quota.
func TestBalanceRegionFilteredFollowerResampling(t *testing.T) {
	for _, retryLimit := range []int{1, 2, 10} {
		for _, placementRules := range []bool{false, true} {
			t.Run(fmt.Sprintf("retries=%d/placement=%t", retryLimit, placementRules), func(t *testing.T) {
				re := require.New(t)
				cancel, _, tc, oc := prepareSchedulersTest(false)
				defer cancel()
				tc.SetEnablePlacementRules(placementRules)
				tc.SetMaxReplicasWithLabel(placementRules, 3)
				for id := uint64(1); id <= 5; id++ {
					tc.AddRegionStore(id, 0)
				}
				for id := uint64(1); id <= 4; id++ {
					region := tc.AddLeaderRegion(id, 1, 2, 3)
					tc.PutRegion(region.Clone(core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
				}
				large := tc.AddLeaderRegion(5, 4, 1, 3).Clone(core.SetApproximateSize(100), core.SetApproximateKeys(1000))
				tc.PutRegion(large)
				for id := uint64(10); id < 110; id++ {
					region := tc.AddLeaderRegion(id, 4, 1, 3)
					tc.PutRegion(region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(0)))
				}
				for id := uint64(1); id <= 5; id++ {
					tc.UpdateStoreStatus(id)
				}
				tc.SetStoreBusy(2, true)
				tc.SetStoreBusy(3, true)
				cluster := &balanceRegionSampler{Cluster: tc, followers: []*core.RegionInfo{large}}
				scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
					ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
				re.NoError(err)
				// Prove that the follower is movable before controlling which batch contains it.
				ops, _ := scheduler.Schedule(cluster, false)
				re.Len(ops, 1)
				re.Equal(large.GetID(), ops[0].RegionID())
				var filtered []*core.RegionInfo
				for range 10 {
					filtered = append(filtered, tc.GetRegion(10))
				}
				cluster.batches = [][]*core.RegionInfo{filtered, {large}}
				cluster.calls, cluster.samples = 0, 0
				scheduler.(*balanceRegionScheduler).limits[1] = retryLimit
				ops, _ = scheduler.Schedule(cluster, false)
				re.Len(ops, 1)
				if retryLimit == 1 {
					re.LessOrEqual(ops[0].RegionID(), uint64(4))
				} else {
					re.Equal(large.GetID(), ops[0].RegionID())
				}
				operatorutil.CheckTransferPeer(re, ops[0], 0, 1, 5)
				re.Equal(min(2, retryLimit), cluster.calls)
			})
		}
	}
}

// TestBalanceRegionFilteredFollowerBatchCost bounds filtered sampling before falling back to leaders.
func TestBalanceRegionFilteredFollowerBatchCost(t *testing.T) {
	for _, placementRules := range []bool{false, true} {
		for _, rejection := range []string{"empty", "hot", "no target"} {
			t.Run(fmt.Sprintf("placement=%t/%s", placementRules, rejection), func(t *testing.T) {
				re := require.New(t)
				cancel, _, tc, oc := prepareSchedulersTest(false)
				defer cancel()
				tc.SetEnablePlacementRules(placementRules)
				tc.SetMaxReplicasWithLabel(placementRules, 3)
				tc.SetHotRegionCacheHitsThreshold(3)
				for id := uint64(1); id <= 5; id++ {
					tc.AddRegionStore(id, 0)
				}
				for id := uint64(1); id <= 4; id++ {
					region := tc.AddLeaderRegion(id, 1, 2, 3)
					tc.PutRegion(region.Clone(core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
				}
				for id := uint64(10); id < 110; id++ {
					region := tc.AddLeaderRegion(id, 4, 1, 3)
					tc.PutRegion(region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(0)))
				}
				var rejected *core.RegionInfo
				switch rejection {
				case "hot":
					tc.AddLeaderRegionWithWriteInfo(5, 4, 1<<30, 100000, 0, 60, []uint64{1, 3}, 5)
					rejected = tc.GetRegion(5).Clone(core.SetApproximateSize(1), core.SetApproximateKeys(2000))
					tc.PutRegion(rejected)
					re.True(tc.IsRegionHot(rejected))
				case "no target":
					rejected = tc.AddLeaderRegion(5, 4, 1, 5).Clone(core.SetApproximateSize(1), core.SetApproximateKeys(2000))
					tc.PutRegion(rejected)
					re.False(tc.IsRegionHot(rejected))
				}
				for id := uint64(1); id <= 5; id++ {
					tc.UpdateStoreStatus(id)
				}
				tc.SetStoreBusy(2, true)
				tc.SetStoreBusy(3, true)
				cluster := &balanceRegionSampler{Cluster: tc}
				if rejected != nil {
					for range 5 {
						cluster.followers = append(cluster.followers, rejected, tc.GetRegion(10))
					}
				}
				scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
					ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
				re.NoError(err)
				// Every successful move resets the source quota, so verify repeated calls.
				for _, retryLimit := range []int{1, 2, 10} {
					for range 3 {
						cluster.calls, cluster.samples = 0, 0
						scheduler.(*balanceRegionScheduler).limits[1] = retryLimit
						ops, _ := scheduler.Schedule(cluster, false)
						re.Len(ops, 1)
						re.LessOrEqual(ops[0].RegionID(), uint64(4))
						re.Equal(min(2, retryLimit), cluster.calls, "retry quota: %d", retryLimit)
						re.Equal(10*min(2, retryLimit), cluster.samples, "retry quota: %d", retryLimit)
					}
				}
			})
		}
	}
}

// TestBalanceRegionIdleRetryQuota bounds candidate checks across batches and peer roles.
func TestBalanceRegionIdleRetryQuota(t *testing.T) {
	for _, placementRules := range []bool{false, true} {
		for _, regionSizes := range [][]int64{{1}, {96}, {1, 96}} {
			t.Run(fmt.Sprintf("placement=%t/sizes=%v", placementRules, regionSizes), func(t *testing.T) {
				re := require.New(t)
				cancel, _, tc, oc := prepareSchedulersTest(false)
				defer cancel()
				tc.SetEnablePlacementRules(placementRules)
				tc.SetMaxReplicasWithLabel(placementRules, 3)
				const storeCount = uint64(20)
				for id := uint64(1); id <= storeCount; id++ {
					tc.AddRegionStore(id, 0)
				}
				for id := range uint64(600) {
					regionSize := regionSizes[id/storeCount%uint64(len(regionSizes))]
					region := tc.AddLeaderRegion(id+1, id%storeCount+1, (id+1)%storeCount+1, (id+2)%storeCount+1)
					tc.PutRegion(region.Clone(core.SetApproximateSize(regionSize), core.SetApproximateKeys(2000)))
				}
				for id := uint64(1); id <= storeCount; id++ {
					tc.UpdateStoreStatus(id)
					re.Equal(tc.GetStoreRegionSize(1), tc.GetStoreRegionSize(id))
				}
				cluster := &balanceRegionSampler{Cluster: tc}
				scheduler, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
					ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
				re.NoError(err)
				for _, retryLimit := range []int{1, 2, 10} {
					for id := uint64(1); id <= storeCount; id++ {
						scheduler.(*balanceRegionScheduler).limits[id] = retryLimit
					}
					cluster.regionChecks = 0
					ops, _ := scheduler.Schedule(cluster, false)
					re.Empty(ops)
					re.Positive(cluster.regionChecks)
					re.LessOrEqual(cluster.regionChecks, int(storeCount-1)*retryLimit)
					t.Logf("retry quota: %d; candidates checked: %d", retryLimit, cluster.regionChecks)
				}
			})
		}
	}
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
			re.NoError(sche.config.buildWithArgs(args))
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
			StartKey: fmt.Appendf(nil, "s_%02d", i),
			EndKey:   fmt.Appendf(nil, "s_%02d", i+1),
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
	for limit <= count {
		ops, _ := hb.Schedule(tc, false)
		if ops == nil {
			limit++
			continue
		}
		operator.ApplyOperator(tc, ops[0])
	}
}
