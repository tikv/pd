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

package checker

import (
	"slices"
	"strconv"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newLearnerConflictController(t *testing.T) (*Controller, *mockcluster.Cluster, *operator.Controller) {
	t.Helper()
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	tc.SetEnableUseJointConsensus(false)
	tc.SetMaxMergeRegionSize(0)
	tc.SetMaxAffinityMergeRegionSize(0)
	for id := uint64(1); id <= 5; id++ {
		tc.AddRegionStore(id, 10)
	}
	tc.AddLeaderRegionWithRange(1, "a", "b", 1, 2, 3)
	tc.PutRegion(tc.GetRegion(1).Clone(core.SetApproximateSize(1)))
	stream := hbstream.NewTestHeartbeatStreams(t.Context(), tc, false)
	t.Cleanup(stream.Close)
	oc := operator.NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	c := NewController(t.Context(), tc, tc.GetCheckerConfig(), oc)
	c.affinityChecker.startTime = time.Now().Add(-2 * recentMergeTTL)
	return c, tc, oc
}

// newLearnerRule returns a rule that requires one learner, on one of stores if any.
func newLearnerRule(stores ...string) *placement.Rule {
	rule := &placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}
	if len(stores) > 0 {
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "ID", Op: placement.In, Values: stores}}
	}
	return rule
}

func putLearner(tc *mockcluster.Cluster, regionID, storeID uint64) *core.RegionInfo {
	region := tc.GetRegion(regionID).Clone(core.SetApproximateSize(1), core.WithAddPeer(&metapb.Peer{
		Id: 100 + regionID, StoreId: storeID, Role: metapb.PeerRole_Learner,
	}))
	tc.PutRegion(region)
	return region
}

// Apply the generated steps, including joint states, instead of supplying an
// expected final layout that could hide an invalid intermediate transition.
func applyLearnerOperator(t *testing.T, tc *mockcluster.Cluster, op *operator.Operator) *core.RegionInfo {
	t.Helper()
	re := require.New(t)
	region := tc.GetRegion(op.RegionID())
	original := region
	re.True(op.Start())
	for n := 0; n <= op.Len(); n++ {
		step := op.Check(region)
		if step == nil {
			re.True(op.IsEnd())
			tc.PutRegion(region)
			for id := range original.GetStoreIDs() {
				tc.UpdateStoreStatus(id)
			}
			for id := range region.GetStoreIDs() {
				tc.UpdateStoreStatus(id)
			}
			return region
		}
		re.NoError(step.CheckInProgress(tc.GetBasicCluster(), tc.GetSharedConfig(), region, false), "%s", step)
		re.NotNil(step.GetCmd(region, true), "%s", step)
		switch s := step.(type) {
		case operator.AddLearner:
			re.Nil(region.GetStorePeer(s.ToStore))
			region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: s.PeerID, StoreId: s.ToStore, Role: metapb.PeerRole_Learner, IsWitness: s.IsWitness}))
		case operator.PromoteLearner:
			re.Equal(s.PeerID, region.GetStoreLearner(s.ToStore).GetId())
			region = region.Clone(core.WithRole(s.PeerID, metapb.PeerRole_Voter))
		case operator.ChangePeerV2Enter:
			var opts []core.RegionCreateOption
			for _, p := range s.PromoteLearners {
				re.Equal(p.PeerID, region.GetStoreLearner(p.ToStore).GetId())
				opts = append(opts, core.WithRole(p.PeerID, metapb.PeerRole_IncomingVoter))
			}
			for _, p := range s.DemoteVoters {
				re.Equal(p.PeerID, region.GetStoreVoter(p.ToStore).GetId())
				opts = append(opts, core.WithRole(p.PeerID, metapb.PeerRole_DemotingVoter))
			}
			region = region.Clone(opts...)
		case operator.ChangePeerV2Leave:
			var opts []core.RegionCreateOption
			for _, p := range s.PromoteLearners {
				opts = append(opts, core.WithRole(p.PeerID, metapb.PeerRole_Voter))
			}
			for _, p := range s.DemoteVoters {
				re.NotEqual(p.ToStore, region.GetLeader().GetStoreId())
				opts = append(opts, core.WithRole(p.PeerID, metapb.PeerRole_Learner))
			}
			region = region.Clone(opts...)
		case operator.TransferLeader:
			re.NotNil(region.GetStoreVoter(s.ToStore))
			re.NotEqual(metapb.PeerRole_DemotingVoter, region.GetStoreVoter(s.ToStore).GetRole())
			region = region.Clone(core.WithLeader(region.GetStorePeer(s.ToStore)))
		case operator.RemovePeer:
			re.NotNil(region.GetStorePeer(s.FromStore))
			re.NotEqual(s.FromStore, region.GetLeader().GetStoreId())
			region = region.Clone(core.WithRemoveStorePeer(s.FromStore))
		default:
			t.Fatalf("unexpected step %T: %s", step, step)
		}
		re.True(step.IsFinish(region), "%s", step)
	}
	t.Fatal("operator did not finish")
	return nil
}

func checkAndApply(t *testing.T, c *Controller, tc *mockcluster.Cluster, regionIDs ...uint64) {
	t.Helper()
	for _, regionID := range regionIDs {
		for _, op := range c.CheckRegion(tc.GetRegion(regionID)) {
			applyLearnerOperator(t, tc, op)
		}
	}
}

// A learner on a target voter store swaps roles with the voter that the target
// drops, so no data is copied and the group keeps its target. While the target
// leader store rejects leaders, no operator is built and no move is counted.
func TestAffinityCheckerLearnerSwap(t *testing.T) {
	for _, target := range []struct {
		name   string
		leader uint64
	}{{"voter", 1}, {"leader", 4}} {
		t.Run(target.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			re.NoError(tc.GetRuleManager().SetRule(newLearnerRule()))
			region := putLearner(tc, 1, 4)
			manager := tc.GetAffinityManager()
			const id = "learner-swap"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: target.leader, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

			tc.SetStoreEvictLeader(target.leader, true)
			re.Empty(c.CheckRegion(region))
			tc.SetStoreEvictLeader(target.leader, false)
			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			re.Zero(ops[0].Kind()&operator.OpRegion, "a swap adds or removes no peer")
			for i := range ops[0].Len() {
				_, adds := ops[0].Step(i).(operator.AddLearner)
				_, removes := ops[0].Step(i).(operator.RemovePeer)
				re.False(adds || removes, "a swap copies no peer")
			}
			swapped := applyLearnerOperator(t, tc, ops[0])
			re.NotNil(swapped.GetStoreLearner(3))
			re.NotNil(swapped.GetStoreVoter(4))
			re.Empty(c.CheckRegion(swapped))
			state := manager.GetAffinityGroupState(id)
			re.Equal(affinity.PhaseStable, state.Phase)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
			re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter), "only the built operator counts")
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

// The target drops the voters on stores 1 and 3. A learner on store 1 would
// share a zone with the learner on store 5, so the learner on store 4 takes
// store 3 instead, although store 1 comes first.
func TestAffinityCheckerLearnerSwapIsolation(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.AddRegionStore(6, 10)
	for i, zone := range []string{"z1", "z2", "z3", "z4", "z1", "z3"} {
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": zone})
	}
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels, rule.IsolationLevel = []string{"zone"}, "zone"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	rule = newLearnerRule()
	rule.Count, rule.LocationLabels, rule.IsolationLevel = 2, []string{"zone"}, "zone"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	putLearner(tc, 1, 4)
	region := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 201, StoreId: 5, Role: metapb.PeerRole_Learner}))
	tc.PutRegion(region)
	manager := tc.GetAffinityManager()
	const id = "learner-swap-isolation"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
		ID: id, LeaderStoreID: 2, VoterStoreIDs: []uint64{2, 4, 6},
	}, region.GetStartKey(), region.GetEndKey()))
	moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
	conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

	checkAndApply(t, c, tc, 1, 1)
	moved := tc.GetRegion(1)
	re.NotNil(moved.GetStoreLearner(3), "the learner takes store 3")
	re.NotNil(moved.GetStoreLearner(5))
	re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
	state := manager.GetAffinityGroupState(id)
	re.Equal(affinity.PhaseStable, state.Phase)
	re.Equal([]uint64{2, 4, 6}, state.VoterStoreIDs)
	re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
	re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
}

func TestAffinityCheckerLearnerSwapFallback(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.SetEnableUseJointConsensus(true)
	tc.AddRegionStore(6, 10)
	re.NoError(tc.GetRuleManager().SetRule(newLearnerRule("2", "4", "6")))
	rule := newLearnerRule("2", "5")
	rule.ID = "learner-2"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	putLearner(tc, 1, 4)
	region := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 201, StoreId: 5, Role: metapb.PeerRole_Learner}))
	tc.PutRegion(region)
	manager := tc.GetAffinityManager()
	const id = "learner-swap-fallback"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
		ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 4, 5},
	}, region.GetStartKey(), region.GetEndKey()))
	re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
	re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, 1, 1, 4, 5), false))

	// The first learner can use store 2 or 6; the second can use only store 2.
	ops := c.CheckRegion(region)
	re.Len(ops, 1, "the mixed layout fits (group available: %t)", manager.GetAffinityGroupState(id).AffinitySchedulingAllowed)
	applyLearnerOperator(t, tc, ops[0])
	checkAndApply(t, c, tc, 1)
	moved := tc.GetRegion(1)
	re.NotNil(moved.GetStoreLearner(6))
	re.NotNil(moved.GetStoreLearner(2))
	re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
	state := manager.GetAffinityGroupState(id)
	re.Equal(affinity.PhaseStable, state.Phase)
	re.Equal([]uint64{1, 4, 5}, state.VoterStoreIDs)
}

// If the rule of the learner does not allow the voter store that the target
// drops, or with location labels another store is a better location, the
// learner moves to that store. While that store is busy, the move waits
// instead of expiring the group.
func TestAffinityCheckerLearnerMoveToRuleStore(t *testing.T) {
	for _, kind := range []string{"label-constraint", "location-label", "location-label-less-regions", "single-learner-isolation"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			rule := newLearnerRule("4", "5")
			switch kind {
			case "single-learner-isolation":
				rule.LocationLabels, rule.IsolationLevel = []string{"zone"}, "zone"
				for id := uint64(1); id <= 5; id++ {
					tc.SetStoreLabel(id, map[string]string{"ID": strconv.FormatUint(id, 10), "zone": strconv.FormatUint(id, 10)})
				}
			case "location-label", "location-label-less-regions":
				// Store 5 is the only store in a zone that no target voter uses. While
				// it is busy, the swap to store 3 and the move to store 6 fail the
				// layout check only because store 5 is a better location.
				tc.AddRegionStore(6, 100)
				if kind == "location-label-less-regions" {
					tc.AddRegionStore(6, 0)
				}
				for i, zone := range []string{"a", "b", "a", "c", "d", "b"} {
					tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": zone})
				}
				rule = newLearnerRule()
				rule.LocationLabels = []string{"zone"}
			}
			re.NoError(tc.GetRuleManager().SetRule(rule))
			region := putLearner(tc, 1, 4)
			manager := tc.GetAffinityManager()
			const id = "learner-move"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

			tc.SetStoreBusy(5, true)
			for range 3 {
				re.Empty(c.CheckRegion(region))
				state := manager.GetAffinityGroupState(id)
				re.True(state.AffinitySchedulingAllowed, "a busy store must not expire the group")
				re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
				re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
			}
			tc.SetStoreBusy(5, false)
			checkAndApply(t, c, tc, 1, 1)
			moved := tc.GetRegion(1)
			state := manager.GetAffinityGroupState(id)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs, "the group keeps its target")
			re.NotNil(moved.GetStoreLearner(5))
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
			re.Equal(affinity.PhaseStable, state.Phase)
			re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

func TestAffinityCheckerLearnerRuleLocations(t *testing.T) {
	for _, kind := range []string{"cross-rule-location", "rule-isolation"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			tc.AddRegionStore(6, 0)
			tc.AddRegionStore(7, 10)
			zones := []string{"a", "b", "c", "d", "e", "e", "f"}
			rule := newLearnerRule("4", "6", "7")
			destination := uint64(7)
			if kind == "rule-isolation" {
				zones = []string{"a", "b", "c", "c", "d", "a", "b"}
				rule = newLearnerRule("1", "4", "5", "6")
				rule.Count, rule.IsolationLevel = 2, "zone"
				destination = 6
			} else {
				other := newLearnerRule("5")
				other.ID = "learner-b"
				re.NoError(tc.GetRuleManager().SetRule(other))
			}
			for i, zone := range zones {
				tc.SetStoreLabel(uint64(i+1), map[string]string{"ID": strconv.Itoa(i + 1), "zone": zone})
			}
			rule.LocationLabels = []string{"zone"}
			re.NoError(tc.GetRuleManager().SetRule(rule))
			putLearner(tc, 1, 4)
			region := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 201, StoreId: 5, Role: metapb.PeerRole_Learner}))
			tc.PutRegion(region)
			manager := tc.GetAffinityManager()
			const id = "learner-rule-locations"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, 1, 1, 2, 4), false))
			tc.SetStoreBusy(destination, true)
			re.Empty(c.CheckRegion(region))
			re.True(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed, "a busy destination must not expire the group")
			tc.SetStoreBusy(destination, false)
			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			applyLearnerOperator(t, tc, ops[0])
			checkAndApply(t, c, tc, 1)
			moved := tc.GetRegion(1)
			re.NotNil(moved.GetStoreLearner(destination))
			re.NotNil(moved.GetStoreLearner(5))
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
			state := manager.GetAffinityGroupState(id)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
			re.Equal(affinity.PhaseStable, state.Phase)
		})
	}
}

// A blocked improvement can explain this rule's insufficient hard isolation,
// but must not hide another rule's definite rejection, in either rule order.
func TestAffinityCheckerPlacementTemporaryIsolation(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.AddRegionStore(6, 10)
	for i, zone := range []string{"v", "v", "v", "a", "a", "b"} {
		tc.SetStoreLabel(uint64(i+1), map[string]string{"ID": strconv.Itoa(i + 1), "zone": zone})
	}
	rule := newLearnerRule("4", "5", "6")
	rule.Count, rule.LocationLabels, rule.IsolationLevel = 2, []string{"zone"}, "zone"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	region := putLearner(tc, 1, 4).Clone(core.WithAddPeer(&metapb.Peer{Id: 201, StoreId: 5, Role: metapb.PeerRole_Learner}))
	tc.PutRegion(region)
	tc.SetStoreBusy(6, true)
	fit := tc.GetRuleManager().FitRegionWithoutCache(tc, region)
	re.True(fit.IsSatisfied())
	re.Equal(affinityPlacementTemporary, c.affinityChecker.checkRegionFitAtBestLocation(region, fit))
	tc.SetStoreBusy(6, false)
	re.Equal(affinityPlacementRejected, c.affinityChecker.checkRegionFitAtBestLocation(region, fit), "the improvement is now available")
	moved := region.Clone(core.WithRemoveStorePeer(5), core.WithAddPeer(&metapb.Peer{Id: 202, StoreId: 6, Role: metapb.PeerRole_Learner}))
	re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(moved, false))

	voterRule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	voterRule.LabelConstraints = []placement.LabelConstraint{{Key: "ID", Op: placement.In, Values: []string{"1", "2", "3"}}}
	voterRule.LocationLabels, voterRule.IsolationLevel = []string{"zone"}, "zone"
	re.NoError(tc.GetRuleManager().SetRule(voterRule))
	tc.SetStoreBusy(6, true)
	fit = tc.GetRuleManager().FitRegionWithoutCache(tc, region)
	re.True(fit.IsSatisfied())
	for range 2 {
		re.Equal(affinityPlacementRejected, c.affinityChecker.checkRegionFitAtBestLocation(region, fit))
		slices.Reverse(fit.RuleFits)
	}
}

// A local heuristic can miss a layout or be constrained by the original fitted
// rules. The failing Region must relearn immediately: otherwise fixed A,B
// patrols let B restore the old target on every round. Execute every operator
// and keep checking after Stable to detect repeated expiration or oscillation.
func TestAffinityCheckerLearnerRelearn(t *testing.T) {
	for _, kind := range []string{"contention", "rule-refit", "direct-move"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			tc.AddRegionStore(6, 0)
			tc.AddRegionStore(7, 100)
			rule := newLearnerRule("4", "6", "7")
			initialTarget := []uint64{1, 4, 5}
			otherRoles := map[uint64]placement.PeerRoleType{1: placement.Leader, 4: placement.Voter, 5: placement.Voter, 6: placement.Learner, 7: placement.Learner}
			switch kind {
			case "rule-refit":
				tc.AddRegionStore(6, 10)
				tc.AddRegionStore(7, 10)
				rule = newLearnerRule("4", "5", "6", "7")
				rule.Count, rule.LocationLabels, rule.IsolationLevel = 2, []string{"zone"}, "zone"
				initialTarget = []uint64{1, 2, 4}
				otherRoles[2], otherRoles[5] = placement.Voter, placement.Learner
			case "direct-move":
				tc.AddRegionStore(6, 100)
				tc.AddRegionStore(7, 0)
			}
			for i, zone := range []string{"v1", "v2", "v3", "a", "b", "c", "b"} {
				tc.SetStoreLabel(uint64(i+1), map[string]string{"ID": strconv.Itoa(i + 1), "zone": zone})
			}
			re.NoError(tc.GetRuleManager().SetRule(rule))
			rule = newLearnerRule("5", "6")
			rule.ID = "learner-b"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			a := putLearner(tc, 1, 4).Clone(core.WithAddPeer(&metapb.Peer{Id: 201, StoreId: 5, Role: metapb.PeerRole_Learner}))
			if kind == "rule-refit" {
				a = a.Clone(core.WithAddPeer(&metapb.Peer{Id: 301, StoreId: 6, Role: metapb.PeerRole_Learner}))
			}
			tc.PutRegion(a)
			b := regionWithRoles(a, 1, otherRoles).Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c")))
			peerIDs := make([]uint64, len(b.GetPeers()))
			for i := range peerIDs {
				peerIDs[i] = uint64(1001 + i)
			}
			b = b.Clone(core.WithNewPeerIDs(peerIDs...))
			b = b.Clone(core.WithLeader(b.GetStorePeer(1)))
			tc.PutRegion(b)
			for _, region := range []*core.RegionInfo{a, b} {
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, 1, initialTarget...), false))
			}
			manager := tc.GetAffinityManager()
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{ID: kind, LeaderStoreID: 1, VoterStoreIDs: initialTarget}, []byte("a"), []byte("c")))
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			wantTarget, operatorRegion := []uint64{1, 2, 3}, uint64(2)
			wantAdded, wantRemoved := []uint64{2, 3}, []uint64{4, 5}
			switch kind {
			case "rule-refit":
				wantAdded, wantRemoved = []uint64{3}, []uint64{4}
			case "direct-move":
				wantTarget, operatorRegion = initialTarget, 1
				wantAdded, wantRemoved = []uint64{6, 7}, []uint64{2, 3}
			}
			var added, removed []uint64
			for round := range 4 {
				for _, id := range []uint64{1, 2} {
					ops := c.CheckRegion(tc.GetRegion(id))
					if round == 0 && id == operatorRegion {
						re.Len(ops, 1, "round %d, Region %d, target %v", round, id, manager.GetAffinityGroupState(kind).VoterStoreIDs)
					} else {
						re.Empty(ops)
					}
					for _, op := range ops {
						for i := range op.Len() {
							switch step := op.Step(i).(type) {
							case operator.AddLearner:
								added = append(added, step.ToStore)
							case operator.RemovePeer:
								removed = append(removed, step.FromStore)
							}
						}
						applyLearnerOperator(t, tc, op)
					}
					state := manager.GetAffinityGroupState(kind)
					re.True(state.AffinitySchedulingAllowed)
					re.Equal(wantTarget, state.VoterStoreIDs, "round %d, Region %d", round, id)
				}
				if round > 0 {
					re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(kind).Phase)
				}
			}
			re.ElementsMatch(wantAdded, added)
			re.ElementsMatch(wantRemoved, removed)
			if kind == "direct-move" {
				re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
				re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			} else {
				re.Equal(conflicts+1, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
				re.Equal(moves, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
				for _, original := range []*core.RegionInfo{a, b} {
					re.Equal(original.GetLearners(), tc.GetRegion(original.GetID()).GetLearners(), "relearning preserves the learner peers")
				}
			}
			for _, id := range []uint64{1, 2} {
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(tc.GetRegion(id), false))
			}
		})
	}
}

// Each of two Regions keeps its learner on a voter store of the other and no
// other store is up, so no target fits both Regions while the learners stay.
// A learner moves and the group converges without expiring.
func TestAffinityCheckerScatteredLearners(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.SetStoreOffline(5)
	re.NoError(tc.GetRuleManager().SetRule(newLearnerRule()))
	tc.AddLeaderRegionWithRange(2, "b", "c", 1, 2, 4)
	putLearner(tc, 1, 4)
	putLearner(tc, 2, 3)
	manager := tc.GetAffinityManager()
	const id = "learner-scattered"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{ID: id}, []byte("a"), []byte("c")))
	conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

	for range 3 {
		for _, regionID := range []uint64{1, 2} {
			checkAndApply(t, c, tc, regionID)
			re.True(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed, "the group must not expire")
		}
	}
	re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
	for _, regionID := range []uint64{1, 2} {
		re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, tc.GetRegion(regionID)).IsSatisfied())
	}
	re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
}

// Only a rule-required TiKV learner on a target voter store that another store
// can take moves. A TiFlash learner is on a store that no valid target uses as
// a voter store, and the learners of a peer move in progress and an orphan
// learner fail the placement check first. A witness, or a learner that its rule
// pins to its store, cannot move, so the group relearns from this Region.
func TestAffinityCheckerLearnerNotMoved(t *testing.T) {
	for _, kind := range []string{"tiflash", "peer-move", "orphan", "witness", "pinned"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, oc := newLearnerConflictController(t)
			switch kind {
			case "tiflash":
				tc.SetStoreLabel(5, map[string]string{"engine": "tiflash"})
				rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"tiflash"}}}
				re.NoError(tc.GetRuleManager().SetRule(rule))
				rule = newLearnerRule()
				rule.GroupID, rule.LabelConstraints = "tiflash", []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}
				re.NoError(tc.GetRuleManager().SetRule(rule))
				putLearner(tc, 1, 5)
			case "witness":
				tc.SetEnableWitness(true)
				rule := newLearnerRule()
				rule.IsWitness = true
				re.NoError(tc.GetRuleManager().SetRule(rule))
				tc.PutRegion(tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{
					Id: 101, StoreId: 4, Role: metapb.PeerRole_Learner, IsWitness: true,
				})))
			case "pinned":
				re.NoError(tc.GetRuleManager().SetRule(newLearnerRule("4")))
				putLearner(tc, 1, 4)
			}
			manager := tc.GetAffinityManager()
			const id = "learner-kept"
			region := tc.GetRegion(1)
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			before := manager.GetAffinityGroupState(id).Group
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

			ops := c.CheckRegion(region)
			relearn := kind == "witness" || kind == "pinned"
			switch kind {
			case "tiflash":
				re.Len(ops, 1)
				verifyLearnersPreserved(re, ops[0], []uint64{5})
				operator.ApplyOperator(tc, ops[0])
				// A target that names the TiFlash store fails the placement check, so
				// the group expires and first-win relearns the voters of the Region.
				_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 5})
				re.NoError(err)
				re.Empty(c.CheckRegion(tc.GetRegion(1)))
				re.NotNil(tc.GetRegion(1).GetStoreLearner(5))
			case "peer-move", "orphan":
				// The move adds a learner on store 4 first, which is a target voter store.
				re.Len(ops, 1)
				re.True(oc.AddOperator(ops[0]))
				region = operator.ApplyOperatorStep(region, ops[0])
				tc.PutRegion(region)
				re.NotNil(region.GetStoreLearner(4))
				if kind == "peer-move" {
					c.tryAddOperators(region)
					re.Same(ops[0], oc.GetOperator(region.GetID()))
					re.Empty(c.affinityChecker.Check(region))
					// The same move with joint consensus passes through a joint state.
					joint := region.Clone(core.WithRole(region.GetStoreLearner(4).GetId(), metapb.PeerRole_IncomingVoter),
						core.WithRole(region.GetStoreVoter(3).GetId(), metapb.PeerRole_DemotingVoter))
					re.Empty(c.affinityChecker.Check(joint))
					break
				}
				// The learner is left behind and the rule checker cannot remove it yet.
				re.True(oc.RemoveOperator(ops[0]))
				cfg := tc.GetScheduleConfig().Clone()
				cfg.ReplicaScheduleLimit = 0
				tc.SetScheduleConfig(cfg)
				re.Empty(c.CheckRegion(region))
			default:
				re.Empty(ops)
				re.NotNil(tc.GetRegion(1).GetStoreLearner(4))
			}
			state := manager.GetAffinityGroupState(id)
			re.True(state.AffinitySchedulingAllowed)
			re.False(state.RegularSchedulingAllowed)
			if relearn {
				re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
				re.NotEqual(before, state.Group)
			} else {
				re.Equal(before, state.Group)
			}
			re.Equal(moves, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			wantConflicts := conflicts
			if relearn {
				wantConflicts++
			}
			re.Equal(wantConflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}
