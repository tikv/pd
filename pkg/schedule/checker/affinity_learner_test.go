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

func putLearners(tc *mockcluster.Cluster, regionID uint64, storeIDs ...uint64) *core.RegionInfo {
	region := tc.GetRegion(regionID).Clone(core.SetApproximateSize(1))
	for _, storeID := range storeIDs {
		region = region.Clone(core.WithAddPeer(&metapb.Peer{
			Id: 100*uint64(len(region.GetLearners())+1) + regionID, StoreId: storeID, Role: metapb.PeerRole_Learner,
		}))
	}
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
			region := putLearners(tc, 1, 4)
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

func TestAffinityCheckerConflictingLearnerTargetPlacement(t *testing.T) {
	for _, tt := range []struct {
		name    string
		leader  uint64
		voters  []uint64
		relearn bool
	}{
		{"valid", 1, []uint64{1, 2, 3}, false},
		{"invalid-isolation", 1, []uint64{1, 5, 3}, true},
		{"invalid-count", 1, []uint64{1, 3}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			for id := uint64(1); id <= 8; id++ {
				tc.AddLabelsStore(id, 10, map[string]string{
					"zone": strconv.FormatUint((id-1)%4+1, 10), "host": strconv.FormatUint(id, 10),
				})
			}
			rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.LocationLabels, rule.IsolationLevel = []string{"zone", "host"}, "zone"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			rule = newLearnerRule()
			rule.Count, rule.LocationLabels, rule.IsolationLevel = 2, []string{"zone", "host"}, "zone"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			r1 := putLearners(tc, 1, 4, 6)
			tc.AddLeaderRegionWithRange(2, "b", "c", 5, 2, 4)
			r2 := putLearners(tc, 2, 3, 6)
			checker := c.affinityChecker
			re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(r1, true))
			re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(r2, true))
			manager := tc.GetAffinityManager()
			const id = "learner-target-placement"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: tt.leader, VoterStoreIDs: tt.voters,
			}, []byte("a"), []byte("c")))
			if tt.name == "valid" || tt.relearn {
				target := cloneRegionWithReplacePeerStores(r2, tt.leader, tt.voters...)
				re.False(checker.isRegionPlacementRuleSatisfiedWithBestLocation(target, false))
				op, relearn := checker.createAffinityOperator(r2, manager.GetAffinityGroupState(id), tc.GetRuleManager().FitRegion(tc, r2))
				re.Equal(tt.relearn, relearn)
				re.Equal(tt.relearn, op == nil)
			}
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			ops := checker.Check(r2)
			if tt.name != "valid" {
				re.Empty(ops)
				state := manager.GetAffinityGroupState(id)
				re.True(state.AffinitySchedulingAllowed)
				re.Equal(uint64(5), state.LeaderStoreID)
				re.Equal([]uint64{2, 4, 5}, state.VoterStoreIDs)
				if tt.relearn {
					re.Equal(conflicts+1, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
				}
				return
			}
			re.Len(ops, 1)
			moved := applyLearnerOperator(t, tc, ops[0])
			re.NotNil(moved.GetStoreVoter(3))
			re.NotNil(moved.GetStoreLearner(4))
			re.NotNil(moved.GetStoreLearner(6))
			re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(moved, false))
			for _, region := range []*core.RegionInfo{r1, moved} {
				re.Empty(checker.Check(region))
				state := manager.GetAffinityGroupState(id)
				re.True(state.AffinitySchedulingAllowed)
				re.Equal(tt.leader, state.LeaderStoreID)
				re.Equal(tt.voters, state.VoterStoreIDs)
			}
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

func TestAffinityCheckerUnpinnedWitness(t *testing.T) {
	for _, tt := range []struct {
		name    string
		leader  uint64
		voter   uint64
		witness uint64
	}{
		{"removed-witness", 1, 2, 4},
		{"witness-on-target-leader", 2, 3, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableWitness(true)
			rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.Count = 2
			re.NoError(tc.GetRuleManager().SetRule(rule))
			re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
				GroupID: placement.DefaultGroupID, ID: "witness", Role: placement.Voter, Count: 1, IsWitness: true,
			}))
			tc.AddLeaderRegionWithRange(1, "a", "b", tt.leader, 3-tt.leader, 3)
			r1 := tc.GetRegion(1)
			r1 = r1.Clone(core.WithWitness(r1.GetStorePeer(3).GetId()))
			tc.PutRegion(r1)
			tc.AddLeaderRegionWithRange(2, "b", "c", 1, tt.voter, tt.witness)
			r2 := tc.GetRegion(2)
			r2 = r2.Clone(core.WithWitness(r2.GetStorePeer(tt.witness).GetId()))
			tc.PutRegion(r2)
			checker := c.affinityChecker
			manager := tc.GetAffinityManager()
			const id = "unpinned-witness"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: tt.leader, VoterStoreIDs: []uint64{1, 2, 3},
			}, []byte("a"), []byte("c")))
			failed := promtestutil.ToFloat64(affinityCheckerCreateOpFailedCounter)
			for range 5 {
				for _, region := range []*core.RegionInfo{r2, r1} {
					re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
					re.Empty(checker.Check(region))
					state := manager.GetAffinityGroupState(id)
					re.True(state.AffinitySchedulingAllowed)
					re.Equal(tt.leader, state.LeaderStoreID)
					re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
				}
			}
			re.Equal(failed, promtestutil.ToFloat64(affinityCheckerCreateOpFailedCounter))

			// A changed voter count still invalidates the old target.
			rule.Count = 3
			re.NoError(tc.GetRuleManager().SetRule(rule))
			r2 = r2.Clone(core.WithAddPeer(&metapb.Peer{Id: 200, StoreId: 5}))
			tc.PutRegion(r2)
			re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(r2, true))
			re.Empty(checker.Check(r2))
			state := manager.GetAffinityGroupState(id)
			re.True(state.AffinitySchedulingAllowed)
			re.Equal(uint64(1), state.LeaderStoreID)
			storeIDsEq(re, slices.Clone(state.VoterStoreIDs), r2.GetVoters())
		})
	}
}

func TestAffinityCheckerWitnessRuleChange(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.SetEnableWitness(true)
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	re.NoError(tc.GetRuleManager().SetRule(rule))
	re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "witness", Role: placement.Voter, Count: 1, IsWitness: true,
	}))
	region := tc.GetRegion(1)
	region = region.Clone(core.WithWitness(region.GetStorePeer(3).GetId()))
	tc.PutRegion(region)
	manager := tc.GetAffinityManager()
	const id = "witness-rule-change"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
		ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3},
	}, region.GetStartKey(), region.GetEndKey()))
	re.Empty(c.affinityChecker.Check(region))
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "ID", Op: placement.In, Values: []string{"1", "4"}}}
	re.NoError(tc.GetRuleManager().SetRule(rule))
	region = region.Clone(core.WithRemoveStorePeer(2), core.WithAddPeer(&metapb.Peer{Id: 200, StoreId: 4}))
	tc.PutRegion(region)
	re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
	re.Empty(c.affinityChecker.Check(region))
	state := manager.GetAffinityGroupState(id)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.Equal([]uint64{1, 3, 4}, state.VoterStoreIDs)
}

func TestAffinityCheckerLearnerSearchBudget(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			re := require.New(t)
			tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
			for id := 1; id <= 2*n; id++ {
				zone := "old"
				if id > n {
					zone = strconv.Itoa(id)
				}
				tc.AddLabelsStore(uint64(id), 10, map[string]string{"zone": zone})
			}
			rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.Count = n
			re.NoError(tc.GetRuleManager().SetRule(rule))
			rule = newLearnerRule()
			rule.Count, rule.LocationLabels, rule.IsolationLevel = n, []string{"zone"}, "zone"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			peers := make([]*metapb.Peer, 0, 2*n)
			roles := make(map[uint64]placement.PeerRoleType, n)
			for id := 1; id <= 2*n; id++ {
				peer := &metapb.Peer{Id: uint64(100 + id), StoreId: uint64(id)}
				if id > n {
					peer.Role = metapb.PeerRole_Learner
					roles[uint64(id)] = placement.Voter
				}
				peers = append(peers, peer)
			}
			leader := uint64(n + 1)
			roles[leader] = placement.Leader
			region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
			tc.PutRegion(region)
			checker := newTestAffinityChecker(t.Context(), tc, tc.GetCheckerConfig())
			fit := tc.GetRuleManager().FitRegionWithoutCache(tc, region)
			re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
			visits := 0
			remaining := 32
			start := time.Now()
			target, temporary := checker.placeLearners(region, fit, roles, map[*placement.Rule][]*core.StoreInfo{}, region.GetLearners(), &remaining,
				func(roles map[uint64]placement.PeerRoleType) affinityPlacementStatus {
					visits++
					layout := regionWithRoles(region, leader, roles)
					return checker.checkRegionFitAtBestLocation(layout, tc.GetRuleManager().FitRegionWithoutCache(tc, layout))
				})
			re.False(temporary)
			if n == 1 {
				re.NotNil(target)
			} else {
				re.Nil(target)
			}
			want := 1
			for i := 2; i <= n; i++ {
				want *= i
			}
			t.Logf("learners=%d complete fits=%d elapsed=%s", n, visits, time.Since(start))
			re.Equal(min(want, 32), visits)
		})
	}
}

func TestAffinityCheckerLearnerPreservesWitness(t *testing.T) {
	for _, tt := range []struct {
		name        string
		stores      []string
		destination uint64
		joint       bool
	}{
		{"swap", []string{"3", "4"}, 3, false},
		{"swap-joint", []string{"3", "4"}, 3, true},
		{"move", []string{"4", "5"}, 5, false},
		{"move-joint", []string{"4", "5"}, 5, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(tt.joint)
			tc.SetEnableWitness(true)
			rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			rule.Count = 2
			re.NoError(tc.GetRuleManager().SetRule(rule))
			re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
				GroupID: placement.DefaultGroupID, ID: "pinned-witness", Role: placement.Voter, Count: 1, IsWitness: true,
				LabelConstraints: []placement.LabelConstraint{{Key: "ID", Op: placement.In, Values: []string{"2"}}},
			}))
			re.NoError(tc.GetRuleManager().SetRule(newLearnerRule(tt.stores...)))
			region := putLearners(tc, 1, 4)
			region = region.Clone(core.WithWitness(region.GetStorePeer(2).GetId()))
			tc.PutRegion(region)
			manager := tc.GetAffinityManager()
			const id = "learner-retained-witness"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			fit := tc.GetRuleManager().FitRegionWithoutCache(tc, region)
			re.True(fit.IsSatisfied())
			group := manager.GetAffinityGroupState(id)
			var op *operator.Operator
			// Repeated builds exercise the roles map's varying iteration order.
			for range 32 {
				var relearn bool
				op, relearn = c.affinityChecker.createAffinityOperator(region, group, fit)
				re.False(relearn)
				re.NotNil(op)
				for i := range op.Len() {
					switch step := op.Step(i).(type) {
					case operator.BecomeWitness, operator.BecomeNonWitness, operator.BatchSwitchWitness:
						t.Fatalf("unexpected witness change: %s", step)
					}
				}
			}
			moved := applyLearnerOperator(t, tc, op)
			re.Equal(region.GetStorePeer(2), moved.GetStorePeer(2))
			re.NotNil(moved.GetStoreLearner(tt.destination))
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
			re.Empty(c.CheckRegion(moved))
			state := manager.GetAffinityGroupState(id)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
			re.Equal(affinity.PhaseStable, state.Phase)
		})
	}
}

func TestAffinityCheckerPreservesWitness(t *testing.T) {
	for _, joint := range []bool{false, true} {
		t.Run("joint="+strconv.FormatBool(joint), func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				voters  []uint64
				learner bool
			}{
				{"no-learner", []uint64{4, 2, 3}, false},
				{"non-conflicting-learner", []uint64{4, 2, 3}, true},
				{"replace-two-voters", []uint64{4, 2, 5}, true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					re := require.New(t)
					c, tc, _ := newLearnerConflictController(t)
					tc.SetEnableUseJointConsensus(joint)
					tc.SetEnableWitness(true)
					rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
					rule.Count = 2
					re.NoError(tc.GetRuleManager().SetRule(rule))
					re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
						GroupID: placement.DefaultGroupID, ID: "pinned-witness", Role: placement.Voter, Count: 1, IsWitness: true,
						LabelConstraints: []placement.LabelConstraint{{Key: "ID", Op: placement.In, Values: []string{"2"}}},
					}))
					region := tc.GetRegion(1)
					if tt.learner {
						tc.AddRegionStore(10, 10)
						tc.SetStoreLabel(10, map[string]string{"ID": "10", "engine": "tiflash"})
						learnerRule := newLearnerRule("10")
						learnerRule.GroupID = "tiflash"
						learnerRule.LabelConstraints = append(learnerRule.LabelConstraints,
							placement.LabelConstraint{Key: "engine", Op: placement.In, Values: []string{"tiflash"}})
						re.NoError(tc.GetRuleManager().SetRule(learnerRule))
						region = putLearners(tc, 1, 10)
					}
					region = region.Clone(core.WithWitness(region.GetStorePeer(2).GetId()))
					tc.PutRegion(region)
					manager := tc.GetAffinityManager()
					const id = "retained-witness"
					re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
						ID: id, LeaderStoreID: 4, VoterStoreIDs: tt.voters,
					}, region.GetStartKey(), region.GetEndKey()))
					var op *operator.Operator
					for range 50 {
						ops := c.CheckRegion(region)
						re.Len(ops, 1)
						op = ops[0]
						state := manager.GetAffinityGroupState(id)
						re.True(state.AffinitySchedulingAllowed)
						re.Equal(uint64(4), state.LeaderStoreID)
						re.ElementsMatch(tt.voters, state.VoterStoreIDs)
					}
					moved := applyLearnerOperator(t, tc, op)
					re.Equal(region.GetStorePeer(2), moved.GetStorePeer(2))
					re.Equal(region.GetLearners(), moved.GetLearners())
					re.Equal(uint64(4), moved.GetLeader().GetStoreId())
					storeIDsEq(re, slices.Clone(tt.voters), moved.GetVoters())
					re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
					re.Empty(c.CheckRegion(moved))
					re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
				})
			}
		})
	}
}

func TestCloneRegionWithPeerStoresPreservesWitness(t *testing.T) {
	for _, tt := range []struct {
		name   string
		voters []uint64 // First voter is the target leader.
	}{
		{"retained-witness", []uint64{4, 2, 5}},
		{"removed-witness", []uint64{4, 5, 3}},
		{"removed-witness-retained-leader", []uint64{1, 4, 5}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			_, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			tc.SetEnablePlacementRules(false)
			tc.AddRegionStore(10, 10)
			region := tc.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{10})
			region = region.Clone(core.WithWitness(region.GetStorePeer(2).GetId()))
			roles := map[uint64]placement.PeerRoleType{10: placement.Learner}
			for _, storeID := range tt.voters {
				roles[storeID] = placement.Voter
			}
			roles[tt.voters[0]] = placement.Leader
			for range 50 {
				target := cloneRegionWithReplacePeerStores(region, tt.voters[0], tt.voters...)
				re.NotNil(target)
				re.Same(target.GetStorePeer(tt.voters[0]), target.GetLeader())
				re.False(target.GetLeader().GetIsWitness())
				re.Equal(region.GetLearners(), target.GetLearners())
				storeIDsEq(re, slices.Clone(tt.voters), target.GetVoters())
				for _, storeID := range tt.voters {
					peer := target.GetStoreVoter(storeID)
					re.Equal(metapb.PeerRole_Voter, peer.GetRole())
					if old := region.GetStoreVoter(storeID); old != nil {
						re.Equal(old, peer)
					} else {
						re.False(peer.GetIsWitness(), "new voter on store %d", storeID)
					}
				}
				op, err := operator.CreateMoveRegionOperator("test", tc, region, operator.OpAffinity, roles)
				re.NoError(err)
				tc.PutRegion(region)
				moved := applyLearnerOperator(t, tc, op)
				for _, peer := range target.GetPeers() {
					actual := moved.GetStorePeer(peer.GetStoreId())
					re.NotNil(actual)
					re.Equal(peer.GetRole(), actual.GetRole())
					re.Equal(peer.GetIsWitness(), actual.GetIsWitness())
				}
				re.True(region.GetStorePeer(2).GetIsWitness())
				re.Equal(uint64(1), region.GetLeader().GetStoreId())
			}
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
	region := putLearners(tc, 1, 4, 5)
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
	region := putLearners(tc, 1, 4, 5)
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
	for _, kind := range []string{"label-constraint", "location-label", "location-label-less-regions", "single-learner-isolation", "cross-rule-location", "rule-isolation"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			rule := newLearnerRule("4", "5")
			learnerStores, destination := []uint64{4}, uint64(5)
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
			case "cross-rule-location", "rule-isolation":
				tc.SetEnableUseJointConsensus(true)
				tc.AddRegionStore(6, 0)
				tc.AddRegionStore(7, 10)
				zones := []string{"a", "b", "c", "d", "e", "e", "f"}
				rule = newLearnerRule("4", "6", "7")
				learnerStores, destination = []uint64{4, 5}, 7
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
			}
			re.NoError(tc.GetRuleManager().SetRule(rule))
			region := putLearners(tc, 1, learnerStores...)
			manager := tc.GetAffinityManager()
			const id = "learner-move"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			if len(learnerStores) > 1 {
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, 1, 1, 2, 4), false))
			}
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

			tc.SetStoreBusy(destination, true)
			for range 3 {
				re.Empty(c.CheckRegion(region))
				state := manager.GetAffinityGroupState(id)
				re.True(state.AffinitySchedulingAllowed, "a busy store must not expire the group")
				re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
				re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
			}
			tc.SetStoreBusy(destination, false)
			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			applyLearnerOperator(t, tc, ops[0])
			checkAndApply(t, c, tc, 1)
			moved := tc.GetRegion(1)
			state := manager.GetAffinityGroupState(id)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs, "the group keeps its target")
			re.NotNil(moved.GetStoreLearner(destination))
			for _, storeID := range learnerStores[1:] {
				re.NotNil(moved.GetStoreLearner(storeID))
			}
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
			re.Equal(affinity.PhaseStable, state.Phase)
			re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

func TestAffinityCheckerLearnerBusyAndPinned(t *testing.T) {
	for _, kind := range []string{"pinned-last", "pinned-first", "temporary"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.SetEnableUseJointConsensus(true)
			tc.AddRegionStore(6, 10)
			tc.AddRegionStore(7, 10)
			re.NoError(tc.GetRuleManager().SetRule(newLearnerRule("4", "6")))
			rule := newLearnerRule("5")
			if kind == "temporary" {
				rule = newLearnerRule("5", "7")
			}
			rule.ID = "learner-2"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			learnerStores := []uint64{4, 5}
			if kind == "pinned-first" {
				slices.Reverse(learnerStores)
			}
			region := tc.GetRegion(1)
			for i, storeID := range learnerStores {
				region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: uint64(101 + i), StoreId: storeID, Role: metapb.PeerRole_Learner}))
			}
			tc.PutRegion(region)
			re.Equal(learnerStores[0], region.GetLearners()[0].GetStoreId())
			manager := tc.GetAffinityManager()
			const id = "learner-busy-pinned"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 4, 5},
			}, region.GetStartKey(), region.GetEndKey()))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, 1, 1, 4, 5), false))
			tc.SetStoreBusy(6, true)
			tc.SetStoreBusy(7, true)
			wantTarget := []uint64{1, 2, 3}
			if kind == "temporary" {
				wantTarget = []uint64{1, 4, 5}
			}
			for range 3 {
				re.Empty(c.CheckRegion(region))
				state := manager.GetAffinityGroupState(id)
				re.True(state.AffinitySchedulingAllowed)
				re.Equal(wantTarget, state.VoterStoreIDs)
			}
			tc.SetStoreBusy(6, false)
			tc.SetStoreBusy(7, false)
			checkAndApply(t, c, tc, 1, 1)
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, tc.GetRegion(1)).IsSatisfied())
			state := manager.GetAffinityGroupState(id)
			re.Equal(wantTarget, state.VoterStoreIDs)
			re.Equal(affinity.PhaseStable, state.Phase)
		})
	}
}

// A hard isolation violation is a definite rejection even when its repair
// is temporarily blocked, in either rule order.
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
	region := putLearners(tc, 1, 4, 5)
	tc.SetStoreBusy(6, true)
	fit := tc.GetRuleManager().FitRegionWithoutCache(tc, region)
	re.True(fit.IsSatisfied())
	re.Equal(affinityPlacementRejected, c.affinityChecker.checkRegionPlacement(region, fit, false).status)
	re.Equal(affinityPlacementRejected, c.affinityChecker.regionPlacementFeedback(region, fit).status)
	tc.SetStoreBusy(6, false)
	re.Equal(affinityPlacementRejected, c.affinityChecker.checkRegionPlacement(region, fit, false).status, "the improvement is now available")
	re.NotEmpty(c.affinityChecker.regionPlacementFeedback(region, fit).replacements)
	re.Empty(c.affinityChecker.checkRegionPlacement(region, fit, false).replacements)
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
		re.Equal(affinityPlacementRejected, c.affinityChecker.checkRegionPlacement(region, fit, false).status)
		re.Equal(affinityPlacementRejected, c.affinityChecker.regionPlacementFeedback(region, fit).status)
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
			zones := []string{"v1", "v2", "v3", "a", "b", "c", "b"}
			switch kind {
			case "rule-refit":
				// When Region B moves its voter off store 4, freeing the
				// store must not open a better zone for its learner on 7.
				zones[3] = zones[5]
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
			for i, zone := range zones {
				tc.SetStoreLabel(uint64(i+1), map[string]string{"ID": strconv.Itoa(i + 1), "zone": zone})
			}
			re.NoError(tc.GetRuleManager().SetRule(rule))
			rule = newLearnerRule("5", "6")
			rule.ID = "learner-b"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			learnerStores := []uint64{4, 5}
			if kind == "rule-refit" {
				learnerStores = append(learnerStores, 6)
			}
			a := putLearners(tc, 1, learnerStores...)
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
				targetB := cloneRegionWithReplacePeerStores(b, 1, wantTarget...)
				re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(targetB, false), "Region B can keep its learners on the relearned target")
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
	putLearners(tc, 1, 4)
	putLearners(tc, 2, 3)
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
				putLearners(tc, 1, 5)
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
				putLearners(tc, 1, 4)
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
				// The TiFlash learner cannot move off its store, so learner placement
				// rejects the conflicting target and relearns the Region's voters.
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
			if relearn || kind == "tiflash" {
				wantConflicts++
			}
			re.Equal(wantConflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}
