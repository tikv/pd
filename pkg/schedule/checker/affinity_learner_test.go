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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newLearnerConflictController(t *testing.T) (*Controller, *mockcluster.Cluster, *operator.Controller) {
	t.Helper()
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	tc.SetEnableUseJointConsensus(true)
	tc.SetMaxMergeRegionSize(0)
	tc.SetMaxAffinityMergeRegionSize(0)
	for id := uint64(1); id <= 5; id++ {
		tc.AddRegionStore(id, 10)
	}
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	tc.PutRegion(region.Clone(withAffinityTestRange("a", "b"), core.SetApproximateSize(1)))
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

func putLearners(tc *mockcluster.Cluster, storeIDs ...uint64) *core.RegionInfo {
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(1))
	for _, storeID := range storeIDs {
		region = region.Clone(core.WithAddPeer(&metapb.Peer{
			Id: 100*uint64(len(region.GetLearners())+1) + region.GetID(), StoreId: storeID, Role: metapb.PeerRole_Learner,
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

func TestAffinityCheckerLearnerSwap(t *testing.T) {
	for _, leader := range []uint64{1, 4} {
		t.Run(strconv.FormatUint(leader, 10), func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			re.NoError(tc.GetRuleManager().SetRule(newLearnerRule()))
			region := putLearners(tc, 4)
			manager := tc.GetAffinityManager()
			const id = "learner-swap"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: leader, VoterStoreIDs: []uint64{1, 2, 4},
			}, affinityTestRange("a", "b")))

			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			re.Zero(ops[0].Kind()&operator.OpRegion, "a swap adds or removes no peer")
			swapped := applyLearnerOperator(t, tc, ops[0])
			re.Equal(region.GetStoreVoter(3).GetId(), swapped.GetStoreLearner(3).GetId())
			re.Equal(region.GetStoreLearner(4).GetId(), swapped.GetStoreVoter(4).GetId())
			re.Equal(leader, swapped.GetLeader().GetStoreId())
			re.Empty(c.CheckRegion(swapped))
			re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
		})
	}
}

func TestAffinityCheckerLearnerSwapIsolation(t *testing.T) {
	for _, test := range []struct {
		name      string
		isolation string
		voters    []uint64
		learner   uint64
		zone6     string
	}{
		{name: "soft zone swap", voters: []uint64{2, 4, 6}, learner: 3, zone6: "z3"},
		{name: "hard zone swap", isolation: "zone", voters: []uint64{2, 4, 6}, learner: 3, zone6: "z3"},
		{name: "soft zone add", voters: []uint64{2, 3, 4}, learner: 6, zone6: "z5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.AddRegionStore(6, 10)
			for i, zone := range []string{"z1", "z2", "z3", "z4", "z1", test.zone6} {
				tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": zone})
			}
			rule := newLearnerRule()
			rule.Count, rule.LocationLabels, rule.IsolationLevel = 2, []string{"zone"}, test.isolation
			re.NoError(tc.GetRuleManager().SetRule(rule))
			region := putLearners(tc, 4, 5)
			manager := tc.GetAffinityManager()
			const id = "learner-swap-isolation"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 2, VoterStoreIDs: test.voters,
			}, affinityTestRange("a", "b")))

			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			moved := applyLearnerOperator(t, tc, ops[0])
			re.Nil(moved.GetStorePeer(1), "store 1 shares a zone with the retained learner")
			re.NotNil(moved.GetStoreLearner(test.learner))
			if old := region.GetStoreVoter(test.learner); old != nil {
				re.Equal(old.GetId(), moved.GetStoreLearner(test.learner).GetId())
			}
			re.Equal(region.GetStoreLearner(5), moved.GetStoreLearner(5))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(moved, false))
		})
	}
}

func TestAffinityCheckerLearnerOverlappingConstraints(t *testing.T) {
	for _, test := range []struct {
		name string
		a, b uint64
		add  bool
	}{
		{name: "swap", a: 2, b: 3},
		{name: "add", a: 8, b: 7, add: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.AddRegionStore(6, 10)
			if test.add {
				// Prefer store 7 for the first learner by giving it a lower score.
				tc.AddRegionStore(7, 10)
				tc.AddRegionStore(8, 20)
			}
			tc.SetStoreLabel(6, map[string]string{"a": "yes"})
			tc.SetStoreLabel(4, map[string]string{"a": "yes", "b": "yes"})
			tc.SetStoreLabel(test.a, map[string]string{"a": "yes"})
			tc.SetStoreLabel(test.b, map[string]string{"a": "yes", "b": "yes"})
			for _, label := range []string{"a", "b"} {
				rule := newLearnerRule()
				rule.ID = "learner-" + label
				rule.LabelConstraints = []placement.LabelConstraint{{Key: label, Op: placement.In, Values: []string{"yes"}}}
				re.NoError(tc.GetRuleManager().SetRule(rule))
			}
			regionA := tc.MockRegionInfo(1, 1, []uint64{4, 6}, []uint64{test.a, test.b}, nil).Clone(withAffinityTestRange("a", "b"), core.SetApproximateSize(1))
			// The first candidate (store 3 or 7) matches both rules and is
			// the only destination for the second learner, which requires b=yes.
			regionB := tc.MockRegionInfo(2, 1, []uint64{3, 2}, []uint64{6, 4}, nil).Clone(withAffinityTestRange("b", "c"), core.SetApproximateSize(1))
			tc.PutRegion(regionA)
			tc.PutRegion(regionB)
			manager := tc.GetAffinityManager()
			const id = "learner-overlap"
			target := []uint64{1, 4, 6}
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: target,
			}, affinityTestRange("a", "c")))

			re.Empty(c.CheckRegion(regionA))
			ops := c.CheckRegion(regionB)
			re.Len(ops, 1)
			re.Equal(target, manager.GetAffinityGroupState(id).VoterStoreIDs)
			moved := applyLearnerOperator(t, tc, ops[0])
			re.NotNil(moved.GetStoreLearner(test.a))
			re.NotNil(moved.GetStoreLearner(test.b))
			if test.add {
				re.NotZero(ops[0].Kind() & operator.OpRegion)
			} else {
				re.Zero(ops[0].Kind()&operator.OpRegion, "both conflicts can reuse existing peers")
				re.Equal(regionB.GetStoreVoter(test.a).GetId(), moved.GetStoreLearner(test.a).GetId())
				re.Equal(regionB.GetStoreVoter(test.b).GetId(), moved.GetStoreLearner(test.b).GetId())
			}
			re.Empty(c.CheckRegion(moved))
			state := manager.GetAffinityGroupState(id)
			re.Equal(target, state.VoterStoreIDs)
			re.Equal(affinity.PhaseStable, state.Phase)
		})
	}
}

func TestAffinityCheckerLearnerSwapAndAdd(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	for id := uint64(6); id <= 8; id++ {
		tc.AddRegionStore(id, 10)
	}
	for i, zone := range []string{"a", "b", "c", "e", "f", "a", "d", "a"} {
		tc.SetStoreLabel(uint64(i+1), map[string]string{"ID": strconv.Itoa(i + 1), "zone": zone})
	}
	rule := newLearnerRule("2", "4", "5", "6", "7", "8")
	rule.Count, rule.LocationLabels, rule.IsolationLevel = 3, []string{"zone"}, "zone"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	region := putLearners(tc, 4, 5, 8)
	manager := tc.GetAffinityManager()
	const id = "learner-swap-and-add"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
		ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 4, 5},
	}, affinityTestRange("a", "b")))

	ops := c.CheckRegion(region)
	re.Len(ops, 1)
	moved := applyLearnerOperator(t, tc, ops[0])
	re.Equal(region.GetStoreVoter(2).GetId(), moved.GetStoreLearner(2).GetId())
	re.Nil(moved.GetStorePeer(6), "store 6 shares a zone with the retained learner")
	re.NotNil(moved.GetStoreLearner(7))
	re.Equal(region.GetStoreLearner(8), moved.GetStoreLearner(8))
	re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(moved, false))
}

func TestAffinityCheckerLearnerBlockedOrRejected(t *testing.T) {
	for _, kind := range []string{"busy", "store limit", "busy before permanent rejection"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			tc.AddRegionStore(6, 10)
			tc.AddRegionStore(7, 10)
			re.NoError(tc.GetRuleManager().SetRule(newLearnerRule("4", "6")))
			rule := newLearnerRule("5", "7")
			permanent := kind == "busy before permanent rejection"
			if permanent {
				rule = newLearnerRule("5")
			}
			rule.ID = "learner-second"
			re.NoError(tc.GetRuleManager().SetRule(rule))
			region := putLearners(tc, 4, 5)
			re.Equal(uint64(4), region.GetLearners()[0].GetStoreId())
			manager := tc.GetAffinityManager()
			const id = "learner-blocked"
			target := []uint64{1, 4, 5}
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: target,
			}, affinityTestRange("a", "b")))

			if kind == "store limit" {
				tc.SetStoreLimit(6, storelimit.AddPeer, 0.00006)
				tc.ResetStoreLimit(6, storelimit.AddPeer, 0.000001)
				re.True(tc.GetStore(6).GetStoreLimit().Take(storelimit.RegionInfluence[storelimit.AddPeer], storelimit.AddPeer, constant.Medium))
			} else {
				tc.SetStoreBusy(6, true)
			}
			re.Empty(c.CheckRegion(region))
			if permanent {
				re.Equal([]uint64{1, 2, 3}, manager.GetAffinityGroupState(id).VoterStoreIDs)
				return
			}
			re.Equal(target, manager.GetAffinityGroupState(id).VoterStoreIDs)
			tc.SetStoreBusy(6, false)
			tc.ResetStoreLimit(6, storelimit.AddPeer, 60)
			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			moved := applyLearnerOperator(t, tc, ops[0])
			re.NotNil(moved.GetStoreLearner(6))
			re.NotNil(moved.GetStoreLearner(7))
			re.Empty(c.CheckRegion(moved))
			re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
		})
	}
}

func TestAffinityCheckerLearnerSwapKeepsTiFlash(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.SetStoreLabel(5, map[string]string{"engine": "tiflash"})
	voter := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rowStore := placement.LabelConstraint{Key: "engine", Op: placement.NotIn, Values: []string{"tiflash"}}
	voter.LabelConstraints = []placement.LabelConstraint{rowStore}
	rowLearner := newLearnerRule()
	rowLearner.LabelConstraints = []placement.LabelConstraint{rowStore}
	tiflash := newLearnerRule()
	tiflash.GroupID = "tiflash"
	tiflash.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}
	for _, rule := range []*placement.Rule{voter, rowLearner, tiflash} {
		re.NoError(tc.GetRuleManager().SetRule(rule))
	}
	region := putLearners(tc, 4, 5)
	re.NoError(createAffinityGroupForTest(tc.GetAffinityManager(), &affinity.Group{
		ID: "keep-tiflash", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
	}, affinityTestRange("a", "b")))

	ops := c.CheckRegion(region)
	re.Len(ops, 1)
	moved := applyLearnerOperator(t, tc, ops[0])
	re.Equal(region.GetStoreVoter(3).GetId(), moved.GetStoreLearner(3).GetId())
	re.Equal(region.GetStoreLearner(5), moved.GetStoreLearner(5))
	re.Empty(c.CheckRegion(moved))
}
