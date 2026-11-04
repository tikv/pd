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
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestAffinityCheckerLearnerConflictRecovery(t *testing.T) {
	for _, target := range []struct {
		name   string
		leader uint64
	}{{"voter", 1}, {"leader", 4}} {
		t.Run(target.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
				GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
			}))
			learner := &metapb.Peer{Id: 100, StoreId: 4, Role: metapb.PeerRole_Learner}
			region := tc.GetRegion(1).Clone(core.WithAddPeer(learner))
			tc.PutRegion(region)
			manager := tc.GetAffinityManager()
			const id = "learner-conflict"
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3},
			}, region.GetStartKey(), region.GetEndKey()))
			_, err := manager.UpdateAffinityGroupPeers(id, target.leader, []uint64{1, 2, 4})
			re.NoError(err)
			before := manager.GetAffinityGroupState(id)
			re.Equal(affinity.PhasePreparing, before.Phase)
			virtual := cloneRegionWithReplacePeerStores(region, target.leader, before.VoterStoreIDs...)
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
			re.True(c.affinityChecker.isRegionPlacementRuleSatisfiedWithBestLocation(virtual, false))
			conflicts := testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)

			re.Empty(c.CheckRegion(region))
			expired := manager.GetAffinityGroupState(id)
			t.Logf("after conflict: phase=%s affinity=%t regular=%t", expired.Phase,
				expired.AffinitySchedulingAllowed, expired.RegularSchedulingAllowed)
			re.False(expired.AffinitySchedulingAllowed, "conflicting target must expire")
			re.True(expired.RegularSchedulingAllowed)
			re.Equal(1.0, testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)-conflicts)

			// A later check relearns the healthy voters without promoting the learner.
			re.Empty(c.CheckRegion(region))
			recovered := manager.GetAffinityGroupState(id)
			re.Equal(affinity.PhaseStable, recovered.Phase)
			re.True(recovered.AffinitySchedulingAllowed)
			re.False(recovered.RegularSchedulingAllowed)
			re.Equal(uint64(1), recovered.LeaderStoreID)
			re.Equal([]uint64{1, 2, 3}, recovered.VoterStoreIDs)
			re.Equal(learner, tc.GetRegion(1).GetStoreLearner(4))
			raw, err := tc.GetStorage().LoadAffinityGroup(id)
			re.NoError(err)
			var stored affinity.Group
			re.NoError(json.Unmarshal([]byte(raw), &stored))
			re.Equal(recovered.Group, stored)
			for range 3 {
				re.Empty(c.CheckRegion(region))
				re.Equal(recovered.Group, manager.GetAffinityGroupState(id).Group)
			}
			re.Equal(1.0, testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)-conflicts)
		})
	}
}

func TestAffinityCheckerLearnerDuringPeerMove(t *testing.T) {
	for _, state := range []string{"running", "orphan", "rule-limited"} {
		t.Run(state, func(t *testing.T) {
			re := require.New(t)
			c, tc, oc := newLearnerConflictController(t)
			manager := tc.GetAffinityManager()
			const id = "peer-move"
			region := tc.GetRegion(1)
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
			}, region.GetStartKey(), region.GetEndKey()))
			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			re.True(oc.AddOperator(ops[0]))
			region = operator.ApplyOperatorStep(region, ops[0])
			tc.PutRegion(region)
			re.NotNil(region.GetStoreLearner(4))
			before := manager.GetAffinityGroupState(id)
			conflicts := testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			if state == "running" {
				c.tryAddOperators(region)
				re.Same(ops[0], oc.GetOperator(region.GetID()))
				// Direct checks are also guarded by the current Region's rule fit.
				re.Empty(c.affinityChecker.Check(region))
			} else {
				re.True(oc.RemoveOperator(ops[0]))
				if state == "rule-limited" {
					cfg := tc.GetScheduleConfig().Clone()
					cfg.ReplicaScheduleLimit = 0
					tc.SetScheduleConfig(cfg)
					re.Empty(c.CheckRegion(region))
				} else {
					fixes := c.CheckRegion(region)
					re.Len(fixes, 1)
					re.Equal("remove-orphan-peer", fixes[0].Desc())
					operator.ApplyOperator(tc, fixes[0])
					re.Nil(tc.GetRegion(1).GetStoreLearner(4))
				}
			}
			after := manager.GetAffinityGroupState(id)
			re.True(after.AffinitySchedulingAllowed)
			re.False(after.RegularSchedulingAllowed)
			re.Equal(before.Group, after.Group)
			re.Equal(conflicts, testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

func TestAffinityCheckerTiFlashLearnerDoesNotExpire(t *testing.T) {
	re := require.New(t)
	c, tc, _ := newLearnerConflictController(t)
	tc.SetStoreLabel(5, map[string]string{"engine": "tiflash"})
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.NotIn, Values: []string{"tiflash"}}}
	re.NoError(tc.GetRuleManager().SetRule(rule))
	re.NoError(tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: "tiflash", ID: "learner", Role: placement.Learner, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}},
	}))
	learner := &metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}
	region := tc.GetRegion(1).Clone(core.WithAddPeer(learner))
	tc.PutRegion(region)
	manager := tc.GetAffinityManager()
	const id = "tiflash-learner"
	re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
		ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4},
	}, region.GetStartKey(), region.GetEndKey()))
	target := manager.GetAffinityGroupState(id).Group
	conflicts := testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
	ops := c.CheckRegion(region)
	re.Len(ops, 1)
	verifyLearnersPreserved(re, ops[0], []uint64{5})
	operator.ApplyOperator(tc, ops[0])
	moved := tc.GetRegion(1)
	re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
	re.Equal(learner, moved.GetStoreLearner(5))
	re.Empty(c.CheckRegion(moved))
	group := manager.GetAffinityGroupState(id)
	re.Equal(target, group.Group)
	re.Equal(affinity.PhaseStable, group.Phase)
	re.False(group.RegularSchedulingAllowed)
	re.Equal(conflicts, testutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
}
