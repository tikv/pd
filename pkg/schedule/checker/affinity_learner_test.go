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

func checkAndApply(c *Controller, tc *mockcluster.Cluster, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		for _, op := range c.CheckRegion(tc.GetRegion(regionID)) {
			operator.ApplyOperator(tc, op)
		}
	}
}

// A learner on a target voter store swaps roles with the voter that the target
// drops, so no data is copied and the group keeps its target.
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

			ops := c.CheckRegion(region)
			re.Len(ops, 1)
			re.Zero(ops[0].Kind()&operator.OpRegion, "a swap adds or removes no peer")
			swapped := region.Clone(core.WithRole(region.GetStoreLearner(4).GetId(), metapb.PeerRole_Voter),
				core.WithRole(region.GetStoreVoter(3).GetId(), metapb.PeerRole_Learner))
			swapped = swapped.Clone(core.WithLeader(swapped.GetStorePeer(target.leader)))
			re.True(ops[0].Start())
			re.Nil(ops[0].Check(swapped), "the operator ends with the peers on stores 3 and 4 swapped")
			tc.PutRegion(swapped)
			re.Empty(c.CheckRegion(swapped))
			state := manager.GetAffinityGroupState(id)
			re.Equal(affinity.PhaseStable, state.Phase)
			re.Equal([]uint64{1, 2, 4}, state.VoterStoreIDs)
			re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

// If the rule of the learner does not allow the voter store that the target
// drops, or with location labels another store is a better location, the
// learner moves to that store. While that store is busy, the move waits
// instead of expiring the group.
func TestAffinityCheckerLearnerMoveToRuleStore(t *testing.T) {
	for _, kind := range []string{"label-constraint", "location-label"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			c, tc, _ := newLearnerConflictController(t)
			rule := newLearnerRule("4", "5")
			if kind == "location-label" {
				// Store 5 is the only store in a zone that no target voter uses. While
				// it is busy, the swap to store 3 and the move to store 6 fail the
				// layout check only because store 5 is a better location. Store 6 has
				// more Regions, so the learner moves to store 5 once it is not busy.
				tc.AddRegionStore(6, 100)
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
			re.Empty(c.CheckRegion(region))
			re.True(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed, "a busy store must not expire the group")
			tc.SetStoreBusy(5, false)
			checkAndApply(c, tc, 1, 1)
			moved := tc.GetRegion(1)
			re.NotNil(moved.GetStoreLearner(5))
			re.True(tc.GetRuleManager().FitRegionWithoutCache(tc, moved).IsSatisfied())
			re.Equal(affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
			re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
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
			checkAndApply(c, tc, regionID)
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
// pins to its store, cannot move, so the group expires as before.
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
			expire := kind == "witness" || kind == "pinned"
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
			re.Equal(expire, !state.AffinitySchedulingAllowed)
			re.Equal(expire, state.RegularSchedulingAllowed)
			re.Equal(before, state.Group)
			re.Equal(moves, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			wantConflicts := conflicts
			if expire {
				wantConflicts++
			}
			re.Equal(wantConflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}
