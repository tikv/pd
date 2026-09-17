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

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func TestCheckRegionMigratesLegacyWitnessPeer(t *testing.T) {
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()
	tc.SetEnablePlacementRules(false)

	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 900, Peers: peers}, peers[0])
	tc.PutRegion(region)

	ops := controller.CheckRegion(region)
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, "migrate-deprecated-witness-peer", op.Desc())
	require.NotZero(t, op.Kind()&operator.OpReplica)
	require.Equal(t, constant.High, op.GetPriorityLevel())

	for i := range op.Len() {
		step, ok := op.Step(i).(operator.BecomeNonWitness)
		if !ok {
			continue
		}
		cmd := step.GetCmd(region, true)
		require.Len(t, cmd.SwitchWitnesses.GetSwitchWitnesses(), 1)
		require.False(t, cmd.SwitchWitnesses.GetSwitchWitnesses()[0].GetIsWitness())
		return
	}
	require.Fail(t, "migration operator has no conversion step")
}

func TestCheckRegionRepairsDownLegacyWitnessPeer(t *testing.T) {
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()
	tc.SetEnablePlacementRules(false)

	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 901, Peers: peers}, peers[0], core.WithDownPeers([]*pdpb.PeerStats{{
		Peer:        peers[1],
		DownSeconds: 24 * 60 * 60,
	}}))
	tc.PutRegion(region)
	tc.SetStoreDown(2)

	ops := controller.CheckRegion(region)
	require.Len(t, ops, 1)
	require.Equal(t, "replace-down-replica", ops[0].Desc())
}

func TestCheckRegionRepairsRegularPeerBeforeWitnessMigration(t *testing.T) {
	for _, placementRules := range []bool{false, true} {
		for _, condition := range []string{"down", "pending", "missing", "disconnected"} {
			t.Run(strconv.FormatBool(placementRules)+"/"+condition, func(t *testing.T) {
				c, tc, _, cleanup := newTestSplitScatterController(t)
				defer cleanup()
				tc.SetEnablePlacementRules(placementRules)
				peers := []*metapb.Peer{
					{Id: 1, StoreId: 1},
					{Id: 2, StoreId: 2, IsWitness: true},
					{Id: 3, StoreId: 3},
				}
				region := core.NewRegionInfo(&metapb.Region{Id: 902, Peers: peers}, peers[0])
				switch condition {
				case "down":
					region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers[2], DownSeconds: 86400}}))
					tc.SetStoreDown(3)
				case "pending":
					region = region.Clone(core.WithPendingPeers([]*metapb.Peer{peers[2]}))
				case "missing":
					region = region.Clone(core.SetPeers(peers[:2]))
				case "disconnected":
					tc.SetStoreDisconnect(3)
				}
				tc.PutRegion(region)
				ops := c.CheckRegion(region)
				if condition == "down" || condition == "missing" {
					require.Len(t, ops, 1)
					expected := "replace-down-replica"
					if condition == "missing" {
						expected = "make-up-replica"
					}
					if placementRules {
						expected = "replace-rule-down-peer"
						if condition == "missing" {
							expected = "add-rule-peer"
						}
					}
					require.Equal(t, expected, ops[0].Desc())
				} else {
					for _, op := range ops {
						require.NotEqual(t, "migrate-deprecated-witness-peer", op.Desc())
					}
				}
			})
		}
	}
}

func TestCheckRegionRecoversWitnessLeaderWithoutReplicaBudget(t *testing.T) {
	for _, placementRules := range []bool{false, true} {
		for _, activeOperator := range []bool{false, true} {
			t.Run(strconv.FormatBool(placementRules)+"/active="+strconv.FormatBool(activeOperator), func(t *testing.T) {
				c, tc, oc, cleanup := newTestSplitScatterController(t)
				defer cleanup()
				tc.SetEnablePlacementRules(placementRules)
				cfg := tc.GetScheduleConfig().Clone()
				cfg.ReplicaScheduleLimit = 0
				tc.SetScheduleConfig(cfg)
				tc.SetAllStoresLimit(storelimit.AddPeer, 0)
				peers := []*metapb.Peer{
					{Id: 1, StoreId: 1},
					{Id: 2, StoreId: 2, IsWitness: true},
					{Id: 3, StoreId: 3},
				}
				region := core.NewRegionInfo(&metapb.Region{Id: 903, Peers: peers}, peers[1],
					core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers[0], DownSeconds: 86400}}))
				tc.SetStoreDown(1)
				tc.PutRegion(region)
				if activeOperator {
					op := operator.NewOperator("unfinished-migration", "", region.GetID(), region.GetRegionEpoch(), operator.OpReplica, 0,
						operator.BecomeNonWitness{StoreID: 2, PeerID: 2})
					op.SetPriorityLevel(constant.High)
					// Install the existing operator before exhausting its store budget.
					tc.SetAllStoresLimit(storelimit.AddPeer, 60)
					require.True(t, oc.AddOperator(op))
					tc.SetAllStoresLimit(storelimit.AddPeer, 0)
				}
				ops := c.CheckRegion(region)
				require.Len(t, ops, 1)
				require.Equal(t, constant.Urgent, ops[0].GetPriorityLevel())
				require.Equal(t, operator.OpLeader, ops[0].Kind())
				require.Equal(t, 1, ops[0].Len())
				require.Equal(t, operator.TransferLeader{FromStore: 2, ToStore: 3}, ops[0].Step(0))
				require.False(t, oc.ExceedStoreLimit(ops...))
				c.tryAddOperators(region)
				op := oc.GetOperator(region.GetID())
				require.NotNil(t, op)
				require.Equal(t, "transfer-deprecated-witness-leader", op.Desc())
			})
		}
	}
}

func TestCheckRegionWitnessLeaderRejectsUnsafeTargets(t *testing.T) {
	for _, condition := range []string{"down", "pending", "learner", "witness", "disconnected"} {
		t.Run(condition, func(t *testing.T) {
			c, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()
			tc.SetEnablePlacementRules(false)
			peers := []*metapb.Peer{{Id: 1, StoreId: 1, IsWitness: true}, {Id: 2, StoreId: 2}}
			switch condition {
			case "learner":
				peers[1].Role = metapb.PeerRole_Learner
			case "witness":
				peers[1].IsWitness = true
			}
			region := core.NewRegionInfo(&metapb.Region{Id: 904, Peers: peers}, peers[0])
			switch condition {
			case "down":
				region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers[1], DownSeconds: 1}}))
			case "pending":
				region = region.Clone(core.WithPendingPeers([]*metapb.Peer{peers[1]}))
			case "disconnected":
				tc.SetStoreDisconnect(2)
			}
			require.Empty(t, c.CheckRegion(region))
		})
	}
}

func TestCheckRegionWitnessLeaderFallsBackToJointStateRepair(t *testing.T) {
	c, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()
	tc.SetEnablePlacementRules(false)
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1, IsWitness: true},
		{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 906, Peers: peers}, peers[0])

	ops := c.CheckRegion(region)
	require.Len(t, ops, 1)
	require.Equal(t, operator.OpDescLeaveJointState, ops[0].Desc())
	require.Equal(t, constant.High, ops[0].GetPriorityLevel())
	require.Equal(t, 1, ops[0].Len())
	require.IsType(t, operator.ChangePeerV2Leave{}, ops[0].Step(0))
}

func TestCheckRegionCompletesLegacyWitnessMigration(t *testing.T) {
	for _, placementRules := range []bool{false, true} {
		for _, role := range []metapb.PeerRole{metapb.PeerRole_Voter, metapb.PeerRole_Learner} {
			t.Run(strconv.FormatBool(placementRules)+"/"+role.String(), func(t *testing.T) {
				c, tc, _, cleanup := newTestSplitScatterController(t)
				defer cleanup()
				tc.SetEnablePlacementRules(placementRules)
				peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: role, IsWitness: true}, {Id: 3, StoreId: 3}}
				region := core.NewRegionInfo(&metapb.Region{Id: 905, Peers: peers}, peers[0])
				tc.PutRegion(region)
				ops := c.CheckRegion(region)
				require.Len(t, ops, 1)
				require.Equal(t, "migrate-deprecated-witness-peer", ops[0].Desc())
				converted := false
				for i := range ops[0].Len() {
					if step, ok := ops[0].Step(i).(operator.BecomeNonWitness); ok {
						require.Equal(t, uint64(2), step.PeerID)
						converted = true
					}
				}
				require.True(t, converted)

				// A completed conversion leaves ordinary promotion to the replica
				// checkers, and regular voters must not be migrated again.
				region = region.Clone()
				region.GetPeer(2).IsWitness = false
				tc.PutRegion(region)
				ops = c.CheckRegion(region)
				if role == metapb.PeerRole_Learner {
					require.Len(t, ops, 1)
					require.IsType(t, operator.PromoteLearner{}, ops[0].Step(0))
				} else {
					require.Empty(t, ops)
				}
			})
		}
	}
}
