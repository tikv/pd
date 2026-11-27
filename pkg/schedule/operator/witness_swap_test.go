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

package operator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestScatterWitnessSwap(t *testing.T) {
	for _, test := range []struct {
		name         string
		joint        bool
		keepIDs      bool
		targetLeader uint64
	}{
		{"joint/zero-IDs", true, false, 1},
		{"joint/existing-IDs", true, true, 1},
		{"legacy/zero-IDs", false, false, 1},
		{"legacy/existing-IDs", false, true, 1},
		{"joint/promoted-leader", true, true, 3},
		{"legacy/promoted-leader", false, true, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
			tc.SetEnableUseJointConsensus(test.joint)
			tc.SetEnableWitness(true)
			for id := uint64(1); id <= 3; id++ {
				tc.AddLabelsStore(id, 0, nil)
			}
			rm := tc.GetRuleManager()
			re.NoError(rm.SetRules([]*placement.Rule{
				{GroupID: "pd", ID: "default", Role: placement.Leader, Count: 1},
				{GroupID: "pd", ID: "ordinary", Index: 1, Role: placement.Voter, Count: 1},
				{GroupID: "pd", ID: "witness", Index: 2, Role: placement.Voter, Count: 1, IsWitness: true},
			}))
			region := tc.AddLeaderRegionWithWitness(1, 1, []uint64{2, 3}, 3)
			re.True(rm.FitRegionWithoutCache(tc, region).IsSatisfied())
			newWitness := uint64(2)
			if test.targetLeader == 3 {
				newWitness = 1 // Transfer leadership before turning its old peer into a witness.
			}
			target := region.Clone()
			target.GetStorePeer(newWitness).IsWitness = true
			target.GetStorePeer(3).IsWitness = false
			target = target.Clone(core.WithLeader(target.GetStorePeer(test.targetLeader)))
			re.True(rm.FitRegionWithoutCache(tc, target).IsSatisfied())
			peers := newPeersMap()
			for _, peer := range target.GetPeers() {
				if !test.keepIDs {
					peer.Id = 0
				}
				peers.set(peer)
			}
			op, err := CreateNonAdminScatterRegionOperator("test", tc, region, peers, test.targetLeader, false)
			re.NoError(err)
			re.Equal(metapb.PeerRole_Voter, peers[3].GetRole())
			re.False(peers[3].GetIsWitness())

			oldID := region.GetStorePeer(3).GetId()
			witness := BecomeWitness{StoreID: newWitness, PeerID: region.GetStorePeer(newWitness).GetId()}
			transfer := TransferLeader{FromStore: 1, ToStore: 3}
			var expected []OpStep
			if test.joint {
				demote := ChangePeerV2Enter{DemoteVoters: []DemoteVoter{{ToStore: 3, PeerID: oldID, IsWitness: true}}}
				promote := ChangePeerV2Enter{PromoteLearners: []PromoteLearner{{ToStore: 3, PeerID: oldID}}, DemoteVoters: []DemoteVoter{}}
				restore := BecomeNonWitness{StoreID: 3, PeerID: oldID, SendStore: 1}
				if test.targetLeader == 3 {
					expected = []OpStep{demote, restore, promote, transfer, BatchSwitchWitness{ToWitnesses: []BecomeWitness{witness}, ToNonWitnesses: []BecomeNonWitness{}}}
				} else {
					demote.PromoteLearners = []PromoteLearner{}
					expected = []OpStep{demote, BatchSwitchWitness{ToWitnesses: []BecomeWitness{witness}, ToNonWitnesses: []BecomeNonWitness{restore}}, promote}
				}
			} else {
				re.GreaterOrEqual(op.Len(), 5)
				re.IsType(AddLearner{}, op.Step(1))
				newID := op.Step(1).(AddLearner).PeerID
				re.Greater(newID, oldID)
				expected = []OpStep{
					RemovePeer{FromStore: 3, PeerID: oldID},
					AddLearner{ToStore: 3, PeerID: newID, IsWitness: true, IsLightWeight: true, SendStore: 1},
					BecomeNonWitness{StoreID: 3, PeerID: newID, SendStore: 1},
					PromoteLearner{ToStore: 3, PeerID: newID},
				}
				if test.targetLeader == 3 {
					expected = append(expected, transfer)
				}
				expected = append(expected, witness)
			}
			re.Equal(expected, op.steps)
		})
	}
}

func TestScatterWitnessSwapWithOutgoingLearner(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetEnableUseJointConsensus(false)
	tc.SetEnableWitness(true)
	for id := uint64(1); id <= 4; id++ {
		tc.AddLabelsStore(id, 0, nil)
	}
	region := tc.AddLeaderRegionWithWitness(1, 1, []uint64{2, 3, 4}, 3)
	region = region.Clone(core.WithLearners([]*metapb.Peer{region.GetStorePeer(4)}))
	op, err := NewBuilder("test", tc, region, SkipPlacementRulesCheck).
		BecomeNonWitness(3).BecomeWitness(2).RemovePeer(4).SetLeader(1).Build(OpRegion)
	require.NoError(t, err)
	// The planner can pair a learner addition with the outgoing learner.
	// Store 3 must be empty before it can accept a new learner.
	oldWitnessRemoved := false
	for _, step := range op.steps {
		switch s := step.(type) {
		case RemovePeer:
			if s.FromStore == 3 {
				oldWitnessRemoved = true
			}
		case AddLearner:
			require.Equal(t, uint64(3), s.ToStore)
			require.True(t, oldWitnessRemoved)
			return
		}
	}
	t.Fatal("missing replacement learner")
}

func TestScatterWitnessSwapRaftRoles(t *testing.T) {
	for _, joint := range []bool{false, true} {
		for _, learnerStore := range []uint64{3, 4} {
			t.Run(fmt.Sprintf("joint=%t/learner=%d", joint, learnerStore), func(t *testing.T) {
				re := require.New(t)
				tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
				tc.SetEnableUseJointConsensus(joint)
				tc.SetEnableWitness(true)
				for id := uint64(1); id <= 4; id++ {
					tc.AddLabelsStore(id, 0, nil)
				}
				dataRole, witnessRole := placement.Voter, placement.Learner
				if learnerStore == 3 {
					dataRole, witnessRole = placement.Learner, placement.Voter
				}
				rm := tc.GetRuleManager()
				re.NoError(rm.SetRules([]*placement.Rule{
					{GroupID: "pd", ID: "default", Role: placement.Voter, Count: 2},
					{GroupID: "pd", ID: "data", Index: 1, Role: dataRole, Count: 1},
					{GroupID: "pd", ID: "witness", Index: 2, Role: witnessRole, Count: 1, IsWitness: true},
				}))
				region := tc.AddLeaderRegion(1, 1, 2, 3, 4)
				region = region.Clone(core.WithLearners([]*metapb.Peer{region.GetStorePeer(learnerStore)}), core.WithWitness(region.GetStorePeer(4).GetId()))
				re.True(rm.FitRegionWithoutCache(tc, region).IsSatisfied())
				target := region.Clone()
				a, b := target.GetStorePeer(3), target.GetStorePeer(4)
				a.Role, b.Role = b.Role, a.Role
				a.IsWitness, b.IsWitness = true, false
				target = target.Clone()
				re.True(rm.FitRegionWithoutCache(tc, target).IsSatisfied())
				for _, keepIDs := range []bool{false, true} {
					t.Run(fmt.Sprintf("existing-IDs=%t", keepIDs), func(t *testing.T) {
						peers := newPeersMap()
						for _, p := range target.Clone().GetPeers() {
							if !keepIDs {
								p.Id = 0
							}
							peers.set(p)
						}
						op, err := CreateNonAdminScatterRegionOperator("test", tc, region, peers, 1, false)
						require.NoError(t, err)
						checkWitnessSwapCommands(t, region, target, op, joint)
					})
				}
			})
		}
	}
}

func checkWitnessSwapCommands(t *testing.T, region, target *core.RegionInfo, op *Operator, joint bool) {
	t.Helper()
	re := require.New(t)
	current := region.Clone()
	for _, step := range op.steps {
		cmd := step.GetCmd(current, joint)
		changes := cmd.ChangePeerV2.GetChanges()
		if cmd.ChangePeer != nil {
			changes = append(changes, cmd.ChangePeer)
		}
		for _, change := range changes {
			peer := change.GetPeer()
			old := current.GetStorePeer(peer.GetStoreId())
			if change.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
				re.Equal(old.GetId(), peer.GetId())
				current = current.Clone(core.WithRemoveStorePeer(peer.GetStoreId()))
				continue
			}
			if old == nil {
				re.Greater(peer.GetId(), region.GetStorePeer(peer.GetStoreId()).GetId())
			} else {
				// TiKV requires the same ID and witness status to change a peer's Raft role.
				re.Equal(old.GetId(), peer.GetId())
				re.Equal(old.GetIsWitness(), peer.GetIsWitness(), "step %v", step)
			}
			current = current.Clone(core.WithRemoveStorePeer(peer.GetStoreId()), core.WithAddPeer(peer))
		}
		for _, sw := range cmd.SwitchWitnesses.GetSwitchWitnesses() {
			peer := current.GetPeer(sw.GetPeerId())
			re.NotNil(peer, "witness switch must address the current peer")
			peer.IsWitness = sw.GetIsWitness()
		}
	}
	for _, p := range target.GetPeers() {
		got := current.GetStorePeer(p.GetStoreId())
		re.NotNil(got)
		re.Equal(p.GetRole(), got.GetRole())
		re.Equal(p.GetIsWitness(), got.GetIsWitness())
	}
}
