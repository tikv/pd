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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// newLeaderVoterIsolationCluster has an unrestricted Leader x1 rule and a
// Voter x2 rule. Stores 1/2/3/4 have hosts a/b/a/a; peers 1/2/3, leader 1.
func newLeaderVoterIsolationCluster(t *testing.T, level string) (*mockcluster.Cluster, *core.RegionInfo) {
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for i, host := range []string{"a", "b", "a", "a"} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
	}
	rm := tc.GetRuleManager()
	leader := rm.GetRule("pd", "default").Clone()
	leader.Role, leader.Count = placement.Leader, 1
	require.NoError(t, rm.SetRule(leader))
	voters := leader.Clone()
	voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
	voters.LocationLabels, voters.IsolationLevel = []string{"host"}, level
	require.NoError(t, rm.SetRule(voters))
	return tc, tc.AddLeaderRegion(1, 1, 2, 3)
}

func finalLeader(op *Operator) uint64 {
	var leader uint64
	for i := range op.Len() {
		if step, ok := op.Step(i).(TransferLeader); ok {
			leader = step.ToStore
		}
	}
	return leader
}

func TestMovePeerChoosesLeaderPreservingIsolation(t *testing.T) {
	// Moving the leader peer makes the builder choose a new leader. Store 2
	// would take the leader slot and leave voters 3/4 on host a.
	tc, region := newLeaderVoterIsolationCluster(t, "host")
	op, err := CreateMovePeerOperator("test", tc, region, OpRegion, 1, &metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Voter})
	require.NoError(t, err)
	require.Contains(t, []uint64{3, 4}, finalLeader(op))

	// Without a configured level, the existing leader choice is unchanged.
	tc, region = newLeaderVoterIsolationCluster(t, "")
	op, err = CreateMovePeerOperator("test", tc, region, OpRegion, 1, &metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Voter})
	require.NoError(t, err)
	require.Equal(t, uint64(2), finalLeader(op))
}

func TestReplaceLeaderPeerRejectsIsolationBreakingLeader(t *testing.T) {
	tc, region := newLeaderVoterIsolationCluster(t, "host")
	newPeer := &metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Voter}
	_, err := CreateReplaceLeaderPeerOperator("test", tc, region, OpRegion, 1, newPeer, region.GetStorePeer(2))
	require.Error(t, err)
	op, err := CreateReplaceLeaderPeerOperator("test", tc, region, OpRegion, 1, newPeer, region.GetStorePeer(3))
	require.NoError(t, err)
	require.Equal(t, uint64(3), finalLeader(op))
}

func TestMovePeerRejectsWithoutAdmissibleFinalLeader(t *testing.T) {
	// Stores 3/4 would preserve isolation but cannot receive leadership. The
	// remaining store 2 breaks it, so neither plan may return an operator.
	for _, joint := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-joint", true: "joint"}[joint], func(t *testing.T) {
			tc, region := newLeaderVoterIsolationCluster(t, "host")
			tc.SetEnableUseJointConsensus(joint)
			tc.SetStoreEvictLeader(3, true)
			tc.SetStoreEvictLeader(4, true)
			op, err := CreateMovePeerOperator("test", tc, region, OpRegion, 1, &metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Voter})
			require.Error(t, err)
			require.Nil(t, op)
		})
	}
}

func TestMoveWitnessChecksFinalRoles(t *testing.T) {
	// Witness 2 becomes an ordinary voter. The builder demotes it to a witness
	// learner as an execution step; the check must use its final role.
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	tc.SetEnableWitness(true)
	tc.SetMaxReplicas(5)
	for i, host := range []string{"l", "a", "b", "a", "c"} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
	}
	rm := tc.GetRuleManager()
	leader := rm.GetRule("pd", "default").Clone()
	leader.Role, leader.Count = placement.Leader, 1
	require.NoError(t, rm.SetRule(leader))
	voters := leader.Clone()
	voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
	voters.LocationLabels, voters.IsolationLevel = []string{"host"}, "host"
	require.NoError(t, rm.SetRule(voters))
	witnesses := voters.Clone()
	witnesses.ID, witnesses.IsWitness = "witnesses", true
	require.NoError(t, rm.SetRule(witnesses))

	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 12, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
		{Id: 13, StoreId: 3, Role: metapb.PeerRole_Voter, IsWitness: true},
		{Id: 14, StoreId: 4, Role: metapb.PeerRole_Voter},
		{Id: 15, StoreId: 5, Role: metapb.PeerRole_Voter},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}, peers[0])
	tc.PutRegion(region)
	require.True(t, rm.FitRegion(tc, region).IsSatisfied())

	// After moving the witness from 2 to 5, leader 1 would leave ordinary
	// voters 2/4 on host a. Leader 4 takes the leader slot instead, leaving
	// ordinary voters 1/2 and witnesses 3/5 isolated.
	op, err := CreateMoveWitnessOperator("test", tc, region, 2, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(4), finalLeader(op))
	// Without leader 4, no final leader preserves isolation.
	tc.SetStoreEvictLeader(4, true)
	_, err = CreateMoveWitnessOperator("test", tc, region, 2, 5)
	require.Error(t, err)
	// Moving the witness to store 4 keeps leader 1 with voters 2/5 isolated.
	tc.SetStoreEvictLeader(4, false)
	op, err = CreateMoveWitnessOperator("test", tc, region, 2, 4)
	require.NoError(t, err)
	require.Zero(t, finalLeader(op))
}

func TestFinalIsolationCheckKeepsRepairs(t *testing.T) {
	// Replacing a peer on an offline store keeps the existing behavior.
	tc, region := newLeaderVoterIsolationCluster(t, "host")
	tc.SetStoreOffline(1)
	newPeer := &metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Voter}
	_, err := CreateReplaceLeaderPeerOperator("test", tc, region, OpReplica, 1, newPeer, region.GetStorePeer(2))
	require.NoError(t, err)
}
