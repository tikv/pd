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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newLeaderRuleTestCluster(t *testing.T, constrainLeader bool) (*mockcluster.Cluster, *core.RegionInfo) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for i, zone := range []string{"A", "B", "B", "A"} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": zone})
	}
	rm := tc.GetRuleManager()
	leader := rm.GetRule("pd", "default").Clone()
	leader.Role, leader.Count = placement.Leader, 1
	if constrainLeader {
		leader.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
	}
	require.NoError(t, rm.SetRule(leader))
	voters := leader.Clone()
	voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
	voters.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"B"}}}
	require.NoError(t, rm.SetRule(voters))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	require.True(t, rm.FitRegion(tc, region).IsSatisfied())
	return tc, region
}

func TestLeaderTransferPreservesAllRules(t *testing.T) {
	for _, constrainLeader := range []bool{false, true} {
		name := "remaining-voter-slots"
		if constrainLeader {
			name = "leader-zone"
		}
		t.Run(name, func(t *testing.T) {
			tc, region := newLeaderRuleTestCluster(t, constrainLeader)
			guard := filter.NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), tc.GetRuleManager(), region, tc.GetStore(1), false)
			require.False(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(2)).IsOK())
			require.False(t, IsAllowedLeaderTarget(tc, region, region.GetStorePeer(2)))
			op, err := CreateTransferLeaderOperator("test", tc, region, 2, nil, OpLeader)
			require.Error(t, err)
			require.Nil(t, op)
			// Moving the leader peer to another allowed A store is still possible.
			moveGuard := filter.NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), tc.GetRuleManager(), region, tc.GetStore(1), true)
			require.True(t, moveGuard.Target(tc.GetSharedConfig(), tc.GetStore(4)).IsOK())
		})
	}
}

func TestMovePeerChoosesValidFinalLeader(t *testing.T) {
	for _, joint := range []bool{false, true} {
		name := "without-joint-consensus"
		if joint {
			name = "with-joint-consensus"
		}
		t.Run(name, func(t *testing.T) {
			tc, region := newLeaderRuleTestCluster(t, true)
			// Permit B peers as temporary leaders while membership is changed.
			rule := tc.GetRuleManager().GetRule("pd", "voters").Clone()
			rule.LabelConstraints = nil
			require.NoError(t, tc.GetRuleManager().SetRule(rule))
			b := NewBuilder("test", tc, region).RemovePeer(1).AddPeer(&metapb.Peer{StoreId: 4})
			b.useJointConsensus = joint
			op, err := b.Build(OpRegion)
			require.NoError(t, err)
			require.NotNil(t, op)
			require.Equal(t, uint64(4), b.currentLeaderStoreID)
			require.True(t, b.allowTargetLeader(b.targetPeers[4]))
		})
	}
}

func TestLeaderRuleCheckRetainsExplicitBypassAndRepair(t *testing.T) {
	tc, region := newLeaderRuleTestCluster(t, true)
	// Explicit force and skip retain the existing administrative contract.
	for _, b := range []*Builder{
		NewBuilder("test", tc, region).SetLeader(2).EnableForceTargetLeader(),
		NewBuilder("test", tc, region, SkipPlacementRulesCheck).SetLeader(2),
	} {
		op, err := b.Build(OpLeader)
		require.NoError(t, err)
		require.NotNil(t, op)
	}
	// A layout that is not yet satisfied can still be repaired incrementally.
	incomplete := region.Clone(core.WithRemoveStorePeer(3))
	op, err := CreateMovePeerOperator("test", tc, incomplete, OpRegion, 1, &metapb.Peer{StoreId: 4})
	require.NoError(t, err)
	require.NotNil(t, op)
	// A legal transfer with interchangeable peers remains allowed.
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LabelConstraints = nil
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	voters := tc.GetRuleManager().GetRule("pd", "voters").Clone()
	voters.LabelConstraints = nil
	require.NoError(t, tc.GetRuleManager().SetRule(voters))
	op, err = CreateTransferLeaderOperator("test", tc, region, 2, nil, OpLeader)
	require.NoError(t, err)
	require.NotNil(t, op)
}

func TestTargetLeaderBeforePeerIDAllocation(t *testing.T) {
	tc, region := newLeaderRuleTestCluster(t, true)
	rule := tc.GetRuleManager().GetRule("pd", "voters").Clone()
	rule.Role = placement.Follower
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	for _, id := range []uint64{5, 6} {
		tc.AddLabelsStore(id, 0, map[string]string{"zone": "B"})
	}
	peers := map[uint64]*metapb.Peer{4: {StoreId: 4}, 5: {StoreId: 5}, 6: {StoreId: 6}}
	b := NewBuilder("test", tc, region).SetPeers(peers).SetLeader(4)
	op, err := b.Build(OpRegion)
	require.NoError(t, err)
	require.NotNil(t, op)
	require.Equal(t, uint64(4), b.currentLeaderStoreID)
	for _, peer := range peers {
		require.Zero(t, peer.GetId())
	}
}
