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

package scatter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func roleCheckFixture(t *testing.T, newStores bool) (*mockcluster.Cluster, *RegionScatterer, *core.RegionInfo) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	zones := []string{"A", "B", "B"}
	if newStores {
		zones = append(zones, "A", "B", "B")
	}
	for i, z := range zones {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": z})
	}
	rm := tc.GetRuleManager()
	rule := rm.GetRule("pd", "default").Clone()
	rule.Role = placement.Leader
	rule.Count = 1
	require.NoError(t, rm.SetRule(rule))
	voters := rule.Clone()
	voters.ID = "voters"
	voters.Role = placement.Voter
	voters.Count = 2
	voters.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"B"}}}
	require.NoError(t, rm.SetRule(voters))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	require.True(t, rm.FitRegionWithoutCache(tc, region).IsSatisfied())
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	t.Cleanup(stream.Close)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	return tc, NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions), region
}

func TestScatterAcceptsNewLeaderWithCompleteMembership(t *testing.T) {
	tc, sc, region := roleCheckFixture(t, true)
	peers := map[uint64]*metapb.Peer{4: {Id: 104, StoreId: 4}, 5: {Id: 105, StoreId: 5}, 6: {Id: 106, StoreId: 6}}
	target := region.Clone(core.SetPeers([]*metapb.Peer{peers[4], peers[5], peers[6]}), core.WithLeader(peers[4]))
	require.True(t, tc.GetRuleManager().FitRegionWithoutCache(tc, target).IsSatisfied(), "planned membership is valid")
	op, err := operator.CreateNonAdminScatterRegionOperator("review", tc, region, peers, 4, false)
	require.NoError(t, err)
	require.NotNil(t, op)
	got, _ := sc.filterAllowedLeaderCandidateStores(region, peers, []uint64{4}, nil, 0)
	require.Equal(t, []uint64{4}, got, "scatter must retain the valid new leader")
}

func TestAdminScatterPreservesCompleteRoles(t *testing.T) {
	tc, sc, region := roleCheckFixture(t, false)
	group := "review-admin"
	for id, count := range map[uint64]int{1: 10, 3: 5} {
		for range count {
			sc.ordinaryEngine.selectedLeader.Put(id, group)
		}
	}
	op, err := sc.scatterRegionWithType(region, group, false, false, nil)
	require.NoError(t, err)
	targetPeers, targetLeader := finalPlacementAfterOperator(region, op)
	require.Len(t, targetPeers, 3)
	require.Equal(t, uint64(1), targetLeader)
	target := region.Clone(core.WithLeader(region.GetStorePeer(targetLeader)))
	require.True(t, tc.GetRuleManager().FitRegionWithoutCache(tc, target).IsSatisfied(), "admin scatter must preserve complete role matching")
}

func TestScatterRoleCandidateExhaustion(t *testing.T) {
	tc, sc, region := roleCheckFixture(t, true)
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	// Every target is now in B; the original source remains valid in A/B/B.
	tc.PutStoreWithLabels(4, "zone", "B")
	peers := map[uint64]*metapb.Peer{4: {StoreId: 4}, 5: {StoreId: 5}, 6: {StoreId: 6}}
	candidates, _ := sc.filterAllowedLeaderCandidateStores(region, peers, []uint64{4, 5, 6}, nil, 0)
	require.Empty(t, candidates)
	op, err := operator.CreateScatterRegionOperator("test", tc, region, peers, 4, false)
	require.Error(t, err)
	require.Nil(t, op)
	require.Equal(t, uint64(1), region.GetLeader().GetStoreId())
	for _, peer := range peers {
		require.Zero(t, peer.Id)
	}
}

func TestScatterCanReassignFollowerRule(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
			tc, sc, region := roleCheckFixture(t, false)
			tc.PutStoreWithLabels(3, "zone", "A")
			rm := tc.GetRuleManager()
			a := rm.GetRule("pd", "default").Clone()
			a.Role = placement.Voter
			a.Count = 1
			a.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
			require.NoError(t, rm.SetRule(a))
			b := rm.GetRule("pd", "voters").Clone()
			b.Count = 1
			require.NoError(t, rm.SetRule(b))
			follower := a.Clone()
			follower.ID = "z-follower"
			follower.Role = placement.Follower
			follower.LabelConstraints = nil
			require.NoError(t, rm.SetRule(follower))
			fit := rm.FitRegionWithoutCache(tc, region)
			require.True(t, fit.IsSatisfied())
			require.Equal(t, placement.Follower, fit.GetRuleFit(region.GetStorePeer(3).Id).Rule.Role)
			for id, count := range map[uint64]int{1: 10, 2: 5} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, "reassign")
				}
			}
			op, err := sc.scatterRegionWithType(region, "reassign", false, internal, nil)
			require.NoError(t, err)
			require.NotNil(t, op)
			_, leader := finalPlacementAfterOperator(region, op)
			require.Equal(t, uint64(3), leader)
			require.True(t, rm.FitRegionWithoutCache(tc, region.Clone(core.WithLeader(region.GetStorePeer(leader)))).IsSatisfied())
		})
	}
}
