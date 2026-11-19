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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
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

func TestScatterRetainsForceRoleBypass(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
			tc, sc, region := roleCheckFixture(t, false)
			group := "force-roles"
			// Prefer store 2, which can lead only by bypassing complete role checks.
			for id, count := range map[uint64]int{1: 10, 3: 5} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, group)
				}
			}
			op, err := sc.scatterRegionWithType(region, group, false, internal, nil)
			require.NoError(t, err)
			targetPeers, targetLeader := finalPlacementAfterOperator(region, op)
			require.Len(t, targetPeers, 3)
			if internal {
				require.Equal(t, uint64(1), targetLeader)
			} else {
				require.NotNil(t, op)
				require.Equal(t, uint64(2), targetLeader)
			}
			target := region.Clone(core.WithLeader(region.GetStorePeer(targetLeader)))
			require.Equal(t, internal, tc.GetRuleManager().FitRegionWithoutCache(tc, target).IsSatisfied())
		})
	}
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
	op, err := operator.CreateNonAdminScatterRegionOperator("test", tc, region, peers, 4, false)
	require.Error(t, err)
	require.Nil(t, op)
	// The same target remains available to the admin factory through force.
	op, err = operator.CreateScatterRegionOperator("test", tc, region, peers, 4, false)
	require.NoError(t, err)
	require.NotNil(t, op)
	_, leader := finalPlacementAfterOperator(region, op)
	require.Equal(t, uint64(4), leader)
	require.Equal(t, uint64(1), region.GetLeader().GetStoreId())
	for _, peer := range peers {
		require.Zero(t, peer.Id)
	}
}

func TestScatterCanReassignFollowerRule(t *testing.T) {
	for _, tt := range []struct {
		name     string
		internal bool
		pending  bool
		leader   uint64
	}{
		{name: "admin", leader: 2},
		{name: "internal", internal: true, leader: 3},
		{name: "internal with pending alternative", internal: true, pending: true, leader: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
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
			if tt.pending {
				region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
			}
			for id, count := range map[uint64]int{1: 10, 2: 5} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, "reassign")
				}
			}
			op, err := sc.scatterRegionWithType(region, "reassign", false, tt.internal, nil)
			require.NoError(t, err)
			require.NotNil(t, op)
			_, leader := finalPlacementAfterOperator(region, op)
			require.Equal(t, tt.leader, leader)
			require.True(t, rm.FitRegionWithoutCache(tc, region.Clone(core.WithLeader(region.GetStorePeer(leader)))).IsSatisfied())
		})
	}
}

func TestInternalScatterRetriesLeaderPeerDestination(t *testing.T) {
	for _, tt := range []struct {
		name          string
		pausedTargets []uint64
		leader        uint64
	}{
		{name: "one paused target", pausedTargets: []uint64{4}, leader: 5},
		{name: "two paused targets", pausedTargets: []uint64{4, 5}, leader: 6},
		{name: "all targets paused", pausedTargets: []uint64{4, 5, 6}, leader: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc, sc, _ := roleCheckFixture(t, false)
			for _, id := range []uint64{4, 5, 6} {
				tc.AddLabelsStore(id, 0, map[string]string{"zone": "A"})
			}
			for _, id := range tt.pausedTargets {
				require.NoError(t, tc.PauseLeaderTransfer(id, constant.In))
			}
			rm := tc.GetRuleManager()
			rule := rm.GetRule("pd", "default").Clone()
			rule.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
			require.NoError(t, rm.SetRule(rule))
			// Stores 1, 4, 5, and 6 have 4, 0, 1, and 2 peers in the range.
			tc.AddLeaderRegionWithRange(1, "a", "b", 1, 2, 3)
			tc.AddLeaderRegionWithRange(2, "b", "c", 1, 2, 3)
			tc.AddLeaderRegionWithRange(3, "c", "d", 1, 2, 3)
			tc.AddLeaderRegionWithRange(4, "d", "e", 1, 2, 3)
			tc.AddLeaderRegionWithRange(5, "e", "f", 5, 2, 3)
			tc.AddLeaderRegionWithRange(6, "f", "g", 6, 2, 3)
			tc.AddLeaderRegionWithRange(7, "g", "h", 6, 2, 3)
			region := tc.GetRegion(1)
			op, err := sc.ScatterInternal(region, "retry-leader-peer", []byte("a"), []byte("h"))
			require.NoError(t, err)
			if tt.leader == 1 {
				require.Nil(t, op)
			} else {
				require.NotNil(t, op)
			}
			peers, leader := finalPlacementAfterOperator(region, op)
			require.Equal(t, tt.leader, leader)
			require.Contains(t, peers, tt.leader)
			for _, id := range tt.pausedTargets {
				require.NotContains(t, peers, id)
			}
			target := region.Clone(core.SetPeers([]*metapb.Peer{peers[2], peers[3], peers[leader]}), core.WithLeader(peers[leader]))
			require.True(t, rm.FitRegionWithoutCache(tc, target).IsSatisfied())
		})
	}
}

func TestInternalScatterPreservesFollowerIsolation(t *testing.T) {
	tc, sc, _ := roleCheckFixture(t, false)
	tc.PutStoreWithLabels(3, "zone", "A")
	rm := tc.GetRuleManager()
	followers := rm.GetRule("pd", "default").Clone()
	followers.Role = placement.Follower
	followers.Count = 2
	followers.LocationLabels = []string{"zone"}
	followers.IsolationLevel = "zone"
	require.NoError(t, rm.SetRule(followers))
	voter := rm.GetRule("pd", "voters").Clone()
	voter.Count = 1
	voter.LabelConstraints = nil
	require.NoError(t, rm.SetRule(voter))
	tc.AddLeaderRegionWithRange(1, "a", "b", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "b", "c", 1, 2, 3)
	tc.AddLeaderRegionWithRange(3, "c", "d", 1, 2, 3)
	region := tc.GetRegion(1)
	before := rm.FitRegionWithoutCache(tc, region)
	require.True(t, before.IsSatisfied())
	require.Positive(t, before.RuleFits[0].IsolationScore)
	op, err := sc.ScatterInternal(region, "keep-follower-isolation", []byte("a"), []byte("d"))
	require.NoError(t, err)
	require.NotNil(t, op)
	peers, leader := finalPlacementAfterOperator(region, op)
	require.Equal(t, uint64(3), leader)
	target := region.Clone(core.WithLeader(peers[leader]))
	after := rm.FitRegionWithoutCache(tc, target)
	require.True(t, after.IsSatisfied())
	require.Equal(t, before.RuleFits[0].IsolationScore, after.RuleFits[0].IsolationScore)
}

func TestInternalScatterRetriesFollowerPeerDestination(t *testing.T) {
	tc, sc, _ := roleCheckFixture(t, false)
	tc.PutStoreWithLabels(1, "scope", "leader")
	tc.PutStoreWithLabels(2, "scope", "follower", "zone", "A", "rack", "r1")
	tc.PutStoreWithLabels(3, "scope", "follower", "rack", "r2")
	tc.AddLabelsStore(4, 0, map[string]string{"scope": "follower", "zone": "A", "rack": "r2", "hold-voter": "yes"})
	tc.AddLabelsStore(5, 0, map[string]string{"scope": "follower", "zone": "A", "rack": "r2"})
	tc.AddLabelsStore(6, 0, map[string]string{"scope": "follower", "zone": "C", "rack": "r1"})
	tc.AddLabelsStore(7, 0, map[string]string{"scope": "follower", "zone": "A", "rack": "r3"})
	rm := tc.GetRuleManager()
	followers := rm.GetRule("pd", "default").Clone()
	followers.Role = placement.Follower
	followers.Count = 2
	followers.LabelConstraints = []placement.LabelConstraint{{Key: "scope", Op: placement.In, Values: []string{"follower"}}}
	followers.LocationLabels = []string{"zone", "rack"}
	require.NoError(t, rm.SetRule(followers))
	voter := rm.GetRule("pd", "voters").Clone()
	voter.Count = 1
	voter.LabelConstraints = []placement.LabelConstraint{{Key: "hold-voter", Op: placement.In, Values: []string{"yes"}}}
	require.NoError(t, rm.SetRule(voter))
	leader := voter.Clone()
	leader.ID, leader.Role = "z-leader", placement.Leader
	leader.LabelConstraints = []placement.LabelConstraint{{Key: "scope", Op: placement.In, Values: []string{"leader"}}}
	require.NoError(t, rm.SetRule(leader))
	tc.AddLeaderRegionWithRange(1, "a", "b", 1, 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "b", "c", 1, 2, 3, 4)
	tc.AddLeaderRegionWithRange(3, "c", "d", 1, 2, 3, 4)
	region := tc.GetRegion(1)
	before := rm.FitRegionWithoutCache(tc, region)
	require.True(t, before.IsSatisfied())
	require.Equal(t, placement.Voter, before.GetRuleFit(region.GetStorePeer(4).GetId()).Rule.Role)
	// Move 2 first. The refit uses 4 in the follower rule, so the next move to
	// 5 can pass the peer safeguard even though only store 4 matches the voter rule.
	order := []uint64{2, 4, 3, 1}
	calls := 0
	const fp = "github.com/tikv/pd/pkg/schedule/scatter/scatterPeerOrder"
	require.NoError(t, failpoint.EnableCall(fp, func(peer **metapb.Peer) {
		*peer = region.GetStorePeer(order[calls%len(order)])
		calls++
	}))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
	op, err := sc.ScatterInternal(region, "retry-follower-peer", []byte("a"), []byte("d"))
	require.NoError(t, err)
	require.NotNil(t, op)
	require.GreaterOrEqual(t, calls, 3*len(order))
	peers, targetLeader := finalPlacementAfterOperator(region, op)
	require.Equal(t, uint64(1), targetLeader)
	require.Contains(t, peers, uint64(4))
	require.NotContains(t, peers, uint64(6))
	require.Contains(t, peers, uint64(7))
	require.NotContains(t, peers, uint64(2))
	targetPeers := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		targetPeers = append(targetPeers, peer)
	}
	target := region.Clone(core.SetPeers(targetPeers), core.WithLeader(peers[targetLeader]))
	require.True(t, rm.FitRegionWithoutCache(tc, target).IsSatisfied())
}

func TestInternalScatterPressureRequiresEligibleLeader(t *testing.T) {
	tc, sc, region := roleCheckFixture(t, true)
	peers := map[uint64]*metapb.Peer{4: {StoreId: 4}, 5: {StoreId: 5}, 6: {StoreId: 6}}
	readCPU := map[uint64]float64{4: 10000}
	candidates, blockedByPressure := sc.filterAllowedLeaderCandidateStores(region, peers, []uint64{4}, readCPU, 12)
	require.Empty(t, candidates)
	require.True(t, blockedByPressure)
	require.NoError(t, tc.PauseLeaderTransfer(4, constant.In))
	candidates, blockedByPressure = sc.filterAllowedLeaderCandidateStores(region, peers, []uint64{4}, readCPU, 12)
	require.Empty(t, candidates)
	require.False(t, blockedByPressure)
}

func TestScatterKeepsLeaderOffFollowerRule(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
			tc, sc, region := roleCheckFixture(t, false)
			rm := tc.GetRuleManager()
			leader := rm.GetRule("pd", "default").Clone()
			leader.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
			require.NoError(t, rm.SetRule(leader))
			followers := rm.GetRule("pd", "voters").Clone()
			followers.Role = placement.Follower
			require.NoError(t, rm.SetRule(followers))
			require.True(t, rm.FitRegionWithoutCache(tc, region).IsSatisfied())
			// Prefer store 2, whose peer is fitted to a Follower rule in zone B.
			for id, count := range map[uint64]int{1: 10, 3: 5} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, "follower-rule")
				}
			}
			op, err := sc.scatterRegionWithType(region, "follower-rule", false, internal, nil)
			require.NoError(t, err)
			_, targetLeader := finalPlacementAfterOperator(region, op)
			require.Equal(t, uint64(1), targetLeader)
			require.True(t, rm.FitRegionWithoutCache(tc, region.Clone(core.WithLeader(region.GetStorePeer(targetLeader)))).IsSatisfied())
		})
	}
}
