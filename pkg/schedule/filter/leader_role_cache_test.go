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

package filter

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestLeaderRoleCheckerMetadataAndMembership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for id := uint64(1); id <= 4; id++ {
		tc.AddLabelsStore(id, 0, map[string]string{"zone": "A"})
	}
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	guard := newRuleLeaderFitFilter("test", tc.GetBasicCluster(), tc.GetRuleManager(), region, nil, 1, true).(*ruleLeaderFitFilter)
	target := func(id uint64) bool { return guard.Target(tc.GetSharedConfig(), tc.GetStore(id)).IsOK() }
	require.True(t, target(2))
	prepared := guard.roleChecker.Load()
	require.True(t, target(3))
	require.Same(t, prepared, guard.roleChecker.Load())
	// A different membership must not reuse or overwrite the original matcher.
	require.True(t, target(4))
	require.Same(t, prepared, guard.roleChecker.Load())
	require.True(t, target(2))
	require.Nil(t, region.GetStorePeer(4))
	require.Equal(t, uint64(1), region.GetLeader().GetStoreId())
	// A non-candidate store's label change must invalidate the full-layout check.
	tc.PutStoreWithLabels(3, "zone", "B")
	require.False(t, target(2))
	tc.PutStoreWithLabels(3, "zone", "A")
	require.True(t, target(2))
	tc.PutStoreWithLabels(4, "zone", "B")
	rule = rule.Clone()
	rule.LabelConstraints[0].Values = []string{"B"}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	require.False(t, target(2))
	rule = rule.Clone()
	rule.LabelConstraints[0].Values = []string{"A"}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	// Concurrent cold calls may build separate snapshots, but must stay safe.
	guard.roleChecker.Store(nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if !target(2) {
					t.Error("legal target rejected")
				}
			}
		})
	}
	wg.Wait()
}

func TestLeaderSafeguardReassignsOverlappingRules(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for i, zone := range []string{"A", "B", "C", "C"} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": zone})
	}
	rm := tc.GetRuleManager()
	require.NoError(t, rm.SetRules([]*placement.Rule{
		{GroupID: "pd", ID: "default", Role: placement.Voter, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}},
		{GroupID: "pd", ID: "voter", Index: 1, Role: placement.Voter, Count: 1},
		{GroupID: "pd", ID: "follower", Index: 2, Role: placement.Follower, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"B", "C"}}}},
	}))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	fit := rm.FitRegionWithoutCache(tc, region)
	require.True(t, fit.IsSatisfied())
	require.False(t, fit.Replace(1, tc.GetStore(3)))
	target := region.Clone(core.WithLeader(region.GetStorePeer(3)))
	require.True(t, rm.FitRegionWithoutCache(tc, target).IsSatisfied())
	for _, allowMove := range []bool{false, true} {
		guard := NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(1), allowMove)
		require.True(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(3)).IsOK())
		// Moving the A peer to a new C store would lose the A-only voter.
		require.False(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(4)).IsOK())
	}
	// Incomplete layouts still use the existing incremental replacement check.
	incomplete := region.Clone(core.WithRemoveStorePeer(2))
	guard := NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, incomplete, tc.GetStore(1), false)
	require.Equal(t, rm.FitRegionWithoutCache(tc, incomplete).Replace(1, tc.GetStore(3)), guard.Target(tc.GetSharedConfig(), tc.GetStore(3)).IsOK())
	// Isolation on another rule must not prevent a legal reassignment.
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "D"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "E"})
	require.NoError(t, rm.SetRule(&placement.Rule{GroupID: "pd", ID: "learners", Index: 3, Role: placement.Learner, Count: 2, LocationLabels: []string{"zone"}}))
	region = region.Clone(
		core.WithAddPeer(&metapb.Peer{Id: 5, StoreId: 5, Role: metapb.PeerRole_Learner}),
		core.WithAddPeer(&metapb.Peer{Id: 6, StoreId: 6, Role: metapb.PeerRole_Learner}),
	)
	require.True(t, rm.FitRegionWithoutCache(tc, region).IsSatisfied())
	guard = NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(1), false)
	require.True(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(3)).IsOK())
}

func TestLeaderSafeguardReassignmentPreservesIsolation(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	tc.AddLabelsStore(1, 0, map[string]string{"zone": "A", "rack": "eligible"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "B", "rack": "other"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "B", "rack": "eligible"})
	rm := tc.GetRuleManager()
	require.NoError(t, rm.SetRules([]*placement.Rule{
		{GroupID: "pd", ID: "default", Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}},
		{GroupID: "pd", ID: "follower", Index: -1, Role: placement.Follower, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "rack", Op: placement.In, Values: []string{"eligible"}}}},
	}))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	before := rm.FitRegionWithoutCache(tc, region)
	require.True(t, before.IsSatisfied())
	require.False(t, before.Replace(1, tc.GetStore(3)))
	after := rm.FitRegionWithoutCache(tc, region.Clone(core.WithLeader(region.GetStorePeer(3))))
	require.True(t, after.IsSatisfied())
	require.Greater(t, before.RuleFits[1].IsolationScore, after.RuleFits[1].IsolationScore)
	require.True(t, placement.NewRoleChecker(tc, region.GetPeers(), rm.GetRulesForApplyRegion(region), tc.GetSharedConfig().IsWitnessAllowed()).IsSatisfied(3))
	guard := NewPlacementLeaderSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(1), false)
	require.False(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(3)).IsOK())
}
