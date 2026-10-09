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

func isolationTestScatter(t *testing.T, hosts []string) (*mockcluster.Cluster, *RegionScatterer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	for i, host := range hosts {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
	}
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	t.Cleanup(stream.Close)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	return tc, NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
}

func TestScatterFinalIsolationAndMetadata(t *testing.T) {
	tc, sc := isolationTestScatter(t, []string{"a", "b", "c", "d", "d", "d"})
	tc.SetLocationLabels([]string{"host"})
	source := tc.AddLeaderRegion(1, 1, 2, 3)
	target := source.Clone(core.WithReplacePeerStore(1, 4), core.WithReplacePeerStore(2, 5), core.WithReplacePeerStore(3, 6), core.WithReplaceLeaderStore(4))
	for _, enabled := range []bool{false, true} {
		tc.SetEnablePlacementRules(enabled)
		if enabled {
			rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
			rule.LocationLabels = []string{"host"}
			rule.IsolationLevel = "host"
			require.NoError(t, tc.GetRuleManager().SetRule(rule))
		}
		require.False(t, sc.scatterIsolationChecker(source, target)(4))
		require.True(t, sc.scatterIsolationChecker(source, source)(1))
	}
	// Comparisons retain one label snapshot; rebuilding observes changed labels.
	check := sc.scatterIsolationChecker(source, source)
	tc.PutStoreWithLabels(2, "host", "a")
	require.True(t, check(1))
	// A rule change after candidate planning is seen by final validation.
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	tc.AddLabelsStore(7, 0, map[string]string{"host": "z"})
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"z"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	require.False(t, sc.scatterIsolationChecker(source, source)(1))
}

func TestScatterIsolationRetriesLeader(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
			tc, sc := isolationTestScatter(t, []string{"a", "b", "a"})
			tc.SetEnablePlacementRules(true)
			rm := tc.GetRuleManager()
			leader := rm.GetRule("pd", "default").Clone()
			leader.Role = placement.Leader
			leader.Count = 1
			require.NoError(t, rm.SetRule(leader))
			voters := leader.Clone()
			voters.ID = "voters"
			voters.Role = placement.Voter
			voters.Count = 2
			voters.LocationLabels = []string{"host"}
			voters.IsolationLevel = "host"
			require.NoError(t, rm.SetRule(voters))
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			check := sc.scatterIsolationChecker(region, region)
			require.False(t, check(2))
			require.True(t, check(3))
			group := "isolation-retry"
			for id, count := range map[uint64]int{1: 10, 3: 1} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, group)
				}
			}
			op, err := sc.scatterRegionWithType(region, group, false, internal, nil)
			require.NoError(t, err)
			require.NotNil(t, op)
			_, targetLeader := finalPlacementAfterOperator(region, op)
			require.Equal(t, uint64(3), targetLeader)
		})
	}
}

func TestScatterIsolationIsPerRule(t *testing.T) {
	tc, sc := isolationTestScatter(t, []string{"a", "b", "a", "c"})
	tc.SetEnablePlacementRules(true)
	rm := tc.GetRuleManager()
	voters := rm.GetRule("pd", "default").Clone()
	voters.Count = 2
	voters.LocationLabels = []string{"host"}
	voters.IsolationLevel = "host"
	require.NoError(t, rm.SetRule(voters))
	learner := voters.Clone()
	learner.ID = "learner"
	learner.Count = 1
	learner.Role = placement.Learner
	require.NoError(t, rm.SetRule(learner))
	source := tc.AddLeaderRegion(1, 1, 2, 3)
	peers := source.Clone().GetPeers()
	peers[2].Role = metapb.PeerRole_Learner
	source = source.Clone(core.SetPeers(peers))
	require.True(t, rm.FitRegionWithoutCache(tc, source).IsSatisfied())
	// The learner may share a host with a voter in another rule.
	require.True(t, sc.scatterIsolationChecker(source, source)(1))
	target := source.Clone(core.WithReplacePeerStore(2, 4))
	require.True(t, sc.scatterIsolationChecker(source, target)(1))
	require.Equal(t, metapb.PeerRole_Learner, target.GetStorePeer(3).Role)
}
