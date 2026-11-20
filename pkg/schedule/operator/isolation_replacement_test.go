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
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestMovePeerRejectsNewConflictInUnderfilledRule(t *testing.T) {
	for _, joint := range []bool{true, false} {
		name := "non-joint"
		if joint {
			name = "joint"
		}
		t.Run(name, func(t *testing.T) {
			cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
			cluster.SetEnablePlacementRules(true)
			cluster.SetEnableUseJointConsensus(joint)
			for i, labels := range []struct {
				zone, host, group string
			}{
				{"", "a", "isolated"}, {"", "e", "other"}, {"", "a", "isolated"},
				{"", "b", "isolated"}, {"A", "c", "isolated"}, {"B", "d", "isolated"},
			} {
				cluster.AddLabelsStore(uint64(i+1), 0, map[string]string{
					"zone": labels.zone, "host": labels.host, "group": labels.group,
				})
			}
			manager := cluster.GetRuleManager()
			require.NoError(t, manager.SetRules([]*placement.Rule{
				{GroupID: "pd", ID: "default", Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}},
				{
					GroupID: "pd", ID: "isolated", Role: placement.Voter, Count: 3,
					LocationLabels: []string{"host"}, IsolationLevel: "host",
					LabelConstraints: []placement.LabelConstraint{{Key: "group", Op: placement.In, Values: []string{"isolated"}}},
				},
			}))
			region := cluster.AddLeaderRegion(1, 1, 2, 3, 4, 5)
			before := manager.FitRegionWithoutCache(cluster, region)
			require.True(t, before.IsSatisfied())
			require.True(t, before.RuleFits[1].IsIsolationSatisfied())

			guard := filter.NewPlacementSafeguard(
				"review", cluster.GetSharedConfig(), cluster.GetBasicCluster(), manager, region, cluster.GetStore(4), before,
			)
			require.True(t, guard.Target(cluster.GetSharedConfig(), cluster.GetStore(6)).IsOK())
			const peerID = 100
			peers := region.Clone(core.WithReplacePeerStore(4, 6)).GetPeers()
			for _, peer := range peers {
				if peer.GetStoreId() == 6 {
					peer.Id = peerID
				}
			}
			target := region.Clone(core.SetPeers(peers))
			after := manager.FitRegionWithoutCache(cluster, target)
			require.Len(t, target.GetPeers(), len(region.GetPeers()))
			require.False(t, after.IsSatisfied())
			require.Len(t, after.RuleFits[1].Peers, 2)
			require.ElementsMatch(t, []uint64{1, 3}, []uint64{
				after.RuleFits[1].Peers[0].GetStoreId(), after.RuleFits[1].Peers[1].GetStoreId(),
			})
			require.False(t, after.RuleFits[1].IsIsolationSatisfied())

			op, err := CreateMovePeerOperator("review", cluster, region, OpRegion, 4,
				&metapb.Peer{Id: peerID, StoreId: 6})
			require.ErrorIs(t, err, ErrTargetIsolation,
				"a same-size move from a complete source must not introduce a host conflict, got op %v", op)
		})
	}
}
