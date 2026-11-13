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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newIsolationTestController(t testing.TB) (*Controller, *mockcluster.Cluster) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	tc.SetEnableUseJointConsensus(false)
	tc.SetPlacementRulesCacheEnabled(true)
	tc.SetKeyType("raw")
	tc.SetSplitMergeInterval(0)
	tc.SetMaxMergeRegionSize(2)
	tc.SetMaxMergeRegionKeys(2)
	for i := uint64(1); i <= 6; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"host": strconv.FormatUint(i, 10)})
	}
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LocationLabels = []string{"host"}
	rule.IsolationLevel = "host"
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	c := NewController(ctx, tc, tc.GetCheckerConfig(), oc)
	t.Cleanup(func() {
		c.splitScatter.clearPendingSplitScatter()
		stream.Close()
		cancel()
	})
	return c, tc
}

func TestControllerIsolationPreservesRepair(t *testing.T) {
	for _, mode := range []string{"missing-peer", "down-peer", "follower-role"} {
		t.Run(mode, func(t *testing.T) {
			c, tc := newIsolationTestController(t)
			r := tc.AddLeaderRegion(1, 1, 2, 3)
			switch mode {
			case "missing-peer":
				r = r.Clone(core.WithRemoveStorePeer(3))
			case "down-peer":
				tc.SetStoreDown(3)
				r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(3), DownSeconds: 6000}}))
			case "follower-role":
				for i := uint64(1); i <= 3; i++ {
					role := "follower"
					if i == 3 {
						role = "voter"
					}
					tc.AddLabelsStore(i, 0, map[string]string{"host": strconv.FormatUint(i, 10), "role": role})
				}
				rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
				rule.ID, rule.Index, rule.Override, rule.Count = "voter", 100, true, 1
				rule.LabelConstraints = []placement.LabelConstraint{{Key: "role", Op: placement.In, Values: []string{"voter"}}}
				require.NoError(t, tc.GetRuleManager().SetRule(rule))
				rule = rule.Clone()
				rule.ID, rule.Index, rule.Override, rule.Role, rule.Count = "followers", 101, false, placement.Follower, 2
				rule.LabelConstraints = []placement.LabelConstraint{{Key: "role", Op: placement.In, Values: []string{"follower"}}}
				require.NoError(t, tc.GetRuleManager().SetRule(rule))
			}
			tc.PutRegion(r)
			ops := c.CheckRegion(r)
			require.Len(t, ops, 1)
			switch mode {
			case "missing-peer":
				require.Equal(t, "add-rule-peer", ops[0].Desc())
			case "down-peer":
				require.Equal(t, "replace-rule-down-peer", ops[0].Desc())
			case "follower-role":
				require.Equal(t, "fix-follower-role", ops[0].Desc())
			}
			for range ops[0].Len() {
				r = operator.ApplyOperatorStep(r, ops[0])
			}
			require.Len(t, r.GetPeers(), 3)
			require.Empty(t, r.GetLearners())
			if mode == "down-peer" {
				require.Nil(t, r.GetStorePeer(3))
			}
			if mode == "follower-role" {
				require.Equal(t, uint64(3), r.GetLeader().GetStoreId())
			}
			require.True(t, tc.GetRuleManager().FitRegionWithoutCache(tc, r).IsSatisfied())
			hosts := make(map[string]struct{})
			for _, peer := range r.GetPeers() {
				hosts[tc.GetStore(peer.GetStoreId()).GetLabelValue("host")] = struct{}{}
			}
			require.Len(t, hosts, 3)
		})
	}
}

func assertIsolationMergePair(t testing.TB, ops []*operator.Operator, source, target *core.RegionInfo) {
	t.Helper()
	require.Len(t, ops, 2)
	for i, op := range ops {
		step, ok := op.Step(op.Len() - 1).(operator.MergeRegion)
		require.True(t, ok)
		require.Equal(t, source.GetID(), step.FromRegion.GetId())
		require.Equal(t, target.GetID(), step.ToRegion.GetId())
		require.Equal(t, i == 1, step.IsPassive)
	}
	// Apply the active operator's peer changes before its final merge step.
	result := source
	for range ops[0].Len() - 1 {
		result = operator.ApplyOperatorStep(result, ops[0])
	}
	require.Equal(t, target.GetStoreIDs(), result.GetStoreIDs())
	require.Equal(t, target.GetLeader().GetStoreId(), result.GetLeader().GetStoreId())
}

func TestControllerMergeIsolation(t *testing.T) {
	for _, targetIsolated := range []bool{false, true} {
		t.Run(fmt.Sprintf("target-isolated=%t", targetIsolated), func(t *testing.T) {
			c, tc := newIsolationTestController(t)
			if !targetIsolated {
				for i := uint64(4); i <= 6; i++ {
					tc.AddLabelsStore(i, 0, map[string]string{"host": "shared"})
				}
			}
			source := tc.AddLeaderRegion(1, 1, 2, 3).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("m")), core.SetApproximateSize(1), core.SetApproximateKeys(1))
			target := tc.AddLeaderRegion(2, 4, 5, 6).Clone(core.WithStartKey([]byte("m")), core.WithEndKey([]byte("z")), core.SetApproximateSize(1), core.SetApproximateKeys(1))
			tc.PutRegion(source)
			tc.PutRegion(target)
			ops := c.CheckRegion(source)
			if targetIsolated {
				assertIsolationMergePair(t, ops, source, target)
			} else {
				require.Empty(t, ops)
				// The merge predicate permits recovery into the isolated layout.
				// The full controller may repair the source before reaching merge.
				assertIsolationMergePair(t, c.mergeChecker.Check(target), target, source)
			}
		})
	}
}

func TestControllerReplaceDownPeerWithOrphanIsolation(t *testing.T) {
	for _, isolated := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement-isolated=%t", isolated), func(t *testing.T) {
			c, tc := newIsolationTestController(t)
			if !isolated {
				tc.AddLabelsStore(4, 0, map[string]string{"host": "1"})
			}
			source := tc.AddLeaderRegion(1, 1, 2, 3)
			source = source.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 4, Role: metapb.PeerRole_Learner}), core.WithDownPeers([]*pdpb.PeerStats{{Peer: source.GetStorePeer(3), DownSeconds: 1}}))
			tc.SetStoreDisconnect(3)
			tc.PutRegion(source)
			fit := tc.GetRuleManager().FitRegionWithoutCache(tc, source)
			require.Len(t, fit.OrphanPeers, 1)
			require.Equal(t, uint64(4), fit.OrphanPeers[0].GetStoreId())
			ops := c.CheckRegion(source)
			if !isolated {
				// Reject the conflicting orphan replacement. RuleChecker may
				// still improve the layout through another peer move.
				for _, op := range ops {
					require.NotEqual(t, "replace-down-peer-with-orphan-peer", op.Desc())
					for i := range op.Len() {
						if step, ok := op.Step(i).(operator.PromoteLearner); ok {
							require.NotEqual(t, uint64(4), step.ToStore)
						}
					}
				}
				return
			}
			require.Len(t, ops, 1)
			require.Equal(t, "replace-down-peer-with-orphan-peer", ops[0].Desc())
			require.Equal(t, 2, ops[0].Len())
			require.IsType(t, operator.PromoteLearner{}, ops[0].Step(0))
			require.IsType(t, operator.RemovePeer{}, ops[0].Step(1))
			result := operator.ApplyOperatorStep(source, ops[0])
			result = operator.ApplyOperatorStep(result, ops[0])
			require.Nil(t, result.GetStorePeer(3))
			require.Equal(t, metapb.PeerRole_Voter, result.GetStorePeer(4).GetRole())
			require.Len(t, result.GetPeers(), 3)
		})
	}
}

func TestControllerOrphanIsolationAcrossLevels(t *testing.T) {
	c, tc := newIsolationTestController(t)
	// Replacing store 3 with store 5 improves the zone score (3:1 to 2:2),
	// but puts two voters on host d. The controller must not promote it.
	for i, host := range []string{"a", "b", "c", "d", "d", "e"} {
		zone := "a"
		if i >= 3 {
			zone = "b"
		}
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": zone, "host": host})
	}
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.Count = 4
	rule.LocationLabels = []string{"zone", "host"}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	r := tc.AddLeaderRegion(1, 1, 2, 3, 4)
	r = r.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}), core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(3), DownSeconds: 1}}))
	tc.SetStoreDisconnect(3)
	tc.PutRegion(r)
	fit := tc.GetRuleManager().FitRegionWithoutCache(tc, r)
	require.Len(t, fit.OrphanPeers, 1)
	require.Equal(t, uint64(5), fit.OrphanPeers[0].GetStoreId())
	// Isolate the affected decision before checking the full controller path.
	op, err := c.ruleChecker.fixOrphanPeers(r, fit)
	require.NoError(t, err)
	require.Nil(t, op)
	for _, op := range c.CheckRegion(r) {
		require.NotEqual(t, "replace-down-peer-with-orphan-peer", op.Desc())
		for i := range op.Len() {
			if step, ok := op.Step(i).(operator.PromoteLearner); ok {
				require.NotEqual(t, uint64(5), step.ToStore)
			}
		}
	}
}
