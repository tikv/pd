// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newPlacementTestScatter(t testing.TB, rules bool, hosts []string) (*RegionScatterer, *mockcluster.Cluster, *core.RegionInfo) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	opt := mockconfig.NewTestOptions()
	opt.SetLocationLabels([]string{"host"})
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetEnablePlacementRules(rules)
	for i, host := range hosts {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
	}
	if rules {
		rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
		rule.LocationLabels = []string{"host"}
		rule.IsolationLevel = "host"
		require.NoError(t, tc.GetRuleManager().SetRule(rule))
	}
	streams := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), streams)
	sc := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	return sc, tc, tc.AddLeaderRegion(100, 1, 2, 3)
}

func TestScatterPreservesPlacement(t *testing.T) {
	for _, rules := range []bool{false, true} {
		for _, internal := range []bool{false, true} {
			for _, alternatives := range []bool{false, true} {
				t.Run(fmt.Sprintf("rules=%v/internal=%v/alternatives=%v", rules, internal, alternatives), func(t *testing.T) {
					hosts := []string{"A", "B", "C", "D", "D", "D"}
					if alternatives {
						hosts = append(hosts, "E", "F")
					}
					sc, tc, region := newPlacementTestScatter(t, rules, hosts)
					original := region.Clone()
					var state *scatterState
					counter := sc.ordinaryEngine.selectedPeer
					if internal {
						state = sc.newScatterState()
					}
					for id := uint64(1); id <= 3; id++ {
						for range 10 {
							if internal {
								state.ordinaryEngine.selectedPeer.Update("test", nil, []uint64{id})
							} else {
								counter.Put(id, "test")
							}
						}
					}
					op, err := sc.scatterRegionWithType(region, "test", true, internal, state)
					require.NoError(t, err)
					require.NotNil(t, op)
					stores := map[uint64]bool{1: true, 2: true, 3: true}
					for i := range op.Len() {
						switch step := op.Step(i).(type) {
						case operator.AddPeer:
							stores[step.ToStore] = true
						case operator.AddLearner:
							stores[step.ToStore] = true
						case operator.RemovePeer:
							delete(stores, step.FromStore)
						}
					}
					targetHosts := make(map[string]bool)
					moved := 0
					for id := range stores {
						targetHosts[tc.GetStore(id).GetLabelValue("host")] = true
						if id > 3 {
							moved++
						}
					}
					require.Len(t, stores, 3)
					require.Len(t, targetHosts, 3)
					if alternatives {
						require.Equal(t, 3, moved, "must continue selecting legal alternatives")
						require.True(t, targetHosts["D"] && targetHosts["E"] && targetHosts["F"])
					} else {
						require.Equal(t, 1, moved)
					}
					require.Equal(t, original.GetMeta(), region.GetMeta())
					require.Equal(t, original.GetLeader(), region.GetLeader())
				})
			}
		}
	}
}

func placementTargets(stores ...uint64) map[uint64]*metapb.Peer {
	targets := make(map[uint64]*metapb.Peer, len(stores))
	for _, id := range stores {
		targets[id] = &metapb.Peer{StoreId: id}
	}
	return targets
}

func TestScatterPlacementValidation(t *testing.T) {
	for _, rules := range []bool{false, true} {
		t.Run(strconv.FormatBool(rules), func(t *testing.T) {
			sc, tc, region := newPlacementTestScatter(t, rules, []string{"A", "B", "C", "D", "D", "D", "E", "F"})
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
			require.True(t, sc.scatterPlacementValid(region, placementTargets(4, 7, 8), 7))
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 7, 8), 9))
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 7, 99), 4))
			wrongRole := placementTargets(4, 7, 8)
			wrongRole[7].Role = metapb.PeerRole_Learner
			require.False(t, sc.scatterPlacementValid(region, wrongRole, 4))
			wrongWitness := placementTargets(4, 7, 8)
			wrongWitness[7].IsWitness = true
			require.False(t, sc.scatterPlacementValid(region, wrongWitness, 4))
			tc.SetStoreLabel(7, map[string]string{"host": "D"})
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 7, 8), 4))
		})
	}
}

func TestScatterRegionView(t *testing.T) {
	_, _, region := newPlacementTestScatter(t, true, []string{"A", "B", "C"})
	view := scatterRegionView(region, map[uint64]uint64{1: 2, 2: 4})
	require.Equal(t, uint64(2), view.GetLeader().GetStoreId())
	require.Equal(t, uint64(2), view.GetPeer(region.GetLeader().GetId()).GetStoreId())
	require.NotNil(t, view.GetStorePeer(4))
	require.Equal(t, uint64(1), region.GetLeader().GetStoreId())
	require.NotNil(t, region.GetStorePeer(2))
}

func BenchmarkScatterPlacementValidation(b *testing.B) {
	sc, _, region := newPlacementTestScatter(b, true, []string{"A", "B", "C", "D", "E", "F"})
	targets := placementTargets(4, 5, 6)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !sc.scatterPlacementValid(region, targets, 4) {
			b.Fatal("valid target rejected")
		}
	}
}

type placementChangingCluster struct {
	sche.SharedCluster
	onGetStore func(uint64)
}

func (c *placementChangingCluster) GetStore(id uint64) *core.StoreInfo {
	c.onGetStore(id)
	return c.SharedCluster.GetStore(id)
}

func TestScatterPlacementMetadataChanges(t *testing.T) {
	for _, changeRules := range []bool{false, true} {
		t.Run(strconv.FormatBool(changeRules), func(t *testing.T) {
			sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
			calls := 0
			sc.cluster = &placementChangingCluster{SharedCluster: tc, onGetStore: func(_ uint64) {
				calls++
				// The first six reads capture the original and target stores. Change
				// metadata while validating the captured StoreInfos, after both fits.
				if calls == 7 {
					if changeRules {
						rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
						rule.Count = 4
						require.NoError(t, tc.GetRuleManager().SetRule(rule))
					} else {
						tc.SetStoreLabel(4, map[string]string{"host": "E"})
					}
				}
			}}
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
			require.GreaterOrEqual(t, calls, 7)
		})
	}
}

func TestScatterFinalLeaderPlacement(t *testing.T) {
	sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.Role = placement.Leader
	rule.Count = 1
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"A", "D"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	followers := rule.Clone()
	followers.ID = "followers"
	followers.Role = placement.Follower
	followers.Count = 2
	followers.LabelConstraints = nil
	require.NoError(t, tc.GetRuleManager().SetRule(followers))
	require.True(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
	require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 5))
}

func TestScatterPlacementFailureAccounting(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(strconv.FormatBool(internal), func(t *testing.T) {
			sc, _, region := newPlacementTestScatter(t, true, []string{"A", "B", "C"})
			// Bypass the public replication precheck to exercise the final gate on
			// an invalid complete membership. It must run even for a no-op layout.
			region = region.Clone(core.WithRemoveStorePeer(3))
			var state *scatterState
			if internal {
				state = sc.newScatterState()
				state.ordinaryEngine.selectedPeer.InitGroupDistribution("test", map[uint64]uint64{1: 10, 2: 10})
			}
			for attempt := 1; attempt <= 2; attempt++ {
				op, err := sc.scatterRegionWithType(region, "test", true, internal, state)
				require.Error(t, err)
				require.Nil(t, op)
				for id := uint64(1); id <= 2; id++ {
					if internal {
						require.Equal(t, uint64(10), state.ordinaryEngine.selectedPeer.Get(id, "test"))
					} else {
						require.Equal(t, uint64(attempt), sc.ordinaryEngine.selectedPeer.Get(id, "test"))
					}
				}
			}
		})
	}
}

func TestScatterPreservesWitness(t *testing.T) {
	sc, _, region := newPlacementTestScatter(t, false, []string{"A", "B", "C", "D", "E", "F"})
	witness := *region.GetStorePeer(2)
	witness.IsWitness = true
	region = region.Clone(core.SetPeers([]*metapb.Peer{region.GetStorePeer(1), &witness, region.GetStorePeer(3)}))
	view := scatterRegionView(region, map[uint64]uint64{2: 5})
	require.True(t, view.GetStorePeer(5).GetIsWitness())
	sc.ordinaryEngine.selectedPeer.Put(2, "test")
	candidate := sc.selectNewPeer(sc.ordinaryEngine.asSelectionContext(), "test", &witness, nil, false)
	require.NotEqual(t, uint64(2), candidate.GetStoreId())
	require.True(t, candidate.GetIsWitness())
	targets := placementTargets(4, 5, 6)
	targets[5].IsWitness = true
	require.True(t, sc.scatterPlacementValid(region, targets, 4))
	require.False(t, sc.scatterPlacementValid(region, targets, 5))
}

func BenchmarkScatterPlacementBatch(b *testing.B) {
	for _, rules := range []bool{false, true} {
		b.Run(strconv.FormatBool(rules), func(b *testing.B) {
			sc, _, region := newPlacementTestScatter(b, rules, []string{"A", "B", "C", "D", "D", "D", "E", "F"})
			for id := uint64(1); id <= 3; id++ {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(id, "test")
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := sc.Scatter(region, "test", true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Fail allocation after placement validation, so the operator failure test
// does not depend on which peer is selected as leader.
type scatterAllocFailureCluster struct{ sche.SharedCluster }

func (*scatterAllocFailureCluster) AllocID(uint32) (uint64, uint32, error) {
	return 0, 0, errors.New("injected allocation failure")
}
