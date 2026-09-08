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
	"fmt"
	"reflect"
	"strconv"
	"sync"
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
	view := region.Clone()
	moveScatterPeer(view, region.GetStorePeer(1).GetId(), 2)
	moveScatterPeer(view, region.GetStorePeer(2).GetId(), 4)
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

func TestScatterRetriesLegalLeader(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(strconv.FormatBool(internal), func(t *testing.T) {
			sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
			rm := tc.GetRuleManager()
			leaderRule := rm.GetRule("pd", "default").Clone()
			leaderRule.Role = placement.Leader
			leaderRule.Count = 1
			leaderRule.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"A", "D"}}}
			require.NoError(t, rm.SetRule(leaderRule))
			voterRule := leaderRule.Clone()
			voterRule.ID = "voters"
			voterRule.Role = placement.Voter
			voterRule.Count = 2
			voterRule.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"B", "C", "E", "F"}}}
			require.NoError(t, rm.SetRule(voterRule))
			require.True(t, rm.FitRegion(tc, region).IsSatisfied())
			for attempt := range 32 {
				group := fmt.Sprintf("leader-retry-%d", attempt)
				var state *scatterState
				if internal {
					state = sc.newScatterState()
					state.ordinaryEngine.selectedPeer.InitGroupDistribution(group, map[uint64]uint64{1: 10, 2: 10, 3: 10})
					state.ordinaryEngine.selectedLeader.InitGroupDistribution(group, map[uint64]uint64{4: 100})
				} else {
					for id := uint64(1); id <= 3; id++ {
						for range 10 {
							sc.ordinaryEngine.selectedPeer.Put(id, group)
						}
					}
					for range 100 {
						sc.ordinaryEngine.selectedLeader.Put(4, group)
					}
				}
				// Lower-count voter targets are considered first, but only store 4
				// can lead the final membership. Retry without changing the peers.
				var op *operator.Operator
				var err error
				if internal {
					op, err = sc.scatterRegionWithType(region, group, false, true, state)
				} else {
					op, err = sc.Scatter(region, group, true)
				}
				require.NoError(t, err)
				require.NotNil(t, op)
				targets, leader := scatterOperatorTargets(t, region, op)
				require.Len(t, targets, 3)
				for _, id := range []uint64{4, 5, 6} {
					require.Contains(t, targets, id)
				}
				require.Equal(t, uint64(4), leader)
				require.True(t, sc.scatterPlacementValid(region, targets, leader))
			}
		})
	}
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
	view := region.Clone()
	moveScatterPeer(view, region.GetStorePeer(2).GetId(), 5)
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

func TestScatterOverlappingAndOverrideRules(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(strconv.FormatBool(override), func(t *testing.T) {
			sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
			rm := tc.GetRuleManager()
			first := rm.GetRule("pd", "default").Clone()
			first.GroupID = "overlap"
			first.ID = "first"
			first.Count = 2
			second := first.Clone()
			second.ID = "second"
			second.Count = 1
			require.NoError(t, rm.SetRule(first))
			require.NoError(t, rm.SetRule(second))
			if override {
				require.NoError(t, rm.SetRuleGroup(&placement.RuleGroup{ID: "overlap", Index: 10, Override: true}))
			} else {
				require.NoError(t, rm.DeleteRule("pd", "default"))
			}
			// Both rules match all stores. They do not define disjoint pools.
			require.True(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
			for id := uint64(1); id <= 3; id++ {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(id, "test")
				}
			}
			op, err := sc.Scatter(region, "test", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			require.True(t, sc.scatterPlacementValid(region, targets, leader))
			for id := range targets {
				require.Greater(t, id, uint64(3))
			}
		})
	}
}

func scatterOperatorTargets(t testing.TB, region *core.RegionInfo, op *operator.Operator) (map[uint64]*metapb.Peer, uint64) {
	t.Helper()
	targets := make(map[uint64]*metapb.Peer)
	for _, p := range region.GetPeers() {
		peer := *p
		targets[p.GetStoreId()] = &peer
	}
	leader := region.GetLeader().GetStoreId()
	if op == nil {
		return targets, leader
	}
	for i := range op.Len() {
		switch s := op.Step(i).(type) {
		case operator.AddPeer:
			targets[s.ToStore] = &metapb.Peer{Id: s.PeerID, StoreId: s.ToStore, IsWitness: s.IsWitness}
		case operator.AddLearner:
			targets[s.ToStore] = &metapb.Peer{Id: s.PeerID, StoreId: s.ToStore, Role: metapb.PeerRole_Learner, IsWitness: s.IsWitness}
		case operator.PromoteLearner:
			targets[s.ToStore].Role = metapb.PeerRole_Voter
		case operator.ChangePeerV2Enter:
			// Inspect the complete terminal membership below, after leaving joint state.
		case operator.ChangePeerV2Leave:
			for _, p := range s.PromoteLearners {
				targets[p.ToStore].Role = metapb.PeerRole_Voter
			}
			for _, p := range s.DemoteVoters {
				targets[p.ToStore].Role = metapb.PeerRole_Learner
			}
		case operator.RemovePeer:
			delete(targets, s.FromStore)
		case operator.TransferLeader:
			leader = s.ToStore
		default:
			t.Fatalf("unexpected step %T", s)
		}
	}
	return targets, leader
}

func TestScatterMixedLearnerAndWitness(t *testing.T) {
	for _, witness := range []bool{false, true} {
		t.Run(strconv.FormatBool(witness), func(t *testing.T) {
			sc, tc, _ := newPlacementTestScatter(t, true, []string{"A", "B", "C", "A", "D", "E", "F", "D"})
			tc.GetSharedConfig().SetEnableWitness(witness)
			for _, id := range []uint64{4, 8} {
				tc.SetStoreLabel(id, map[string]string{"host": "flash", "engine": "tiflash"})
			}
			rm := tc.GetRuleManager()
			normal := rm.GetRule("pd", "default").Clone()
			if witness {
				normal.Count = 2
				require.NoError(t, rm.SetRule(normal))
				wr := normal.Clone()
				wr.ID = "witness"
				wr.Count = 1
				wr.IsWitness = true
				require.NoError(t, rm.SetRule(wr))
			}
			flash := normal.Clone()
			flash.ID = "tiflash"
			flash.Role = placement.Learner
			flash.Count = 1
			flash.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}
			require.NoError(t, rm.SetRule(flash))
			region := tc.AddRegionWithLearner(101, 1, []uint64{2, 3}, []uint64{4})
			if witness {
				region = region.Clone(core.WithWitness(region.GetStorePeer(3).GetId()))
				tc.PutRegion(region)
			}
			for id := uint64(1); id <= 3; id++ {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(id, "test")
				}
			}
			for range 10 {
				sc.getOrCreateSpecialEngineContext("tiflash").selectedPeer.Put(4, "test")
			}
			op, err := sc.Scatter(region, "test", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			require.Len(t, targets, 4)
			require.True(t, sc.scatterPlacementValid(region, targets, leader))
			require.Equal(t, metapb.PeerRole_Learner, targets[8].GetRole())
			witnessCount := 0
			for id, p := range targets {
				require.Greater(t, id, uint64(4))
				if p.GetIsWitness() {
					witnessCount++
				}
			}
			if witness {
				require.Equal(t, 1, witnessCount)
			} else {
				require.Zero(t, witnessCount)
			}
			require.False(t, targets[leader].GetIsWitness())
		})
	}
}

func TestScatterDegradedMultilevelIsolation(t *testing.T) {
	sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
	for id := uint64(1); id <= 6; id++ {
		host := "same"
		if id == 3 || id == 6 {
			host = "other"
		}
		tc.SetStoreLabel(id, map[string]string{"zone": "z", "rack": "r", "host": host})
	}
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LocationLabels = []string{"zone", "rack", "host"}
	rule.IsolationLevel = "host"
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	// The source is already degraded. Equal isolation remains acceptable.
	require.True(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
	tc.SetStoreLabel(6, map[string]string{"zone": "z", "rack": "r", "host": "same"})
	require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
}

func newScatterBatchFixture(t testing.TB, storeCount, peerCount, ruleCount, regionCount int) (*RegionScatterer, []*core.RegionInfo) {
	t.Helper()
	hosts := make([]string, storeCount)
	for i := range hosts {
		hosts[i] = strconv.Itoa(i)
	}
	sc, tc, _ := newPlacementTestScatter(t, ruleCount > 0, hosts)
	if ruleCount > 0 {
		rm := tc.GetRuleManager()
		rule := rm.GetRule("pd", "default").Clone()
		rule.Count = peerCount - ruleCount + 1
		require.NoError(t, rm.SetRule(rule))
		for i := 1; i < ruleCount; i++ {
			next := rule.Clone()
			next.ID = "overlap-" + strconv.Itoa(i)
			next.Count = 1
			require.NoError(t, rm.SetRule(next))
		}
	} else {
		tc.SetMaxReplicas(peerCount)
	}
	followers := make([]uint64, peerCount-1)
	for i := range followers {
		followers[i] = uint64(i + 2)
	}
	regions := make([]*core.RegionInfo, regionCount)
	for i := range regions {
		tc.AddLeaderRegionWithRange(uint64(i+1000), fmt.Sprintf("%08d", i), fmt.Sprintf("%08d", i+1), 1, followers...)
		regions[i] = tc.GetRegion(uint64(i + 1000))
		for id := uint64(1); id <= uint64(peerCount); id++ {
			sc.ordinaryEngine.selectedPeer.Put(id, "batch")
		}
	}
	return sc, regions
}

func TestScatterDistinctRegionBatch(t *testing.T) {
	const regionCount = 1000
	sc, regions := newScatterBatchFixture(t, 64, 3, 1, regionCount)
	counts := make(map[uint64]int)
	for _, region := range regions {
		op, err := sc.Scatter(region, "batch", true)
		require.NoError(t, err)
		require.NotNil(t, op)
		peers, leader := scatterOperatorTargets(t, region, op)
		require.True(t, sc.scatterPlacementValid(region, peers, leader))
		for id := range peers {
			require.Greater(t, id, uint64(3))
			counts[id]++
		}
	}
	require.Len(t, counts, 61)
	minCount, maxCount := regionCount, 0
	for _, count := range counts {
		minCount = min(minCount, count)
		maxCount = max(maxCount, count)
	}
	require.LessOrEqual(t, maxCount-minCount, 1)
}

func BenchmarkScatterDistinctRegions(b *testing.B) {
	for _, c := range []struct{ stores, peers, rules int }{{64, 3, 0}, {64, 3, 1}, {64, 5, 3}, {256, 3, 1}} {
		b.Run(fmt.Sprintf("stores%d/peers%d/rules%d", c.stores, c.peers, c.rules), func(b *testing.B) {
			sc, regions := newScatterBatchFixture(b, c.stores, c.peers, c.rules, 10000)
			b.ReportAllocs()
			b.ResetTimer()
			i, ops := 0, 0
			for b.Loop() {
				op, err := sc.Scatter(regions[i%len(regions)], "batch", true)
				if err != nil {
					b.Fatal(err)
				}
				if op != nil {
					ops++
				}
				i++
			}
			b.ReportMetric(float64(ops)/float64(i), "operators/op")
		})
	}
}

func TestScatterConcurrentPlacementViews(t *testing.T) {
	sc, regions := newScatterBatchFixture(t, 64, 3, 1, 256)
	results := make(chan error, len(regions))
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := worker; i < len(regions); i += 8 {
				region := regions[i]
				original := region.Clone()
				op, err := sc.Scatter(region, "batch", true)
				if err != nil {
					results <- err
					continue
				}
				if op == nil {
					results <- errors.New("expected scatter operator")
					continue
				}
				// Every fixture store has its own host. Exactly three terminal stores
				// are required, and the input must remain immutable across requests.
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
				if len(stores) != 3 || !reflect.DeepEqual(original.GetMeta(), region.GetMeta()) || !reflect.DeepEqual(original.GetLeader(), region.GetLeader()) {
					results <- errors.New("invalid target or mutated source")
					continue
				}
				results <- nil
			}
		}()
	}
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
}

func TestScatterPlacementConfigChanges(t *testing.T) {
	for _, witnessFlag := range []bool{false, true} {
		t.Run(strconv.FormatBool(witnessFlag), func(t *testing.T) {
			sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D", "E", "F"})
			calls := 0
			sc.cluster = &placementChangingCluster{SharedCluster: tc, onGetStore: func(_ uint64) {
				calls++
				if calls == 7 {
					if witnessFlag {
						tc.GetSharedConfig().SetEnableWitness(true)
					} else {
						tc.SetEnablePlacementRules(false)
					}
				}
			}}
			require.False(t, sc.scatterPlacementValid(region, placementTargets(4, 5, 6), 4))
		})
	}
}

func TestScatterBatchRequestAccounting(t *testing.T) {
	const regionCount = 1000
	sc, regions := newScatterBatchFixture(t, 64, 3, 1, regionCount)
	ids := make([]uint64, len(regions))
	for i, region := range regions {
		ids[i] = region.GetID()
	}
	count, failures, err := sc.ScatterRegionsByID(ids, "batch", 0, true)
	require.NoError(t, err)
	require.Equal(t, regionCount, count+len(failures))
	require.Positive(t, count)
	for _, region := range regions {
		op := sc.opController.GetOperator(region.GetID())
		if _, failed := failures[region.GetID()]; failed {
			require.Nil(t, op)
			continue
		}
		require.NotNil(t, op)
		peers, leader := scatterOperatorTargets(t, region, op)
		require.True(t, sc.scatterPlacementValid(region, peers, leader))
	}
}

func TestScatterRetainsReservedWitness(t *testing.T) {
	sc, tc, region := newPlacementTestScatter(t, true, []string{"A", "B", "C", "D"})
	tc.GetSharedConfig().SetEnableWitness(true)
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.Count = 2
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	witness := rule.Clone()
	witness.ID = "witness"
	witness.Count = 1
	witness.IsWitness = true
	require.NoError(t, tc.GetRuleManager().SetRule(witness))
	region = region.Clone(core.WithWitness(region.GetStorePeer(2).GetId()))
	tc.PutRegion(region)
	for _, internal := range []bool{false, true} {
		// Go map iteration can visit the witness before or after a voter. Both
		// orders must preserve the reserved peer's attributes.
		for attempt := range 32 {
			group := fmt.Sprintf("reserved-%t-%d", internal, attempt)
			var state *scatterState
			if internal {
				state = sc.newScatterState()
				state.ordinaryEngine.selectedPeer.InitGroupDistribution(group, map[uint64]uint64{1: 10, 3: 10, 4: 1})
			} else {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(1, group)
					sc.ordinaryEngine.selectedPeer.Put(3, group)
				}
				sc.ordinaryEngine.selectedPeer.Put(4, group)
			}
			op, err := sc.scatterRegionWithType(region, group, true, internal, state)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			require.True(t, targets[2].GetIsWitness())
			require.Contains(t, targets, uint64(4))
			require.True(t, sc.scatterPlacementValid(region, targets, leader))
		}
	}
}
