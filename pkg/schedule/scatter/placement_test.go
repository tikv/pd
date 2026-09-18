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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
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
		// Keep fixture stores connected during slow CI runs without heartbeats.
		tc.SetStoreLastHeartbeatInterval(uint64(i+1), -10*time.Minute)
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

func TestScatterPlacementLabelCase(t *testing.T) {
	for _, hosts := range [][]string{
		{"A", "a", "B", "A"},
		{"Σ", "ς", "B", "Σ"},
	} {
		t.Run(hosts[1], func(t *testing.T) {
			sc, _, region := newPlacementTestScatter(t, true, hosts)
			// The first two peers already share a host under CompareLocation.
			// Replacing the second peer with an equivalent label must remain valid.
			for range 10 {
				sc.ordinaryEngine.selectedPeer.Put(2, "case")
			}
			op, err := sc.Scatter(region, "case", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			assertScatterMembership(t, region, targets, leader)
			require.Contains(t, targets, uint64(4))
			require.NotContains(t, targets, uint64(2))
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
			for id := uint64(1); id <= 3; id++ {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(id, "test")
				}
			}
			op, err := sc.Scatter(region, "test", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			assertScatterMembership(t, region, targets, leader)
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
			targets[s.ToStore] = &metapb.Peer{Id: s.PeerID, StoreId: s.ToStore}
		case operator.AddLearner:
			targets[s.ToStore] = &metapb.Peer{Id: s.PeerID, StoreId: s.ToStore, Role: metapb.PeerRole_Learner}
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

func TestScatterMixedLearner(t *testing.T) {
	sc, tc, _ := newPlacementTestScatter(t, true, []string{"A", "B", "C", "A", "D", "E", "F", "D"})
	for _, id := range []uint64{4, 8} {
		tc.SetStoreLabel(id, map[string]string{"host": "flash", "engine": "tiflash"})
	}
	rm := tc.GetRuleManager()
	flash := rm.GetRule("pd", "default").Clone()
	flash.ID = "tiflash"
	flash.Role = placement.Learner
	flash.Count = 1
	flash.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}
	require.NoError(t, rm.SetRule(flash))
	region := tc.AddRegionWithLearner(101, 1, []uint64{2, 3}, []uint64{4})
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
	assertScatterMembership(t, region, targets, leader)
	require.Equal(t, metapb.PeerRole_Learner, targets[8].GetRole())
	for id := range targets {
		require.Greater(t, id, uint64(4))
	}
	require.Equal(t, metapb.PeerRole_Voter, targets[leader].GetRole())
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
		assertScatterMembership(t, region, peers, leader)
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
		assertScatterMembership(t, region, peers, leader)
	}
}

// newScatterTopologyFixture uses complete hierarchy paths and keeps original voters
// on stores 1..peerCount. Only store 1 is biased toward moving in the tests.
func newScatterTopologyFixture(t *testing.T, rules bool, labels []string, paths [][]string, peerCount int) (*RegionScatterer, *mockcluster.Cluster, *core.RegionInfo) {
	t.Helper()
	hosts := make([]string, len(paths))
	for i := range hosts {
		hosts[i] = strconv.Itoa(i)
	}
	sc, tc, _ := newPlacementTestScatter(t, rules, hosts)
	tc.SetLocationLabels(labels)
	tc.SetMaxReplicas(peerCount)
	for i, path := range paths {
		values := make(map[string]string)
		for j, key := range labels {
			values[key] = path[j]
		}
		tc.SetStoreLabel(uint64(i+1), values)
	}
	if rules {
		rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
		rule.Count, rule.LocationLabels, rule.IsolationLevel = peerCount, labels, ""
		require.NoError(t, tc.GetRuleManager().SetRule(rule))
	}
	followers := make([]uint64, peerCount-1)
	for i := range followers {
		followers[i] = uint64(i + 2)
	}
	return sc, tc, tc.AddLeaderRegion(200, 1, followers...)
}

func TestScatterLearnerIsolation(t *testing.T) {
	sc, tc, region := newScatterTopologyFixture(t, true, []string{"host"}, [][]string{{"a"}, {"b"}, {"c"}, {"a"}}, 3)
	region = region.Clone(core.WithRole(region.GetStorePeer(3).GetId(), metapb.PeerRole_Learner))
	rm := tc.GetRuleManager()
	rule := rm.GetRule("pd", "default").Clone()
	rule.Count = 2
	require.NoError(t, rm.SetRule(rule))
	learner := rule.Clone()
	learner.ID = "learner"
	learner.Role = placement.Learner
	learner.Count = 1
	require.NoError(t, rm.SetRule(learner))
	for range 10 {
		sc.ordinaryEngine.selectedPeer.Put(3, "host")
	}
	op, err := sc.scatterRegionWithType(region, "host", true, false, nil)
	require.NoError(t, err)
	require.NotNil(t, op)
	targets, _ := scatterOperatorTargets(t, region, op)
	require.Contains(t, targets, uint64(4))
	require.Equal(t, metapb.PeerRole_Learner, targets[4].GetRole())
}

func TestScatterReservesPeerRole(t *testing.T) {
	for _, tt := range []struct {
		name     string
		source   uint64
		reserved uint64
	}{
		{"voter-reserves-learner", 2, 3},
		{"learner-reserves-voter", 3, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sc, tc, region := newScatterTopologyFixture(t, true, []string{"host"}, [][]string{{"a"}, {"b"}, {"c"}, {"d"}}, 3)
			region = region.Clone(core.WithRole(region.GetStorePeer(3).GetId(), metapb.PeerRole_Learner))
			original := region.Clone()
			rm := tc.GetRuleManager()
			rule := rm.GetRule("pd", "default").Clone()
			rule.Count = 2
			require.NoError(t, rm.SetRule(rule))
			learner := rule.Clone()
			learner.ID, learner.Role, learner.Count = "learner", placement.Learner, 1
			require.NoError(t, rm.SetRule(learner))

			// Process the moving peer first, independently of map iteration order.
			order := []uint64{tt.source, tt.reserved, 1}
			next := 0
			const fp = "github.com/tikv/pd/pkg/schedule/scatter/scatterPeerOrder"
			require.NoError(t, failpoint.EnableCall(fp, func(peer **metapb.Peer) {
				require.Less(t, next, len(order))
				*peer = region.GetStorePeer(order[next])
				next++
			}))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })

			// Unique counts force the source to reserve the other role's store,
			// then the leader's store, before finding the free store 4.
			for storeID, count := range map[uint64]int{tt.source: 30, 1: 10, 4: 20} {
				for range count {
					sc.ordinaryEngine.selectedPeer.Put(storeID, "reservation")
				}
			}
			op, err := sc.Scatter(region, "reservation", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			require.Equal(t, len(order), next)
			targets, leader := scatterOperatorTargets(t, region, op)
			assertScatterMembership(t, region, targets, leader)
			require.NotContains(t, targets, tt.source)
			require.Contains(t, targets, tt.reserved)
			require.Contains(t, targets, uint64(1))
			require.Contains(t, targets, uint64(4))
			require.Equal(t, region.GetStorePeer(tt.reserved), targets[tt.reserved])
			require.Equal(t, region.GetStorePeer(1), targets[1])
			require.Equal(t, region.GetStorePeer(tt.source).GetRole(), targets[4].GetRole())
			require.Equal(t, original.GetMeta(), region.GetMeta())
			require.Equal(t, original.GetLeader(), region.GetLeader())
		})
	}
}

func BenchmarkScatterHostHierarchy(b *testing.B) {
	for _, peerCount := range []int{3, 7} {
		b.Run(fmt.Sprintf("peers%d", peerCount), func(b *testing.B) {
			sc, regions := newScatterBatchFixture(b, 64, peerCount, 1, 10000)
			tc := sc.cluster.(*mockcluster.Cluster)
			for id := uint64(1); id <= 64; id++ {
				tc.SetStoreLabel(id, map[string]string{
					"zone": strconv.Itoa(int(id-1) / 16),
					"rack": strconv.Itoa(int(id-1) / 4),
					"host": strconv.FormatUint(id, 10),
				})
			}
			rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
			rule.LocationLabels = []string{"zone", "rack", "host"}
			require.NoError(b, tc.GetRuleManager().SetRule(rule))
			b.ReportAllocs()
			b.ResetTimer()
			i, ops := 0, 0
			for b.Loop() {
				op, err := sc.Scatter(regions[i%len(regions)], "batch", true)
				require.NoError(b, err)
				if op != nil {
					ops++
				}
				i++
			}
			b.ReportMetric(float64(ops)/float64(i), "operators/op")
		})
	}
}

// Check the operator's terminal membership independently of the planning guard.
func assertScatterMembership(t testing.TB, region *core.RegionInfo, targets map[uint64]*metapb.Peer, leader uint64) {
	t.Helper()
	require.Len(t, targets, len(region.GetPeers()))
	var oldLearners, newLearners int
	for _, peer := range region.GetPeers() {
		if core.IsLearner(peer) {
			oldLearners++
		}
	}
	for id, peer := range targets {
		require.Equal(t, id, peer.GetStoreId())
		if core.IsLearner(peer) {
			newLearners++
		}
	}
	require.Equal(t, oldLearners, newLearners)
	require.Contains(t, targets, leader)
	require.False(t, core.IsLearner(targets[leader]))
}

func TestScatterAccumulatesFivePeers(t *testing.T) {
	for _, rules := range []bool{false, true} {
		t.Run(strconv.FormatBool(rules), func(t *testing.T) {
			hosts := []string{"A", "B", "C", "D", "E", "F", "F", "F", "F", "F", "G", "H", "I", "J"}
			sc, tc, _ := newPlacementTestScatter(t, rules, hosts)
			tc.SetMaxReplicas(5)
			if rules {
				rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
				rule.Count = 5
				require.NoError(t, tc.GetRuleManager().SetRule(rule))
			}
			region := tc.AddLeaderRegion(200, 1, 2, 3, 4, 5)
			for id := uint64(1); id <= 5; id++ {
				for range 10 {
					sc.ordinaryEngine.selectedPeer.Put(id, "five")
				}
			}
			op, err := sc.Scatter(region, "five", true)
			require.NoError(t, err)
			require.NotNil(t, op)
			targets, leader := scatterOperatorTargets(t, region, op)
			assertScatterMembership(t, region, targets, leader)
			selectedHosts := make(map[string]struct{})
			for id := range targets {
				require.Greater(t, id, uint64(5))
				selectedHosts[hosts[id-1]] = struct{}{}
			}
			require.Len(t, selectedHosts, 5)
		})
	}
}
