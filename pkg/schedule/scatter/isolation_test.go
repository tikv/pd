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
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
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
		require.False(t, sc.scatterIsolationChecker(source, target, false)(4))
		require.True(t, sc.scatterIsolationChecker(source, source, false)(1))
	}
	// Comparisons retain one label snapshot; rebuilding observes changed labels.
	check := sc.scatterIsolationChecker(source, source, false)
	tc.PutStoreWithLabels(2, "host", "a")
	require.True(t, check(1))
	// A rule change after candidate planning is seen by final validation.
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	tc.AddLabelsStore(7, 0, map[string]string{"host": "z"})
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"z"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	require.False(t, sc.scatterIsolationChecker(source, source, false)(1))
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
			check := sc.scatterIsolationChecker(region, region, false)
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
	require.True(t, sc.scatterIsolationChecker(source, source, false)(1))
	target := source.Clone(core.WithReplacePeerStore(2, 4))
	require.True(t, sc.scatterIsolationChecker(source, target, false)(1))
	require.Equal(t, metapb.PeerRole_Learner, target.GetStorePeer(3).Role)
}

func TestScatterForcePreservesIsolationWithoutRequiringLeaderRoles(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
			tc, sc := isolationTestScatter(t, []string{"a", "b", "c"})
			tc.SetEnablePlacementRules(true)
			rm := tc.GetRuleManager()
			leader := rm.GetRule("pd", "default").Clone()
			leader.Role, leader.Count = placement.Leader, 1
			require.NoError(t, rm.SetRule(leader))
			voters := leader.Clone()
			voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
			voters.LocationLabels, voters.IsolationLevel = []string{"host"}, "host"
			voters.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"b", "c"}}}
			require.NoError(t, rm.SetRule(voters))
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			group := "force-role-isolation"
			for id, count := range map[uint64]int{1: 10, 3: 1} {
				for range count {
					sc.ordinaryEngine.selectedLeader.Put(id, group)
				}
			}
			op, err := sc.scatterRegionWithType(region, group, false, internal, nil)
			require.NoError(t, err)
			if internal {
				require.Nil(t, op)
			} else {
				require.NotNil(t, op)
				_, target := finalPlacementAfterOperator(region, op)
				require.Equal(t, uint64(2), target)
			}
		})
	}
}

func TestScatterForceStillChecksAllPeers(t *testing.T) {
	tc, sc := isolationTestScatter(t, []string{"a", "b", "c", "c", "d"})
	tc.SetEnablePlacementRules(true)
	rm := tc.GetRuleManager()
	leader := rm.GetRule("pd", "default").Clone()
	leader.Role, leader.Count = placement.Leader, 1
	require.NoError(t, rm.SetRule(leader))
	voters := leader.Clone()
	voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
	voters.LocationLabels, voters.IsolationLevel = []string{"host"}, "host"
	voters.LabelConstraints = []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"b", "c"}}}
	require.NoError(t, rm.SetRule(voters))
	source := tc.AddLeaderRegion(1, 1, 2, 3)
	require.False(t, sc.scatterIsolationChecker(source, source, false)(2))
	require.True(t, sc.scatterIsolationChecker(source, source, true)(2))
	// Relaxing the leader rule must not hide host collisions or orphan peers.
	collision := source.Clone(core.WithReplacePeerStore(2, 4))
	require.False(t, sc.scatterIsolationChecker(source, collision, true)(4))
	orphan := source.Clone(core.WithReplacePeerStore(2, 5))
	require.False(t, sc.scatterIsolationChecker(source, orphan, true)(5))
	fewer := source.Clone(core.WithRemoveStorePeer(3))
	require.False(t, sc.scatterIsolationChecker(source, fewer, true)(2))
	learner := source.Clone()
	learner.GetStorePeer(2).Role = metapb.PeerRole_Learner
	require.False(t, sc.scatterIsolationChecker(source, learner, true)(1))
	witness := source.Clone()
	witness.GetStorePeer(2).IsWitness = true
	require.False(t, sc.scatterIsolationChecker(source, witness, true)(1))
	require.False(t, sc.scatterIsolationChecker(source, source, true)(99))
}

func TestScatterIsolationWithAllocatedPeerIDs(t *testing.T) {
	for _, joint := range []bool{false, true} {
		for _, degraded := range []bool{false, true} {
			t.Run(fmt.Sprintf("joint=%t/degraded=%t", joint, degraded), func(t *testing.T) {
				hosts := []string{"", "b", "b", "c", "", "c", "b"}
				zones := []string{"b", "", "a", "a", "c", "a", "a"}
				tc, sc := isolationTestScatter(t, hosts)
				tc.SetEnablePlacementRules(true)
				tc.SetEnableUseJointConsensus(joint)
				tc.SetMaxReplicas(6)
				for i, zone := range zones {
					if !degraded {
						zone = strconv.Itoa(i)
					}
					tc.PutStoreWithLabels(uint64(i+1), "zone", zone, "host", hosts[i])
				}
				rm := tc.GetRuleManager()
				first := rm.GetRule("pd", "default").Clone()
				first.Count, first.LocationLabels = 2, []string{"zone", "host"}
				require.NoError(t, rm.SetRule(first))
				second := first.Clone()
				second.ID, second.Count, second.IsolationLevel = "second", 4, "host"
				require.NoError(t, rm.SetRule(second))
				region := tc.AddLeaderRegion(1, 1, 2, 3, 4, 5, 6)
				const group = "allocated"
				// Move only peer 3 to store 7 and keep the leader on store 1.
				sc.ordinaryEngine.selectedPeer.Put(3, group)
				for id := uint64(2); id <= 7; id++ {
					sc.ordinaryEngine.selectedLeader.Put(id, group)
				}
				op, err := sc.Scatter(region, group, true)
				if degraded {
					// With old peer ID 3 the fit passes. With the new ID the
					// maximum conflict degree in the second rule grows from 1 to 2.
					require.ErrorIs(t, err, errs.ErrGetTargetStore)
					require.Nil(t, op)
				} else {
					require.NoError(t, err)
					require.NotNil(t, op)
					peers, leader := scatterOperatorTargets(t, region, op)
					require.NotContains(t, peers, uint64(3))
					require.Equal(t, uint64(7), peers[7].GetId())
					require.Equal(t, uint64(1), leader)
					var actual []*metapb.Peer
					for _, p := range peers {
						actual = append(actual, p)
					}
					final := region.Clone(core.SetPeers(actual), core.WithLeader(peers[leader]))
					require.True(t, sc.scatterIsolationChecker(region, final, true)(leader))
				}
				require.Equal(t, uint64(3), region.GetStorePeer(3).GetId())
				require.Nil(t, region.GetStorePeer(7))
			})
		}
	}
}

type scatterFailingAllocator struct {
	*mockcluster.Cluster
	calls int
	err   error
}

func (c *scatterFailingAllocator) AllocID(uint32) (uint64, uint32, error) {
	c.calls++
	return 0, 0, c.err
}

func TestScatterPeerIDAllocationFailure(t *testing.T) {
	for _, allocErr := range []error{errors.New("allocation failure"), nil} {
		t.Run(fmt.Sprintf("error=%v", allocErr), func(t *testing.T) {
			tc, sc := isolationTestScatter(t, []string{"a", "b", "c", "d"})
			tc.SetEnablePlacementRules(true)
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			allocator := &scatterFailingAllocator{Cluster: tc, err: allocErr}
			sc.cluster = allocator
			const group = "allocation"
			// Favor a move of peer 1 to store 4. On failure record the unchanged layout.
			sc.ordinaryEngine.selectedPeer.Put(1, group)
			op, err := sc.Scatter(region, group, true)
			require.ErrorIs(t, err, errs.ErrCreateOperator)
			require.Nil(t, op)
			require.Equal(t, 1, allocator.calls)
			require.EqualValues(t, 1, sc.ordinaryEngine.selectedPeer.Get(2, group))
			require.Zero(t, sc.ordinaryEngine.selectedPeer.Get(4, group))
			require.EqualValues(t, 1, region.GetLeader().GetId())
			require.Nil(t, region.GetStorePeer(4))
		})
	}
}

func TestDeferredInternalScatterDoesNotAllocatePeerIDs(t *testing.T) {
	for _, rules := range []bool{false, true} {
		t.Run(strconv.FormatBool(rules), func(t *testing.T) {
			sc, tc, region := newInternalScatterReadCPUTestFixture(t)
			tc.SetEnablePlacementRules(rules)
			setUnifiedReadPoolThreadCount(tc)
			setStoreReadCPU(tc, 1, 500)
			setStoreReadCPU(tc, 4, 450)
			allocator := &scatterFailingAllocator{Cluster: tc, err: errors.New("allocation failure")}
			sc.cluster = allocator
			op, err := sc.ScatterInternal(region, "deferred", []byte("a"), []byte("z"))
			require.ErrorIs(t, err, ErrInternalScatterBalancedReadCPU)
			require.Nil(t, op)
			require.Zero(t, allocator.calls)
		})
	}
}
