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

package schedulers

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newRangeIsolationScheduler(t *testing.T, oc *operator.Controller, rule core.Rule) *balanceRangeScheduler {
	t.Helper()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{rule.String(), core.EngineTiKV, "1h", "isolation", "", ""}))
	require.NoError(t, err)
	return scheduler.(*balanceRangeScheduler)
}

func TestBalanceRangeLeaderIsolation(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	for i, host := range []string{"a", "b", "a"} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
	}
	rm := tc.GetRuleManager()
	leader := rm.GetRule("pd", "default").Clone()
	leader.Count, leader.Role = 1, placement.Leader
	require.NoError(t, rm.SetRule(leader))
	voters := leader.Clone()
	voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
	voters.LocationLabels, voters.IsolationLevel = []string{"host"}, "host"
	require.NoError(t, rm.SetRule(voters))
	for i := 1; i <= 100; i++ {
		tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("a%03d", i), fmt.Sprintf("a%03d", i+1), 1, 2, 3)
	}
	// Store 2 has fewer leaders than store 3 and is tried first, but making
	// it leader puts the two peers assigned to the voter rule on host a.
	tc.AddLeaderRegionWithRange(101, "b001", "b002", 3, 1, 2)
	region := tc.GetRegion(1)
	before := rm.FitRegionWithoutCache(tc, region)
	require.True(t, before.IsSatisfied())
	require.True(t, before.RuleFits[1].IsIsolationSatisfied())
	sc := newRangeIsolationScheduler(t, oc, core.LeaderScatter)
	job := &balanceRangeSchedulerJob{Engine: core.EngineTiKV, Rule: core.LeaderScatter, Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("", "")}}
	require.NoError(t, sc.prepare(tc, operator.NewOpInfluence(), job))
	sc.solver.Source, sc.solver.Region, sc.solver.fit = tc.GetStore(1), region, before
	sc.solver.sourceScore = sc.score(1)
	for _, id := range []uint64{2, 3} {
		sc.solver.Target = tc.GetStore(id)
		sc.solver.targetScore = sc.score(id)
		require.True(t, sc.shouldBalance(sc.GetName()))
	}
	require.Equal(t, float64(0), sc.score(2))
	require.Equal(t, float64(1), sc.score(3))
	require.Nil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(2)}, nil))
	op := sc.transferPeer([]*core.StoreInfo{tc.GetStore(3), tc.GetStore(2)}, nil)
	require.NotNil(t, op)
	require.Equal(t, uint64(3), op.Step(0).(operator.TransferLeader).ToStore)
	ops, _ := sc.Schedule(tc, false)
	require.Len(t, ops, 1)
	require.Equal(t, uint64(3), ops[0].Step(0).(operator.TransferLeader).ToStore)
}

func TestBalanceRangeRetriesPeerIsolation(t *testing.T) {
	for _, joint := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-joint", true: "joint"}[joint], func(t *testing.T) {
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(true)
			tc.SetEnableUseJointConsensus(joint)
			for i, host := range []string{"b", "b", "a", "b", "a", "a"} {
				zone := "z1"
				if i == 4 {
					zone = "z2"
				}
				tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": zone, "host": host})
			}
			rm := tc.GetRuleManager()
			first := rm.GetRule("pd", "default").Clone()
			first.Count, first.LocationLabels = 2, []string{"zone"}
			require.NoError(t, rm.SetRule(first))
			second := first.Clone()
			second.ID = "second"
			second.LocationLabels, second.IsolationLevel = []string{"zone", "host"}, "host"
			require.NoError(t, rm.SetRule(second))
			for i := 1; i <= 100; i++ {
				tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("a%03d", i), fmt.Sprintf("a%03d", i+1), 1, 2, 3, 4)
			}
			region := tc.GetRegion(1)
			before := rm.FitRegionWithoutCache(tc, region)
			require.True(t, before.IsSatisfied())
			require.True(t, before.RuleFits[1].IsIsolationSatisfied())
			guard := filter.NewPlacementSafeguard("test", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(3), before)
			for _, id := range []uint64{5, 6} {
				require.True(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(id)).IsOK())
			}
			// Store 5 improves the first rule's zone score and is assigned to it.
			// Its final fit leaves the second rule's peers on one host. Store 6
			// preserves the source layout. Retry must follow the final fit result.
			_, err := operator.CreateMovePeerOperator("bad", tc, region, operator.OpRange, 3, &metapb.Peer{StoreId: 5})
			require.ErrorIs(t, err, operator.ErrTargetIsolation)
			_, err = operator.CreateMovePeerOperator("good", tc, region, operator.OpRange, 3, &metapb.Peer{StoreId: 6})
			require.NoError(t, err)
			sc := newRangeIsolationScheduler(t, oc, core.PeerScatter)
			job := &balanceRangeSchedulerJob{Engine: core.EngineTiKV, Rule: core.PeerScatter, Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("", "")}}
			require.NoError(t, sc.prepare(tc, operator.NewOpInfluence(), job))
			sc.solver.Source, sc.solver.Region, sc.solver.fit = tc.GetStore(3), region, before
			sc.solver.sourceScore = sc.score(3)
			for _, id := range []uint64{5, 6} {
				sc.solver.Target = tc.GetStore(id)
				sc.solver.targetScore = sc.score(id)
				require.True(t, sc.shouldBalance(sc.GetName()))
			}
			require.NotNil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(6)}, nil))
			require.Nil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(5)}, nil))
			op := sc.transferPeer([]*core.StoreInfo{tc.GetStore(6), tc.GetStore(5)}, nil)
			require.NotNil(t, op)
			added, removed := false, false
			for i := range op.Len() {
				switch step := op.Step(i).(type) {
				case operator.AddLearner:
					require.Equal(t, uint64(6), step.ToStore)
					added = true
				case operator.RemovePeer:
					require.Equal(t, uint64(3), step.FromStore)
					removed = true
				}
			}
			require.True(t, added)
			require.True(t, removed)
			after := rm.FitRegionWithoutCache(tc, region.Clone(core.WithReplacePeerStore(3, 6)))
			require.True(t, after.IsSatisfied())
			require.True(t, after.RuleFits[1].IsIsolationSatisfied())
		})
	}
}

func TestBalanceRangeLeaderToNewStore(t *testing.T) {
	for _, rulesEnabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "rules-disabled", true: "rules-enabled"}[rulesEnabled], func(t *testing.T) {
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(rulesEnabled)
			tc.SetLocationLabels([]string{"host"})
			for i, host := range []string{"a", "b", "c", "a", "b"} {
				tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
			}
			if rulesEnabled {
				rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
				rule.LocationLabels, rule.IsolationLevel = []string{"host"}, "host"
				require.NoError(t, tc.GetRuleManager().SetRule(rule))
			}
			for i := 1; i <= 10; i++ {
				tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("a%03d", i), fmt.Sprintf("a%03d", i+1), 1, 2, 3)
			}
			sc := newRangeIsolationScheduler(t, oc, core.LeaderScatter)
			job := &balanceRangeSchedulerJob{Engine: core.EngineTiKV, Rule: core.LeaderScatter, Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("", "")}}
			require.NoError(t, sc.prepare(tc, operator.NewOpInfluence(), job))
			region := tc.GetRegion(1)
			sc.solver.Source, sc.solver.Region = tc.GetStore(1), region
			sc.solver.sourceScore = sc.score(1)
			// Store 5 would reduce peer separation. Store 4 has no peer yet,
			// but can replace the source peer and become leader.
			require.Nil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(5)}, nil))
			op := sc.transferPeer([]*core.StoreInfo{tc.GetStore(4)}, nil)
			require.NotNil(t, op)
			var targetLeader uint64
			for i := range op.Len() {
				if step, ok := op.Step(i).(operator.TransferLeader); ok {
					targetLeader = step.ToStore
				}
			}
			require.Equal(t, uint64(4), targetLeader)
		})
	}
}

type rangeAllocationFailureCluster struct {
	*mockcluster.Cluster
	attempts int
}

func (c *rangeAllocationFailureCluster) AllocID(uint32) (uint64, uint32, error) {
	c.attempts++
	return 0, 0, errors.New("allocation unavailable")
}

func TestBalanceRangeStopsOnAllocationFailure(t *testing.T) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	for id := uint64(1); id <= 5; id++ {
		tc.AddLeaderStore(id, 0)
	}
	for i := 1; i <= 10; i++ {
		tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("a%03d", i), fmt.Sprintf("a%03d", i+1), 1, 2, 3)
	}
	sc := newRangeIsolationScheduler(t, oc, core.PeerScatter)
	job := &balanceRangeSchedulerJob{Engine: core.EngineTiKV, Rule: core.PeerScatter, Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("", "")}}
	require.NoError(t, sc.prepare(tc, operator.NewOpInfluence(), job))
	sc.solver.Source, sc.solver.Region = tc.GetStore(2), tc.GetRegion(1)
	sc.solver.sourceScore = sc.score(2)
	for _, id := range []uint64{4, 5} {
		require.NotNil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(id)}, nil))
	}
	// Both candidates work with the normal allocator. A service failure
	// must stop the attempt instead of calling the allocator for each store.
	broken := &rangeAllocationFailureCluster{Cluster: tc}
	require.NoError(t, sc.prepare(broken, operator.NewOpInfluence(), job))
	sc.solver.Source, sc.solver.Region = tc.GetStore(2), tc.GetRegion(1)
	sc.solver.sourceScore = sc.score(2)
	require.Nil(t, sc.transferPeer([]*core.StoreInfo{tc.GetStore(4), tc.GetStore(5)}, nil))
	require.Equal(t, 1, broken.attempts)
}
