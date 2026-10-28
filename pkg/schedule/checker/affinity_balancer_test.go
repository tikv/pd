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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
)

func (f *affinityBalanceTest) settle() {
	f.tb.Helper()
	re := require.New(f.tb)
	for _, region := range f.GetRegions() {
		group, _ := f.affinityManager.GetRegionAffinityGroupState(region)
		aligned := cloneRegionWithReplacePeerStores(region, group.LeaderStoreID, group.VoterStoreIDs...)
		f.PutRegion(aligned)
		f.affinityManager.GetAndCacheRegionAffinityGroupState(aligned)
		re.Equal(affinity.PhaseStable, f.affinityManager.GetAffinityGroupState(group.ID).Phase)
	}
}

func TestAffinityBalancerIdleLifecycle(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	ids := f.seed("100", 1, 1, 1, 2)
	f.policy(affinity.BalancePolicyFixed, ids[1:]...)
	before := f.snapshot()
	written := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceWritten])
	plannedTargets := testutil.ToFloat64(affinityBalanceTargetsPlanned)
	writtenTargets := testutil.ToFloat64(affinityBalanceTargetsWritten)
	stops := testutil.ToFloat64(affinityBalancePlannerStops[balanceplan.StopBound])
	re.True(f.step())
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceWritten])-written)
	re.Equal(1.0, testutil.ToFloat64(affinityBalanceTargetsPlanned)-plannedTargets)
	re.Equal(1.0, testutil.ToFloat64(affinityBalanceTargetsWritten)-writtenTargets)
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlannerStops[balanceplan.StopBound])-stops)
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable))
	f.settle()
	b, m := f.balancer, f.affinityManager
	b.enqueue(ids[0])
	for _, id := range []uint64{4, 5} {
		// An optimal table is idle; a new store alone does not invalidate it.
		noImprovement := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])
		re.False(b.balance(t.Context()))
		re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])-noImprovement)
		re.Equal(1.0, testutil.ToFloat64(affinityBalanceTablesRegistered))
		re.Equal(1.0, testutil.ToFloat64(affinityBalanceTablesIdle))
		idle := b.idle[balanceTestTable]
		version, _ := m.GetBalanceVersion(balanceTestTable)
		re.Equal(version, idle.version)
		re.WithinDuration(time.Now().Add(5*time.Minute), idle.until, 2*time.Second)
		f.AddRegionStore(id, 10)
		f.SetStoreUp(id)
		re.False(b.balance(t.Context()))
		re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])-noImprovement)
		re.Equal(idle, b.idle[balanceTestTable])
		if id == 4 {
			f.policy(affinity.BalancePolicyFixed, ids[0])
			f.policy(affinity.BalancePolicyAuto, ids[0])
		} else {
			idle.until = time.Now().Add(-time.Second)
			b.idle[balanceTestTable] = idle
		}
		re.True(b.balance(t.Context()))
		re.Positive(m.GetBalanceCounts().Voter[id])
		f.settle()
	}
	for _, group := range before.Groups[1:] {
		re.Equal(group.Group, m.GetAffinityGroupState(group.ID).Group)
	}
	re.False(b.balance(t.Context()))
	re.NoError(m.DeleteAffinityGroups(ids, true))
	re.False(b.balance(t.Context()))
	re.Empty(b.tables)
	re.Empty(b.idle)
	re.Zero(testutil.ToFloat64(affinityBalanceTablesRegistered))
	re.Zero(testutil.ToFloat64(affinityBalanceTablesIdle))
}

func TestAffinityBalancerBatchAndDisabledTables(t *testing.T) {
	for _, reason := range []string{"limit", "paused", "halted", "placement rules disabled"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 3)
			for i, table := range []string{"100", "200"} {
				ids := f.seed(table, uint64(2*i+1), 1, 1)
				f.balancer.enqueue(ids[0])
			}
			switch reason {
			case "limit":
				f.SetAffinityScheduleLimit(0)
			case "paused":
				f.PauseOrResume(60)
			case "halted":
				f.SetHaltScheduling(true, "test")
			case "placement rules disabled":
				f.SetPlacementRuleEnabled(false)
			}
			disabled := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceDisabled])
			re.False(f.balancer.balance(t.Context()))
			re.False(f.step())
			re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceDisabled])-disabled)
			re.Equal(2.0, testutil.ToFloat64(affinityBalanceTablesRegistered))
			re.Len(f.balancer.tables, 2)
			re.Equal(map[uint64]int{1: 4}, f.affinityManager.GetBalanceCounts().Leader)
			f.SetAffinityScheduleLimit(4)
			f.PauseOrResume(0)
			f.SetHaltScheduling(false, "test")
			f.SetPlacementRuleEnabled(true)
			re.True(f.balancer.balance(t.Context()))
			for _, table := range []affinity.TableID{balanceTestTable, "_tidb_pt_200"} {
				re.Equal([2]int{12, 2}, balanceTableScore(f.affinityManager, table))
			}
			re.Equal(map[uint64]int{1: 2, 2: 1, 3: 1}, f.affinityManager.GetBalanceCounts().Leader)
		})
	}
}

func TestAffinityBalancerCreating(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	f.seed("100", 1, 1)
	re.NoError(f.affinityManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "_tidb_pt_100_p1"}}))
	creating := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceCreating])
	re.False(f.step())
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceCreating])-creating)
	re.Empty(f.balancer.idle)
}
