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
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func (f *affinityBalanceTest) settle() {
	f.tb.Helper()
	re := require.New(f.tb)
	groups := make(map[string]struct{})
	for _, region := range f.GetRegions() {
		group, _ := f.affinityManager.GetRegionAffinityGroupState(region)
		aligned := cloneRegionWithReplacePeerStores(region, group.LeaderStoreID, group.VoterStoreIDs...)
		f.PutRegion(aligned)
		f.affinityManager.GetAndCacheRegionAffinityGroupState(aligned)
		groups[group.ID] = struct{}{}
	}
	// A group with several Regions is stable once all of them are aligned.
	for id := range groups {
		re.Equal(affinity.PhaseStable, f.affinityManager.GetAffinityGroupState(id).Phase)
	}
}

func (f *affinityBalanceTest) assertConverged() {
	f.tb.Helper()
	re := require.New(f.tb)
	f.settle()
	snapshot := f.snapshot()
	stable, err := f.planBalanceTargets(f.tb.Context(), snapshot)
	re.NoError(err)
	re.Empty(stable.targets)
	re.Zero(stable.planner.Changed)
	re.False(f.step())
	re.Equal(snapshot.Version, f.snapshot().Version)
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
	stops := testutil.ToFloat64(affinityBalancePlannerStops[balanceplan.StopGreedy])
	re.True(f.step())
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceWritten])-written)
	re.Equal(1.0, testutil.ToFloat64(affinityBalanceTargetsPlanned)-plannedTargets)
	re.Equal(1.0, testutil.ToFloat64(affinityBalanceTargetsWritten)-writtenTargets)
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlannerStops[balanceplan.StopGreedy])-stops)
	re.Equal([2]int{27, 3}, balanceTableScore(f.affinityManager, balanceTestTable()))
	f.settle()
	b, m := f.balancer, f.affinityManager
	b.enqueue(ids[0])
	for _, id := range []uint64{4, 5} {
		// An optimal table is idle; a new store alone does not invalidate it.
		noImprovement := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])
		re.False(b.balance(t.Context()))
		re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])-noImprovement)
		idle := b.idle[balanceTestTable()]
		version, _ := m.GetBalanceVersion(balanceTestTable())
		re.Equal(version, idle.version)
		re.WithinDuration(time.Now().Add(5*time.Minute), idle.until, 2*time.Second)
		f.AddRegionStore(id, 10)
		f.SetStoreUp(id)
		re.False(b.balance(t.Context()))
		re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceNoImprovement])-noImprovement)
		re.Equal(idle, b.idle[balanceTestTable()])
		if id == 4 {
			f.policy(affinity.BalancePolicyFixed, ids[0])
			f.policy(affinity.BalancePolicyAuto, ids[0])
		} else {
			idle.until = time.Now().Add(-time.Second)
			b.idle[balanceTestTable()] = idle
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
			re.Len(f.balancer.tables, 2)
			re.Equal(map[uint64]int{1: 4}, f.affinityManager.GetBalanceCounts().Leader)
			f.SetAffinityScheduleLimit(4)
			f.PauseOrResume(0)
			f.SetHaltScheduling(false, "test")
			f.SetPlacementRuleEnabled(true)
			re.True(f.balancer.balance(t.Context()))
			for _, table := range []affinity.TableID{balanceTestTable(), affinity.TableID(balanceTestGroupID("_tidb_pt_200"))} {
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
	re.NoError(f.affinityManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: balanceTestGroupID("_tidb_pt_100_p2")}}))
	creating := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceCreating])
	re.False(f.step())
	re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceCreating])-creating)
	re.Empty(f.balancer.idle)
}

func TestAffinityBalancerSkippedAndPartialWrites(t *testing.T) {
	for _, outcome := range []string{"skipped", "changed", "partial"} {
		t.Run(outcome, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 3)
			ids := f.seed("100", 1, 1, 1, 1)
			f.balancer.enqueue(ids[0])
			counts, calls := f.affinityManager.GetBalanceCounts(), 0
			f.affinityManager.SetTargetWriter(balanceExecutionTargetWriter{write: func(targets []affinity.GroupTarget) ([]string, error) {
				calls++
				if outcome == "partial" {
					return []string{targets[0].Expected.ID}, errors.New("second transaction failed")
				}
				if outcome == "changed" {
					f.policy(affinity.BalancePolicyFixed, ids[0])
				}
				return nil, nil
			}})
			written, err := f.balancer.step(t.Context(), balanceTestTable(), counts)
			re.Equal(outcome == "partial", written)
			if outcome == "partial" {
				re.ErrorContains(err, "second transaction failed")
				re.Equal(2, counts.Leader[1])
				re.Equal(1, counts.Leader[2]+counts.Leader[3])
			} else {
				re.NoError(err)
			}
			re.Empty(f.balancer.idle)
			if outcome == "skipped" {
				_, err := f.balancer.step(t.Context(), balanceTestTable(), counts)
				re.NoError(err)
				re.Equal(2, calls, "a benign skip does not freeze the table")
			}
		})
	}
}

func TestAffinityBalancerDisabledAtCommit(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	f.seed("100", 1, 1, 1, 1)
	const fp = "github.com/tikv/pd/pkg/schedule/checker/beforeAffinityBalanceCommit"
	re.NoError(failpoint.EnableCall(fp, func() { f.SetAffinityScheduleLimit(0) }))
	t.Cleanup(func() { re.NoError(failpoint.Disable(fp)) })
	disabled := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceDisabled])
	re.False(f.step())
	re.Equal(disabled+1, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceDisabled]))
	re.Empty(f.balancer.idle)
	re.NoError(failpoint.Disable(fp))
	f.SetAffinityScheduleLimit(4)
	re.True(f.step())
}

// Capacity expansion with two Regions per group and distinct learners.
func TestAffinityBalanceCapacityExpansion(t *testing.T) {
	f := newAffinityBalanceTest(t, 0)
	addStore := func(id uint64, z int, blocked bool) {
		f.AddRegionStore(id, 100+int(id%101))
		f.SetStoreLastHeartbeatInterval(id, -time.Hour)
		f.SetStoreLabel(id, map[string]string{"zone": fmt.Sprintf("z%d", z)})
		store := f.GetStore(id)
		capacity := uint64(1) << 40
		available := capacity - store.GetStoreStats().GetUsedSize()
		if blocked {
			available = uint64(1)
		}
		f.PutStore(store.Clone(core.SetStoreStats(&pdpb.StoreStats{Capacity: capacity, Available: available, UsedSize: capacity - available})))
		if blocked {
			require.True(t, f.GetStore(id).IsLowSpace(f.GetLowSpaceRatio()))
		}
	}
	first := []uint64{0, 0, 0}
	for s := range 18 {
		z := s / 6
		addStore(uint64(s+1), z, s%5 == 4)
		if first[z] == 0 {
			first[z] = uint64(s + 1)
		}
	}
	addStore(1001, 1, false)
	addStore(1002, 2, false)
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "leader", Index: 1, Override: true, Role: placement.Leader, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z0"}}}}))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "voter", Index: 2, Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Index: 3, Role: placement.Learner, Count: 1}))
	for g := range 60 {
		id := uint64(2*g + 1)
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1)), id, first[0], first, fmt.Sprintf("k%04da", g), fmt.Sprintf("k%04db", g), fmt.Sprintf("k%04dc", g))
		for j := range 2 {
			r := f.GetRegion(id + uint64(j))
			f.PutRegion(r.Clone(core.WithAddPeer(&metapb.Peer{Id: 10000 + r.GetID(), StoreId: 1001 + uint64(j), Role: metapb.PeerRole_Learner})))
		}
	}
	for phase := range 2 {
		if phase == 1 {
			for s := range 12 {
				z := s / 4
				addStore(uint64(19+s), z, s%5 == 4)
			}
		}
		snapshot := f.snapshot()
		input := f.balancePlanInput(snapshot, nil, nil)
		for _, rule := range input.input.Groups[0].Rules {
			if rule.Count == 2 {
				require.Len(t, rule.Stores, []int{17, 27}[phase])
			}
		}
		result, err := f.planBalanceTargets(t.Context(), snapshot)
		require.NoError(t, err)
		require.NotEmpty(t, result.targets)
		require.True(t, result.planner.Score.Less(result.planner.Baseline))
		require.Greater(t, result.rounds, 1)
		require.LessOrEqual(t, result.rounds, maxBalanceValidationRounds)
		require.Equal(t, balanceplan.StopGreedy, result.planner.Stop)
		written, _, err := f.commitBalancePlan(t.Context(), snapshot, result, f.ruleManager.GetRevision())
		require.NoError(t, err)
		require.Len(t, written, len(result.targets))
		if phase == 1 {
			f.assertConverged()
		} else {
			f.settle()
		}
	}
}

// Validate 1000 groups on 300 stores in five zones, with both numbering and rule orders.
func TestAffinityBalanceAtScale(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		roundRobin := reverse
		t.Run(fmt.Sprintf("roundRobin=%v/reverse=%v", roundRobin, reverse), func(t *testing.T) {
			const stores, groups = 300, 1000
			f := newAffinityBalanceTest(t, stores)
			first := make([]uint64, 5)
			for s := range stores {
				z, id := s/60, uint64(s+1)
				if roundRobin {
					z = s % 5
				}
				f.UpdateRegionCount(id, 10+(s*7919)%1009)
				store := f.GetStore(id)
				capacity := uint64(1) << 40
				used := store.GetStoreStats().GetUsedSize()
				f.PutStore(store.Clone(core.SetStoreStats(&pdpb.StoreStats{Capacity: capacity, UsedSize: used, Available: capacity - used})))
				f.SetStoreLabel(id, map[string]string{"zone": fmt.Sprintf("z%d", z)})
				if first[z] == 0 {
					first[z] = id
				}
			}
			li, vi := 1, 2
			if reverse {
				li, vi = 2, 1
			}
			require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "leader", Index: li, Override: !reverse,
				Role: placement.Leader, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z0"}}}}))
			require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "voter", Index: vi, Override: reverse,
				Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
			for i := range groups {
				f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)), uint64(i+1), first[0], first[:3], fmt.Sprintf("k%04d", i), fmt.Sprintf("k%04d", i+1))
			}
			snapshot := f.snapshot()
			result, err := f.planBalanceTargets(t.Context(), snapshot)
			require.NoError(t, err)
			require.Equal(t, balanceplan.Score{Voter: 33400, Leader: 16680}, result.planner.Score)
			require.NotEmpty(t, result.targets)
			require.Equal(t, 1, result.rounds)
			require.Equal(t, balanceplan.StopGreedy, result.planner.Stop)
			require.False(t, result.retry)
			written, _, err := f.commitBalancePlan(t.Context(), snapshot, result, f.ruleManager.GetRevision())
			require.NoError(t, err)
			require.Len(t, written, len(result.targets))
			f.assertConverged()
		})
	}
}

func TestAffinityBalancerCancellationAndNotReadyAreSkipped(t *testing.T) {
	for _, outcome := range []string{"planning canceled", "write canceled", "not ready", "error"} {
		t.Run(outcome, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 3)
			ids := f.seed("100", 1, 1, 1)
			f.balancer.enqueue(ids[0])
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if outcome == "planning canceled" {
				cancel()
			}
			f.affinityManager.SetTargetWriter(balanceExecutionTargetWriter{write: func([]affinity.GroupTarget) ([]string, error) {
				switch outcome {
				case "write canceled":
					cancel()
					return nil, ctx.Err()
				case "not ready":
					return nil, errors.Annotate(affinity.ErrTargetWriterNotReady, "PD leader missing")
				default:
					return nil, errors.New("write failed")
				}
			}})
			core, logs := observer.New(zap.WarnLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
			before := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceError])
			re.False(f.balancer.balance(ctx))
			delta := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceError]) - before
			if outcome == "error" {
				re.Equal(1.0, delta)
				re.Equal(1, logs.FilterMessage("failed to balance affinity table").Len())
			} else {
				re.Zero(delta)
				re.Zero(logs.FilterMessage("failed to balance affinity table").Len())
			}
			re.Empty(f.balancer.idle)
		})
	}
}

func TestAffinityBalancerHonorsUnsupportedTargetCooldown(t *testing.T) {
	for _, source := range []string{"first-win", "balancer"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				f := newAffinityBalanceTest(t, 3)
				m := f.affinityManager
				ids := f.seed("100", 1, 1, 1, 1)
				f.balancer.enqueue(ids[0])
				other := f.seed("200", 4, 1, 1, 1)
				f.balancer.enqueue(other[0])

				var calls int
				m.SetTargetWriter(balanceExecutionTargetWriter{write: func([]affinity.GroupTarget) ([]string, error) {
					calls++
					return nil, errors.Annotate(affinity.ErrTargetWriterUnsupported, "404 Not Found")
				}})
				core, logs := observer.New(zap.WarnLevel)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
				failures := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceError])

				const groupID = "new-group"
				re.NoError(m.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID}}))
				m.ObserveAvailableRegion(f.GetRegion(1), m.GetAffinityGroupState(groupID))
				if source == "first-win" {
					time.Sleep(50 * time.Millisecond)
					synctest.Wait()
					re.True(m.GetAffinityGroupState(groupID).HasUnpersistedTarget())
					re.Equal(1, calls)
				}
				re.False(f.balancer.balance(t.Context()))
				re.Equal(1, calls, "a single 404 covers both tables")

				// In the balancer-first case, the pending observation can now
				// initialize locally without issuing another request.
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				re.True(m.GetAffinityGroupState(groupID).HasUnpersistedTarget())
				re.Equal(1, calls)
				re.Empty(f.balancer.idle, "a skipped write must be retried after the cooldown")

				time.Sleep(time.Second)
				re.False(f.balancer.balance(t.Context()))
				re.Equal(2, calls, "the first table can probe again after the cooldown")
				re.Equal(failures, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceError]))
				re.Zero(logs.Len())
				for _, id := range append(ids, other...) {
					state := m.GetAffinityGroupState(id)
					re.Equal(uint64(1), state.LeaderStoreID)
					re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
					re.False(state.HasUnpersistedTarget())
				}
			})
		})
	}
}
