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
	"encoding/hex"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/labeler"
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
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 3)
	ids := f.seed("100", 1, 1, 1, 2)
	f.policy(affinity.BalancePolicyFixed, ids[1:]...)
	f.waitForStoreScan(ids[0])
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
	now := time.Now()
	created := uint64(now.Unix())
	for i, group := range []affinity.Group{
		{CreateTimestamp: created}, // Only a new auto group without a target waits.
		{CreateTimestamp: created - 120},
		{CreateTimestamp: created, BalancePolicy: affinity.BalancePolicyFixed},
		{CreateTimestamp: created, VoterStoreIDs: []uint64{1, 2, 3}},
	} {
		snapshot := &affinity.BalanceSnapshot{Groups: []*affinity.GroupState{{Group: group}}}
		re.Equal(i == 0, balanceCreatingTable(snapshot, now), "group: %+v", group)
	}
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
			f.affinityManager.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
				calls++
				if outcome == "partial" {
					return affinity.GroupTargetsResponse{WrittenGroupIDs: []string{targets[0].Expected.ID}}, errors.New("second transaction failed")
				}
				if outcome == "changed" {
					f.policy(affinity.BalancePolicyFixed, ids[0])
				}
				return affinity.GroupTargetsResponse{}, nil
			}))
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

// Capacity expansion with two Regions per group and distinct learners.
func TestAffinityBalanceCapacityExpansion(t *testing.T) {
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 0)
	addStore := func(id uint64, z int, blocked bool) {
		f.AddRegionStore(id, 100+int(id%101))
		f.SetStoreUp(id)
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
	// A target loaded by the watcher is available after the first store scan.
	const probeID = "availability-probe"
	f.affinityManager.SyncGroupFromEtcd(&affinity.Group{ID: probeID, LeaderStoreID: first[0], VoterStoreIDs: first})
	require.Eventually(t, func() bool {
		return f.affinityManager.GetAffinityGroupState(probeID).AffinitySchedulingAllowed
	}, 2*time.Second, 10*time.Millisecond)
	for g := range 60 {
		id := uint64(2*g + 1)
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1)), id, first[0], first, fmt.Sprintf("k%04da", g), fmt.Sprintf("k%04db", g), fmt.Sprintf("k%04dc", g))
		for j := range 2 {
			r := f.GetRegion(id + uint64(j))
			f.PutRegion(r.Clone(core.WithAddPeer(&metapb.Peer{Id: 10000 + r.GetID(), StoreId: 1001 + uint64(j), Role: metapb.PeerRole_Learner})))
		}
	}
	f.affinityManager.SyncGroupDeleteFromEtcd(probeID)
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
			enableFastAffinityAvailabilityCheck(t)
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
			f.waitForStoreScan(balanceTestGroupID("_tidb_pt_100_p1"))
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
			f.affinityManager.SetTargetWriter(targetWriterFunc(func(context.Context, []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
				switch outcome {
				case "write canceled":
					cancel()
					return affinity.GroupTargetsResponse{}, ctx.Err()
				case "not ready":
					return affinity.GroupTargetsResponse{}, errors.Annotate(affinity.ErrTargetWriterNotReady, "PD leader missing")
				default:
					return affinity.GroupTargetsResponse{}, errors.New("write failed")
				}
			}))
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
				m.SetTargetWriter(targetWriterFunc(func(context.Context, []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
					calls++
					return affinity.GroupTargetsResponse{}, errors.Annotate(affinity.ErrTargetWriterUnsupported, "404 Not Found")
				}))
				core, logs := observer.New(zap.WarnLevel)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
				failures := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceError])

				const groupID = "new-group"
				re.NoError(m.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID}}))
				if source == "first-win" {
					m.ObserveAvailableRegion(f.GetRegion(1), m.GetAffinityGroupState(groupID))
					synctest.Wait()
					re.True(m.GetAffinityGroupState(groupID).HasUnpersistedTarget())
					re.Equal(1, calls)
				}
				re.False(f.balancer.balance(t.Context()))
				re.Equal(1, calls, "a single 404 covers both tables")

				if source == "balancer" {
					// Observe the group after the balancer has established the cooldown.
					m.ObserveAvailableRegion(f.GetRegion(1), m.GetAffinityGroupState(groupID))
					synctest.Wait()
				}
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

type balanceBenchShape struct {
	name            string
	groups, regions int
	improves        bool
}

// BenchmarkAffinityBalance covers execution of a useful plan, a pinned table,
// and late learner conflicts. The solver benchmarks cover placement topologies.
func BenchmarkAffinityBalance(b *testing.B) {
	level := log.GetLevel()
	log.SetLevel(zapcore.FatalLevel)
	b.Cleanup(func() { log.SetLevel(level) })
	for _, shape := range []balanceBenchShape{
		{name: "one_per_zone", groups: 1000, regions: 1, improves: true},
		{name: "no_improvement", groups: 1000, regions: 1},
		{name: "learner_conflicts", groups: 24, regions: 30, improves: true},
	} {
		b.Run(shape.name, func(b *testing.B) { benchmarkAffinityBalanceStep(b, shape) })
	}
}

func benchmarkAffinityBalanceStep(b *testing.B, shape balanceBenchShape) {
	groups, regions := shape.groups, shape.regions
	b.StopTimer()
	re := require.New(b)
	f := newAffinityBalanceTest(b, 300)
	for _, store := range f.GetStores() {
		id := store.GetID()
		labels := map[string]string{"zone": fmt.Sprintf("z%d", (id-1)%3)}
		if shape.name == "learner_conflicts" && id <= 30 {
			labels["pool"] = "voter"
		}
		if shape.name == "no_improvement" {
			if id == 1 {
				labels["pin"] = "leader"
			} else if id <= 3 {
				labels["pin"] = "follower"
			}
		}
		f.SetStoreLabel(id, labels)
	}
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"zone"}
	if shape.name == "no_improvement" {
		rule.Role, rule.Count, rule.Index, rule.Override = placement.Leader, 1, 1, true
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "pin", Op: placement.In, Values: []string{"leader"}}}
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "pin", Op: placement.In, Values: []string{"follower"}}}}))
	}
	if shape.name == "learner_conflicts" {
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "pool", Op: placement.In, Values: []string{"voter"}}}
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	}
	re.NoError(f.ruleManager.SetRule(rule))
	ids := make([]string, 0, groups)
	for g := range groups {
		id := balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1))
		keys := make([]string, 0, regions+1)
		for r := range regions + 1 {
			keys = append(keys, fmt.Sprintf("100-%04d-%05d", g, r))
		}
		firstRegion := uint64(g*regions + 1)
		f.group(id, firstRegion, 1, []uint64{1, 2, 3}, keys...)
		for r := 0; shape.name == "learner_conflicts" && r < regions; r++ {
			store := uint64(31 + r%270)
			if r >= regions-27 {
				store = uint64(4 + r - (regions - 27))
			}
			peer, err := f.AllocPeer(store)
			re.NoError(err)
			peer.Role = metapb.PeerRole_Learner
			region := f.GetRegion(firstRegion + uint64(r)).Clone(core.WithAddPeer(peer))
			f.PutRegion(region)
			f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
		}
		ids = append(ids, id)
	}
	stepRounds := func() float64 {
		var rounds float64
		for _, counter := range affinityBalancePlannerStops {
			rounds += testutil.ToFloat64(counter)
		}
		return rounds
	}
	var rounds, written float64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		for _, id := range ids {
			_, err := f.affinityManager.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
			re.NoError(err)
		}
		f.settle()
		clear(f.balancer.idle)
		before, writtenBefore := stepRounds(), testutil.ToFloat64(affinityBalanceTargetsWritten)
		b.StartTimer()
		re.Equal(shape.improves, f.step())
		b.StopTimer()
		rounds += stepRounds() - before
		written += testutil.ToFloat64(affinityBalanceTargetsWritten) - writtenBefore
	}
	b.ReportMetric(rounds/float64(b.N), "rounds/op")
	b.ReportMetric(written/float64(b.N), "targets/op")
}

func TestAffinityBalanceRetries(t *testing.T) {
	for _, reason := range []string{"pending", "schedule denied", "converging", "busy", "store limit"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 4)
			f.PutStore(f.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
			ids := f.seed("100", 1, 1, 2)
			f.policy(affinity.BalancePolicyFixed, ids[1])
			region := f.GetRegion(1)
			switch reason {
			case "pending":
				f.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
			case "schedule denied":
				re.NoError(f.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{ID: "deny", Index: 100,
					Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: "key-range",
					Data: []any{map[string]any{"start_key": "", "end_key": ""}}}))
			case "converging":
				_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[0], 3, []uint64{1, 2, 3})
				re.NoError(err)
			default:
				f.blockStore(4, reason)
			}
			before := f.snapshot()
			blocked := testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceBlocked])
			re.False(f.step())
			re.Equal(1.0, testutil.ToFloat64(affinityBalancePlanCounters[affinityBalanceBlocked])-blocked)
			re.Equal(before.Groups, f.snapshot().Groups)
			re.WithinDuration(time.Now().Add(30*time.Second), f.balancer.idle[balanceTestTable()].until, 2*time.Second)
			if reason == "converging" {
				// The worker plans the table again only once the record expires.
				b := f.balancer
				expire := func() {
					idle := b.idle[balanceTestTable()]
					idle.until = time.Now().Add(-time.Second)
					b.idle[balanceTestTable()] = idle
				}
				b.enqueue(ids[0])
				idle := b.idle[balanceTestTable()]
				re.False(b.balance(t.Context()))
				re.Equal(idle, b.idle[balanceTestTable()])
				expire()
				re.False(b.balance(t.Context()))
				re.WithinDuration(time.Now().Add(30*time.Second), b.idle[balanceTestTable()].until, 2*time.Second)
				f.settle()
				expire()
				re.True(b.balance(t.Context()))
			}
			if reason == "busy" || reason == "store limit" {
				f.SetStoreBusy(4, false)
				f.SetStoreLimit(4, storelimit.AddPeer, storelimit.Unlimited)
				f.ResetStoreLimit(4, storelimit.AddPeer, storelimit.Unlimited/time.Minute.Seconds())
				re.True(f.step())
				re.Contains(f.affinityManager.GetAffinityGroupState(ids[0]).VoterStoreIDs, uint64(4))
			}
		})
	}
}

func TestAffinityBalanceValidationRoundRetry(t *testing.T) {
	re := require.New(t)
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 3)
	const autoGroups = 10
	ids := f.seed("100", 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		2, 2, 2, 2, 2, 2, 2, 2, 2,
		3, 3, 3, 3, 3, 3, 3, 3,
	)
	f.policy(affinity.BalancePolicyFixed, ids[autoGroups:]...)
	f.waitForStoreScan(ids[0])
	// A single leader transfer from store 1 to 3 balances the table. Keep
	// the groups stable in the snapshot so the first candidate reaches Region validation.
	for i := range autoGroups {
		region := f.GetRegion(uint64(i + 1))
		f.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
	}
	before := f.snapshot()
	for _, id := range ids[:autoGroups] {
		re.Equal(affinity.PhaseStable, before.Group(id).Phase)
	}
	res := f.plan(before)
	re.Equal(2, res.rounds)
	re.Equal(autoGroups, res.groupCounts[balanceGroupUnready])
	re.Empty(res.targets)
	re.True(res.retry)
	re.False(f.step())
	re.Equal(before.Groups, f.snapshot().Groups)
	re.WithinDuration(time.Now().Add(30*time.Second), f.balancer.idle[balanceTestTable()].until, 2*time.Second)
}

func TestAffinityBalancePinsScheduleDeniedGroupsTogether(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	const denied = maxBalanceValidationRounds + 1
	leaders := slices.Repeat([]uint64{1}, denied+1)
	// Fixed groups keep the relaxed plan to one leader transfer per round.
	leaders = append(leaders, slices.Repeat([]uint64{2}, denied-1)...)
	leaders = append(leaders, slices.Repeat([]uint64{3}, denied-1)...)
	ids := f.seed("100", 1, leaders...)
	f.policy(affinity.BalancePolicyFixed, ids[denied+1:]...)
	re.NoError(f.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "deny", Index: 100, Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, RuleType: "key-range",
		Data: []any{map[string]any{"start_key": hex.EncodeToString(f.GetRegion(1).GetStartKey()),
			"end_key": hex.EncodeToString(f.GetRegion(denied).GetEndKey())}},
	}))
	snapshot := f.snapshot()
	input := f.balancePlanInput(snapshot, nil, nil)
	relaxed := balanceplan.Plan(&input.input, balanceplan.Options{Seed: snapshot.Version})
	re.Len(input.changedTargets(&relaxed), 1)
	res := f.plan(snapshot)
	re.Equal(2, res.rounds)
	re.Equal(denied, res.groupCounts[balanceGroupUnready])
	re.Len(res.targets, 1)
	re.Equal(ids[denied], res.targets[0].ID)
	re.NotEqual(uint64(1), res.targets[0].LeaderStoreID)
}
