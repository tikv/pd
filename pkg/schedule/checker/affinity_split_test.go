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
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func newAffinitySplitTest(t *testing.T) (*mockcluster.Cluster, *Controller, *operator.Controller) {
	t.Helper()
	controller, cluster, ops := newLearnerConflictController(t)
	for _, store := range cluster.GetStores() {
		cluster.SetStoreUp(store.GetID())
	}
	setAffinitySplitStoreConfig(cluster)
	group := &affinity.Group{ID: "split", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, createAffinityGroupForTest(cluster.GetAffinityManager(), group, []byte("a"), []byte("z")))
	region := cluster.GetRegion(1).Clone(core.WithEndKey([]byte("z")), core.WithReplacePeerStore(3, 4),
		core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
	cluster.PutRegion(region)
	return cluster, controller, ops
}

func setAffinitySplitStoreConfig(cluster *mockcluster.Cluster) {
	cfg := &config.StoreConfig{RegionMaxSize: "24MiB", RegionSplitSize: "16MiB", RegionSplitKeys: 160000}
	cfg.Adjust()
	cluster.SetStoreConfig(cfg)
}

// advanceAffinitySplitTime advances the synctest clock while keeping mock
// stores connected, so the affinity manager keeps the same target.
func advanceAffinitySplitTime(cluster *mockcluster.Cluster, wait time.Duration) {
	for wait > 0 {
		step := min(wait, 5*time.Second)
		time.Sleep(step)
		for _, store := range cluster.GetStores() {
			cluster.SetStoreLastHeartbeatInterval(store.GetID(), 0)
		}
		wait -= step
	}
}

func requireAffinitySplit(t *testing.T, ops []*operator.Operator) *operator.Operator {
	t.Helper()
	require.Len(t, ops, 1)
	op := ops[0]
	require.NotNil(t, op)
	require.Equal(t, "affinity-split-region", op.Desc())
	require.Equal(t, operator.OpAffinity, op.SchedulerKind())
	require.Equal(t, operator.OpAffinity|operator.OpSplit, op.Kind())
	require.Equal(t, 1, op.Len())
	step, ok := op.Step(0).(operator.SplitRegion)
	require.True(t, ok)
	require.Equal(t, pdpb.CheckPolicy_SCAN, step.Policy)
	require.Empty(t, step.SplitKeys)
	return op
}

func requireAffinityMove(t *testing.T, ops []*operator.Operator) *operator.Operator {
	t.Helper()
	require.Len(t, ops, 1)
	require.NotNil(t, ops[0])
	require.Equal(t, "affinity-move-region", ops[0].Desc())
	require.Zero(t, ops[0].Kind()&operator.OpSplit)
	return ops[0]
}

func TestAffinitySplitBeforeMoveFallback(t *testing.T) {
	for _, test := range []struct {
		name       string
		size, keys int64
		retry      bool
	}{
		{name: "unsubmitted split over size threshold", size: 17},
		{name: "retry over keys threshold", size: 1, keys: 160001, retry: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, controller, ops := newAffinitySplitTest(t)
				checker := controller.GetAffinityChecker()
				regions := putOversizedAffinityRegions(cluster)
				region := regions[0].Clone(core.SetApproximateSize(test.size), core.SetApproximateKeys(test.keys))
				cluster.PutRegion(region)
				first := requireAffinitySplit(t, checker.Check(region))
				remaining := affinitySplitTimeout
				if test.retry {
					require.Equal(t, 1, ops.AddWaitingOperator(first))
					advanceAffinitySplitTime(cluster, 2*time.Second)
					require.True(t, ops.RemoveOperator(first))
					remaining -= 2 * time.Second
				} else {
					original := cluster.GetScheduleConfig().Clone()
					blocked := original.Clone()
					blocked.SchedulerMaxWaitingOperator = 0
					cluster.SetScheduleConfig(blocked)
					require.Zero(t, ops.AddWaitingOperator(first))
					require.Equal(t, operator.CANCELED, first.Status())
					require.False(t, first.HasStarted())
					cluster.SetScheduleConfig(original)
					first = requireAffinitySplit(t, checker.Check(region))
					require.Empty(t, checker.Check(region))
					advanceAffinitySplitTime(cluster, affinitySplitTimeout)
				}
				// Concurrent checks can produce only one replacement.
				results := make(chan []*operator.Operator, 1)
				go func() { results <- checker.Check(region) }()
				split := requireAffinitySplit(t, append(checker.Check(region), <-results...))
				if !test.retry {
					require.Equal(t, operator.EXPIRED, first.Status())
				}
				require.Equal(t, 1, ops.AddWaitingOperator(split))
				require.Equal(t, uint64(1), ops.OperatorCount(operator.OpAffinity))
				cluster.SetAffinityScheduleLimit(1)
				require.Empty(t, controller.CheckRegion(regions[1]))
				startedAt := split.GetStartTime().Add(remaining - affinitySplitTimeout)
				advanceAffinitySplitTime(cluster, remaining-time.Second)
				dispatch := func() { ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {}) }
				dispatch()
				require.Same(t, split, ops.GetOperator(1))
				require.Empty(t, checker.Check(region))
				require.Equal(t, startedAt, checker.splitAttempts[1].startedAt)
				require.Equal(t, time.Now().Add(affinitySplitAttemptTTL), checker.splitAttempts[1].expiresAt)

				advanceAffinitySplitTime(cluster, time.Second)
				dispatch()
				require.Equal(t, operator.TIMEOUT, split.Status())
				region = region.Clone(core.WithIncConfVer())
				cluster.PutRegion(region)
				fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
				if test.retry {
					checker.startTime = time.Now()
				} else {
					checker.recentMergeCache.PutWithTTL(1, nil, recentMergeTTL)
				}
				waits := checkerCounter.WithLabelValues(affinityChecker, "wait-recent-merge-or-startup")
				before := promtestutil.ToFloat64(waits)
				small := region.Clone(core.SetApproximateSize(1), core.SetApproximateKeys(1))
				require.Empty(t, checker.Check(small))
				require.Empty(t, checker.Check(region))
				require.Equal(t, before+2, promtestutil.ToFloat64(waits))
				require.Equal(t, fallbacks, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
				checker.startTime = time.Now().Add(-recentMergeTTL)
				checker.recentMergeCache.Remove(1)
				requireAffinityMove(t, checker.Check(small))
				requireAffinityMove(t, checker.Check(region))
				advanceAffinitySplitTime(cluster, time.Second)
				controller.tryAddOperators(region)
				requireAffinityMove(t, []*operator.Operator{ops.GetOperator(1)})
				require.Equal(t, fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
			})
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackTransitions(t *testing.T) {
	for _, change := range []string{"new epoch", "expired attempt", "new target", "reached target", "canceled move", "children"} {
		t.Run(change, func(t *testing.T) {
			cluster, controller, _ := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			region := cluster.GetRegion(1)
			manager := cluster.GetAffinityManager()
			replaceTarget := func(store uint64) {
				_, err := manager.UpdateAffinityGroupPeers("split", 1, []uint64{1, 2, store})
				require.NoError(t, err)
			}

			split := requireAffinitySplit(t, checker.Check(region))
			if change != "children" {
				timeoutAffinitySplit(t, split)
				requireAffinityMove(t, checker.Check(region))
			}
			atTarget := region.Clone(core.WithReplacePeerStore(4, 3), core.WithIncConfVer())
			switch change {
			case "children":
				children := putOversizedAffinityRegions(cluster)
				small := children[0].Clone(core.SetApproximateSize(16), core.SetApproximateKeys(160000))
				unreported := children[2].Clone(core.WithLeader(nil), core.SetSource(core.Storage), core.SetApproximateSize(0))
				cluster.PutRegion(small)
				cluster.PutRegion(unreported)
				requireAffinityMove(t, checker.Check(small))
				require.NotContains(t, checker.splitAttempts, uint64(1))
				requireAffinitySplit(t, checker.Check(children[1]))
				require.Empty(t, checker.Check(unreported))
				_, err := manager.UpdateAffinityGroupPeers("split", 2, []uint64{1, 2, 4})
				require.NoError(t, err)
				requireAffinityMove(t, checker.Check(children[1]))
				return
			case "new epoch":
				region = region.Clone(core.WithIncVersion())
			case "expired attempt":
				attempt := checker.splitAttempts[1]
				attempt.expiresAt = time.Now()
				checker.splitAttempts[1] = attempt
			case "new target":
				region = atTarget
				replaceTarget(4)
			case "reached target":
				cluster.PutRegion(atTarget)
				require.Empty(t, checker.Check(atTarget))
				region = atTarget.Clone(core.WithReplacePeerStore(3, 4), core.WithIncConfVer())
			case "canceled move":
				replaceTarget(4)
				require.Empty(t, checker.Check(region))
				replaceTarget(3)
			}

			cluster.PutRegion(region)
			requireAffinitySplit(t, checker.Check(region))
			attempt := checker.splitAttempts[1]
			require.Equal(t, region.GetRegionEpoch().GetVersion(), attempt.version)
			require.True(t, attempt.startedAt.IsZero())
			require.False(t, attempt.fallbackRecorded)
			require.NoError(t, manager.DeleteAffinityGroups([]string{"split"}, true))
			require.Empty(t, checker.Check(region))
		})
	}
}

func timeoutAffinitySplit(t *testing.T, split *operator.Operator) {
	t.Helper()
	require.True(t, split.Start())
	split.SetStatusReachTime(operator.STARTED, time.Now().Add(-affinitySplitTimeout))
	require.True(t, split.CheckTimeout())
}

func TestAffinitySplitBeforeMoveUnknownSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, ops := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		large := cluster.GetRegion(1)
		region := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{Region: large.GetMeta(), Leader: large.GetLeader()}, 0)
		region.Inherit(large.Clone(core.SetApproximateSize(0), core.SetSource(core.Storage)), false)
		require.Zero(t, region.GetApproximateSize())
		cluster.PutRegion(region)
		waits := checkerCounter.WithLabelValues(affinityChecker, "wait-unknown-size")
		before := promtestutil.ToFloat64(waits)
		controller.tryAddOperators(region)
		require.Equal(t, before+1, promtestutil.ToFloat64(waits))
		requireAffinityMove(t, checker.Check(region.Clone(core.SetApproximateSize(16))))
		require.NotContains(t, checker.splitAttempts, uint64(1))

		fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
		controller.tryAddOperators(region)
		advanceAffinitySplitTime(cluster, 5*time.Minute-time.Second)
		controller.tryAddOperators(region)
		require.Zero(t, ops.OperatorCount(operator.OpAffinity))
		require.Nil(t, checker.splitAttempts[1].split)
		require.Equal(t, fallbacks, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

		advanceAffinitySplitTime(cluster, time.Second)
		requireAffinityMove(t, controller.CheckRegion(region))
		requireAffinityMove(t, controller.CheckRegion(region))
		require.Equal(t, fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
		cluster.PutRegion(large)
		requireAffinitySplit(t, controller.CheckRegion(large))
		require.True(t, checker.splitAttempts[1].startedAt.IsZero())
	})
}

func putOversizedAffinityRegions(cluster *mockcluster.Cluster) []*core.RegionInfo {
	parent := cluster.GetRegion(1)
	keys := []string{"a", "g", "m", "z"}
	regions := make([]*core.RegionInfo, 0, len(keys)-1)
	for i := range len(keys) - 1 {
		region := parent.Clone(core.WithNewRegionID(uint64(i+1)), core.WithStartKey([]byte(keys[i])),
			core.WithEndKey([]byte(keys[i+1])), core.WithIncVersion())
		cluster.PutRegion(region)
		regions = append(regions, region)
	}
	return regions
}

func TestAffinitySplitBeforeMoveAttemptLimit(t *testing.T) {
	for _, state := range []string{"started", "idle", "rejected", "expired"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, controller, _ := newAffinitySplitTest(t)
				checker := controller.GetAffinityChecker()
				checker.maxSplitAttempts = 2
				regions := putOversizedAffinityRegions(cluster)
				first := requireAffinitySplit(t, checker.Check(regions[0]))
				second := requireAffinitySplit(t, checker.Check(regions[1]))
				next := regions[2]
				limitWaits := checkerCounter.WithLabelValues(affinityChecker, "wait-split-attempt-limit")
				before := promtestutil.ToFloat64(limitWaits)
				require.Empty(t, checker.Check(next), "pending splits can still start")
				require.Equal(t, before+1, promtestutil.ToFloat64(limitWaits))
				attempts := checker.splitAttempts
				if state == "started" {
					require.True(t, first.Start())
					time.Sleep(time.Second)
					require.True(t, second.Start())
					for i, split := range []*operator.Operator{first, second} {
						advanceAffinitySplitTime(cluster, time.Until(split.GetStartTime().Add(affinitySplitTimeout)))
						require.True(t, split.CheckTimeout())
						require.Empty(t, checker.Check(next), "keep even a timed-out split's original budget")
						move := requireAffinityMove(t, checker.Check(regions[i]))
						require.Equal(t, split.GetStartTime(), attempts[regions[i].GetID()].startedAt)
						if i == 1 {
							require.Empty(t, checker.Check(applyLearnerOperator(t, cluster, move)))
						}
					}
					require.NotContains(t, attempts, uint64(2))
					requireAffinitySplit(t, checker.Check(next))
					return
				}

				switch state {
				case "idle":
					attempt := attempts[1]
					attempt.expiresAt = time.Now()
					attempts[1] = attempt
				case "rejected":
					require.True(t, first.Cancel())
				case "expired":
					first.SetStatusReachTime(operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
				}
				time.Sleep(affinitySplitPruneInterval - time.Nanosecond)
				require.Empty(t, checker.Check(next))
				time.Sleep(time.Nanosecond)
				requireAffinitySplit(t, checker.Check(next))
				require.NotContains(t, attempts, uint64(1))
				require.Same(t, second, attempts[2].split)

				require.True(t, second.Cancel())
				require.Empty(t, checker.Check(regions[0]), "a successful sweep also starts the cooldown")
				time.Sleep(affinitySplitPruneInterval)
				requireAffinitySplit(t, checker.Check(regions[0]))
				require.NotContains(t, attempts, uint64(2))
			})
		})
	}
}

func TestAffinitySplitBeforeLearnerMove(t *testing.T) {
	for _, test := range []struct {
		name   string
		stores []string
	}{
		{name: "copy to another store", stores: []string{"4", "5"}},
		{name: "swap roles"},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, cluster, _ := newLearnerConflictController(t)
			setAffinitySplitStoreConfig(cluster)
			cluster.SetEnableUseJointConsensus(true)
			require.NoError(t, cluster.GetRuleManager().SetRule(newLearnerRule(test.stores...)))
			region := putLearners(cluster, 1, 4).Clone(core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
			cluster.PutRegion(region)
			manager := cluster.GetAffinityManager()
			group := &affinity.Group{ID: "large", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}}
			require.NoError(t, createAffinityGroupForTest(manager, group, region.GetStartKey(), region.GetEndKey()))
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			if len(test.stores) > 0 {
				split := requireAffinitySplit(t, controller.CheckRegion(region))
				require.Equal(t, moves, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
				require.Equal(t, group.VoterStoreIDs, manager.GetAffinityGroupState(group.ID).VoterStoreIDs)
				timeoutAffinitySplit(t, split)
				requireAffinityMove(t, controller.CheckRegion(region))
				moves++
				region = region.Clone(core.WithIncVersion(), core.SetApproximateSize(16))
				cluster.PutRegion(region)
			}
			move := requireAffinityMove(t, controller.CheckRegion(region))
			addsPeer := false
			for i := range move.Len() {
				if step, ok := move.Step(i).(operator.AddLearner); ok && step.ToStore == 5 {
					addsPeer = true
				}
			}
			require.Equal(t, len(test.stores) > 0, addsPeer)
			require.Equal(t, moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			require.Equal(t, conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
			require.Empty(t, controller.CheckRegion(applyLearnerOperator(t, cluster, move)))
		})
	}
}

func TestAffinitySplitBeforeMoveStaleSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, ops := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		previous := cluster.GetRegion(1)
		atTarget := previous.Clone(core.WithIncVersion(), core.WithReplacePeerStore(4, 3))
		large := atTarget.Clone(core.WithIncConfVer(), core.WithReplacePeerStore(3, 4))
		cluster.PutRegion(large)
		small := large.Clone(core.SetApproximateSize(1))
		controller.tryAddOperators(large)
		split := requireAffinitySplit(t, []*operator.Operator{ops.GetOperator(1)})
		// The smaller snapshot passed the controller's operator check before SCAN admission.
		move := requireAffinityMove(t, checker.Check(small))
		require.Zero(t, ops.AddWaitingOperator(move))
		require.Same(t, split, checker.splitAttempts[1].split)
		requireAffinityMove(t, checker.Check(small.Clone(core.SetRegionVersion(previous.GetRegionEpoch().GetVersion()))))
		require.Same(t, split, checker.splitAttempts[1].split)
		require.Empty(t, checker.Check(atTarget))
		require.Same(t, split, checker.splitAttempts[1].split)
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		ops.Dispatch(large, operator.DispatchFromHeartBeat, func(uint64) {})
		require.Equal(t, operator.TIMEOUT, split.Status())
		requireAffinityMove(t, checker.Check(small))
		requireAffinityMove(t, controller.CheckRegion(large))
	})
}
