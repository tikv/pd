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
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/go-units"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newAffinitySplitTest(t *testing.T) (*mockcluster.Cluster, *AffinityChecker) {
	t.Helper()
	re := require.New(t)
	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(64)
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	storeConfig := &config.StoreConfig{
		RegionMaxSize: "24MiB", RegionSplitSize: "16MiB", RegionSplitKeys: 160000,
	}
	storeConfig.Adjust()
	tc.SetStoreConfig(storeConfig)
	group := &affinity.Group{ID: "split_before_move", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(createAffinityGroupForTest(tc.GetAffinityManager(), group, []byte("a"), []byte("z")))
	tc.AddLeaderRegionWithRange(1, "a", "z", 1, 2, 4)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
	tc.PutRegion(region)
	return tc, newTestAffinityChecker(t.Context(), tc, opt)
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

func requireAffinitySplit(t *testing.T, ops []*operator.Operator) {
	t.Helper()
	re := require.New(t)
	re.Len(ops, 1)
	op := ops[0]
	re.NotNil(op)
	re.Equal("affinity-split-region", op.Desc())
	re.Equal(operator.OpAffinity, op.SchedulerKind())
	re.Equal(operator.OpAffinity|operator.OpSplit, op.Kind())
	re.Equal(1, op.Len())
	step, ok := op.Step(0).(operator.SplitRegion)
	re.True(ok)
	re.Equal(pdpb.CheckPolicy_SCAN, step.Policy)
	re.Empty(step.SplitKeys)
}

func requireAffinityMove(t *testing.T, ops []*operator.Operator) {
	t.Helper()
	require.Len(t, ops, 1)
	require.Equal(t, "affinity-move-region", ops[0].Desc())
}

func TestAffinitySplitBeforeMoveThresholds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		size, keys int64
		leaderOnly bool
		wantSplit  bool
		wantWait   bool
	}{
		{name: "over size", size: 17, keys: 10, wantSplit: true},
		{name: "over keys", size: 1, keys: 160001, wantSplit: true},
		{name: "at thresholds", size: 16, keys: 160000},
		{name: "small", size: 1, keys: 10},
		{name: "zero size", size: 0, wantWait: true},
		{name: "large leader transfer", size: 64, keys: 320000, leaderOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			if tc.leaderOnly {
				_, err := cluster.GetAffinityManager().UpdateAffinityGroupPeers("split_before_move", 2, []uint64{1, 2, 4})
				re.NoError(err)
			}
			region := cluster.GetRegion(1).Clone(core.SetApproximateSize(tc.size), core.SetApproximateKeys(tc.keys))
			cluster.PutRegion(region)
			unknownWait := checkerCounter.WithLabelValues(affinityChecker, "wait-unknown-size")
			before := promtestutil.ToFloat64(unknownWait)
			ops := checker.Check(region)
			if tc.wantWait {
				re.Empty(ops)
				re.Equal(before+1, promtestutil.ToFloat64(unknownWait))
			} else if tc.wantSplit {
				requireAffinitySplit(t, ops)
			} else {
				requireAffinityMove(t, ops)
				re.Zero(ops[0].Kind() & operator.OpSplit)
			}
		})
	}
}

// ageAffinitySplitAttempt ends the current split and ages its attempt by wait.
func ageAffinitySplitAttempt(t *testing.T, checker *AffinityChecker, regionID uint64, wait time.Duration) {
	t.Helper()
	attempt, ok := checker.splitAttempts[regionID]
	require.True(t, ok)
	if attempt.split != nil && !attempt.split.IsEnd() {
		require.True(t, attempt.split.Cancel())
	}
	attempt.startedAt = time.Now().Add(-wait)
	checker.splitAttempts[regionID] = attempt
}

func TestAffinitySplitBeforeMoveFallback(t *testing.T) {
	for _, tc := range []struct {
		name       string
		size, keys int64
	}{
		{name: "over size", size: 64, keys: 10},
		{name: "over keys", size: 1, keys: 160001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				cluster, checker := newAffinitySplitTest(t)
				region := cluster.GetRegion(1).Clone(core.SetApproximateSize(tc.size), core.SetApproximateKeys(tc.keys))
				cluster.PutRegion(region)
				first := checker.Check(region)
				requireAffinitySplit(t, first)
				re.True(first[0].Start())
				startedAt := first[0].GetStartTime()

				advanceAffinitySplitTime(cluster, 2*time.Second)
				re.True(first[0].Cancel())
				retry := checker.Check(region)
				requireAffinitySplit(t, retry)
				re.Equal(startedAt, checker.splitAttempts[region.GetID()].startedAt)
				re.True(retry[0].Start())
				advanceAffinitySplitTime(cluster, affinitySplitTimeout-3*time.Second)
				re.False(retry[0].CheckTimeout())
				advanceAffinitySplitTime(cluster, time.Second)
				re.True(retry[0].CheckTimeout(), "a retry only has the remaining attempt budget")

				// A peer configuration change does not change the split epoch version.
				region = region.Clone(core.WithIncConfVer())
				cluster.PutRegion(region)
				requireAffinityMove(t, checker.Check(region))
			})
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackBoundary(t *testing.T) {
	for _, wait := range []time.Duration{affinitySplitTimeout, affinitySplitTimeout + time.Second} {
		t.Run(wait.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				cluster, checker := newAffinitySplitTest(t)
				region := cluster.GetRegion(1)
				requireAffinitySplit(t, checker.Check(region))
				ageAffinitySplitAttempt(t, checker, region.GetID(), wait)
				attempt, ok := checker.splitAttempts[region.GetID()]
				re.True(ok)
				re.Equal(wait, time.Since(attempt.startedAt))
				group := cluster.GetAffinityManager().GetAffinityGroupState("split_before_move")
				roles := map[uint64]placement.PeerRoleType{1: placement.Leader, 2: placement.Voter, 3: placement.Voter}
				fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
				for range 3 {
					op, ready := checker.prepareAffinityPeerMove(region, group, roles)
					re.Nil(op)
					re.True(ready)
					re.Equal(fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
				}
			})
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackTransitions(t *testing.T) {
	for _, reason := range []string{"epoch", "expired", "new target", "new move", "current target", "split succeeds"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1)
			manager := cluster.GetAffinityManager()
			if reason == "split succeeds" {
				region = region.Clone(core.SetApproximateSize(32))
				cluster.PutRegion(region)
			}
			requireAffinitySplit(t, checker.Check(region))
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitTimeout+time.Second)
			// Later patrols still allow the move when it has not been added.
			if reason == "split succeeds" {
				ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitTimeout/2)
			} else {
				for range 2 {
					ops := checker.Check(region)
					requireAffinityMove(t, ops)
				}
			}
			switch reason {
			case "epoch":
				region = region.Clone(core.WithIncVersion())
			case "expired":
				attempt, ok := checker.splitAttempts[region.GetID()]
				re.True(ok)
				attempt.expiresAt = time.Now().Add(-time.Second)
				checker.splitAttempts[region.GetID()] = attempt
			case "new target":
				// Reach the old target, but change it before the next patrol.
				region = region.Clone(core.WithReplacePeerStore(4, 3), core.WithIncConfVer())
				_, err := manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 4})
				re.NoError(err)
			case "new move":
				atTarget := region.Clone(core.WithReplacePeerStore(4, 3), core.WithIncConfVer())
				cluster.PutRegion(atTarget)
				re.Empty(checker.Check(atTarget), "at the target")
				// A replacement takes the Region away from the same target again.
				region = atTarget.Clone(core.WithReplacePeerStore(3, 4), core.WithIncConfVer())
			case "current target":
				_, err := manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 4})
				re.NoError(err)
				re.Empty(checker.Check(region), "the canceled move no longer needs a split")
				_, err = manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 3})
				re.NoError(err)
			case "split succeeds":
				left := region.Clone(core.WithEndKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
				right := region.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
				for _, child := range []*core.RegionInfo{left, right} {
					cluster.PutRegion(child)
					ops := checker.Check(child)
					requireAffinityMove(t, ops)
				}
				return
			}
			cluster.PutRegion(region)
			requireAffinitySplit(t, checker.Check(region))
			attempt, ok := checker.splitAttempts[region.GetID()]
			re.True(ok)
			re.Equal(region.GetRegionEpoch().GetVersion(), attempt.version)
			re.True(attempt.startedAt.IsZero(), "a new attempt must start a fresh wait")
			re.False(attempt.fallbackRecorded, "a new attempt must record its own fallback")
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitTimeout+time.Second)
			ops := checker.Check(region)
			requireAffinityMove(t, ops)
			re.NoError(manager.DeleteAffinityGroups([]string{"split_before_move"}, true))
			re.Empty(checker.Check(region))
		})
	}
}

func TestAffinitySplitBeforeMoveUnknownSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster, controller, ops := newAffinitySplitControllerTest(t)
		checker := controller.GetAffinityChecker()
		region := cluster.GetRegion(1)
		region = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: region.GetMeta(), Leader: region.GetLeader(),
		}, 0)
		// A raw-zero heartbeat can inherit zero from a storage/sync origin.
		origin := region.Clone(core.SetApproximateSize(0), core.SetSource(core.Storage))
		region.Inherit(origin, false)
		cluster.PutRegion(region)
		re.Zero(region.GetApproximateSize())
		fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
		controller.tryAddOperators(region)
		re.Zero(ops.OperatorCount(operator.OpAffinity))

		advanceAffinitySplitTime(cluster, 5*time.Minute-time.Second)
		controller.tryAddOperators(region)
		re.Nil(ops.GetOperator(region.GetID()))
		re.Zero(ops.OperatorCount(operator.OpAffinity))
		re.Nil(checker.splitAttempts[region.GetID()].split)
		re.Equal(fallbacks, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

		advanceAffinitySplitTime(cluster, time.Second)
		for range 2 {
			requireAffinityMove(t, controller.CheckRegion(region))
		}
		re.Equal(fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

		// The move has not been admitted. A positive estimate gets its own split budget.
		region = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: region.GetMeta(), Leader: region.GetLeader(), ApproximateSize: 64 * units.MiB,
		}, 0)
		cluster.PutRegion(region)
		requireAffinitySplit(t, controller.CheckRegion(region))
		re.True(checker.splitAttempts[region.GetID()].startedAt.IsZero())
	})
}

func putOversizedAffinityRegions(cluster *mockcluster.Cluster) []*core.RegionInfo {
	parent := cluster.GetRegion(1)
	keys := []string{"a", "g", "m", "s", "z"}
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
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster, checker := newAffinitySplitTest(t)
		checker.maxSplitAttempts = 2
		regions := putOversizedAffinityRegions(cluster)
		first, second, third := regions[0], regions[1], regions[2]
		attempts := checker.splitAttempts
		limitWaits := checkerCounter.WithLabelValues(affinityChecker, "wait-split-attempt-limit")
		checkBlocked := func() {
			before := promtestutil.ToFloat64(limitWaits)
			for _, region := range regions[2:] {
				re.Empty(checker.Check(region))
				re.NotContains(attempts, region.GetID())
			}
			re.Len(attempts, 2)
			re.Equal(before+2, promtestutil.ToFloat64(limitWaits))
		}

		firstSplit := checker.Check(first)
		requireAffinitySplit(t, firstSplit)
		re.True(firstSplit[0].Start())
		re.Empty(checker.Check(first))
		advanceAffinitySplitTime(cluster, 2*time.Second)
		secondSplit := checker.Check(second)
		requireAffinitySplit(t, secondSplit)
		re.True(secondSplit[0].Start())
		checkBlocked()
		re.Equal(firstSplit[0].GetStartTime(), attempts[first.GetID()].startedAt)
		re.Same(secondSplit[0], attempts[second.GetID()].split)

		fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
		advanceAffinitySplitTime(cluster, affinitySplitTimeout-2*time.Second)
		re.True(firstSplit[0].CheckTimeout())
		firstMove := checker.Check(first)
		requireAffinityMove(t, firstMove)
		re.False(secondSplit[0].CheckTimeout())
		checkBlocked()
		re.True(attempts[first.GetID()].fallbackRecorded)

		advanceAffinitySplitTime(cluster, 2*time.Second)
		re.True(secondSplit[0].CheckTimeout())
		// Saturation must preserve a completed split even before the checker
		// has had a chance to copy its first start time into the attempt.
		re.True(attempts[second.GetID()].startedAt.IsZero())
		checkBlocked()
		secondMove := checker.Check(second)
		requireAffinityMove(t, secondMove)
		for range 2 {
			checkBlocked()
			requireAffinityMove(t, checker.Check(first))
			requireAffinityMove(t, checker.Check(second))
			re.Equal(firstSplit[0].GetStartTime(), attempts[first.GetID()].startedAt)
			re.Equal(secondSplit[0].GetStartTime(), attempts[second.GetID()].startedAt)
			re.True(attempts[first.GetID()].fallbackRecorded)
			re.True(attempts[second.GetID()].fallbackRecorded)
		}
		re.Equal(fallbacks+2, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

		re.Empty(checker.Check(applyLearnerOperator(t, cluster, firstMove[0])))
		re.NotContains(attempts, first.GetID())
		thirdSplit := checker.Check(third)
		requireAffinitySplit(t, thirdSplit)
		re.True(thirdSplit[0].Start())
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		re.True(thirdSplit[0].CheckTimeout())
		thirdMove := checker.Check(third)
		requireAffinityMove(t, thirdMove)
		re.Empty(checker.Check(applyLearnerOperator(t, cluster, thirdMove[0])))
		re.NotContains(attempts, third.GetID())
		re.Equal(fallbacks+3, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

		re.Empty(checker.Check(applyLearnerOperator(t, cluster, secondMove[0])))
		re.Empty(attempts)
	})
}

func TestAffinitySplitBeforeMoveReclaimsAttempts(t *testing.T) {
	for _, reason := range []string{"idle attempt", "rejected split", "expired split"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				cluster, checker := newAffinitySplitTest(t)
				checker.maxSplitAttempts = 2
				regions := putOversizedAffinityRegions(cluster)
				first, second, third, fourth := regions[0], regions[1], regions[2], regions[3]
				attempts := checker.splitAttempts
				firstSplit := checker.Check(first)
				requireAffinitySplit(t, firstSplit)
				secondSplit := checker.Check(second)
				requireAffinitySplit(t, secondSplit)
				re.Empty(checker.Check(third), "both splits can still be admitted")
				switch reason {
				case "idle attempt":
					attempt := attempts[first.GetID()]
					attempt.expiresAt = time.Now()
					attempts[first.GetID()] = attempt
				case "rejected split":
					re.True(firstSplit[0].Cancel())
				case "expired split":
					firstSplit[0].SetStatusReachTime(operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
				}
				// Even a reclaimable attempt stays tracked until the cooldown ends.
				advanceAffinitySplitTime(cluster, affinitySplitPruneInterval-time.Nanosecond)
				limitWaits := checkerCounter.WithLabelValues(affinityChecker, "wait-split-attempt-limit")
				before := promtestutil.ToFloat64(limitWaits)
				for range 2 {
					re.Empty(checker.Check(third))
					re.Contains(attempts, first.GetID())
				}
				re.Equal(before+2, promtestutil.ToFloat64(limitWaits))
				advanceAffinitySplitTime(cluster, time.Nanosecond)
				thirdSplit := checker.Check(third)
				requireAffinitySplit(t, thirdSplit)
				re.Len(attempts, 2)
				re.NotContains(attempts, first.GetID())
				re.Same(secondSplit[0], attempts[second.GetID()].split)
				if reason == "expired split" {
					re.Equal(operator.EXPIRED, firstSplit[0].Status())
				}

				// Reject the replacement while the cooldown from the successful sweep is active.
				re.True(thirdSplit[0].Cancel())
				re.False(thirdSplit[0].HasStarted())
				advanceAffinitySplitTime(cluster, affinitySplitPruneInterval-time.Nanosecond)
				before = promtestutil.ToFloat64(limitWaits)
				re.Empty(checker.Check(fourth))
				re.Equal(before+1, promtestutil.ToFloat64(limitWaits))
				re.Same(thirdSplit[0], attempts[third.GetID()].split)
				advanceAffinitySplitTime(cluster, time.Nanosecond)
				requireAffinitySplit(t, checker.Check(fourth))
				re.NotContains(attempts, third.GetID())
			})
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackAfterSplitStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster, controller, opController := newAffinitySplitControllerTest(t)
		checker := controller.GetAffinityChecker()
		region := cluster.GetRegion(1)
		cfg := cluster.GetScheduleConfig().Clone()
		waitingLimit := cfg.SchedulerMaxWaitingOperator
		cfg.SchedulerMaxWaitingOperator = 0
		cluster.SetScheduleConfig(cfg)
		for range 2 {
			ops := checker.Check(region)
			requireAffinitySplit(t, ops)
			re.Zero(opController.AddWaitingOperator(ops...))
			re.Equal(operator.CANCELED, ops[0].Status())
			re.True(ops[0].GetStartTime().IsZero())
			re.Nil(opController.GetOperator(region.GetID()))
		}
		cfg = cluster.GetScheduleConfig().Clone()
		cfg.SchedulerMaxWaitingOperator = waitingLimit
		cluster.SetScheduleConfig(cfg)
		ops := checker.Check(region)
		requireAffinitySplit(t, ops)
		re.Empty(checker.Check(region), "overlapping checks must retain the unstarted split")
		// A split that never starts must not consume the wait budget.
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		group := cluster.GetAffinityManager().GetAffinityGroupState("split_before_move")
		roles := map[uint64]placement.PeerRoleType{1: placement.Leader, 2: placement.Voter, 3: placement.Voter}
		prepared := make(chan *operator.Operator, 32)
		start := make(chan struct{})
		for range cap(prepared) {
			go func() {
				<-start
				op, _ := checker.prepareAffinityPeerMove(region, group, roles)
				prepared <- op
			}()
		}
		close(start)
		var retry []*operator.Operator
		for range cap(prepared) {
			if op := <-prepared; op != nil {
				retry = append(retry, op)
			}
		}
		re.Equal(operator.EXPIRED, ops[0].Status())
		ops = retry
		requireAffinitySplit(t, ops)
		re.Equal(1, opController.AddWaitingOperator(ops...))
		re.False(ops[0].GetStartTime().IsZero())
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		opController.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		re.Nil(opController.GetOperator(region.GetID()))
		requireAffinityMove(t, checker.Check(region))
	})
}

func TestAffinitySplitBeforeMoveOverlappingChecks(t *testing.T) {
	re := require.New(t)
	cluster, controller, opController := newAffinitySplitControllerTest(t)
	checker := controller.GetAffinityChecker()
	region := cluster.GetRegion(1).Clone(core.SetApproximateSize(1315))
	cluster.PutRegion(region)
	ops := checker.Check(region)
	requireAffinitySplit(t, ops)
	split := ops[0]
	re.Empty(checker.Check(region), "retain the split before admission")
	re.Equal(1, opController.AddWaitingOperator(split))
	startedAt := time.Now().Add(-affinitySplitTimeout / 2)
	split.SetStatusReachTime(operator.STARTED, startedAt)
	attempt, ok := checker.splitAttempts[region.GetID()]
	re.True(ok)
	expiresAt := time.Now().Add(time.Minute)
	attempt.expiresAt = expiresAt
	checker.splitAttempts[region.GetID()] = attempt

	// A caller that passed the controller's operator check before admission
	// can still reach the affinity checker after the split has started.
	for range 2 {
		re.Empty(checker.Check(region), "an in-flight split must not produce a duplicate")
		attempt, ok = checker.splitAttempts[region.GetID()]
		re.True(ok)
		re.Same(split, attempt.split)
		re.Equal(startedAt, attempt.startedAt)
		re.True(attempt.expiresAt.After(expiresAt))
	}
}

func TestAffinitySplitBeforeMoveCooldown(t *testing.T) {
	for _, scenario := range []string{"small", "fallback"} {
		t.Run(scenario, func(t *testing.T) {
			for _, reason := range []string{"recent merge", "startup"} {
				t.Run(reason, func(t *testing.T) {
					re := require.New(t)
					cluster, checker := newAffinitySplitTest(t)
					region := cluster.GetRegion(1)
					if scenario == "fallback" {
						requireAffinitySplit(t, checker.Check(region))
						ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitTimeout+time.Second)
					} else {
						region = region.Clone(core.SetApproximateSize(16))
						cluster.PutRegion(region)
					}
					switch reason {
					case "recent merge":
						checker.recentMergeCache.PutWithTTL(region.GetID(), nil, recentMergeTTL)
					case "startup":
						checker.startTime = time.Now()
					}
					cooldownWait := checkerCounter.WithLabelValues(affinityChecker, "wait-recent-merge-or-startup")
					before := promtestutil.ToFloat64(cooldownWait)
					re.Empty(checker.Check(region))
					re.Equal(before+1, promtestutil.ToFloat64(cooldownWait))

					switch reason {
					case "recent merge":
						checker.recentMergeCache.Remove(region.GetID())
					case "startup":
						checker.startTime = time.Now().Add(-2 * recentMergeTTL)
					}
					ops := checker.Check(region)
					requireAffinityMove(t, ops)
					re.Equal(before+1, promtestutil.ToFloat64(cooldownWait))
				})
			}
		})
	}
}

func TestAffinitySplitBeforeMoveChecksEachRegion(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	parent := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(parent))
	// The rest of the group does not hold back a small child: one sibling is still
	// oversized, and another has had no heartbeat since it was loaded from storage.
	small := parent.Clone(core.WithEndKey([]byte("g")), core.WithIncVersion(), core.SetApproximateSize(16))
	large := parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("g")), core.WithEndKey([]byte("m")),
		core.WithIncVersion(), core.SetApproximateSize(32))
	unreported := parent.Clone(core.WithNewRegionID(3), core.WithStartKey([]byte("m")), core.WithIncVersion(),
		core.WithLeader(nil), core.SetApproximateSize(0), core.SetSource(core.Storage))
	for _, region := range []*core.RegionInfo{small, large, unreported} {
		cluster.PutRegion(region)
	}
	ops := checker.Check(small)
	requireAffinityMove(t, ops)
	requireAffinitySplit(t, checker.Check(large))
	re.Empty(checker.Check(unreported))
}

func newAffinitySplitControllerTest(t *testing.T) (*mockcluster.Cluster, *Controller, *operator.Controller) {
	t.Helper()
	cluster, _ := newAffinitySplitTest(t)
	stream := hbstream.NewTestHeartbeatStreams(t.Context(), cluster, false)
	t.Cleanup(stream.Close)
	ops := operator.NewController(t.Context(), cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	controller := NewController(t.Context(), cluster, cluster.GetCheckerConfig(), ops)
	controller.GetAffinityChecker().startTime = time.Now().Add(-2 * recentMergeTTL)
	return cluster, controller, ops
}

// A learner moved off a target voter store copies data to its new store.
// Swapping roles with a voter that the target drops copies no data.
func TestAffinitySplitBeforeLearnerMove(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		voters []uint64
		// Nil means no learner rule; empty allows a learner on any store.
		learnerStores []string
		size          int64
		wantSplit     bool
		fallback      bool
	}{
		{name: "new voter store", voters: []uint64{1, 2, 5}, size: 64, wantSplit: true},
		{name: "learner moves to a new store", voters: []uint64{1, 2, 4}, learnerStores: []string{"4", "5"}, size: 64, wantSplit: true},
		{name: "small learner move", voters: []uint64{1, 2, 4}, learnerStores: []string{"4", "5"}, size: 16},
		{name: "learner swaps with a dropped voter", voters: []uint64{1, 2, 4}, learnerStores: []string{}, size: 64},
		{name: "learner move after split timeout", voters: []uint64{1, 2, 4}, learnerStores: []string{"4", "5"}, size: 64, wantSplit: true, fallback: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			c, tc, oc := newLearnerConflictController(t)
			storeConfig := &config.StoreConfig{RegionMaxSize: "24MiB", RegionSplitSize: "16MiB", RegionSplitKeys: 160000}
			storeConfig.Adjust()
			tc.SetStoreConfig(storeConfig)
			if testCase.learnerStores != nil {
				// Demoting the dropped voter in a swap needs joint consensus.
				tc.SetEnableUseJointConsensus(true)
				re.NoError(tc.GetRuleManager().SetRule(newLearnerRule(testCase.learnerStores...)))
				putLearners(tc, 1, 4)
			}
			region := tc.GetRegion(1).Clone(core.SetApproximateSize(testCase.size), core.SetApproximateKeys(10), core.SetSource(core.Heartbeat))
			tc.PutRegion(region)
			manager := tc.GetAffinityManager()
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: "large", LeaderStoreID: 1, VoterStoreIDs: testCase.voters,
			}, region.GetStartKey(), region.GetEndKey()))
			checker := c.GetAffinityChecker()
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
			ops := c.CheckRegion(region)
			if testCase.wantSplit {
				requireAffinitySplit(t, ops)
				re.Equal(moves, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
				re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
				re.Equal(testCase.voters, manager.GetAffinityGroupState("large").VoterStoreIDs)
				if testCase.fallback {
					split := ops[0]
					re.Equal(1, oc.AddWaitingOperator(split))
					re.False(split.GetStartTime().IsZero())
					split.SetStatusReachTime(operator.STARTED, time.Now().Add(-affinitySplitTimeout-time.Second))
					oc.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
					re.Equal(operator.TIMEOUT, split.Status())
					re.Nil(oc.GetOperator(region.GetID()))
				} else {
					// A child at the split size moves without another split.
					region = region.Clone(core.WithIncVersion(), core.SetApproximateSize(16))
					tc.PutRegion(region)
				}
				ops = c.CheckRegion(region)
			}
			requireAffinityMove(t, ops)
			if testCase.fallback {
				re.Equal(fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
			}
			if testCase.learnerStores != nil {
				re.Equal(moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			}
			re.Zero(ops[0].Kind() & operator.OpSplit)
			addsStore5 := false
			for i := range ops[0].Len() {
				if step, ok := ops[0].Step(i).(operator.AddLearner); ok && step.ToStore == 5 {
					addsStore5 = true
				}
			}
			re.Equal(len(testCase.learnerStores) > 0 || slices.Contains(testCase.voters, 5), addsStore5)
			atTarget := applyLearnerOperator(t, tc, ops[0])
			re.Empty(c.CheckRegion(atTarget))
			_, exists := checker.splitAttempts[region.GetID()]
			re.False(exists, "reaching the target must clear the split attempt")
			re.Equal(conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
		})
	}
}

func TestAffinitySplitBeforeMoveController(t *testing.T) {
	re := require.New(t)
	cluster, controller, opController := newAffinitySplitControllerTest(t)
	parent := cluster.GetRegion(1)
	controller.tryAddOperators(parent)
	op := opController.GetOperator(parent.GetID())
	requireAffinitySplit(t, []*operator.Operator{op})
	re.Equal(uint64(1), opController.OperatorCount(operator.OpAffinity))
	// The same limit used for moves also prevents scheduling another split.
	cluster.SetAffinityScheduleLimit(1)
	other := parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")))
	parent = parent.Clone(core.WithEndKey([]byte("m")))
	cluster.PutRegion(parent)
	cluster.PutRegion(other)
	re.Empty(controller.CheckRegion(other))
}

func TestAffinitySplitBeforeMoveControllerTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cluster, controller, ops := newAffinitySplitControllerTest(t)
		cluster.SetAffinityScheduleLimit(1)
		region := cluster.GetRegion(1).Clone(core.SetApproximateSize(1315))
		cluster.PutRegion(region)
		fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
		controller.tryAddOperators(region)
		split := ops.GetOperator(region.GetID())
		requireAffinitySplit(t, []*operator.Operator{split})
		re.Equal(operator.STARTED, split.Status())
		re.Equal(uint64(1), ops.OperatorCount(operator.OpAffinity))

		// No split key is found, so the Region's version stays the same.
		advanceAffinitySplitTime(cluster, 29*time.Second)
		ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		re.Equal(operator.STARTED, split.Status())
		controller.tryAddOperators(region)
		re.Same(split, ops.GetOperator(region.GetID()))

		advanceAffinitySplitTime(cluster, time.Second)
		ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		re.Equal(operator.TIMEOUT, split.Status())
		re.Nil(ops.GetOperator(region.GetID()))
		re.Zero(ops.OperatorCount(operator.OpAffinity))
		re.Equal(region.GetRegionEpoch().GetVersion(), cluster.GetRegion(region.GetID()).GetRegionEpoch().GetVersion())
		controller.tryAddOperators(region)
		move := ops.GetOperator(region.GetID())
		re.NotNil(move)
		requireAffinityMove(t, []*operator.Operator{move})
		re.Zero(move.Kind() & operator.OpSplit)
		re.Equal(fallbacks+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
	})
}
