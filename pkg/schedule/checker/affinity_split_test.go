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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func newAffinitySplitTest(t *testing.T) (*mockcluster.Cluster, *Controller, *operator.Controller) {
	t.Helper()
	controller, cluster, ops := newLearnerConflictController(t)
	for _, store := range cluster.GetStores() {
		cluster.SetStoreUp(store.GetID())
	}
	setAffinitySplitStoreConfig(cluster)
	region := cluster.GetRegion(1).Clone(withAffinitySplitTestRange("a", "z"), core.WithReplacePeerStore(3, 4),
		core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
	group := &affinity.Group{ID: "split", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	createAffinitySplitTestGroup(t, cluster.GetAffinityManager(), group, region)
	cluster.PutRegion(region)
	return cluster, controller, ops
}

func withAffinitySplitTestRange(start, end string) core.RegionCreateOption {
	startKey, endKey := []byte(start), []byte(end)
	if kerneltype.IsNextGen() {
		const keyspaceID = 1
		startKey = encodeKeyspaceRawKey(keyspaceID, startKey)
		endKey = encodeKeyspaceRawKey(keyspaceID, endKey)
	}
	return func(region *core.RegionInfo) {
		core.WithStartKey(startKey)(region)
		core.WithEndKey(endKey)(region)
	}
}

func createAffinitySplitTestGroup(t *testing.T, manager *affinity.Manager, group *affinity.Group, region *core.RegionInfo) {
	t.Helper()
	require.NoError(t, manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID:   group.ID,
		KeyRanges: []keyutil.KeyRange{{StartKey: region.GetStartKey(), EndKey: region.GetEndKey()}},
	}}))
	_, err := manager.UpdateAffinityGroupPeers(group.ID, group.LeaderStoreID, group.VoterStoreIDs)
	require.NoError(t, err)
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

func requireClassicAffinitySplit(t *testing.T) {
	t.Helper()
	if kerneltype.IsNextGen() {
		t.Skip("affinity pre-splitting is only used in Classic")
	}
}

func TestAffinityMoveWithoutSplitOnNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("NextGen skips affinity pre-splitting")
	}
	for _, test := range []struct {
		name string
		size int64
	}{
		{name: "oversized", size: 64},
		{name: "unknown size", size: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster, controller, _ := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			checker.startTime = time.Now()
			region := cluster.GetRegion(1).Clone(core.SetApproximateSize(test.size))
			cluster.PutRegion(region)
			requireAffinityMove(t, controller.CheckRegion(region))
			require.Empty(t, checker.splitAttempts)
		})
	}
}

func TestAffinitySplitBeforeMoveFallback(t *testing.T) {
	requireClassicAffinitySplit(t)
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
					require.Equal(t, operator.CREATED, first.Status())
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
				cluster.PutRegion(small)
				require.Empty(t, checker.Check(small))
				cluster.PutRegion(region)
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
	requireClassicAffinitySplit(t)
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
	requireClassicAffinitySplit(t)
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
		small := region.Clone(core.SetApproximateSize(16))
		cluster.PutRegion(small)
		requireAffinityMove(t, checker.Check(region))
		require.NotContains(t, checker.splitAttempts, uint64(1))

		fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
		cluster.PutRegion(region)
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
		split := requireAffinitySplit(t, controller.CheckRegion(large))
		require.Equal(t, affinitySplitWaitNone, checker.splitAttempts[1].waitKind)
		require.True(t, ops.AddOperator(split))

		// An unknown size after admission uses the longer timeout, measured
		// from the SCAN start rather than from the first unknown-size check.
		cluster.PutRegion(region)
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		require.Equal(t, operator.TIMEOUT, split.Status())
		require.Empty(t, checker.Check(region))
		advanceAffinitySplitTime(cluster, defaultAffinityUnknownSizeWait-affinitySplitTimeout-time.Second)
		require.Empty(t, checker.Check(region))
		advanceAffinitySplitTime(cluster, time.Second)
		requireAffinityMove(t, checker.Check(region))
	})
}

func putOversizedAffinityRegions(cluster *mockcluster.Cluster) []*core.RegionInfo {
	parent := cluster.GetRegion(1)
	keys := []string{"a", "g", "m", "z"}
	regions := make([]*core.RegionInfo, 0, len(keys)-1)
	for i := range len(keys) - 1 {
		region := parent.Clone(core.WithNewRegionID(uint64(i+1)), withAffinitySplitTestRange(keys[i], keys[i+1]),
			core.WithIncVersion())
		cluster.PutRegion(region)
		regions = append(regions, region)
	}
	return regions
}

func TestAffinitySplitBeforeMoveAttemptLimit(t *testing.T) {
	requireClassicAffinitySplit(t)
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
				if state == "expired" {
					require.Equal(t, operator.CREATED, first.Status())
				}

				require.True(t, second.Cancel())
				require.Empty(t, checker.Check(regions[0]), "a successful sweep also starts the cooldown")
				time.Sleep(affinitySplitPruneInterval)
				requireAffinitySplit(t, checker.Check(regions[0]))
				require.NotContains(t, attempts, uint64(2))
			})
		})
	}
}

func TestAffinitySplitPrunesDeletedGroup(t *testing.T) {
	requireClassicAffinitySplit(t)
	cluster, controller, _ := newAffinitySplitTest(t)
	checker := controller.GetAffinityChecker()
	checker.maxSplitAttempts = 1
	region := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(region))
	require.Len(t, checker.splitAttempts, 1)

	manager := cluster.GetAffinityManager()
	require.NoError(t, manager.DeleteAffinityGroups([]string{"split"}, true))
	next := region.Clone(core.WithNewRegionID(2), withAffinitySplitTestRange("za", "zz"))
	createAffinitySplitTestGroup(t, manager, &affinity.Group{
		ID: "next", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3},
	}, next)
	cluster.PutRegion(next)
	requireAffinitySplit(t, checker.Check(next))
	require.NotContains(t, checker.splitAttempts, region.GetID())
}

func TestAffinitySplitPrunesExpiredGroup(t *testing.T) {
	requireClassicAffinitySplit(t)
	cluster, controller, _ := newAffinitySplitTest(t)
	checker := controller.GetAffinityChecker()
	region := cluster.GetRegion(1).Clone(core.SetApproximateSize(0))
	cluster.PutRegion(region)
	require.Empty(t, checker.Check(region))
	require.Contains(t, checker.splitAttempts, region.GetID())

	manager := cluster.GetAffinityManager()
	manager.DegradeAffinityGroup("split")
	checker.pruneSplitAttempts(time.Now())
	require.Contains(t, checker.splitAttempts, region.GetID())

	manager.ExpireAffinityGroup("split")
	checker.pruneSplitAttempts(time.Now())
	require.NotContains(t, checker.splitAttempts, region.GetID())
}

func TestAffinitySplitReleasesAttemptForLeaderTransfer(t *testing.T) {
	requireClassicAffinitySplit(t)
	cluster, controller, _ := newAffinitySplitTest(t)
	checker := controller.GetAffinityChecker()
	region := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(region))

	_, err := cluster.GetAffinityManager().UpdateAffinityGroupPeers("split", 2, []uint64{1, 2, 4})
	require.NoError(t, err)
	move := requireAffinityMove(t, checker.Check(region))
	require.Zero(t, move.Kind()&operator.OpRegion)
	require.NotContains(t, checker.splitAttempts, region.GetID())
}

func TestAffinitySplitLeavesExpiryToController(t *testing.T) {
	requireClassicAffinitySplit(t)
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, ops := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		region := cluster.GetRegion(1)
		first := requireAffinitySplit(t, checker.Check(region))
		advanceAffinitySplitTime(cluster, operator.OperatorExpireTime)

		requireAffinitySplit(t, checker.Check(region))
		require.Equal(t, operator.CREATED, first.Status())
		require.Zero(t, ops.AddWaitingOperator(first))
		require.Equal(t, operator.EXPIRED, first.Status())
		reason, ok := first.GetAdditionalInfo("cancel-reason")
		require.True(t, ok)
		require.Equal(t, string(operator.Expired), reason)
	})
}

func TestAffinitySplitIgnoresZeroSizeThreshold(t *testing.T) {
	requireClassicAffinitySplit(t)
	for _, splitSize := range []string{"512KiB", "0B"} {
		t.Run(splitSize, func(t *testing.T) {
			cluster, controller, _ := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			checker.startTime = time.Now().Add(-recentMergeTTL)
			cfg := &config.StoreConfig{RegionSplitSize: splitSize}
			cfg.Adjust()
			require.Zero(t, cfg.GetRegionSplitSize())
			cluster.SetStoreConfig(cfg)
			region := cluster.GetRegion(1).Clone(core.SetApproximateSize(1), core.SetApproximateKeys(1))
			cluster.PutRegion(region)

			requireAffinityMove(t, checker.Check(region))
			require.Empty(t, checker.splitAttempts)
		})
	}
}

func TestAffinitySplitAttemptReadmission(t *testing.T) {
	requireClassicAffinitySplit(t)
	for _, change := range []string{"rejected", "version", "target voters"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, controller, _ := newAffinitySplitTest(t)
				checker := controller.GetAffinityChecker()
				checker.maxSplitAttempts = 1
				regions := putOversizedAffinityRegions(cluster)
				first, waiter := regions[0], regions[1]
				split := requireAffinitySplit(t, checker.Check(first))
				require.Empty(t, checker.Check(waiter))

				switch change {
				case "rejected":
					require.True(t, split.Cancel())
					require.Zero(t, split.GetStartTime())
				case "version":
					first = first.Clone(core.WithIncVersion())
					cluster.PutRegion(first)
				case "target voters":
					_, err := cluster.GetAffinityManager().UpdateAffinityGroupPeers("split", 1, []uint64{1, 2, 5})
					require.NoError(t, err)
				}

				require.Empty(t, checker.Check(first), "the queued Region gets the slot first")
				require.NotContains(t, checker.splitAttempts, first.GetID())
				require.Contains(t, checker.splitWaitIndex, first.GetID())
				requireAffinitySplit(t, checker.Check(waiter))
			})
		})
	}
}

func TestAffinitySplitWaitQueueCapacity(t *testing.T) {
	requireClassicAffinitySplit(t)
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, _ := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		checker.maxSplitAttempts = 1
		checker.maxSplitWaitQueue = 1
		regions := putOversizedAffinityRegions(cluster)
		owner, first, next := regions[0], regions[1], regions[2]

		split := requireAffinitySplit(t, checker.Check(owner))
		require.Empty(t, checker.Check(first))
		require.Empty(t, checker.Check(next))
		require.Len(t, checker.splitWaitIndex, 1)
		require.Contains(t, checker.splitWaitIndex, first.GetID())

		require.True(t, split.Cancel())
		time.Sleep(affinitySplitPruneInterval)
		require.Empty(t, checker.Check(next))
		require.Contains(t, checker.splitGranted, first.GetID())
		require.Len(t, checker.splitWaitIndex, 1)
		require.Contains(t, checker.splitWaitIndex, next.GetID())
		requireAffinitySplit(t, checker.Check(first))
	})
}

func TestAffinitySplitFallbackExpiresDespiteStoreLimit(t *testing.T) {
	requireClassicAffinitySplit(t)
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, ops := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		checker.maxSplitAttempts = 2
		regions := putOversizedAffinityRegions(cluster)
		blocked := regions[:2]
		healthyParent := regions[2].Clone(withAffinitySplitTestRange("z", "zz"))
		group := &affinity.Group{ID: "healthy", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 5}}
		createAffinitySplitTestGroup(t, cluster.GetAffinityManager(), group, healthyParent)
		healthy := []*core.RegionInfo{
			healthyParent.Clone(core.WithNewRegionID(4), withAffinitySplitTestRange("z", "zm")),
			healthyParent.Clone(core.WithNewRegionID(5), withAffinitySplitTestRange("zm", "zz")),
		}
		for _, region := range healthy {
			cluster.PutRegion(region)
		}

		for _, region := range blocked {
			controller.tryAddOperators(region)
			requireAffinitySplit(t, []*operator.Operator{ops.GetOperator(region.GetID())})
		}
		advanceAffinitySplitTime(cluster, affinitySplitTimeout)
		for _, region := range blocked {
			split := ops.GetOperator(region.GetID())
			ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
			require.Equal(t, operator.TIMEOUT, split.Status())
		}

		// Use up the target store's add-peer budget at a refill rate too low
		// to admit another move during the test.
		cluster.SetStoreLimit(3, storelimit.AddPeer, 0.00006)
		move := requireAffinityMove(t, controller.CheckRegion(blocked[0]))
		require.True(t, ops.AddOperator(move))
		require.True(t, ops.RemoveOperator(move))

		limitRejections := operator.OperatorExceededStoreLimitCounter.WithLabelValues("affinity-move-region")
		revisit := func() {
			before := promtestutil.ToFloat64(limitRejections)
			for _, region := range blocked {
				controller.tryAddOperators(region)
				require.Nil(t, ops.GetOperator(region.GetID()))
			}
			require.Equal(t, before+float64(len(blocked)), promtestutil.ToFloat64(limitRejections))
		}
		revisit()
		limitWaits := checkerCounter.WithLabelValues(affinityChecker, "wait-split-attempt-limit")
		before := promtestutil.ToFloat64(limitWaits)
		for _, region := range healthy {
			require.Empty(t, controller.CheckRegion(region))
		}
		require.Equal(t, before+float64(len(healthy)), promtestutil.ToFloat64(limitWaits))
		for range 3 {
			advanceAffinitySplitTime(cluster, affinitySplitAttemptTTL/4)
			revisit()
		}
		// Refresh the pending entries within their three-minute TTL.
		advanceAffinitySplitTime(cluster, affinitySplitAttemptTTL/4-2*time.Minute)
		revisit()
		advanceAffinitySplitTime(cluster, 2*time.Minute)

		// The pending retry runs before the key-ordered scan, which encounters
		// both blocked Regions again before either healthy Region.
		require.ElementsMatch(t, []uint64{blocked[0].GetID(), blocked[1].GetID()}, controller.GetPendingProcessedRegions())
		before = promtestutil.ToFloat64(limitWaits)
		controller.checkPendingProcessedRegions()
		require.Equal(t, before+float64(len(blocked)), promtestutil.ToFloat64(limitWaits))
		for _, region := range blocked {
			controller.tryAddOperators(region)
			require.Nil(t, ops.GetOperator(region.GetID()))
			require.NotContains(t, checker.splitAttempts, region.GetID())
		}
		require.Equal(t, before+2*float64(len(blocked)), promtestutil.ToFloat64(limitWaits))
		for _, region := range healthy {
			requireAffinitySplit(t, controller.CheckRegion(region))
		}
	})
}

func TestAffinitySplitReservationExpiry(t *testing.T) {
	requireClassicAffinitySplit(t)
	for _, reason := range []string{"expired", "merged", "claimed after expiry"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, controller, _ := newAffinitySplitTest(t)
				checker := controller.GetAffinityChecker()
				checker.maxSplitAttempts = 1
				regions := putOversizedAffinityRegions(cluster)
				owner, first, next := regions[0], regions[1], regions[2]
				requireAffinitySplit(t, checker.Check(owner))
				require.Empty(t, checker.Check(first))
				require.Empty(t, checker.Check(next))

				advanceAffinitySplitTime(cluster, affinitySplitAttemptTTL)
				require.Empty(t, checker.Check(owner))
				require.Contains(t, checker.splitGranted, first.GetID(), "the first waiter gets the only slot")
				require.Empty(t, checker.Check(next), "the later waiter cannot claim the first waiter's slot")

				switch reason {
				case "claimed after expiry":
					advanceAffinitySplitTime(cluster, affinitySplitGrantTTL)
					requireAffinitySplit(t, checker.Check(first))
					require.Empty(t, checker.splitGranted)
					require.Contains(t, checker.splitWaitIndex, next.GetID())
					return
				case "expired":
					advanceAffinitySplitTime(cluster, affinitySplitGrantTTL)
				case "merged":
					next = next.Clone(withAffinitySplitTestRange("g", "z"), core.WithIncVersion())
					cluster.PutRegion(next)
					require.Nil(t, cluster.GetRegion(first.GetID()))
					advanceAffinitySplitTime(cluster, affinitySplitPruneInterval)
				}

				requireAffinitySplit(t, checker.Check(next))
				require.Empty(t, checker.splitGranted)
			})
		})
	}
}

func TestAffinitySplitBeforeLearnerMove(t *testing.T) {
	requireClassicAffinitySplit(t)
	for _, test := range []struct {
		name        string
		stores      []string
		joint       bool
		version4    bool
		copyToStore uint64
	}{
		{name: "copy to another store", stores: []string{"4", "5"}, joint: true, copyToStore: 5},
		{name: "swap roles with joint consensus", joint: true},
		{name: "swap roles without joint consensus", copyToStore: 3},
		{name: "swap roles with joint consensus enabled on 4.0", joint: true, version4: true, copyToStore: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, cluster, _ := newLearnerConflictController(t)
			setAffinitySplitStoreConfig(cluster)
			cluster.SetEnableUseJointConsensus(test.joint)
			if test.version4 {
				cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
			}
			require.NoError(t, cluster.GetRuleManager().SetRule(newLearnerRule(test.stores...)))
			region := putLearners(cluster, 1, 4).Clone(withAffinitySplitTestRange("a", "b"),
				core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
			cluster.PutRegion(region)
			manager := cluster.GetAffinityManager()
			group := &affinity.Group{ID: "large", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}}
			createAffinitySplitTestGroup(t, manager, group, region)
			moves := promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter)
			conflicts := promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter)
			if test.copyToStore != 0 {
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
			var addedStores []uint64
			for i := range move.Len() {
				if step, ok := move.Step(i).(operator.AddLearner); ok {
					addedStores = append(addedStores, step.ToStore)
				}
			}
			if test.copyToStore != 0 {
				require.Equal(t, []uint64{test.copyToStore}, addedStores)
			} else {
				require.Empty(t, addedStores)
			}
			require.Equal(t, moves+1, promtestutil.ToFloat64(affinityCheckerLearnerMoveCounter))
			require.Equal(t, conflicts, promtestutil.ToFloat64(affinityCheckerLearnerPeerConflictCounter))
			require.Empty(t, controller.CheckRegion(applyLearnerOperator(t, cluster, move)))
		})
	}
}

type affinitySplitTestCluster struct {
	*mockcluster.Cluster
	once              sync.Once
	onFirstGetRegion1 func()
}

func (cluster *affinitySplitTestCluster) GetRegion(regionID uint64) *core.RegionInfo {
	if regionID == 1 {
		first := false
		cluster.once.Do(func() { first = true })
		if first {
			cluster.onFirstGetRegion1()
		}
	}
	return cluster.Cluster.GetRegion(regionID)
}

func TestAffinitySplitBeforeMoveStaleHealth(t *testing.T) {
	requireClassicAffinitySplit(t)
	for _, name := range []string{"down peer", "pending peer", "no leader", "leader changed"} {
		t.Run(name, func(t *testing.T) {
			cluster, controller, _ := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			checker.startTime = time.Now().Add(-recentMergeTTL)
			patrol := cluster.GetRegion(1)
			var latest *core.RegionInfo
			switch name {
			case "down peer":
				patrol = patrol.Clone(core.SetApproximateSize(1))
				latest = patrol.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: patrol.GetStorePeer(4)}}))
			case "pending peer":
				patrol = patrol.Clone(core.SetApproximateSize(1))
				latest = patrol.Clone(core.WithPendingPeers([]*metapb.Peer{patrol.GetStorePeer(4)}))
			case "no leader":
				latest = patrol.Clone(core.WithLeader(nil))
			case "leader changed":
				latest = patrol.Clone(core.WithLeader(patrol.GetStorePeer(2)))
			}
			cluster.PutRegion(latest)
			require.Same(t, latest, cluster.GetRegion(patrol.GetID()))
			require.Equal(t, patrol.GetRegionEpoch(), latest.GetRegionEpoch())

			require.Empty(t, checker.Check(patrol))
			require.Empty(t, checker.splitAttempts)
		})
	}
}

func TestAffinitySplitBeforeMoveStaleSize(t *testing.T) {
	requireClassicAffinitySplit(t)
	t.Run("scan split", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cluster, controller, ops := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			previous := cluster.GetRegion(1)
			atTarget := previous.Clone(core.WithIncVersion(), core.WithReplacePeerStore(4, 3))
			large := atTarget.Clone(core.WithIncConfVer(), core.WithReplacePeerStore(3, 4))
			small := large.Clone(core.SetApproximateSize(1))
			cluster.PutRegion(small)
			cluster.PutRegion(large)
			// Patrol still holds the smaller snapshot when the heartbeat has
			// published a larger estimate at the same epoch.
			controller.tryAddOperators(small)
			split := requireAffinitySplit(t, []*operator.Operator{ops.GetOperator(1)})
			require.Empty(t, checker.Check(small))
			require.Same(t, split, checker.splitAttempts[1].split)
			staleVersion := core.SetRegionVersion(previous.GetRegionEpoch().GetVersion())
			staleConfVer := core.SetRegionConfVer(atTarget.GetRegionEpoch().GetConfVer())
			require.Empty(t, checker.Check(small.Clone(staleVersion)))
			require.Empty(t, checker.Check(small.Clone(staleConfVer)))
			require.Same(t, split, checker.splitAttempts[1].split)
			require.Empty(t, checker.Check(atTarget))
			require.Same(t, split, checker.splitAttempts[1].split)

			require.Empty(t, checker.Check(large))
			attempt := checker.splitAttempts[1]
			require.Equal(t, split.GetStartTime(), attempt.startedAt)
			advanceAffinitySplitTime(cluster, affinitySplitTimeout)
			ops.Dispatch(large, operator.DispatchFromHeartBeat, func(uint64) {})
			require.Equal(t, operator.TIMEOUT, split.Status())
			require.Empty(t, checker.Check(large.Clone(staleVersion)))
			require.Equal(t, attempt, checker.splitAttempts[1])
			move := requireAffinityMove(t, checker.Check(small))
			require.Equal(t, large.GetApproximateSize(), move.ApproximateSize)
			requireAffinityMove(t, controller.CheckRegion(large))
		})
	})
	t.Run("target update during cleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cluster, controller, ops := newAffinitySplitTest(t)
			checker := controller.GetAffinityChecker()
			manager := cluster.GetAffinityManager()
			atTargetA := cluster.GetRegion(1).Clone(core.WithIncConfVer(), core.WithReplacePeerStore(4, 3))
			cluster.PutRegion(atTargetA)
			_, aligned := manager.GetRegionAffinityGroupState(atTargetA)
			require.True(t, aligned)

			checker.cluster = &affinitySplitTestCluster{
				Cluster: cluster,
				onFirstGetRegion1: func() {
					_, err := manager.UpdateAffinityGroupPeers("split", 1, []uint64{1, 2, 4})
					require.NoError(t, err)
					controller.tryAddOperators(atTargetA)
				},
			}
			require.Empty(t, checker.Check(atTargetA))
			split := requireAffinitySplit(t, []*operator.Operator{ops.GetOperator(1)})
			assert.Same(t, split, checker.splitAttempts[1].split)

			advanceAffinitySplitTime(cluster, affinitySplitTimeout)
			ops.Dispatch(atTargetA, operator.DispatchFromHeartBeat, func(uint64) {})
			require.Equal(t, operator.TIMEOUT, split.Status())
			requireAffinityMove(t, controller.CheckRegion(atTargetA))
			require.Contains(t, checker.splitAttempts, uint64(1))

			atTargetB := atTargetA.Clone(core.WithIncConfVer(), core.WithReplacePeerStore(3, 4))
			cluster.PutRegion(atTargetB)
			require.Empty(t, checker.Check(atTargetB))
			require.NotContains(t, checker.splitAttempts, uint64(1))
		})
	})
	t.Run("unknown size", func(t *testing.T) {
		cluster, controller, _ := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		previous := cluster.GetRegion(1)
		unknown := previous.Clone(core.WithIncVersion(), core.SetApproximateSize(0))
		cluster.PutRegion(unknown)
		require.Empty(t, checker.Check(unknown))
		attempt := checker.splitAttempts[1]
		require.Equal(t, unknown.GetRegionEpoch().GetVersion(), attempt.version)
		require.Nil(t, attempt.split)

		require.Empty(t, checker.Check(previous.Clone(core.SetApproximateSize(1))))
		require.Equal(t, attempt, checker.splitAttempts[1])
	})
}

func TestAffinitySplitBeforeMoveStaleTarget(t *testing.T) {
	requireClassicAffinitySplit(t)
	synctest.Test(t, func(t *testing.T) {
		cluster, controller, ops := newAffinitySplitTest(t)
		checker := controller.GetAffinityChecker()
		manager := cluster.GetAffinityManager()
		region := cluster.GetRegion(1)
		var attempt affinitySplitAttempt
		checker.cluster = &affinitySplitTestCluster{
			Cluster: cluster,
			onFirstGetRegion1: func() {
				// The outer check has already captured target {1, 2, 3}.
				_, err := manager.UpdateAffinityGroupPeers("split", 1, []uint64{1, 2, 5})
				require.NoError(t, err)
				controller.tryAddOperators(region)
				split := requireAffinitySplit(t, []*operator.Operator{ops.GetOperator(region.GetID())})
				require.Empty(t, checker.Check(region))
				attempt = checker.splitAttempts[region.GetID()]
				require.Equal(t, split.GetStartTime(), attempt.startedAt)
			},
		}
		require.Empty(t, checker.Check(region))
		require.Equal(t, attempt, checker.splitAttempts[region.GetID()])

		advanceAffinitySplitTime(cluster, affinitySplitTimeout-time.Second)
		ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		require.Same(t, attempt.split, ops.GetOperator(region.GetID()))
		advanceAffinitySplitTime(cluster, time.Second)
		ops.Dispatch(region, operator.DispatchFromHeartBeat, func(uint64) {})
		require.Equal(t, operator.TIMEOUT, attempt.split.Status())
		requireAffinityMove(t, controller.CheckRegion(region))
	})
}
