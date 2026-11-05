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

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func newAffinitySplitTest(t *testing.T) (*mockcluster.Cluster, *AffinityChecker) {
	t.Helper()
	re := require.New(t)
	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(64)
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 4; id++ {
		tc.AddRegionStore(id, 10)
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
		{name: "unknown size", size: 0, wantWait: true},
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
			ops := checker.Check(region)
			if tc.wantWait {
				re.Empty(ops)
			} else if tc.wantSplit {
				requireAffinitySplit(t, ops)
			} else {
				re.Len(ops, 1)
				re.Equal("affinity-move-region", ops[0].Desc())
				re.Zero(ops[0].Kind() & operator.OpSplit)
			}
		})
	}
}

func TestAffinitySplitBeforeMoveUnreportedSize(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	// A USEKEY split of the 64 MiB Region 1 adds Regions 2 and 3. TiKV does not
	// share the parent's size with them, so a new Region reports 0 bytes until
	// its first split check, and an empty Region reports 1 byte.
	parent := cluster.GetRegion(1)
	cluster.PutRegion(parent.Clone(core.WithEndKey([]byte("g")), core.WithIncVersion()))
	children := map[uint64]*core.RegionInfo{
		2: parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("g")), core.WithEndKey([]byte("m")), core.WithIncVersion()),
		3: parent.Clone(core.WithNewRegionID(3), core.WithStartKey([]byte("m")), core.WithIncVersion()),
	}
	// heartbeat handles a heartbeat as processRegionHeartbeat does and checks the Region.
	heartbeat := func(id, size uint64) []*operator.Operator {
		region := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: children[id].GetMeta(), Leader: children[id].GetLeader(), ApproximateSize: size,
		}, 0)
		origin := cluster.GetRegion(id)
		region.Inherit(origin, false)
		if _, saveCache, _, _ := core.GenerateRegionGuideFunc(false)(core.ContextTODO(), region, origin); saveCache {
			cluster.PutRegion(region)
		}
		return checker.Check(cluster.GetRegion(id))
	}
	requireMove := func(ops []*operator.Operator) {
		re.Len(ops, 1)
		re.Equal("affinity-move-region", ops[0].Desc())
	}
	re.Empty(heartbeat(2, 0), "the size is not reported yet")
	re.Empty(heartbeat(2, 0), "the size is still not reported")
	requireMove(heartbeat(2, 1))
	requireMove(heartbeat(3, 1))
}

func ageAffinitySplitAttempt(t *testing.T, checker *AffinityChecker, regionID uint64, wait time.Duration) {
	t.Helper()
	value, ok := checker.splitAttempts.Get(regionID)
	require.True(t, ok)
	attempt := value.(affinitySplitAttempt)
	attempt.startedAt = time.Now().Add(-wait)
	checker.splitAttempts.Put(regionID, attempt)
}

func TestAffinitySplitBeforeMoveFallback(t *testing.T) {
	originalTimeout := affinitySplitWaitTimeout
	affinitySplitWaitTimeout = time.Minute
	t.Cleanup(func() { affinitySplitWaitTimeout = originalTimeout })

	for _, tc := range []struct {
		name       string
		size, keys int64
	}{
		{name: "over size", size: 64, keys: 10},
		{name: "over keys", size: 1, keys: 160001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1).Clone(core.SetApproximateSize(tc.size), core.SetApproximateKeys(tc.keys))
			cluster.PutRegion(region)
			before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
			requireAffinitySplit(t, checker.Check(region))

			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout/2)
			firstAttempt, ok := checker.splitAttempts.Get(region.GetID())
			re.True(ok)
			requireAffinitySplit(t, checker.Check(region))
			retried, ok := checker.splitAttempts.Get(region.GetID())
			re.True(ok)
			re.Equal(firstAttempt.(affinitySplitAttempt).startedAt, retried.(affinitySplitAttempt).startedAt,
				"retrying the same epoch must not reset the wait")
			re.Equal(before, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			// A peer configuration change does not change the split epoch version.
			region = region.Clone(core.WithIncConfVer())
			cluster.PutRegion(region)
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
			re.Zero(ops[0].Kind() & operator.OpSplit)
			re.Equal(before+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackEpoch(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	region := cluster.GetRegion(1)
	before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
	requireAffinitySplit(t, checker.Check(region))
	ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)

	region = region.Clone(core.WithIncVersion())
	cluster.PutRegion(region)
	resetAt := time.Now()
	requireAffinitySplit(t, checker.Check(region))
	value, ok := checker.splitAttempts.Get(region.GetID())
	re.True(ok)
	attempt := value.(affinitySplitAttempt)
	re.Equal(region.GetRegionEpoch().GetVersion(), attempt.version)
	re.False(attempt.startedAt.Before(resetAt), "the new epoch must start a fresh wait")
	re.Equal(before, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))

	ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
	ops := checker.Check(region)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(before+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
}

func TestAffinitySplitBeforeMoveFallbackReportsOnce(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
	// fallBack times out a new split attempt and evaluates the Region twice. The
	// move is not added here, so the second evaluation is what the next patrol
	// does. Both evaluations allow the move, and the attempt is counted once.
	fallBack := func(region *core.RegionInfo, reported float64) {
		requireAffinitySplit(t, checker.Check(region))
		ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
		for range 2 {
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
			re.Equal(before+reported, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
		}
	}
	region := cluster.GetRegion(1)
	fallBack(region, 1)
	// A new epoch version starts a new attempt, which is reported again.
	region = region.Clone(core.WithIncVersion())
	cluster.PutRegion(region)
	fallBack(region, 2)

	// Each later move of the unsplit Region starts a new attempt as well.
	move := func(from, to uint64) {
		region = region.Clone(core.WithReplacePeerStore(from, to), core.WithIncConfVer())
		cluster.PutRegion(region)
	}
	move(4, 3)
	// The target changes before a patrol checks the Region at the old target.
	_, err := cluster.GetAffinityManager().UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 4})
	re.NoError(err)
	fallBack(region, 3)
	move(3, 4)
	re.Empty(checker.Check(region), "at the target")
	// The Region leaves the target, for example when a down peer is replaced.
	move(4, 3)
	fallBack(region, 4)
}

func TestAffinitySplitBeforeMoveFallbackKeepsGates(t *testing.T) {
	for _, reason := range []string{"recent merge", "startup"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1)
			before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
			requireAffinitySplit(t, checker.Check(region))
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			switch reason {
			case "recent merge":
				checker.recentMergeCache.Put(region.GetID(), nil)
			case "startup":
				checker.startTime = time.Now()
			}
			re.Empty(checker.Check(region))
			re.Equal(before, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter),
				"a blocked fallback must not be counted")

			switch reason {
			case "recent merge":
				checker.recentMergeCache.Remove(region.GetID())
			case "startup":
				checker.startTime = time.Now().Add(-2 * recentMergeTTL)
			}
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
			re.Equal(before+1, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackSplitSucceeds(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	region := cluster.GetRegion(1).Clone(core.SetApproximateSize(32))
	cluster.PutRegion(region)
	before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
	requireAffinitySplit(t, checker.Check(region))
	ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout/2)

	left := region.Clone(core.WithEndKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
	right := region.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
	for _, child := range []*core.RegionInfo{left, right} {
		cluster.PutRegion(child)
		ops := checker.Check(child)
		re.Len(ops, 1)
		re.Equal("affinity-move-region", ops[0].Desc())
	}
	re.Equal(before, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
}

func TestAffinitySplitBeforeMoveFallbackCache(t *testing.T) {
	for _, reason := range []string{"expired", "evicted"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			checker.splitAttempts = cache.NewCache(2, cache.LRUCache)
			region := cluster.GetRegion(1)
			before := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
			requireAffinitySplit(t, checker.Check(region))
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			switch reason {
			case "expired":
				value, ok := checker.splitAttempts.Get(region.GetID())
				re.True(ok)
				attempt := value.(affinitySplitAttempt)
				attempt.expiresAt = time.Now().Add(-time.Second)
				checker.splitAttempts.Put(region.GetID(), attempt)
			case "evicted":
				group := cluster.GetAffinityManager().GetAffinityGroupState("split_before_move")
				for id := uint64(2); id <= 3; id++ {
					op, ready := checker.prepareAffinityPeerMove(region.Clone(core.WithNewRegionID(id)), group)
					re.False(ready)
					requireAffinitySplit(t, []*operator.Operator{op})
				}
				re.Equal(2, checker.splitAttempts.Len())
				_, ok := checker.splitAttempts.Peek(region.GetID())
				re.False(ok)
			}
			requireAffinitySplit(t, checker.Check(region))
			re.Equal(before, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
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
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	requireAffinitySplit(t, checker.Check(large))
	re.Empty(checker.Check(unreported))
}

func TestAffinitySplitBeforeMoveCooldown(t *testing.T) {
	for _, reason := range []string{"recent merge", "startup"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1).Clone(core.SetApproximateSize(16))
			switch reason {
			case "recent merge":
				checker.recentMergeCache.PutWithTTL(region.GetID(), nil, recentMergeTTL)
			case "startup":
				checker.startTime = time.Now()
			}
			cluster.PutRegion(region)
			re.Empty(checker.Check(region))
		})
	}
}

func TestAffinitySplitBeforeMoveUsesCurrentTarget(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	manager := cluster.GetAffinityManager()
	parent := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(parent))
	_, err := manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 4})
	re.NoError(err)
	re.Empty(checker.Check(parent), "the canceled move no longer needs a split")
	_, err = manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 3})
	re.NoError(err)
	requireAffinitySplit(t, checker.Check(parent))
	re.NoError(manager.DeleteAffinityGroups([]string{"split_before_move"}, true))
	re.Empty(checker.Check(parent))
}

func TestAffinitySplitBeforeMoveController(t *testing.T) {
	re := require.New(t)
	cluster, _ := newAffinitySplitTest(t)
	stream := hbstream.NewTestHeartbeatStreams(t.Context(), cluster, false)
	t.Cleanup(stream.Close)
	opController := operator.NewController(t.Context(), cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	controller := NewController(t.Context(), cluster, cluster.GetCheckerConfig(), opController)
	controller.GetAffinityChecker().startTime = time.Now().Add(-2 * time.Minute)
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
