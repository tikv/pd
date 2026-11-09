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

	"github.com/docker/go-units"
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
	// its first split check, and an empty Region reports 1 byte. Region 1 keeps
	// its ID and reports 0 bytes as well if TiKV had no size for it before the
	// split, for example right after a leader transfer.
	parent := cluster.GetRegion(1)
	regions := map[uint64]*core.RegionInfo{
		1: parent.Clone(core.WithEndKey([]byte("g")), core.WithIncVersion()),
		2: parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("g")), core.WithEndKey([]byte("m")), core.WithIncVersion()),
		3: parent.Clone(core.WithNewRegionID(3), core.WithStartKey([]byte("m")), core.WithIncVersion()),
	}
	// heartbeat handles a heartbeat as processRegionHeartbeat does and checks the Region.
	heartbeat := func(id, size uint64) []*operator.Operator {
		region := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: regions[id].GetMeta(), Leader: regions[id].GetLeader(), ApproximateSize: size,
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
	re.Empty(heartbeat(1, 0), "the size after the split is not reported yet")
	re.Empty(heartbeat(2, 0), "the size is not reported yet")
	re.Empty(heartbeat(2, 0), "the size is still not reported")
	requireMove(heartbeat(2, 1))
	requireMove(heartbeat(3, 1))
}

func TestAffinityMergeWaitsForUnreportedSize(t *testing.T) {
	re := require.New(t)
	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(256)
	cluster := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 3; id++ {
		cluster.AddRegionStore(id, 10)
	}
	group := &affinity.Group{ID: "merge_unknown_size", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(createAffinityGroupForTest(cluster.GetAffinityManager(), group, []byte("a"), []byte("z")))
	cluster.AddLeaderRegionWithRange(1, "a", "m", 1, 2, 3)
	cluster.AddLeaderRegionWithRange(2, "m", "z", 1, 2, 3)
	cluster.PutRegion(cluster.GetRegion(1).Clone(core.SetApproximateSize(200), core.SetSource(core.Heartbeat)))
	cluster.PutRegion(cluster.GetRegion(2).Clone(core.SetApproximateSize(150), core.SetSource(core.Heartbeat)))
	checker := newTestAffinityChecker(t.Context(), cluster, opt)
	re.Empty(checker.Check(cluster.GetRegion(1)), "200 + 150 MiB exceeds the limit")

	// A USEKEY split of Region 2 at the target, for example by SPLIT TABLE. Region 2
	// keeps its ID and reports the size from before the split. The new Region 3 on
	// its left reports 0 bytes until its first split check.
	parent := cluster.GetRegion(2)
	regions := map[uint64]*core.RegionInfo{
		2: parent.Clone(core.WithStartKey([]byte("s")), core.WithIncVersion()),
		3: parent.Clone(core.WithNewRegionID(3), core.WithEndKey([]byte("s")), core.WithIncVersion()),
	}
	// heartbeat handles a heartbeat as processRegionHeartbeat does.
	heartbeat := func(id, size uint64) {
		region := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: regions[id].GetMeta(), Leader: regions[id].GetLeader(), ApproximateSize: size,
		}, 0)
		origin := cluster.GetRegion(id)
		region.Inherit(origin, false)
		if _, saveCache, _, _ := core.GenerateRegionGuideFunc(false)(core.ContextTODO(), region, origin); saveCache {
			cluster.PutRegion(region)
		}
	}
	heartbeat(2, 150*units.MiB)
	heartbeat(3, 0)
	re.True(cluster.GetRegion(3).IsApproximateSizeUnknown())
	re.Equal(int64(core.EmptyRegionApproximateSize), cluster.GetRegion(3).GetApproximateSize())
	adjUnknown := promtestutil.ToFloat64(affinityMergeCheckerAdjUnknownSizeCounter)
	unknown := promtestutil.ToFloat64(affinityMergeCheckerUnknownSizeCounter)
	re.Empty(checker.Check(cluster.GetRegion(1)), "the size of the neighbor is not reported yet")
	re.Equal(adjUnknown+1, promtestutil.ToFloat64(affinityMergeCheckerAdjUnknownSizeCounter))
	re.Empty(checker.Check(cluster.GetRegion(3)), "the size of the Region is not reported yet")
	re.Equal(unknown+1, promtestutil.ToFloat64(affinityMergeCheckerUnknownSizeCounter))

	heartbeat(2, 75*units.MiB)
	heartbeat(3, 75*units.MiB)
	re.Empty(checker.Check(cluster.GetRegion(1)), "200 + 75 MiB exceeds the limit")
	ops := checker.Check(cluster.GetRegion(3))
	re.Len(ops, 2, "75 + 75 MiB fits in the limit")
	re.Equal("affinity-merge-region", ops[0].Desc())
	re.Equal(uint64(3), ops[0].RegionID())
	re.Equal(uint64(2), ops[1].RegionID())
}

// ageAffinitySplitAttempt makes the first split of the attempt start wait ago.
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
			requireAffinitySplit(t, checker.Check(region))

			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout/2)
			firstAttempt, ok := checker.splitAttempts.Get(region.GetID())
			re.True(ok)
			requireAffinitySplit(t, checker.Check(region))
			retried, ok := checker.splitAttempts.Get(region.GetID())
			re.True(ok)
			re.Equal(firstAttempt.(affinitySplitAttempt).startedAt, retried.(affinitySplitAttempt).startedAt,
				"retrying the same epoch must not reset the wait")

			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			// A peer configuration change does not change the split epoch version.
			region = region.Clone(core.WithIncConfVer())
			cluster.PutRegion(region)
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
			re.Zero(ops[0].Kind() & operator.OpSplit)
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackTransitions(t *testing.T) {
	for _, reason := range []string{"epoch", "expired", "evicted", "new target", "new move", "current target", "split succeeds", "unknown size"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			checker.splitAttempts = cache.NewCache(2, cache.LRUCache)
			region := cluster.GetRegion(1)
			manager := cluster.GetAffinityManager()
			if reason == "unknown size" {
				region = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
					Region: region.GetMeta(), Leader: region.GetLeader(),
				}, 0)
				region.Inherit(nil, false)
				cluster.PutRegion(region)
				re.True(region.IsApproximateSizeUnknown())
				re.Equal(int64(core.EmptyRegionApproximateSize), region.GetApproximateSize())
				re.Empty(checker.Check(region))
				re.Empty(checker.Check(region), "a retry must still wait before the timeout")
				originalTimeout := affinitySplitWaitTimeout
				affinitySplitWaitTimeout = 0
				t.Cleanup(func() { affinitySplitWaitTimeout = originalTimeout })
				fallbacks := promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter)
				for range 2 {
					ops := checker.Check(region)
					re.Len(ops, 1, "an unreported size must not block the move forever")
					re.Equal("affinity-move-region", ops[0].Desc())
				}
				re.True(region.IsApproximateSizeUnknown())
				re.Equal(fallbacks+2, promtestutil.ToFloat64(affinityCheckerSplitTimeoutCounter))
				region = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
					Region: region.GetMeta(), Leader: region.GetLeader(), ApproximateSize: 64 * units.MiB,
				}, 0)
				cluster.PutRegion(region)
				requireAffinitySplit(t, checker.Check(region))
				return
			}
			if reason == "split succeeds" {
				region = region.Clone(core.SetApproximateSize(32))
				cluster.PutRegion(region)
			}
			requireAffinitySplit(t, checker.Check(region))
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			// Later patrols still allow the move when it has not been added.
			if reason == "split succeeds" {
				ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout/2)
			} else {
				for range 2 {
					ops := checker.Check(region)
					re.Len(ops, 1)
					re.Equal("affinity-move-region", ops[0].Desc())
				}
			}
			switch reason {
			case "epoch":
				region = region.Clone(core.WithIncVersion())
			case "expired":
				value, ok := checker.splitAttempts.Get(region.GetID())
				re.True(ok)
				attempt := value.(affinitySplitAttempt)
				attempt.expiresAt = time.Now().Add(-time.Second)
				checker.splitAttempts.Put(region.GetID(), attempt)
			case "evicted":
				group := manager.GetAffinityGroupState("split_before_move")
				for id := uint64(2); id <= 3; id++ {
					op, ready := checker.prepareAffinityPeerMove(region.Clone(core.WithNewRegionID(id)), group)
					re.False(ready)
					requireAffinitySplit(t, []*operator.Operator{op})
				}
				re.Equal(2, checker.splitAttempts.Len())
				_, ok := checker.splitAttempts.Peek(region.GetID())
				re.False(ok)
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
					re.Len(ops, 1)
					re.Equal("affinity-move-region", ops[0].Desc())
				}
				return
			}
			cluster.PutRegion(region)
			requireAffinitySplit(t, checker.Check(region))
			value, ok := checker.splitAttempts.Get(region.GetID())
			re.True(ok)
			attempt := value.(affinitySplitAttempt)
			re.Equal(region.GetRegionEpoch().GetVersion(), attempt.version)
			re.True(attempt.startedAt.IsZero(), "a new attempt must start a fresh wait")
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
			re.NoError(manager.DeleteAffinityGroups([]string{"split_before_move"}, true))
			re.Empty(checker.Check(region))
		})
	}
}

func TestAffinitySplitBeforeMoveFallbackAfterSplitStarts(t *testing.T) {
	originalTimeout := affinitySplitWaitTimeout
	// Without a wait, the move is allowed as soon as a split has started.
	affinitySplitWaitTimeout = 0
	t.Cleanup(func() { affinitySplitWaitTimeout = originalTimeout })

	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	region := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(region))
	// The operator controller did not start the split, for example because its
	// waiting queue was full, so it sent no split request and the next patrol
	// requests the split again.
	ops := checker.Check(region)
	requireAffinitySplit(t, ops)
	// The controller starts an operator when it admits it and sends the request.
	re.True(ops[0].Start())
	ops = checker.Check(region)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

func TestAffinitySplitBeforeMoveFallbackKeepsGates(t *testing.T) {
	for _, reason := range []string{"recent merge", "startup"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1)
			requireAffinitySplit(t, checker.Check(region))
			ageAffinitySplitAttempt(t, checker, region.GetID(), affinitySplitWaitTimeout+time.Second)
			switch reason {
			case "recent merge":
				checker.recentMergeCache.Put(region.GetID(), nil)
			case "startup":
				checker.startTime = time.Now()
			}
			re.Empty(checker.Check(region))

			switch reason {
			case "recent merge":
				checker.recentMergeCache.Remove(region.GetID())
			case "startup":
				checker.startTime = time.Now().Add(-2 * recentMergeTTL)
			}
			ops := checker.Check(region)
			re.Len(ops, 1)
			re.Equal("affinity-move-region", ops[0].Desc())
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
