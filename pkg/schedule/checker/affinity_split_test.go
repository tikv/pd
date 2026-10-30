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

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newAffinitySplitTest(t *testing.T) (*mockcluster.Cluster, *AffinityChecker) {
	t.Helper()
	re := require.New(t)
	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(64)
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= 5; id++ {
		tc.AddRegionStore(id, 10)
	}
	storeConfig := &config.StoreConfig{Coprocessor: config.Coprocessor{
		RegionMaxSize: "24MiB", RegionSplitSize: "16MiB", RegionSplitKeys: 160000,
	}}
	storeConfig.Adjust()
	tc.SetStoreConfig(storeConfig)
	group := &affinity.Group{ID: "split_before_move", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(createAffinityGroupForTest(tc.GetAffinityManager(), group, []byte("a"), []byte("z")))
	tc.AddLeaderRegionWithRange(1, "a", "z", 1, 2, 4)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(64), core.SetSource(core.Heartbeat))
	tc.PutRegion(region)
	return tc, newTestAffinityChecker(t.Context(), tc, opt)
}

func requireAffinitySplit(t *testing.T, ops []*operator.Operator) *operator.Operator {
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
	return op
}

func TestAffinitySplitBeforeMoveThresholds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		size, keys int64
		leaderOnly bool
		wantSplit  bool
		wantWait   bool
	}{
		{name: "large", size: 64, keys: 10, wantSplit: true},
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

func TestAffinitySplitBeforeMoveWaitsForCoverage(t *testing.T) {
	for _, missing := range []string{"first", "middle", "last", "unbounded start", "unbounded end", "storage heartbeat"} {
		t.Run(missing, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			// A small child has arrived, but the original range is not fully reported.
			start, end := "a", "z"
			if missing == "unbounded start" || missing == "unbounded end" {
				re.NoError(cluster.GetAffinityManager().DeleteAffinityGroups([]string{"split_before_move"}, true))
				if missing == "unbounded start" {
					start = ""
				} else {
					end = ""
				}
				group := &affinity.Group{ID: "split_before_move", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
				re.NoError(createAffinityGroupForTest(cluster.GetAffinityManager(), group, []byte(start), []byte(end)))
			}
			childStart, childEnd := start, "m"
			if missing == "first" || missing == "unbounded start" {
				childStart, childEnd = "m", end
			}
			child := cluster.GetRegion(1).Clone(core.WithStartKey([]byte(childStart)), core.WithEndKey([]byte(childEnd)),
				core.WithIncVersion(), core.SetApproximateSize(8))
			cluster.PutRegion(child)
			if missing == "middle" {
				last := child.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("t")), core.WithEndKey([]byte(end)))
				cluster.PutRegion(last)
			} else if missing == "storage heartbeat" {
				last := child.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")), core.WithEndKey([]byte(end)),
					core.WithLeader(nil), core.SetApproximateSize(0), core.SetSource(core.Storage))
				cluster.PutRegion(last)
			}
			for range 2 {
				re.Empty(checker.Check(child), "a split's changed boundary does not prove all children are ready")
				// A new checker has no remembered split operation and must reach the same decision.
				checker = newTestAffinityChecker(t.Context(), cluster, cluster.PersistOptions)
			}
		})
	}
}

func TestAffinitySplitBeforeMoveConvergesThenMerges(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	// ApplyOperator simulates individual peer steps, not joint consensus.
	cluster.SetEnableUseJointConsensus(false)
	parent := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(parent))
	// Simulate two rounds of half splits, preserving total reported size.
	left := parent.Clone(core.WithEndKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(32))
	right := parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(32))
	cluster.PutRegion(left)
	cluster.PutRegion(right)
	requireAffinitySplit(t, checker.Check(left))
	requireAffinitySplit(t, checker.Check(right))
	keys := []string{"a", "g", "m", "t", "z"}
	children := make([]*core.RegionInfo, 4)
	for i := range children {
		children[i] = parent.Clone(core.WithNewRegionID(uint64(i+1)), core.WithStartKey([]byte(keys[i])),
			core.WithEndKey([]byte(keys[i+1])), core.WithIncVersion(), core.WithIncVersion(), core.SetApproximateSize(16))
		cluster.PutRegion(children[i])
	}
	mergeChecker := NewMergeChecker(t.Context(), cluster, cluster.PersistOptions)
	mergeChecker.startTime = time.Now().Add(-24 * time.Hour)
	for i, child := range children {
		ops := checker.Check(child)
		re.Len(ops, 1)
		re.Equal("affinity-move-region", ops[0].Desc())
		operator.ApplyOperator(cluster, ops[0])
		children[i] = cluster.GetRegion(child.GetID())
		if i == 1 {
			// Two adjacent children have reached the target, but their siblings have not.
			re.Empty(checker.Check(children[0]))
			re.False(AllowMerge(cluster, children[0], children[1]))
			re.Empty(mergeChecker.Check(children[0]))
			cluster.SetAffinityScheduleLimit(0)
			re.True(AllowMerge(cluster, children[0], children[1]), "disabling affinity keeps the ordinary merge behavior")
			cluster.SetAffinityScheduleLimit(4)
		}
	}
	re.True(AllowMerge(cluster, children[0], children[1]))
	ops := checker.Check(children[0])
	re.Len(ops, 2)
	re.Equal("affinity-merge-region", ops[0].Desc())
	// The ordinary merge path also becomes available after the group converges.
	re.Len(mergeChecker.Check(children[0]), 2)
}

func TestAffinitySplitBeforeMoveKeepsSafetyGates(t *testing.T) {
	for _, reason := range []string{"evict leader", "learner conflict", "unhealthy", "recent merge", "startup"} {
		t.Run(reason, func(t *testing.T) {
			re := require.New(t)
			cluster, checker := newAffinitySplitTest(t)
			region := cluster.GetRegion(1)
			switch reason {
			case "evict leader":
				re.NoError(cluster.PauseLeaderTransfer(1, constant.In))
			case "learner conflict":
				re.NoError(cluster.GetRuleManager().SetRule(&placement.Rule{GroupID: "pd", ID: "learner", Role: placement.Learner, Count: 1}))
				region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 3, Role: metapb.PeerRole_Learner}))
			case "unhealthy":
				region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)}))
			case "recent merge":
				region = region.Clone(core.SetApproximateSize(16))
				checker.recentMergeCache.PutWithTTL(region.GetID(), nil, recentMergeTTL)
			case "startup":
				region = region.Clone(core.SetApproximateSize(16))
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
	stored, err := cluster.GetStorage().LoadAffinityGroup("split_before_move")
	re.NoError(err)
	parent := cluster.GetRegion(1)
	requireAffinitySplit(t, checker.Check(parent))
	afterSplit, err := cluster.GetStorage().LoadAffinityGroup("split_before_move")
	re.NoError(err)
	re.Equal(stored, afterSplit, "splitting does not change the stored target")
	_, err = manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 4})
	re.NoError(err)
	re.Empty(checker.Check(parent), "the canceled move no longer needs a split")
	_, err = manager.UpdateAffinityGroupPeers("split_before_move", 1, []uint64{1, 2, 3})
	re.NoError(err)
	requireAffinitySplit(t, checker.Check(parent))
	re.NoError(manager.DeleteAffinityGroups([]string{"split_before_move"}, true))
	re.Empty(checker.Check(parent))
}

func TestAffinitySplitBeforeMoveMultipleRanges(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	manager := cluster.GetAffinityManager()
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]affinity.GroupKeyRanges{{GroupID: "split_before_move",
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("za"), EndKey: []byte("zz")}},
	}}, nil))
	first := cluster.GetRegion(1).Clone(core.SetApproximateSize(16))
	cluster.PutRegion(first)
	re.Empty(checker.Check(first))
	second := first.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("za")), core.WithEndKey([]byte("zz")))
	cluster.PutRegion(second)
	ops := checker.Check(first)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
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
	controller.tryAddOperators(parent)
	re.Same(op, opController.GetOperator(parent.GetID()))
	re.Equal(uint64(1), opController.OperatorCount(operator.OpAffinity))
	// The same limit used for moves also prevents scheduling another split.
	cluster.SetAffinityScheduleLimit(1)
	other := parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")))
	parent = parent.Clone(core.WithEndKey([]byte("m")))
	cluster.PutRegion(parent)
	cluster.PutRegion(other)
	re.Empty(controller.CheckRegion(other))
}

func TestAffinitySplitBeforeMovePreservesLearners(t *testing.T) {
	re := require.New(t)
	cluster, checker := newAffinitySplitTest(t)
	cluster.SetEnableUseJointConsensus(false)
	re.NoError(cluster.GetRuleManager().SetRule(&placement.Rule{GroupID: "pd", ID: "learner", Role: placement.Learner, Count: 1}))
	learner := &metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}
	parent := cluster.GetRegion(1).Clone(core.WithAddPeer(learner), core.SetApproximateSize(32))
	cluster.PutRegion(parent)
	requireAffinitySplit(t, checker.Check(parent))
	left := parent.Clone(core.WithEndKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
	right := parent.Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("m")), core.WithIncVersion(), core.SetApproximateSize(16))
	cluster.PutRegion(left)
	cluster.PutRegion(right)
	for _, child := range []*core.RegionInfo{left, right} {
		ops := checker.Check(child)
		re.Len(ops, 1)
		re.Equal("affinity-move-region", ops[0].Desc())
		operator.ApplyOperator(cluster, ops[0])
		re.Equal(learner, cluster.GetRegion(child.GetID()).GetStoreLearner(5))
	}
}
