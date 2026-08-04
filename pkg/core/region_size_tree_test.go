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

package core

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestRegionSizeTreeStructuralUpdates(t *testing.T) {
	re := require.New(t)
	tree := newRegionSizeTree(context.Background(), NewRegionsInfo())
	t.Cleanup(tree.cancel)
	merged := newRegionSizeTreeTestRegion(1, "a", "z", 1, 100)
	tree.reconcile([]regionSizeTreeUpdate{{regionID: merged.GetID(), region: merged}})
	re.Equal(int64(100), tree.getRegionSizeByRange(nil, nil))
	re.Equal(int64(100), tree.getRegionSizeByRange([]byte("b"), []byte("m")))

	left := newRegionSizeTreeTestRegion(1, "a", "m", 2, 40)
	right := newRegionSizeTreeTestRegion(2, "m", "z", 2, 60)
	// Apply the right side first to cover out-of-order split notifications.
	tree.reconcile([]regionSizeTreeUpdate{{regionID: right.GetID(), region: right}})
	tree.reconcile([]regionSizeTreeUpdate{{regionID: left.GetID(), region: left}})
	re.Equal(2, tree.length())
	re.Equal(int64(100), tree.getRegionSizeByRange(nil, nil))
	re.Equal(int64(40), tree.getRegionSizeByRange([]byte("b"), []byte("m")))
	re.Equal(int64(60), tree.getRegionSizeByRange([]byte("m"), []byte("z")))

	left = newRegionSizeTreeTestRegion(1, "a", "m", 2, 50)
	tree.reconcile([]regionSizeTreeUpdate{{regionID: left.GetID(), region: left}})
	re.Equal(int64(110), tree.getRegionSizeByRange(nil, nil))

	merged = newRegionSizeTreeTestRegion(3, "a", "z", 3, 120)
	tree.reconcile([]regionSizeTreeUpdate{{regionID: merged.GetID(), region: merged}})
	re.Equal(1, tree.length())
	re.Equal(int64(120), tree.getRegionSizeByRange(nil, nil))

	// A delayed child removal deletes by ID and cannot remove the merged Region.
	tree.reconcile([]regionSizeTreeUpdate{{regionID: right.GetID()}})
	re.Equal(1, tree.length())
	re.Equal(int64(120), tree.getRegionSizeByRange(nil, nil))

	tree.reset()
	re.Zero(tree.length())
	re.Zero(tree.getRegionSizeByRange(nil, nil))
}

func TestRegionSizeTreeIsOptInAndRebuilds(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	region := newRegionSizeTreeTestRegion(1, "a", "z", 1, 10)
	regions.PutRegion(region)

	re.Nil(regions.sizeTree.Load())
	_, ready := regions.GetRegionSizeByRangeFromSizeTree([]byte("a"), []byte("z"))
	re.False(ready)

	startRegionSizeTreeForTest(t, regions)
	requireRegionSizeEventually(t, regions, []byte("a"), []byte("z"), 10)
	regions.StopRegionSizeTree()
	re.Nil(regions.sizeTree.Load())
}

func TestRegionSizeTreeMetrics(t *testing.T) {
	regions := NewRegionsInfo()
	tree := newRegionSizeTree(context.Background(), regions)
	regions.sizeTree.Store(tree)
	t.Cleanup(func() {
		regions.sizeTree.Store(nil)
		tree.cancel()
		resetRegionSizeTreeMetrics()
	})

	tree.notify(1, 2)
	tree.pendingMu.Lock()
	tree.pendingSince = time.Now().Add(-time.Second)
	tree.pendingMu.Unlock()
	tree.ready.Store(true)
	regions.CollectRegionSizeTreeMetrics()
	require.Equal(t, 1.0, testutil.ToFloat64(regionSizeTreeReadyGauge))
	require.Equal(t, 2.0, testutil.ToFloat64(regionSizeTreePendingGauge))
	require.GreaterOrEqual(t,
		testutil.ToFloat64(regionSizeTreeOldestPendingDurationGauge), 1.0,
	)

	tree.drain()
	regions.CollectRegionSizeTreeMetrics()
	require.Zero(t, testutil.ToFloat64(regionSizeTreePendingGauge))
	require.Zero(t, testutil.ToFloat64(regionSizeTreeOldestPendingDurationGauge))
}

func TestRegionSizeTreeIsNotReadyAfterContextCanceled(t *testing.T) {
	regions := NewRegionsInfo()
	regions.PutRegion(newRegionSizeTreeTestRegion(1, "a", "z", 1, 10))
	ctx, cancel := context.WithCancel(context.Background())
	regions.StartRegionSizeTree(ctx)
	t.Cleanup(regions.StopRegionSizeTree)

	tree := regions.sizeTree.Load()
	require.NotNil(t, tree)
	require.Eventually(t, tree.isReady, 5*time.Second, 10*time.Millisecond)
	cancel()

	// Queries must stop using the index as soon as its parent lifecycle ends,
	// even before StopRegionSizeTree removes the pointer.
	require.False(t, tree.isReady())
	_, ready := regions.GetRegionSizeByRangeFromSizeTree([]byte("a"), []byte("z"))
	require.False(t, ready)
	tree.wg.Wait()
	require.False(t, tree.ready.Load())
}

func TestRegionSizeTreeRebuildsAndScansInBatches(t *testing.T) {
	regions := NewRegionsInfo()
	regionCount := ScanRegionLimit + 2
	for i := range regionCount {
		endKey := fmt.Sprintf("%04d", i+1)
		if i == regionCount-1 {
			endKey = ""
		}
		regions.PutRegion(newRegionSizeTreeTestRegion(
			uint64(i+1), fmt.Sprintf("%04d", i), endKey, 1, 1,
		))
	}

	atomic.StoreInt64(&regions.t.lockCount, 0)
	startRegionSizeTreeForTest(t, regions)
	// Rebuild scans 1000 Regions at a time and does not reload every ID.
	require.Equal(t, int64(2), atomic.LoadInt64(&regions.t.lockCount))
	requireRegionSizeEventually(t, regions, nil, nil, int64(regionCount))
	require.Equal(t, int64(regionCount), getRegionSizeFromReadyTree(t, regions,
		[]byte("0000"), []byte("9999"),
	))

	// Placement-rule split keys produce single-ended ranges. Verify both forms
	// across the scan limit through the public size-tree query path.
	for _, keyRange := range []struct {
		name     string
		startKey []byte
		endKey   []byte
	}{
		{name: "prefix", endKey: []byte("1001")},
		{name: "suffix", startKey: []byte("0001")},
	} {
		t.Run(keyRange.name, func(t *testing.T) {
			rootSize := regions.GetRegionSizeByRange(keyRange.startKey, keyRange.endKey)
			require.Equal(t, int64(ScanRegionLimit+1), rootSize)
			require.Equal(t, rootSize, getRegionSizeFromReadyTree(
				t, regions, keyRange.startKey, keyRange.endKey,
			))
		})
	}
}

func TestRegionSizeTreeRebuildDoesNotReplayTakenPending(t *testing.T) {
	regions := NewRegionsInfo()
	regions.PutRegion(newRegionSizeTreeTestRegion(1, "a", "m", 1, 10))
	regions.PutRegion(newRegionSizeTreeTestRegion(2, "m", "", 1, 20))

	// Keep the worker stopped so rebuild and both notifications are taken in
	// the same drain iteration.
	tree := newRegionSizeTree(context.Background(), regions)
	regions.sizeTree.Store(tree)
	t.Cleanup(regions.StopRegionSizeTree)
	tree.notify(1, 2)
	tree.requestRebuild()

	atomic.StoreInt64(&regions.t.lockCount, 0)
	tree.drain()
	// The rebuild scans the root once. Replaying the two already-covered IDs
	// would acquire the root lock a second time.
	require.Equal(t, int64(1), atomic.LoadInt64(&regions.t.lockCount))
	require.Equal(t, int64(30), getRegionSizeFromReadyTree(t, regions, nil, nil))
}

func TestRegionSizeTreeCanceledRebuildIsNeverReady(t *testing.T) {
	regions := NewRegionsInfo()
	regionCount := ScanRegionLimit + 1
	for i := range regionCount {
		endKey := fmt.Sprintf("%04d", i+1)
		if i == regionCount-1 {
			endKey = ""
		}
		regions.PutRegion(newRegionSizeTreeTestRegion(
			uint64(i+1), fmt.Sprintf("%04d", i), endKey, 1, 1,
		))
	}

	tree := newRegionSizeTree(context.Background(), regions)
	tree.reset()
	tree.mu.RLock()
	atomic.StoreInt64(&regions.t.lockCount, 0)
	rebuildDone := make(chan bool, 1)
	go func() {
		rebuildDone <- tree.rebuild()
	}()
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&regions.t.lockCount) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// Cancel while the first batch is blocked on the size-tree lock. The first
	// batch may be applied after the lock is released, but it must not be
	// published as a complete index.
	tree.cancel()
	tree.mu.RUnlock()
	require.False(t, <-rebuildDone)
	tree.markReadyIfCurrent()
	require.False(t, tree.isReady())
	require.Less(t, tree.length(), regionCount)
}

func TestRegionSizeTreeResetDuringRebuildPublishesResetState(t *testing.T) {
	regions := NewRegionsInfo()
	regionCount := ScanRegionLimit + 1
	for i := range regionCount {
		endKey := fmt.Sprintf("%04d", i+1)
		if i == regionCount-1 {
			endKey = ""
		}
		regions.PutRegion(newRegionSizeTreeTestRegion(
			uint64(i+1), fmt.Sprintf("%04d", i), endKey, 1, 1,
		))
	}
	tree := newRegionSizeTree(context.Background(), regions)
	regions.sizeTree.Store(tree)
	t.Cleanup(regions.StopRegionSizeTree)

	tree.reset()
	tree.mu.RLock()
	atomic.StoreInt64(&regions.t.lockCount, 0)
	rebuildDone := make(chan bool, 1)
	go func() {
		rebuildDone <- tree.rebuild()
	}()
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&regions.t.lockCount) > 0
	}, 5*time.Second, 10*time.Millisecond)

	regions.ResetRegionCache()
	regions.PutRegion(newRegionSizeTreeTestRegion(uint64(regionCount+1), "a", "z", 2, 40))
	require.False(t, tree.isReady())
	rootLockCount := atomic.LoadInt64(&regions.t.lockCount)
	tree.mu.RUnlock()
	require.False(t, <-rebuildDone)
	require.Equal(t, rootLockCount, atomic.LoadInt64(&regions.t.lockCount))
	require.Less(t, tree.length(), regionCount)
	tree.markReadyIfCurrent()
	require.False(t, tree.isReady())

	tree.start()
	requireRegionSizeEventually(t, regions, nil, nil, 40)
	require.Equal(t, 1, tree.length())
}

func TestRegionSizeTreeReconcilesPendingInRootBatches(t *testing.T) {
	regions := NewRegionsInfo()
	regionCount := ScanRegionLimit
	pending := make(map[uint64]struct{}, regionCount)
	for i := range regionCount {
		regionID := uint64(i + 1)
		regions.PutRegion(newRegionSizeTreeTestRegion(
			regionID, fmt.Sprintf("%04d", i), fmt.Sprintf("%04d", i+1), 1, 1,
		))
		pending[regionID] = struct{}{}
	}

	tree := newRegionSizeTree(context.Background(), regions)
	t.Cleanup(tree.cancel)
	atomic.StoreInt64(&regions.t.lockCount, 0)
	require.True(t, tree.reconcilePending(pending))
	require.Equal(t, int64((regionCount+batchSearchSize-1)/batchSearchSize),
		atomic.LoadInt64(&regions.t.lockCount))
	require.Equal(t, regionCount, tree.length())
}

func TestRegionSizeTreePendingReconcileCanBeInterrupted(t *testing.T) {
	tests := []struct {
		name      string
		interrupt func(*regionSizeTree)
	}{
		{
			name: "cancel",
			interrupt: func(tree *regionSizeTree) {
				tree.cancel()
			},
		},
		{
			name: "reset",
			interrupt: func(tree *regionSizeTree) {
				tree.requestReset()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regions := NewRegionsInfo()
			regionCount := 2 * ScanRegionLimit
			pending := make(map[uint64]struct{}, regionCount)
			for i := range regionCount {
				regionID := uint64(i + 1)
				regions.PutRegion(newRegionSizeTreeTestRegion(
					regionID, fmt.Sprintf("%05d", i), fmt.Sprintf("%05d", i+1), 1, 1,
				))
				pending[regionID] = struct{}{}
			}

			tree := newRegionSizeTree(context.Background(), regions)
			defer tree.cancel()
			tree.mu.RLock()
			atomic.StoreInt64(&regions.t.lockCount, 0)
			reconcileDone := make(chan bool, 1)
			go func() {
				reconcileDone <- tree.reconcilePending(pending)
			}()

			deadline := time.Now().Add(5 * time.Second)
			for atomic.LoadInt64(&regions.t.lockCount) == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if atomic.LoadInt64(&regions.t.lockCount) == 0 {
				tree.cancel()
				tree.mu.RUnlock()
				<-reconcileDone
				t.Fatal("pending reconciliation did not start")
			}

			// The first group has loaded its root state and is blocked on the
			// size-tree lock. Interrupt it before allowing that group to commit.
			test.interrupt(tree)
			tree.mu.RUnlock()
			require.False(t, <-reconcileDone)
			require.Equal(t, ScanRegionLimit, tree.length())
			require.Equal(t, int64((ScanRegionLimit+batchSearchSize-1)/batchSearchSize),
				atomic.LoadInt64(&regions.t.lockCount))
		})
	}
}

func TestRegionSizeTreeEventuallyReconcilesLatestRootState(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	startRegionSizeTreeForTest(t, regions)
	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 1}
	region1 := NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []*metapb.Peer{peer1},
	}, peer1, SetApproximateSize(10))
	region2 := NewRegionInfo(&metapb.Region{
		Id:       2,
		StartKey: []byte("m"),
		Peers:    []*metapb.Peer{peer2},
	}, peer2, SetApproximateSize(20))
	regions.PutRegion(region1)
	regions.PutRegion(region2)
	requireRegionSizeEventually(t, regions, nil, nil, 30)
	re.Equal(int64(10), getRegionSizeFromReadyTree(t, regions, []byte("b"), []byte("m")))

	tree := regions.sizeTree.Load()
	tree.mu.RLock()
	updated := region1.Clone(SetApproximateSize(40))
	_, err := regions.CheckAndPutRootTree(ContextTODO(), updated)
	re.NoError(err)
	re.Equal(int64(60), regions.GetRegionSizeByRange(nil, nil))
	// The root update only enqueues the ID and does not wait for the size tree.
	re.Equal(int64(30), tree.totalSize)
	tree.mu.RUnlock()

	// The size tree converges even if the corresponding subtree task is dropped.
	requireRegionSizeEventually(t, regions, nil, nil, 60)
	re.Equal(int64(40), getRegionSizeFromReadyTree(t, regions, []byte("b"), []byte("m")))
	re.Equal(int32(1), updated.GetRef())
	regions.CheckAndPutSubTree(updated)
	re.Equal(int32(2), updated.GetRef())

	regions.RemoveRegionIfExist(region2.GetID())
	requireRegionSizeEventually(t, regions, nil, nil, 40)
}

func TestRegionSizeTreeQueryDoesNotBlockSubTreeUpdate(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	tree := startRegionSizeTreeForTest(t, regions)
	region := newRegionSizeTreeTestRegion(1, "a", "z", 1, 10)
	regions.PutRegion(region)
	requireRegionSizeEventually(t, regions, nil, nil, 10)

	// Simulate a long range query. The synchronous root/subtree path must only
	// enqueue the ID and must not wait for the size-tree writer lock.
	tree.mu.RLock()
	updated := newRegionSizeTreeTestRegion(1, "a", "z", 1, 20)
	updateDone := make(chan struct{})
	go func() {
		regions.PutRegion(updated)
		close(updateDone)
	}()

	completed := false
	select {
	case <-updateDone:
		completed = true
	case <-time.After(time.Second):
	}
	tree.mu.RUnlock()
	re.True(completed, "subtree update waited for the size-tree query")
	re.Equal(int64(20), regions.GetRegionSizeByRange(nil, nil))
	requireRegionSizeEventually(t, regions, nil, nil, 20)
}

func TestRegionSizeTreeSplitMerge(t *testing.T) {
	for _, rightFirst := range []bool{false, true} {
		regions := NewRegionsInfo()
		startRegionSizeTreeForTest(t, regions)
		original := newRegionSizeTreeTestRegion(1, "a", "z", 1, 100)
		regions.PutRegion(original)
		requireRegionSizeEventually(t, regions, nil, nil, 100)

		left := newRegionSizeTreeTestRegion(1, "a", "m", 2, 40)
		right := newRegionSizeTreeTestRegion(2, "m", "z", 2, 60)
		if rightFirst {
			regions.PutRegion(right)
			regions.PutRegion(left)
		} else {
			regions.PutRegion(left)
			regions.PutRegion(right)
		}
		require.Eventually(t, func() bool {
			tree := regions.sizeTree.Load()
			size, ready := regions.GetRegionSizeByRangeFromSizeTree(nil, nil)
			return tree != nil && tree.length() == 2 &&
				ready && size == 100
		}, 5*time.Second, 10*time.Millisecond)

		merged := newRegionSizeTreeTestRegion(3, "a", "z", 3, 120)
		regions.PutRegion(merged)
		require.Eventually(t, func() bool {
			tree := regions.sizeTree.Load()
			size, ready := regions.GetRegionSizeByRangeFromSizeTree(nil, nil)
			return tree != nil && tree.length() == 1 &&
				ready && size == 120
		}, 5*time.Second, 10*time.Millisecond)

		regions.StopRegionSizeTree()
	}
}

func TestRegionSizeTreeDelayedChildNotificationsDoNotRemoveMergedRegion(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	merged := newRegionSizeTreeTestRegion(3, "a", "z", 3, 120)
	regions.PutRegion(merged)

	// Keep the worker stopped so both delayed child notifications are present
	// before reconciliation reloads their latest root state.
	tree := newRegionSizeTree(context.Background(), regions)
	tree.reconcile([]regionSizeTreeUpdate{{regionID: merged.GetID(), region: merged}})
	regions.sizeTree.Store(tree)
	t.Cleanup(regions.StopRegionSizeTree)

	regions.notifyRegionSizeTree(1, 2)
	tree.pendingMu.Lock()
	pendingCount := len(tree.pending)
	tree.pendingMu.Unlock()
	re.Equal(2, pendingCount)
	tree.drain()

	re.Equal(1, tree.length())
	re.Equal(int64(120), tree.getRegionSizeByRange(nil, nil))
}

func TestRegionSizeTreeCoalescesRemoveAndSameIDReplacement(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	original := newRegionSizeTreeTestRegion(1, "a", "z", 1, 10)
	regions.PutRegion(original)

	// Keep the worker stopped so remove and replacement deterministically
	// coalesce into one pending ID before the latest root state is loaded.
	tree := newRegionSizeTree(context.Background(), regions)
	tree.reconcile([]regionSizeTreeUpdate{{regionID: original.GetID(), region: original}})
	regions.sizeTree.Store(tree)
	t.Cleanup(regions.StopRegionSizeTree)

	regions.RemoveRegionIfExist(original.GetID())
	replacement := newRegionSizeTreeTestRegion(1, "a", "z", 2, 40)
	regions.PutRegion(replacement)
	tree.pendingMu.Lock()
	pendingCount := len(tree.pending)
	_, pending := tree.pending[replacement.GetID()]
	tree.pendingMu.Unlock()
	re.Equal(1, pendingCount)
	re.True(pending)
	tree.drain()

	re.Equal(1, tree.length())
	re.Equal(int64(40), tree.getRegionSizeByRange(nil, nil))
}

func TestRegionSizeTreeConvergesAfterCoalescedOverlapReplacementRemoval(t *testing.T) {
	tests := []struct {
		name      string
		putRegion func(*RegionsInfo, *RegionInfo) ([]*RegionInfo, error)
	}{
		{
			name: "check-and-put-region",
			putRegion: func(regions *RegionsInfo, region *RegionInfo) ([]*RegionInfo, error) {
				return regions.CheckAndPutRegion(region), nil
			},
		},
		{
			name: "atomic-check-and-put-region",
			putRegion: func(regions *RegionsInfo, region *RegionInfo) ([]*RegionInfo, error) {
				return regions.AtomicCheckAndPutRegion(ContextTODO(), region)
			},
		},
		{
			name: "check-and-put-root-tree",
			putRegion: func(regions *RegionsInfo, region *RegionInfo) ([]*RegionInfo, error) {
				return regions.CheckAndPutRootTree(ContextTODO(), region)
			},
		},
		{
			name: "set-region",
			putRegion: func(regions *RegionsInfo, region *RegionInfo) ([]*RegionInfo, error) {
				_, overlaps, _ := regions.SetRegion(region)
				return overlaps, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			regions := NewRegionsInfo()
			original := newRegionSizeTreeTestRegion(1, "a", "z", 1, 10)
			regions.PutRegion(original)

			// Install a populated index without starting its worker so the replace
			// and remove notifications deterministically coalesce before loading
			// the latest root state.
			tree := newRegionSizeTree(context.Background(), regions)
			tree.reconcile([]regionSizeTreeUpdate{{regionID: original.GetID(), region: original}})
			regions.sizeTree.Store(tree)
			t.Cleanup(regions.StopRegionSizeTree)

			replacement := newRegionSizeTreeTestRegion(2, "a", "z", 2, 20)
			overlaps, err := test.putRegion(regions, replacement)
			re.NoError(err)
			re.Len(overlaps, 1)
			re.Equal(original.GetID(), overlaps[0].GetID())
			regions.RemoveRegion(replacement)

			re.Nil(regions.GetRegion(original.GetID()))
			re.Nil(regions.GetRegion(replacement.GetID()))
			tree.drain()
			re.Zero(tree.length())
			re.Zero(tree.getRegionSizeByRange(nil, nil))
		})
	}
}

func TestRegionSizeTreeResetDoesNotBlockRootOrSubTree(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	tree := startRegionSizeTreeForTest(t, regions)
	regions.PutRegion(newRegionSizeTreeTestRegion(1, "a", "m", 1, 10))
	regions.PutRegion(newRegionSizeTreeTestRegion(2, "m", "z", 1, 20))
	requireRegionSizeEventually(t, regions, nil, nil, 30)

	tree.mu.RLock()
	resetDone := make(chan struct{})
	go func() {
		regions.ResetRegionCache()
		close(resetDone)
	}()
	resetCompleted := false
	select {
	case <-resetDone:
		resetCompleted = true
	case <-time.After(time.Second):
	}

	newRegion := newRegionSizeTreeTestRegion(3, "a", "z", 2, 40)
	updateDone := make(chan struct{})
	go func() {
		regions.PutRegion(newRegion)
		close(updateDone)
	}()
	updateCompleted := false
	select {
	case <-updateDone:
		updateCompleted = true
	case <-time.After(time.Second):
	}
	tree.mu.RUnlock()

	re.True(resetCompleted, "reset waited for the size-tree query")
	re.True(updateCompleted, "post-reset update waited for the size-tree query")
	require.Eventually(t, func() bool {
		size, ready := regions.GetRegionSizeByRangeFromSizeTree(nil, nil)
		return tree.length() == 1 && ready && size == 40
	}, 5*time.Second, 10*time.Millisecond)
}

func startRegionSizeTreeForTest(t *testing.T, regions *RegionsInfo) *regionSizeTree {
	ctx, cancel := context.WithCancel(context.Background())
	regions.StartRegionSizeTree(ctx)
	t.Cleanup(func() {
		regions.StopRegionSizeTree()
		cancel()
	})
	tree := regions.sizeTree.Load()
	require.NotNil(t, tree)
	require.Eventually(t, tree.isReady, 5*time.Second, 10*time.Millisecond)
	return tree
}

func requireRegionSizeEventually(t *testing.T, regions *RegionsInfo, startKey, endKey []byte, expected int64) {
	require.Eventually(t, func() bool {
		size, ready := regions.GetRegionSizeByRangeFromSizeTree(startKey, endKey)
		return ready && size == expected
	}, 5*time.Second, 10*time.Millisecond)
}

func getRegionSizeFromReadyTree(t *testing.T, regions *RegionsInfo, startKey, endKey []byte) int64 {
	size, ready := regions.GetRegionSizeByRangeFromSizeTree(startKey, endKey)
	require.True(t, ready)
	return size
}

func newRegionSizeTreeTestRegion(id uint64, startKey, endKey string, version uint64, size int64) *RegionInfo {
	return NewRegionInfo(&metapb.Region{
		Id:       id,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
		RegionEpoch: &metapb.RegionEpoch{
			Version: version,
			ConfVer: 2,
		},
	}, nil, SetApproximateSize(size))
}
