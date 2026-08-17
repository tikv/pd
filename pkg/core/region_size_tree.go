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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type regionSizeItem struct {
	regionID uint64
	startKey []byte
	endKey   []byte
	size     int64
}

// Less reports whether this item starts before the other item.
func (i *regionSizeItem) Less(other *regionSizeItem) bool {
	return bytes.Compare(i.startKey, other.startKey) < 0
}

func (i *regionSizeItem) contains(key []byte) bool {
	return bytes.Compare(i.startKey, key) <= 0 &&
		(len(i.endKey) == 0 || bytes.Compare(key, i.endKey) < 0)
}

// regionSizeTree is an eventually consistent range-size index. Producers only
// enqueue Region IDs; its single worker reloads the latest root-tree state and
// is the only writer of the independent tree.
type regionSizeTree struct {
	cancel context.CancelFunc
	done   <-chan struct{}
	wg     *sync.WaitGroup
	owner  *RegionsInfo
	ready  atomic.Bool

	mu      syncutil.RWMutex
	tree    *btree.BTreeG[*regionSizeItem]
	regions map[uint64]*regionSizeItem

	pendingMu     syncutil.Mutex
	pending       map[uint64]struct{}
	pendingSince  time.Time
	activePending int
	activeSince   time.Time
	resetPending  bool
	notifyCh      chan struct{}
}

func newRegionSizeTree(ctx context.Context, owner *RegionsInfo) *regionSizeTree {
	workerCtx, cancel := context.WithCancel(ctx)
	return &regionSizeTree{
		cancel:   cancel,
		done:     workerCtx.Done(),
		wg:       &sync.WaitGroup{},
		owner:    owner,
		tree:     btree.NewG[*regionSizeItem](defaultBTreeDegree),
		regions:  make(map[uint64]*regionSizeItem),
		pending:  make(map[uint64]struct{}),
		notifyCh: make(chan struct{}, 1),
	}
}

func (t *regionSizeTree) start() {
	t.wg.Add(1)
	go t.run()
}

func (t *regionSizeTree) stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *regionSizeTree) notify(regionIDs ...uint64) {
	t.pendingMu.Lock()
	for _, regionID := range regionIDs {
		if _, ok := t.pending[regionID]; ok {
			continue
		}
		if len(t.pending) == 0 {
			t.pendingSince = time.Now()
		}
		t.pending[regionID] = struct{}{}
	}
	t.pendingMu.Unlock()
	t.wake()
}

func (t *regionSizeTree) requestReset() {
	t.pendingMu.Lock()
	t.ready.Store(false)
	t.resetPending = true
	t.pendingMu.Unlock()
	t.wake()
}

func (t *regionSizeTree) wake() {
	select {
	case t.notifyCh <- struct{}{}:
	default:
	}
}

func (t *regionSizeTree) takePending() (reset bool, pending map[uint64]struct{}) {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	reset = t.resetPending
	t.resetPending = false
	if len(t.pending) > 0 {
		pending = t.pending
		t.activePending = len(pending)
		t.activeSince = t.pendingSince
		t.pending = make(map[uint64]struct{})
		t.pendingSince = time.Time{}
	}
	return reset, pending
}

func (t *regionSizeTree) finishPending() {
	t.pendingMu.Lock()
	t.activePending = 0
	t.activeSince = time.Time{}
	t.pendingMu.Unlock()
}

func (t *regionSizeTree) run() {
	defer t.wg.Done()
	defer logutil.LogPanic()
	defer t.ready.Store(false)
	start := time.Now()
	if t.rebuild() {
		t.markReadyIfCurrent()
	}
	regionSizeTreeRebuildDuration.Observe(time.Since(start).Seconds())
	for {
		select {
		case <-t.done:
			return
		case <-t.notifyCh:
			t.drain()
		}
	}
}

func (t *regionSizeTree) drain() {
	for {
		select {
		case <-t.done:
			return
		default:
		}

		reset, pending := t.takePending()
		if !reset && len(pending) == 0 {
			return
		}
		if reset {
			t.reset()
		}
		if !t.reconcilePending(pending) {
			t.finishPending()
			continue
		}
		t.finishPending()
		if reset {
			t.markReadyIfCurrent()
		}
	}
}

func (t *regionSizeTree) markReadyIfCurrent() {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	if !t.isStopped() && !t.resetPending {
		t.ready.Store(true)
	}
}

func (t *regionSizeTree) isReady() bool {
	return !t.isStopped() && t.ready.Load()
}

func (t *regionSizeTree) isStopped() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *regionSizeTree) rebuild() bool {
	var startKey []byte
	for {
		if t.shouldInterruptReconcile() {
			return false
		}

		regions := t.owner.ScanRegions(startKey, nil, ScanRegionLimit)
		if t.shouldInterruptReconcile() {
			return false
		}
		if len(regions) == 0 {
			return true
		}
		t.reconcileRegions(regions)
		if t.shouldInterruptReconcile() {
			return false
		}

		endKey := regions[len(regions)-1].GetEndKey()
		if len(endKey) == 0 || bytes.Equal(startKey, endKey) {
			return true
		}
		startKey = endKey
	}
}

func (t *regionSizeTree) reconcilePending(pending map[uint64]struct{}) bool {
	regionIDs := make([]uint64, 0, min(len(pending), ScanRegionLimit))
	for regionID := range pending {
		regionIDs = append(regionIDs, regionID)
		if len(regionIDs) == ScanRegionLimit {
			if t.shouldInterruptReconcile() {
				return false
			}
			t.reconcileIDs(regionIDs)
			regionIDs = regionIDs[:0]
		}
	}
	if t.shouldInterruptReconcile() {
		return false
	}
	t.reconcileIDs(regionIDs)
	return true
}

func (t *regionSizeTree) shouldInterruptReconcile() bool {
	if t.isStopped() {
		return true
	}
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	return t.resetPending
}

func (t *regionSizeTree) collectMetrics() {
	t.pendingMu.Lock()
	pending := len(t.pending) + t.activePending
	oldest := t.pendingSince
	if oldest.IsZero() || (!t.activeSince.IsZero() && t.activeSince.Before(oldest)) {
		oldest = t.activeSince
	}
	t.pendingMu.Unlock()

	ready := 0.0
	if t.isReady() {
		ready = 1
	}
	regionSizeTreeReadyGauge.Set(ready)
	regionSizeTreePendingGauge.Set(float64(pending))
	oldestPendingDuration := 0.0
	if !oldest.IsZero() {
		oldestPendingDuration = time.Since(oldest).Seconds()
	}
	regionSizeTreeOldestPendingDurationGauge.Set(oldestPendingDuration)
}

func (t *regionSizeTree) reconcileIDs(regionIDs []uint64) {
	if len(regionIDs) == 0 {
		return
	}
	regions := t.owner.getRegionsByIDs(regionIDs)
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, regionID := range regionIDs {
		t.reconcileLocked(regionID, regions[i])
	}
}

func (t *regionSizeTree) reconcileRegions(regions []*RegionInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, region := range regions {
		t.reconcileLocked(region.GetID(), region)
	}
}

func (t *regionSizeTree) reconcileLocked(regionID uint64, region *RegionInfo) {
	origin := t.regions[regionID]
	if region == nil {
		if origin != nil {
			t.tree.Delete(origin)
			delete(t.regions, regionID)
		}
		return
	}

	if origin != nil && bytes.Equal(origin.startKey, region.GetStartKey()) &&
		bytes.Equal(origin.endKey, region.GetEndKey()) {
		origin.size = region.GetApproximateSize()
		return
	}

	if origin != nil {
		t.tree.Delete(origin)
		delete(t.regions, regionID)
	}
	// Region ranges are immutable. The compact index retains the key slices
	// captured from the root state without retaining the complete RegionInfo. A
	// later same-range heartbeat may replace the root RegionInfo and its backing
	// arrays without refreshing these slices.
	item := &regionSizeItem{
		regionID: regionID,
		startKey: region.GetStartKey(),
		endKey:   region.GetEndKey(),
		size:     region.GetApproximateSize(),
	}
	for _, overlap := range t.overlapsLocked(item) {
		t.tree.Delete(overlap)
		delete(t.regions, overlap.regionID)
	}
	t.tree.ReplaceOrInsert(item)
	t.regions[regionID] = item
}

func (t *regionSizeTree) findLocked(item *regionSizeItem) *regionSizeItem {
	var result *regionSizeItem
	t.tree.DescendLessOrEqual(item, func(current *regionSizeItem) bool {
		result = current
		return false
	})
	if result == nil || !result.contains(item.startKey) {
		return nil
	}
	return result
}

func (t *regionSizeTree) overlapsLocked(item *regionSizeItem) []*regionSizeItem {
	start := t.findLocked(item)
	if start == nil {
		start = item
	}
	var overlaps []*regionSizeItem
	t.tree.AscendGreaterOrEqual(start, func(current *regionSizeItem) bool {
		if len(item.endKey) > 0 && bytes.Compare(item.endKey, current.startKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, current)
		return true
	})
	return overlaps
}

func (t *regionSizeTree) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tree = btree.NewG[*regionSizeItem](defaultBTreeDegree)
	t.regions = make(map[uint64]*regionSizeItem)
}

func (t *regionSizeTree) getRegionSizeByRange(startKey, endKey []byte) int64 {
	var size int64
	for {
		t.mu.RLock()
		var count int
		start := &regionSizeItem{startKey: startKey}
		startItem := t.findLocked(start)
		if startItem == nil {
			startItem = start
		}
		t.tree.AscendGreaterOrEqual(startItem, func(item *regionSizeItem) bool {
			if len(endKey) > 0 && bytes.Compare(item.startKey, endKey) >= 0 {
				return false
			}
			if count >= ScanRegionLimit {
				return false
			}
			count++
			startKey = item.endKey
			size += item.size
			return true
		})
		// Let reconciliations proceed between chunks. A query can therefore observe
		// multiple index versions, which is acceptable for approximate statistics.
		t.mu.RUnlock()
		if count == 0 || len(startKey) == 0 {
			break
		}
	}
	return size
}
