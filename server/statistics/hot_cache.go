// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"context"

	"github.com/tikv/pd/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

const queueCap = 20000

// HotCache is a cache hold hot regions.
type HotCache struct {
	ctx        context.Context
	writeCache *hotPeerCache
	readCache  *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context) *HotCache {
	w := &HotCache{
		ctx:        ctx,
		writeCache: NewHotPeerCache(Write),
		readCache:  NewHotPeerCache(Read),
	}
	go w.updateItems(w.readCache.taskQueue, w.runReadTask)
	go w.updateItems(w.writeCache.taskQueue, w.runWriteTask)
	return w
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckWritePeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.writeCache.checkPeerFlow(peer, region)
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckReadPeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.readCache.checkPeerFlow(peer, region)
}

// CheckWriteAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(task flowItemTask) bool {
	select {
	case w.writeCache.taskQueue <- task:
		return true
	default:
		return false
	}
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(task flowItemTask) bool {
	select {
	case w.readCache.taskQueue <- task:
		return true
	default:
		return false
	}
}

// Update updates the cache.
// This is used for mockcluster.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case Write:
		updateStat(w.writeCache, item)
	case Read:
		updateStat(w.readCache, item)
	}
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind RWType, minHotDegree int) map[uint64][]*HotPeerStat {
	task := newCollectRegionStatsTask(minHotDegree)
	var succ bool
	switch kind {
	case Write:
		succ = w.CheckWriteAsync(task)
	case Read:
		succ = w.CheckReadAsync(task)
	}
	if !succ {
		return nil
	}
	return task.waitRet(w.ctx)
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, minHotDegree int) bool {
	writeIsRegionHotTask := newIsRegionHotTask(region, minHotDegree)
	readIsRegionHotTask := newIsRegionHotTask(region, minHotDegree)
	succ1 := w.CheckWriteAsync(writeIsRegionHotTask)
	succ2 := w.CheckReadAsync(readIsRegionHotTask)
	if succ1 && succ2 {
		return writeIsRegionHotTask.waitRet(w.ctx) || readIsRegionHotTask.waitRet(w.ctx)
	}
	return false
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	writeMetricsTask := newCollectMetricsTask("write")
	readMetricsTask := newCollectMetricsTask("read")
	w.CheckWriteAsync(writeMetricsTask)
	w.CheckReadAsync(readMetricsTask)
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

// ExpiredReadItems returns the read items which are already expired.
// This is used for mockcluster.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return w.readCache.collectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeCache.collectExpiredItems(region)
}

func incMetrics(name string, storeID uint64, kind RWType) {
	store := storeTag(storeID)
	switch kind {
	case Write:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case Read:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind RWType) int {
	switch kind {
	case Write:
		return w.writeCache.getDefaultTimeMedian().GetFilledPeriod()
	case Read:
		return w.readCache.getDefaultTimeMedian().GetFilledPeriod()
	}
	return 0
}

func (w *HotCache) updateItems(queue <-chan flowItemTask, runTask func(task flowItemTask)) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-queue:
			runTask(task)
		}
	}
}

func (w *HotCache) runReadTask(task flowItemTask) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task.runTask(w.readCache)
		hotCacheFlowQueueStatusGauge.WithLabelValues(Read.String()).Set(float64(len(w.readCache.taskQueue)))
	}
}

func (w *HotCache) runWriteTask(task flowItemTask) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task.runTask(w.writeCache)
		hotCacheFlowQueueStatusGauge.WithLabelValues(Write.String()).Set(float64(len(w.writeCache.taskQueue)))
	}
}

func updateStat(cache *hotPeerCache, item *HotPeerStat) {
	cache.update(item)
	switch item.opType {
	case Add:
		incMetrics("add_item", item.StoreID, item.Kind)
	case Remove:
		incMetrics("remove_item", item.StoreID, item.Kind)
	case Update:
		incMetrics("update_item", item.StoreID, item.Kind)
	}
}
