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
	ctx            context.Context
	readFlowQueue  chan FlowItemTask
	writeFlowQueue chan FlowItemTask
	writeFlow      *hotPeerCache
	readFlow       *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context) *HotCache {
	w := &HotCache{
		ctx:            ctx,
		readFlowQueue:  make(chan FlowItemTask, queueCap),
		writeFlowQueue: make(chan FlowItemTask, queueCap),
		writeFlow:      NewHotPeerCache(WriteFlow),
		readFlow:       NewHotPeerCache(ReadFlow),
	}
	go w.updateItems(w.readFlowQueue, w.runReadTask)
	go w.updateItems(w.writeFlowQueue, w.runWriteTask)
	return w
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckWritePeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.writeFlow.CheckPeerFlow(peer, region)
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster.
func (w *HotCache) CheckReadPeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.readFlow.CheckPeerFlow(peer, region)
}

// CheckWriteAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(task FlowItemTask) bool {
	select {
	case w.writeFlowQueue <- task:
		return true
	default:
		return false
	}
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(task FlowItemTask) bool {
	select {
	case w.readFlowQueue <- task:
		return true
	default:
		return false
	}
}

// Update updates the cache.
// This is used for mockcluster.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case WriteFlow:
		update(item, w.writeFlow)
	case ReadFlow:
		update(item, w.readFlow)
	}
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind FlowKind, minHotDegree int) map[uint64][]*HotPeerStat {
	switch kind {
	case WriteFlow:
		task := newCollectRegionStatsTask(minHotDegree)
		succ := w.CheckWriteAsync(task)
		if !succ {
			return nil
		}
		return task.waitRet(w.ctx)
	case ReadFlow:
		task := newCollectRegionStatsTask(minHotDegree)
		succ := w.CheckReadAsync(task)
		if !succ {
			return nil
		}
		return task.waitRet(w.ctx)
	}
	return nil
}

// HotRegionsFromStore picks hot region in specify store.
func (w *HotCache) HotRegionsFromStore(storeID uint64, kind FlowKind, minHotDegree int) []*HotPeerStat {
	if stats, ok := w.RegionStats(kind, minHotDegree)[storeID]; ok && len(stats) > 0 {
		return stats
	}
	return nil
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
	return w.readFlow.CollectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeFlow.CollectExpiredItems(region)
}

func incMetrics(name string, storeID uint64, kind FlowKind) {
	store := storeTag(storeID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind FlowKind) int {
	switch kind {
	case WriteFlow:
		return w.writeFlow.getDefaultTimeMedian().GetFilledPeriod()
	case ReadFlow:
		return w.readFlow.getDefaultTimeMedian().GetFilledPeriod()
	}
	return 0
}

func (w *HotCache) updateItems(queue <-chan FlowItemTask, runTask func(task FlowItemTask)) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-queue:
			runTask(task)
		}
	}
}

func (w *HotCache) runReadTask(task FlowItemTask) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stucked by a task?
		task.runTask(w.readFlow)
		hotCacheFlowQueueStatusGauge.WithLabelValues(ReadFlow.String()).Set(float64(len(w.readFlowQueue)))
	}
}

func (w *HotCache) runWriteTask(task FlowItemTask) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stucked by a task?
		task.runTask(w.writeFlow)
		hotCacheFlowQueueStatusGauge.WithLabelValues(WriteFlow.String()).Set(float64(len(w.writeFlowQueue)))
	}
}

func update(item *HotPeerStat, flow *hotPeerCache) {
	flow.Update(item)
	if item.IsNeedDelete() {
		incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		incMetrics("update_item", item.StoreID, item.Kind)
	}
}
