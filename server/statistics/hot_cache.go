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
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
)

// HotCache is a cache hold hot regions.
type HotCache struct {
	ctx        context.Context
	writeCache *hotPeerCache
	readCache  *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context, cluster *core.BasicCluster) *HotCache {
	w := &HotCache{
		ctx:        ctx,
		writeCache: NewHotPeerCache(cluster, Write),
		readCache:  NewHotPeerCache(cluster, Read),
	}
	go w.updateItems(w.readCache.taskQueue, w.runReadTask)
	go w.updateItems(w.writeCache.taskQueue, w.runWriteTask)
	return w
}

// CheckWriteAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(task FlowItemTask) bool {
	select {
	case w.writeCache.taskQueue <- task:
		return true
	default:
		return false
	}
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(task FlowItemTask) bool {
	select {
	case w.readCache.taskQueue <- task:
		return true
	default:
		return false
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
	checkRegionHotWriteTask := newCheckRegionHotTask(region, minHotDegree)
	checkRegionHotReadTask := newCheckRegionHotTask(region, minHotDegree)
	succ1 := w.CheckWriteAsync(checkRegionHotWriteTask)
	succ2 := w.CheckReadAsync(checkRegionHotReadTask)
	if succ1 && succ2 {
		return checkRegionHotWriteTask.waitRet(w.ctx) || checkRegionHotReadTask.waitRet(w.ctx)
	}
	return false
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (w *HotCache) GetHotPeerStat(kind RWType, regionID, storeID uint64) *HotPeerStat {
	task := newGetHotPeerStatTask(regionID, storeID)
	var succ bool
	switch kind {
	case Read:
		succ = w.CheckReadAsync(task)
	case Write:
		succ = w.CheckWriteAsync(task)
	}
	if !succ {
		return nil
	}
	return task.waitRet(w.ctx)
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

func incMetrics(name string, storeID uint64, kind RWType) {
	store := storeTag(storeID)
	switch kind {
	case Write:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case Read:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
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
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task.runTask(w.readCache)
		hotCacheFlowQueueStatusGauge.WithLabelValues(Read.String()).Set(float64(len(w.readCache.taskQueue)))
	}
}

func (w *HotCache) runWriteTask(task FlowItemTask) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task.runTask(w.writeCache)
		hotCacheFlowQueueStatusGauge.WithLabelValues(Write.String()).Set(float64(len(w.writeCache.taskQueue)))
	}
}

// Update updates the cache.
// This is used for mockcluster, for test purpose.
func (w *HotCache) Update(item *HotPeerStat, kind RWType) {
	switch kind {
	case Write:
		w.writeCache.updateStat(item)
	case Read:
		w.readCache.updateStat(item)
	}
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckWritePeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.writeCache.checkPeerFlow(peer, region)
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckReadPeerSync(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	return w.readCache.checkPeerFlow(peer, region)
}

// ExpiredReadItems returns the read items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return w.readCache.collectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeCache.collectExpiredItems(region)
}

// GetFilledPeriod returns filled period.
// This is used for mockcluster, for test purpose.
func (w *HotCache) GetFilledPeriod(kind RWType) int {
	var reportIntervalSecs int
	switch kind {
	case Write:
		reportIntervalSecs = w.writeCache.kind.ReportInterval()
	case Read:
		reportIntervalSecs = w.readCache.kind.ReportInterval()
	}
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, time.Duration(reportIntervalSecs)*time.Second).GetFilledPeriod()
}

// GetThresholds returns thresholds.
// This is used for test purpose.
func (w *HotCache) GetThresholds(kind RWType, storeID uint64) []float64 {
	switch kind {
	case Write:
		return w.writeCache.calcHotThresholds(storeID)
	case Read:
		return w.readCache.calcHotThresholds(storeID)
	}
	return nil
}
