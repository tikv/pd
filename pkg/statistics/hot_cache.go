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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/smallnest/chanx"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const chanMaxLength = 6000000

var (
	readTaskMetrics  = hotCacheFlowQueueStatusGauge.WithLabelValues(utils.Read.String())
	writeTaskMetrics = hotCacheFlowQueueStatusGauge.WithLabelValues(utils.Write.String())
)

// HotCache is a cache hold hot regions.
type HotCache struct {
	ctx        context.Context
	writeCache *HotPeerCache
	readCache  *HotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context, cluster *core.BasicCluster) *HotCache {
	w := &HotCache{
		ctx:        ctx,
		writeCache: NewHotPeerCache(ctx, cluster, utils.Write),
		readCache:  NewHotPeerCache(ctx, cluster, utils.Read),
	}
	go w.updateItems(w.readCache.taskQueue, w.runReadTask)
	go w.updateItems(w.writeCache.taskQueue, w.runWriteTask)
	return w
}

// CheckWriteAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(task func(cache *HotPeerCache)) bool {
	if w.writeCache.taskQueue.Len() > chanMaxLength {
		return false
	}
	select {
	case w.writeCache.taskQueue.In <- task:
		return true
	default:
		return false
	}
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(task func(cache *HotPeerCache)) bool {
	if w.readCache.taskQueue.Len() > chanMaxLength {
		return false
	}
	select {
	case w.readCache.taskQueue.In <- task:
		return true
	default:
		return false
	}
}

// RegionStats returns the read or write statistics for hot regions.
// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
func (w *HotCache) GetHotPeerStats(kind utils.RWType, minHotDegree int) map[uint64][]*HotPeerStat {
	ret := make(chan map[uint64][]*HotPeerStat, 1)
	collectRegionStatsTask := func(cache *HotPeerCache) {
		ret <- cache.GetHotPeerStats(minHotDegree)
	}
	var succ bool
	switch kind {
	case utils.Write:
		succ = w.CheckWriteAsync(collectRegionStatsTask)
	case utils.Read:
		succ = w.CheckReadAsync(collectRegionStatsTask)
	}
	if !succ {
		return nil
	}
	select {
	case <-w.ctx.Done():
		return nil
	case r := <-ret:
		return r
	}
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, minHotDegree int) bool {
	retWrite := make(chan bool, 1)
	retRead := make(chan bool, 1)
	checkRegionHotWriteTask := func(cache *HotPeerCache) {
		retWrite <- cache.isRegionHotWithAnyPeers(region, minHotDegree)
	}
	checkRegionHotReadTask := func(cache *HotPeerCache) {
		retRead <- cache.isRegionHotWithAnyPeers(region, minHotDegree)
	}
	succ1 := w.CheckWriteAsync(checkRegionHotWriteTask)
	succ2 := w.CheckReadAsync(checkRegionHotReadTask)
	if succ1 && succ2 {
		return waitRet(w.ctx, retWrite) || waitRet(w.ctx, retRead)
	}
	return false
}

func waitRet(ctx context.Context, ret chan bool) bool {
	select {
	case <-ctx.Done():
		return false
	case r := <-ret:
		return r
	}
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (w *HotCache) GetHotPeerStat(kind utils.RWType, regionID, storeID uint64) *HotPeerStat {
	ret := make(chan *HotPeerStat, 1)
	getHotPeerStatTask := func(cache *HotPeerCache) {
		ret <- cache.getHotPeerStat(regionID, storeID)
	}

	var succ bool
	switch kind {
	case utils.Read:
		succ = w.CheckReadAsync(getHotPeerStatTask)
	case utils.Write:
		succ = w.CheckWriteAsync(getHotPeerStatTask)
	}
	if !succ {
		return nil
	}
	select {
	case <-w.ctx.Done():
		return nil
	case r := <-ret:
		return r
	}
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.CheckWriteAsync(func(cache *HotPeerCache) {
		cache.collectMetrics()
	})
	w.CheckReadAsync(func(cache *HotPeerCache) {
		cache.collectMetrics()
	})
}

// ResetHotCacheStatusMetrics resets the hot cache metrics.
func ResetHotCacheStatusMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) updateItems(queue *chanx.UnboundedChan[func(*HotPeerCache)], runTask func(task func(*HotPeerCache))) {
	defer logutil.LogPanic()

	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-queue.Out:
			runTask(task)
		}
	}
}

func (w *HotCache) runReadTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task(w.readCache)
		readTaskMetrics.Set(float64(w.readCache.taskQueue.Len()))
	}
}

func (w *HotCache) runWriteTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task(w.writeCache)
		writeTaskMetrics.Set(float64(w.writeCache.taskQueue.Len()))
	}
}

// Update updates the cache.
// This is used for mockcluster, for test purpose.
func (w *HotCache) Update(item *HotPeerStat, kind utils.RWType) {
	switch kind {
	case utils.Write:
		w.writeCache.UpdateStat(item)
	case utils.Read:
		w.readCache.UpdateStat(item)
	}
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckWritePeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return w.writeCache.CheckPeerFlow(region, peers, loads, interval)
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckReadPeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return w.readCache.CheckPeerFlow(region, peers, loads, interval)
}

// ExpiredReadItems returns the read items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return w.readCache.CollectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeCache.CollectExpiredItems(region)
}

// GetThresholds returns thresholds.
// This is used for test purpose.
func (w *HotCache) GetThresholds(kind utils.RWType, storeID uint64) []float64 {
	switch kind {
	case utils.Write:
		return w.writeCache.calcHotThresholds(storeID)
	case utils.Read:
		return w.readCache.calcHotThresholds(storeID)
	}
	return nil
}

// CleanCache cleans the cache.
// This is used for test purpose.
func (w *HotCache) CleanCache() {
	w.writeCache.removeAllItem()
	w.readCache.removeAllItem()
}
