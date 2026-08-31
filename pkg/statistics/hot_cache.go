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

	"github.com/smallnest/chanx"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const chanMaxLength = 6000000

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

// CheckRegionFlowAsync checks the expired read and write items and the write
// flow for a region asynchronously. It preserves the original task count and
// enqueue order while keeping only the fields used by HotPeerCache.
func (w *HotCache) CheckRegionFlowAsync(region *core.RegionInfo) {
	checkExpiredTask, checkWritePeerTask := newRegionFlowTasks(region)
	w.CheckWriteAsync(checkExpiredTask)
	w.CheckReadAsync(checkExpiredTask)
	w.CheckWriteAsync(checkWritePeerTask)
}

func newRegionFlowTasks(region *core.RegionInfo) (checkExpiredTask, checkWritePeerTask func(*HotPeerCache)) {
	regionInfo := newHotRegionInfo(region)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	writtenBytes := region.GetBytesWritten()
	writtenKeys := region.GetKeysWritten()
	writeQueryNum := region.GetWriteQueryNum()
	return newExpiredRegionTask(regionInfo), newWriteRegionTask(regionInfo, interval, writtenBytes, writtenKeys, writeQueryNum)
}

func newExpiredRegionTask(regionInfo *hotRegionInfo) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		expiredStats := cache.collectExpiredItemsForRegion(regionInfo)
		for _, stat := range expiredStats {
			cache.UpdateStat(stat)
		}
	}
}

func newWriteRegionTask(regionInfo *hotRegionInfo, interval, writtenBytes, writtenKeys, writeQueryNum uint64) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		var loads [utils.RegionStatCount]float64
		loads[utils.RegionWriteBytes] = float64(writtenBytes)
		loads[utils.RegionWriteKeys] = float64(writtenKeys)
		loads[utils.RegionWriteQueryNum] = float64(writeQueryNum)
		stats := cache.checkPeerFlowForRegion(regionInfo, nil, loads[:], interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
}

// CheckReadPeerAsync checks the read flow for one peer asynchronously without
// retaining the complete RegionInfo or Peer in the pending task.
func (w *HotCache) CheckReadPeerAsync(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) bool {
	return w.CheckReadAsync(newReadPeerTask(region, peer, loads, interval))
}

func newReadPeerTask(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) func(*HotPeerCache) {
	regionInfo := newHotRegionInfo(region)
	storeID := peer.GetStoreId()
	return func(cache *HotPeerCache) {
		stats := cache.checkPeerFlowForRegion(regionInfo, []uint64{storeID}, loads, interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
}

// CheckColdPeerAsync checks peers missing from a store heartbeat
// asynchronously. The pending task only keeps the reported region IDs.
func (w *HotCache) CheckColdPeerAsync(storeID uint64, reportedRegions map[uint64]struct{}, interval uint64) bool {
	checkColdPeerTask := func(cache *HotPeerCache) {
		stats := cache.checkColdPeerByRegionIDs(storeID, reportedRegions, interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
	return w.CheckReadAsync(checkColdPeerTask)
}

// GetHotPeerStats returns hot peer stats for the specified kind (read/write).
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
		// gc() is otherwise only triggered from UpdateStat, so a store removed while
		// the cluster is idle (or was the only store still receiving updates) would
		// never have its hotCacheStatusGauge series cleaned up. Piggyback on this
		// periodic, activity-independent tick instead; gc() already self-throttles
		// via topNTTL, so calling it every tick is cheap.
		cache.gc()
	})
	w.CheckReadAsync(func(cache *HotPeerCache) {
		cache.collectMetrics()
		cache.gc()
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
	}
}

func (w *HotCache) runWriteTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task(w.writeCache)
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
