// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math/rand"
	"time"

	"github.com/pingcap/pd/v4/pkg/cache"
	"github.com/pingcap/pd/v4/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

const (
	statCacheMaxLen            = 1000
	minHotRegionReportInterval = 3
	hotWriteRegionMinFlowRate  = 16 * 1024
	hotReadRegionMinFlowRate   = 128 * 1024
)

// HotCache is a cache hold hot regions.
type HotCache struct {
	writeFlow     *hotPeerCache
	readFlow      *hotPeerCache
	hotWriteCache cache.Cache
	hotReadCache  cache.Cache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache() *HotCache {
	return &HotCache{
		writeFlow:     NewHotStoresStats(WriteFlow),
		readFlow:      NewHotStoresStats(ReadFlow),
		hotWriteCache: cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
		hotReadCache:  cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
	}
}

// CheckWrite checks the write status, returns update items.
func (w *HotCache) CheckWrite(region *core.RegionInfo, stats *StoresStats) []*HotPeerStat {
	w.updatehotWriteCache(region, stats)
	return w.writeFlow.CheckRegionFlow(region, stats)
}

// CheckRead checks the read status, returns update items.
func (w *HotCache) CheckRead(region *core.RegionInfo, stats *StoresStats) []*HotPeerStat {
	w.updatehotReadCache(region, stats)
	return w.readFlow.CheckRegionFlow(region, stats)
}

func (w *HotCache) updatehotWriteCache(region *core.RegionInfo, stats *StoresStats) {
	var (
		WrittenBytesPerSec float64
		value              *HotPeerStat
	)

	WrittenBytesPerSec = float64(region.GetBytesWritten()) / float64(RegionHeartBeatReportInterval)

	v, isExist := w.hotWriteCache.Peek(region.GetID())
	if isExist {
		value = v.(*HotPeerStat)
		interval := time.Since(value.LastUpdateTime).Seconds()
		if interval >= minHotRegionReportInterval {
			WrittenBytesPerSec = float64(region.GetBytesWritten()) / interval
		}
	}

	hotRegionThreshold := calculateWriteHotThreshold(stats)
	if need, item := w.isNeedUpdateStatCache(region, WrittenBytesPerSec, hotRegionThreshold, value, WriteFlow); need {
		w.updateHotCache(region.GetID(), item, WriteFlow)
	}
}

func (w *HotCache) updatehotReadCache(region *core.RegionInfo, stats *StoresStats) {
	var (
		ReadBytesPerSec float64
		value           *HotPeerStat
	)

	ReadBytesPerSec = float64(region.GetBytesRead()) / float64(RegionHeartBeatReportInterval)

	v, isExist := w.hotReadCache.Peek(region.GetID())
	if isExist {
		value = v.(*HotPeerStat)
		interval := time.Since(value.LastUpdateTime).Seconds()
		if interval >= minHotRegionReportInterval {
			ReadBytesPerSec = float64(region.GetBytesRead()) / interval
		}
	}

	hotRegionThreshold := calculateReadHotThreshold(stats)
	if need, item := w.isNeedUpdateStatCache(region, ReadBytesPerSec, hotRegionThreshold, value, ReadFlow); need {
		w.updateHotCache(region.GetID(), item, ReadFlow)
	}
}

func (w *HotCache) isNeedUpdateStatCache(region *core.RegionInfo, flowBytes float64, hotRegionThreshold uint64, oldItem *HotPeerStat, kind FlowKind) (bool, *HotPeerStat) {
	newItem := &HotPeerStat{
		RegionID:       region.GetID(),
		ByteRate:       flowBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.GetLeader().GetStoreId(),
		Version:        region.GetMeta().GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}
	if oldItem != nil {
		newItem.HotDegree = oldItem.HotDegree + 1
		newItem.rollingByteRate = oldItem.rollingByteRate
	}
	if flowBytes >= float64(hotRegionThreshold) {
		if oldItem == nil {
			newItem.rollingByteRate = NewMedianFilter(rollingWindowsSize)
		}
		newItem.rollingByteRate.Add(flowBytes)
		return true, newItem
	}
	// smaller than hotRegionThreshold
	if oldItem == nil {
		return false, newItem
	}
	if oldItem.AntiCount <= 0 {
		return true, nil
	}
	// eliminate some noise
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	newItem.rollingByteRate.Add(flowBytes)
	return true, newItem
}

// Update updates the cache.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case WriteFlow:
		w.writeFlow.Update(item)
	case ReadFlow:
		w.readFlow.Update(item)
	}

	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		w.incMetrics("update_item", item.StoreID, item.Kind)
	}
}

func (w *HotCache) updateHotCache(key uint64, item *HotPeerStat, kind FlowKind) {
	switch kind {
	case WriteFlow:
		if item == nil {
			w.hotWriteCache.Remove(key)
		} else {
			w.hotWriteCache.Put(key, item)
		}
	case ReadFlow:
		if item == nil {
			w.hotReadCache.Remove(key)
		} else {
			w.hotReadCache.Put(key, item)
		}
	}
}

func calculateWriteHotThreshold(stats *StoresStats) uint64 {
	// hotRegionThreshold is used to pick hot region
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2
	hotRegionThreshold := uint64(stats.TotalBytesWriteRate() / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return hotRegionThreshold
}

func calculateReadHotThreshold(stats *StoresStats) uint64 {
	// hotRegionThreshold is used to pick hot region
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total Read Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	divisor := float64(statCacheMaxLen)
	hotRegionThreshold := uint64(stats.TotalBytesReadRate() / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return hotRegionThreshold
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind FlowKind) map[uint64][]*HotPeerStat {
	switch kind {
	case WriteFlow:
		return w.writeFlow.RegionStats()
	case ReadFlow:
		return w.readFlow.RegionStats()
	}
	return nil
}

// HotRegionCache returns hot cache according to kind
func (w *HotCache) HotRegionCache(kind FlowKind) cache.Cache {
	switch kind {
	case WriteFlow:
		return w.hotWriteCache
	case ReadFlow:
		return w.hotReadCache
	}
	return nil
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotCache) RandHotRegionFromStore(storeID uint64, kind FlowKind, hotDegree int) *HotPeerStat {
	if stats, ok := w.RegionStats(kind)[storeID]; ok {
		for _, i := range rand.Perm(len(stats)) {
			if stats[i].HotDegree >= hotDegree {
				return stats[i]
			}
		}
	}
	return nil
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	return w.writeFlow.IsRegionHot(region, hotDegree) ||
		w.readFlow.IsRegionHot(region, hotDegree)
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics(stats *StoresStats) {
	w.writeFlow.CollectMetrics(stats, "write")
	w.readFlow.CollectMetrics(stats, "read")
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) incMetrics(name string, storeID uint64, kind FlowKind) {
	store := storeTag(storeID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
}
