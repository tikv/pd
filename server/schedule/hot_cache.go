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

package schedule

import (
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
)

// FlowKind is a identify Flow types.
type FlowKind uint32

// Flags for flow.
const (
	WriteFlow FlowKind = iota
	ReadFlow
)

// HotSpotCache is a cache hold hot regions.
type HotSpotCache struct {
	writeFlow map[uint64]cache.Cache
	readFlow  map[uint64]cache.Cache
}

func newHotSpotCache() *HotSpotCache {
	return &HotSpotCache{
		writeFlow: make(map[uint64]cache.Cache),
		readFlow:  make(map[uint64]cache.Cache),
	}
}

// CheckWrite checks the write status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckWrite(origin *core.RegionInfo, region *core.RegionInfo, store *core.StoreInfo) (bool, *core.RegionStat) {
	var storeID uint64
	if store == nil || region == nil {
		return false, nil
	}
	if origin == nil {
		origin = region
	}
	if region.RegionEpoch.GetVersion() < origin.RegionEpoch.GetVersion() || region.RegionEpoch.GetConfVer() < origin.RegionEpoch.GetConfVer() {
		return false, nil
	}
	storeID = origin.Leader.GetStoreId()
	writeFlowCache, ok := w.writeFlow[storeID]
	var (
		WrittenBytesPerSec uint64
		v                  interface{}
		isExist            bool
	)
	if ok {
		v, isExist = writeFlowCache.Peek(region.GetId())
	}
	if isExist && !Simulating {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return false, nil
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(RegionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot Regions is cacheLenPerStore
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(cacheLenPerStore) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(store.Stats.GetBytesWritten()) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return w.isNeedUpdateStatCache(region, hotRegionThreshold, v, isExist, WriteFlow)
}

// CheckRead checks the read status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckRead(origin, region *core.RegionInfo, store *core.StoreInfo) (bool, *core.RegionStat) {
	if store == nil || region == nil {
		return false, nil
	}
	if origin == nil {
		origin = region
	}
	if region.RegionEpoch.GetVersion() < origin.RegionEpoch.GetVersion() || region.RegionEpoch.GetConfVer() < origin.RegionEpoch.GetConfVer() {
		return false, nil
	}
	storeID := origin.Leader.GetStoreId()
	readFlowCache, ok := w.readFlow[storeID]
	var (
		ReadBytesPerSec uint64
		v               interface{}
		isExist         bool
	)
	if ok {
		v, isExist = readFlowCache.Peek(region.GetId())
	}
	if isExist && !Simulating {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return false, nil
		}
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / interval)
	} else {
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / float64(RegionHeartBeatReportInterval))
	}
	region.ReadBytes = ReadBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot Regions is statLRUMaxLen
	// and we use total Read Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	divisor := float64(cacheLenPerStore) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(store.Stats.GetBytesRead()) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return w.isNeedUpdateStatCache(region, hotRegionThreshold, v, isExist, ReadFlow)
}

func (w *HotSpotCache) isNeedUpdateStatCache(region *core.RegionInfo, hotRegionThreshold uint64, value interface{}, isExist bool, kind FlowKind) (bool, *core.RegionStat) {
	var (
		v         *core.RegionStat
		flowBytes uint64
	)
	switch kind {
	case WriteFlow:
		flowBytes = region.WrittenBytes
	case ReadFlow:
		flowBytes = region.ReadBytes
	}
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      flowBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}
	switch kind {
	case WriteFlow:
		if region.WrittenBytes >= hotRegionThreshold {
			return true, newItem
		}
	case ReadFlow:
		if region.ReadBytes >= hotRegionThreshold {
			return true, newItem
		}
	}
	// smaller than hotReionThreshold
	if !isExist {
		return false, newItem
	}

	if v.AntiCount <= 0 {
		return true, nil
	}
	// eliminate some noise
	newItem.HotDegree = v.HotDegree - 1
	newItem.AntiCount = v.AntiCount - 1
	newItem.FlowBytes = v.FlowBytes

	return true, newItem
}

// Update updates the cache.
func (w *HotSpotCache) Update(origin, region *core.RegionInfo, item *core.RegionStat, kind FlowKind) {
	needDelete := origin != nil && origin.Leader != nil && origin.Leader.GetStoreId() != region.Leader.GetStoreId()
	switch kind {
	case WriteFlow:
		if needDelete {
			if oldCacheFlow, ok := w.writeFlow[origin.Leader.GetStoreId()]; ok {
				oldCacheFlow.Remove(origin.GetId())
			}
		}
		cacheFlow, ok := w.writeFlow[item.StoreID]
		if !ok {
			cacheFlow = cache.NewCache(cacheLenPerStore, cache.TwoQueueCache)
			w.writeFlow[item.StoreID] = cacheFlow
		}
		if item == nil {
			cacheFlow.Remove(region.GetId())
		} else {
			cacheFlow.Put(region.GetId(), item)
		}
	case ReadFlow:
		if needDelete {
			if oldCacheFlow, ok := w.readFlow[origin.Leader.GetStoreId()]; ok {
				oldCacheFlow.Remove(origin.GetId())
			}
		}
		cacheFlow, ok := w.readFlow[item.StoreID]
		if !ok {
			cacheFlow = cache.NewCache(cacheLenPerStore, cache.TwoQueueCache)
			w.readFlow[item.StoreID] = cacheFlow
		}
		if item == nil {
			cacheFlow.Remove(region.GetId())
		} else {
			cacheFlow.Put(region.GetId(), item)
		}
	}
}

// RegionStats returns hot items according to kind
func (w *HotSpotCache) RegionStats(kind FlowKind) []*core.RegionStat {
	var elements []*cache.Item
	switch kind {
	case WriteFlow:
		for _, cache := range w.writeFlow {
			elements = append(elements, cache.Elems()...)
		}
	case ReadFlow:
		for _, cache := range w.readFlow {
			elements = append(elements, cache.Elems()...)
		}
	}
	stats := make([]core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = *elements[i].Value.(*core.RegionStat)
	}
	sort.Sort(core.RegionsStat(stats))
	len := len(stats)
	if len > totalCacheLen {
		len = totalCacheLen
	}
	res := make([]*core.RegionStat, len)
	for i := 0; i < len; i++ {
		res[i] = &stats[i]
	}
	return res
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotSpotCache) RandHotRegionFromStore(storeID uint64, kind FlowKind, hotThreshold int) *core.RegionStat {
	stats := w.RegionStats(kind)
	for _, i := range rand.Perm(len(stats)) {
		if stats[i].HotDegree >= hotThreshold && stats[i].StoreID == storeID {
			return stats[i]
		}
	}
	return nil
}

func (w *HotSpotCache) isRegionHot(id uint64, hotThreshold int) bool {
	stats := w.RegionStats(WriteFlow)
	for _, stat := range stats {
		if stat.RegionID == id && stat.HotDegree > hotThreshold {
			return true
		}
	}
	stats = w.RegionStats(ReadFlow)
	for _, stat := range stats {
		if stat.RegionID == id {
			return stat.HotDegree >= hotThreshold
		}
	}
	return false
}
