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
	writeFlow cache.Cache
	readFlow  cache.Cache
}

func newHotSpotCache() *HotSpotCache {
	return &HotSpotCache{
		writeFlow: cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
		readFlow:  cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
	}
}

// CheckWrite checks the write status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckWrite(region *core.RegionInfo, stores *core.StoresInfo) (bool, *core.RegionStat) {
	var WrittenBytesPerSec uint64
	v, isExist := w.writeFlow.Peek(region.GetId())
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
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(stores.TotalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return w.isNeedUpdateStatCache(region, hotRegionThreshold, WriteFlow)
}

// CheckRead checks the read status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckRead(region *core.RegionInfo, stores *core.StoresInfo) (bool, *core.RegionStat) {
	var ReadBytesPerSec uint64
	v, isExist := w.readFlow.Peek(region.GetId())
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
	divisor := float64(statCacheMaxLen) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(stores.TotalReadBytes()) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return w.isNeedUpdateStatCache(region, hotRegionThreshold, ReadFlow)
}

func (w *HotSpotCache) isNeedUpdateStatCache(region *core.RegionInfo, hotRegionThreshold uint64, kind FlowKind) (bool, *core.RegionStat) {
	var (
		v         *core.RegionStat
		value     interface{}
		isExist   bool
		flowBytes uint64
	)
	key := region.GetId()

	switch kind {
	case WriteFlow:
		value, isExist = w.writeFlow.Peek(key)
		flowBytes = region.WrittenBytes
	case ReadFlow:
		value, isExist = w.readFlow.Peek(key)
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
func (w *HotSpotCache) Update(key uint64, item *core.RegionStat, kind FlowKind) {
	switch kind {
	case WriteFlow:
		if item == nil {
			w.writeFlow.Remove(key)
		} else {
			w.writeFlow.Put(key, item)
		}
	case ReadFlow:
		if item == nil {
			w.readFlow.Remove(key)
		} else {
			w.readFlow.Put(key, item)
		}
	}
}

// RegionStats returns hot items according to kind
func (w *HotSpotCache) RegionStats(kind FlowKind) []*core.RegionStat {
	var elements []*cache.Item
	switch kind {
	case WriteFlow:
		elements = w.writeFlow.Elems()
	case ReadFlow:
		elements = w.readFlow.Elems()
	}
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
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
	if stat, ok := w.writeFlow.Peek(id); ok {
		if stat.(*core.RegionStat).HotDegree >= hotThreshold {
			return true
		}
	}
	if stat, ok := w.readFlow.Peek(id); ok {
		return stat.(*core.RegionStat).HotDegree >= hotThreshold
	}
	return false
}
