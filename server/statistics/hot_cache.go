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
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
)

// Simulating is an option to overpass the impact of accelerated time. Should
// only turned on by the simulator.
var Simulating bool

const (
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region.
	RegionHeartBeatReportInterval = 60
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10

	statCacheMaxLen            = 1000
	storeStatCacheMaxLen       = 200
	hotWriteRegionMinFlowRate  = 1 * 1024
	hotReadRegionMinFlowRate   = 4 * 1024
	minHotRegionReportInterval = 3
	hotRegionAntiCount         = 1
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
	writeFlow      map[uint64]cache.Cache
	readFlow       map[uint64]cache.Cache
	indexWriteFlow map[uint64]map[uint64]struct{}
	indexReadFlow  map[uint64]map[uint64]struct{}
}

// NewHotSpotCache creates a new hot spot cache.
func NewHotSpotCache() *HotSpotCache {
	return &HotSpotCache{
		writeFlow:      make(map[uint64]cache.Cache),
		readFlow:       make(map[uint64]cache.Cache),
		indexWriteFlow: make(map[uint64]map[uint64]struct{}),
		indexReadFlow:  make(map[uint64]map[uint64]struct{}),
	}
}

// CheckWrite checks the write status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckWrite(region *core.RegionInfo, stats *StoresStats) []*RegionStat {
	var (
		WrittenBytesPerSec uint64
		value              *RegionStat
	)

	var updateItems []*RegionStat
	var storeIDs []uint64
	// got the storeIDs
	{
		ids, ok := w.indexWriteFlow[region.GetID()]
		if ok {
			for storeID := range ids {
				storeIDs = append(storeIDs, storeID)
			}

		}
		for _, peer := range region.GetPeers() {
			if !ok {
				storeIDs = append(storeIDs, peer.GetStoreId())
				continue
			}
			if _, ook := ids[peer.GetStoreId()]; !ook {
				storeIDs = append(storeIDs, peer.GetStoreId())

			}
		}
	}

	for _, storeID := range storeIDs {
		WrittenBytesPerSec = uint64(float64(region.GetBytesWritten()) / float64(RegionHeartBeatReportInterval))
		storeWriteFlow, ok := w.writeFlow[storeID]
		if !ok {
			storeWriteFlow = cache.NewCache(statCacheMaxLen, cache.TwoQueueCache)
			w.writeFlow[storeID] = storeWriteFlow
		}
		regionID := region.GetID()
		if v, isExist := storeWriteFlow.Peek(regionID); isExist {
			value = v.(*RegionStat)
			// This is used for the simulator.
			if !Simulating {
				interval := time.Since(value.LastUpdateTime).Seconds()
				if interval < minHotRegionReportInterval {
					return nil
				}
				WrittenBytesPerSec = uint64(float64(region.GetBytesWritten()) / interval)
			}
		}
		if peer := region.GetStorePeer(storeID); peer == nil {
			// use maxUint64 to delete the no exist peer
			WrittenBytesPerSec = math.MaxUint64
		}
		hotRegionThreshold := calculateWriteHotThresholdWithStore(stats, storeID)
		item := w.isNeedUpdateStatCache(region, storeID, WrittenBytesPerSec, hotRegionThreshold, value, WriteFlow)
		if item != nil {
			updateItems = append(updateItems, item)
		}
	}
	return updateItems
}

// CheckRead checks the read status, returns whether need update statistics and item.
func (w *HotSpotCache) CheckRead(region *core.RegionInfo, stats *StoresStats) []*RegionStat {
	var (
		ReadBytesPerSec uint64
		value           *RegionStat
	)

	leader := region.GetLeader()
	if leader == nil {
		return nil
	}
	var updateItems []*RegionStat
	var storeIDs []uint64
	ids, ok := w.indexReadFlow[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs = append(storeIDs, storeID)
		}
		if _, ok := ids[leader.GetStoreId()]; !ok {
			storeIDs = append(storeIDs, leader.GetStoreId())
		}
	} else {
		storeIDs = append(storeIDs, leader.GetStoreId())
	}

	for _, storeID := range storeIDs {
		ReadBytesPerSec = uint64(float64(region.GetBytesRead()) / float64(RegionHeartBeatReportInterval))
		storeReadFlow, ok := w.readFlow[storeID]
		if !ok {
			storeReadFlow = cache.NewCache(statCacheMaxLen, cache.TwoQueueCache)
			w.readFlow[storeID] = storeReadFlow
		}
		if v, isExist := storeReadFlow.Peek(region.GetID()); isExist {
			value = v.(*RegionStat)
			// This is used for the simulator.
			if !Simulating {
				interval := time.Since(value.LastUpdateTime).Seconds()
				if interval < minHotRegionReportInterval {
					return nil
				}
				ReadBytesPerSec = uint64(float64(region.GetBytesRead()) / interval)
			}
		}
		// Only leader has read flow
		if region.GetStorePeer(storeID).GetId() != leader.GetId() {
			ReadBytesPerSec = math.MaxUint64
		}

		hotRegionThreshold := calculateReadHotThresholdWithStore(stats, storeID)
		item := w.isNeedUpdateStatCache(region, storeID, ReadBytesPerSec, hotRegionThreshold, value, ReadFlow)
		if item != nil {
			updateItems = append(updateItems, item)
		}
	}
	return updateItems
}

func (w *HotSpotCache) incMetrics(name string, storeID uint64, kind FlowKind) {
	storeTag := fmt.Sprintf("store-%d", storeID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, storeTag, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, storeTag, "read").Inc()
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

func calculateWriteHotThresholdWithStore(stats *StoresStats, storeID uint64) uint64 {
	writeBytes, _ := stats.GetStoreBytesWrite(storeID)
	divisor := float64(storeStatCacheMaxLen) * 2
	hotRegionThreshold := uint64(float64(writeBytes) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return hotRegionThreshold
}

func calculateReadHotThresholdWithStore(stats *StoresStats, storeID uint64) uint64 {
	_, readBytes := stats.GetStoreBytesWrite(storeID)
	divisor := float64(storeStatCacheMaxLen) * 2
	hotRegionThreshold := uint64(float64(readBytes) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
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

const rollingWindowsSize = 5

func (w *HotSpotCache) isNeedUpdateStatCache(region *core.RegionInfo, storeID uint64, flowBytes uint64, hotRegionThreshold uint64, oldItem *RegionStat, kind FlowKind) *RegionStat {
	newItem := NewRegionStat(region, flowBytes)
	newItem.StoreID = storeID
	if flowBytes == math.MaxUint64 {
		w.incMetrics("remove_item", storeID, kind)
		newItem.NeedDelete = true
		return newItem
	}
	if region.GetLeader().GetStoreId() == storeID {
		newItem.isLeader = true
	}
	if oldItem != nil {
		newItem.HotDegree = oldItem.HotDegree + 1
		newItem.Stats = oldItem.Stats
	}
	if flowBytes >= hotRegionThreshold {
		if oldItem == nil {
			w.incMetrics("add_item", storeID, kind)
			newItem.Stats = NewRollingStats(rollingWindowsSize)
		}
		newItem.Stats.Add(float64(flowBytes))
		return newItem
	}
	// smaller than hotRegionThreshold
	if oldItem == nil {
		return nil
	}
	if oldItem.AntiCount <= 0 {
		w.incMetrics("remove_item", storeID, kind)
		newItem.NeedDelete = true
		return newItem
	}
	// eliminate some noise
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	newItem.Stats.Add(float64(flowBytes))
	return newItem
}

// Update updates the cache.
func (w *HotSpotCache) Update(item *RegionStat, kind FlowKind) {
	switch kind {
	case WriteFlow:
		if item.IsNeedDelete() {
			w.writeFlow[item.StoreID].Remove(item.RegionID)
			if index, ok := w.indexWriteFlow[item.RegionID]; ok {
				delete(index, item.StoreID)
			}
		} else {
			w.writeFlow[item.StoreID].Put(item.RegionID, item)
			index, ok := w.indexWriteFlow[item.RegionID]
			if !ok {
				index = make(map[uint64]struct{})
			}
			index[item.StoreID] = struct{}{}
			w.indexWriteFlow[item.RegionID] = index
			w.incMetrics("update_item", item.StoreID, kind)
		}
	case ReadFlow:
		if item.IsNeedDelete() {
			w.readFlow[item.StoreID].Remove(item.RegionID)
			if index, ok := w.indexReadFlow[item.RegionID]; ok {
				delete(index, item.StoreID)
			}
		} else {
			w.readFlow[item.StoreID].Put(item.RegionID, item)
			index, ok := w.indexReadFlow[item.RegionID]
			if !ok {
				index = make(map[uint64]struct{})
			}
			index[item.StoreID] = struct{}{}
			w.indexReadFlow[item.RegionID] = index
			w.incMetrics("update_item", item.StoreID, kind)
		}
	}
}

// RegionStats returns hot items according to kind
func (w *HotSpotCache) RegionStats(kind FlowKind) map[uint64][]*RegionStat {
	var flowMap map[uint64]cache.Cache
	switch kind {
	case WriteFlow:
		flowMap = w.writeFlow
	case ReadFlow:
		flowMap = w.readFlow
	}
	res := make(map[uint64][]*RegionStat)
	for storeID, elements := range flowMap {
		values := elements.Elems()
		stat, ok := res[storeID]
		if !ok {
			stat = make([]*RegionStat, len(values))
			res[storeID] = stat
		}
		for i := range values {
			stat[i] = values[i].Value.(*RegionStat)
		}
	}
	return res
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotSpotCache) RandHotRegionFromStore(storeID uint64, kind FlowKind, hotThreshold int) *RegionStat {
	stats, ok := w.RegionStats(kind)[storeID]
	if !ok {
		return nil
	}
	for _, i := range rand.Perm(len(stats)) {
		if stats[i].HotDegree >= hotThreshold && stats[i].StoreID == storeID {
			return stats[i]
		}
	}
	return nil
}

// CollectMetrics collect the hot cache metrics
func (w *HotSpotCache) CollectMetrics(stats *StoresStats) {
	for storeID, flowStats := range w.writeFlow {
		storeTag := fmt.Sprintf("store-%d", storeID)
		threshold := calculateWriteHotThresholdWithStore(stats, storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", storeTag, "write").Set(float64(flowStats.Len()))
		hotCacheStatusGauge.WithLabelValues("hotThreshold", storeTag, "write").Set(float64(threshold))
	}

	for storeID, flowStats := range w.readFlow {
		storeTag := fmt.Sprintf("store-%d", storeID)
		threshold := calculateReadHotThresholdWithStore(stats, storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", storeTag, "read").Set(float64(flowStats.Len()))
		hotCacheStatusGauge.WithLabelValues("hotThreshold", storeTag, "read").Set(float64(threshold))
	}
}

// IsRegionHot checks if the region is hot.
func (w *HotSpotCache) IsRegionHot(region *core.RegionInfo, hotThreshold int) bool {
	for _, peer := range region.GetPeers() {
		if storeWriteFlow, ok := w.writeFlow[peer.GetStoreId()]; ok {
			if stat, ok := storeWriteFlow.Peek(region.GetID()); ok {
				if stat.(*RegionStat).HotDegree >= hotThreshold {
					return true
				}
			}
		}
	}
	storeID := region.GetLeader().GetStoreId()
	storeReadFlow, ok := w.readFlow[storeID]
	if !ok {
		return false
	}
	if stat, ok := storeReadFlow.Peek(region.GetID()); ok {
		return stat.(*RegionStat).HotDegree >= hotThreshold
	}
	return false
}
