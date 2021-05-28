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

const queueCap = 1000

// HotCache is a cache hold hot regions.
type HotCache struct {
	readFlowQueue  chan *FlowItem
	writeFlowQueue chan *FlowItem
	writeFlow      *hotPeerCache
	readFlow       *hotPeerCache
}

// FlowItem indicates the item in the flow, it is a wrapper for peerInfo, expiredItems or coldItem.
type FlowItem struct {
	peerInfo             *core.PeerInfo
	regionInfo           *core.RegionInfo
	expiredStat          *HotPeerStat
	unReportStatsCollect *unReportStatsCollect
}

type unReportStatsCollect struct {
	storeID   uint64
	regionIDs map[uint64]struct{}
	interval  uint64
}

// NewUnReportStatsCollect creates information for unreported stats
func NewUnReportStatsCollect(storeID uint64, regionIDs map[uint64]struct{}, interval uint64) *FlowItem {
	return &FlowItem{
		peerInfo:    nil,
		regionInfo:  nil,
		expiredStat: nil,
		unReportStatsCollect: &unReportStatsCollect{
			storeID:   storeID,
			regionIDs: regionIDs,
			interval:  interval,
		},
	}
}

// NewPeerInfoItem creates FlowItem for PeerInfo
func NewPeerInfoItem(peerInfo *core.PeerInfo, regionInfo *core.RegionInfo) *FlowItem {
	return &FlowItem{
		peerInfo:             peerInfo,
		regionInfo:           regionInfo,
		expiredStat:          nil,
		unReportStatsCollect: nil,
	}
}

// NewExpiredStatItem creates Expired stat
func NewExpiredStatItem(expiredStat *HotPeerStat) *FlowItem {
	return &FlowItem{
		peerInfo:             nil,
		regionInfo:           nil,
		expiredStat:          expiredStat,
		unReportStatsCollect: nil,
	}
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context) *HotCache {
	w := &HotCache{
		readFlowQueue:  make(chan *FlowItem, queueCap),
		writeFlowQueue: make(chan *FlowItem, queueCap),
		writeFlow:      NewHotStoresStats(WriteFlow),
		readFlow:       NewHotStoresStats(ReadFlow),
	}
	go w.updateItems(ctx)
	return w
}

// ExpiredItems returns the items which are already expired.
func (w *HotCache) ExpiredItems(region *core.RegionInfo) (expiredItems []*HotPeerStat) {
	expiredItems = append(expiredItems, w.ExpiredReadItems(region)...)
	expiredItems = append(expiredItems, w.ExpiredWriteItems(region)...)
	return
}

// ExpiredReadItems returns the read items which are already expired.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return w.readFlow.CollectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeFlow.CollectExpiredItems(region)
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
func (w *HotCache) CheckWriteAsync(item *FlowItem) {
	w.writeFlowQueue <- item
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(item *FlowItem) {
	w.readFlowQueue <- item
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

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind FlowKind, minHotDegree int) map[uint64][]*HotPeerStat {
	switch kind {
	case WriteFlow:
		return w.writeFlow.RegionStats(minHotDegree)
	case ReadFlow:
		return w.readFlow.RegionStats(minHotDegree)
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
	return w.writeFlow.isRegionHotWithAnyPeers(region, minHotDegree) ||
		w.readFlow.isRegionHotWithAnyPeers(region, minHotDegree)
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.writeFlow.CollectMetrics("write")
	w.readFlow.CollectMetrics("read")
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

func (w *HotCache) updateItems(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-w.writeFlowQueue:
			if ok && item != nil {
				w.updateItem(item, w.writeFlow)
			}
			hotCacheFlowQueueStatusGauge.WithLabelValues(WriteFlow.String()).Set(float64(len(w.writeFlowQueue)))
		case item, ok := <-w.readFlowQueue:
			if ok && item != nil {
				w.updateItem(item, w.readFlow)
			}
			hotCacheFlowQueueStatusGauge.WithLabelValues(ReadFlow.String()).Set(float64(len(w.readFlowQueue)))
		}
	}
}

func (w *HotCache) updateItem(item *FlowItem, flow *hotPeerCache) {
	if item.peerInfo != nil && item.regionInfo != nil {
		stat := flow.CheckPeerFlow(item.peerInfo, item.regionInfo)
		if stat != nil {
			w.Update(stat)
		}
	} else if item.expiredStat != nil {
		w.Update(item.expiredStat)
	} else if item.unReportStatsCollect != nil {
		handle := item.unReportStatsCollect
		stats := flow.CheckColdPeer(handle.storeID, handle.regionIDs, handle.interval)
		for _, stat := range stats {
			w.Update(stat)
		}
	}
}
