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
	"math/rand"

	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

// HotCache is a cache hold hot regions.
type HotCache struct {
	caches []*hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache() *HotCache {
	caches := make([]*hotPeerCache, CacheKindCount)
	for i := HotCacheKind(0); i < CacheKindCount; i++ {
		caches[i] = NewHotStoresStats(i)
	}
	return &HotCache{
		caches: caches,
	}
}

// CheckRegion checks the region's status and returns update items.
func (w *HotCache) CheckRegion(region *core.RegionInfo) (stats []*HotPeerStat) {
	for _, c := range w.caches {
		stats = append(stats, c.CheckRegion(region)...)
	}
	return
}

// Update updates the cache.
func (w *HotCache) Update(item *HotPeerStat) {
	w.caches[item.Kind].Update(item)

	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		w.incMetrics("update_item", item.StoreID, item.Kind)
	}
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind HotCacheKind, minHotDegree int) map[uint64][]*HotPeerStat {
	return w.caches[kind].RegionStats(minHotDegree)
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotCache) RandHotRegionFromStore(storeID uint64, kind HotCacheKind, minHotDegree int) *HotPeerStat {
	if stats, ok := w.RegionStats(kind, minHotDegree)[storeID]; ok && len(stats) > 0 {
		return stats[rand.Intn(len(stats))]
	}
	return nil
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, minHotDegree int) bool {
	return slice.AnyOf(w.caches, func(i int) bool { return w.caches[i].IsRegionHot(region, minHotDegree) })
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	for _, cache := range w.caches {
		cache.CollectMetrics()
	}
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) incMetrics(name string, storeID uint64, kind HotCacheKind) {
	store := storeTag(storeID)
	hotCacheStatusGauge.WithLabelValues(name, store, kind.String()).Inc()

	// TODO: remove it. For backward compatibility now.
	switch kind {
	case PeerCache:
		hotCacheStatusGauge.WithLabelValues(name, store, "write").Inc()
	case LeaderCache:
		hotCacheStatusGauge.WithLabelValues(name, store, "read").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind HotCacheKind) int {
	return w.caches[kind].getDefaultTimeMedian().GetFilledPeriod()
}
