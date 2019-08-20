package statistics

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
)

// hotStoresStats saves the hotspot peer's statistics.
type hotStoresStats struct {
	hotStoreStats  map[uint64]cache.Cache         // storeID -> hot regions
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats() *hotStoresStats {
	return &hotStoresStats{
		hotStoreStats:  make(map[uint64]cache.Cache),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotStoresStats) CheckRegionFlow(region *core.RegionInfo, kind FlowKind) []HotSpotPeerStatGenerator {
	var (
		generators   []HotSpotPeerStatGenerator
		getBytesFlow func() uint64
		getKeysFlow  func() uint64
		bytesPerSec  uint64
		keysPerSec   uint64

		isExpiredInStore func(region *core.RegionInfo, storeID uint64) bool
	)

	storeIDs := make(map[uint64]struct{})
	// gets the storeIDs, including old region and new region
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
		}
	}

	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
		}
	}

	switch kind {
	case WriteFlow:
		getBytesFlow = region.GetBytesWritten
		getKeysFlow = region.GetKeysWritten
		isExpiredInStore = func(region *core.RegionInfo, storeID uint64) bool {
			return region.GetStorePeer(storeID) == nil
		}
	case ReadFlow:
		getBytesFlow = region.GetBytesRead
		getKeysFlow = region.GetKeysRead
		isExpiredInStore = func(region *core.RegionInfo, storeID uint64) bool {
			return region.GetLeader().GetStoreId() != storeID
		}
	}

	bytesPerSecInit := uint64(float64(getBytesFlow()) / float64(RegionHeartBeatReportInterval))
	keysPerSecInit := uint64(float64(getKeysFlow()) / float64(RegionHeartBeatReportInterval))
	for storeID := range storeIDs {
		bytesPerSec = bytesPerSecInit
		keysPerSec = keysPerSecInit
		var oldRegionStat *HotPeerStat

		hotStoreStats, ok := f.hotStoreStats[storeID]
		if ok {
			if v, isExist := hotStoreStats.Peek(region.GetID()); isExist {
				oldRegionStat = v.(*HotPeerStat)
				// This is used for the simulator.
				if Denoising {
					interval := time.Since(oldRegionStat.LastUpdateTime).Seconds()
					if interval < minHotRegionReportInterval && !isExpiredInStore(region, storeID) {
						continue
					}
					bytesPerSec = uint64(float64(getBytesFlow()) / interval)
					keysPerSec = uint64(float64(getKeysFlow()) / interval)
				}
			}
		}

		generator := &hotSpotPeerStatGenerator{
			Region:    region,
			StoreID:   storeID,
			FlowBytes: bytesPerSec,
			FlowKeys:  keysPerSec,
			Kind:      kind,

			lastHotSpotPeerStats: oldRegionStat,
		}

		if isExpiredInStore(region, storeID) {
			generator.Expired = true
		}
		generators = append(generators, generator)
	}
	return generators
}

// Update updates the items in statistics.
func (f *hotStoresStats) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if hotStoreStat, ok := f.hotStoreStats[item.StoreID]; ok {
			hotStoreStat.Remove(item.RegionID)
		}
		if index, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(index, item.StoreID)
		}
	} else {
		hotStoreStat, ok := f.hotStoreStats[item.StoreID]
		if !ok {
			hotStoreStat = cache.NewCache(statCacheMaxLen, cache.TwoQueueCache)
			f.hotStoreStats[item.StoreID] = hotStoreStat
		}
		hotStoreStat.Put(item.RegionID, item)
		index, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			index = make(map[uint64]struct{})
		}
		index[item.StoreID] = struct{}{}
		f.storesOfRegion[item.RegionID] = index
	}
}

func (f *hotStoresStats) isRegionHotWithAnyPeers(region *core.RegionInfo, hotThreshold int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotThreshold) {
			return true
		}
	}
	return false

}

func (f *hotStoresStats) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotThreshold int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	stats, ok := f.hotStoreStats[storeID]
	if !ok {
		return false
	}
	if stat, ok := stats.Peek(region.GetID()); ok {
		return stat.(*HotPeerStat).HotDegree >= hotThreshold
	}
	return false
}
