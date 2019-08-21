package statistics

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
)

const rollingWindowsSize = 5

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
func (f *hotStoresStats) CheckRegionFlow(region *core.RegionInfo, kind FlowKind, stats *StoresStats) (ret []*HotPeerStat) {
	var (
		bytesFlow        uint64
		keysFlow         uint64
		isExpiredInStore func(storeID uint64) bool
		calcHotThreshold func(storeID uint64) uint64
	)

	switch kind {
	case WriteFlow:
		bytesFlow = region.GetBytesWritten()
		keysFlow = region.GetKeysWritten()
		isExpiredInStore = func(storeID uint64) bool {
			return region.GetStorePeer(storeID) == nil
		}
		calcHotThreshold = func(storeID uint64) uint64 {
			return calculateWriteHotThresholdWithStore(stats, storeID)
		}
	case ReadFlow:
		bytesFlow = region.GetBytesRead()
		keysFlow = region.GetKeysRead()
		isExpiredInStore = func(storeID uint64) bool {
			return region.GetLeader().GetStoreId() != storeID
		}
		calcHotThreshold = func(storeID uint64) uint64 {
			return calculateReadHotThresholdWithStore(stats, storeID)
		}
	}

	storeIDs := f.getAllStoreIDs(region, kind)

	bytesPerSecInit := uint64(float64(bytesFlow) / float64(RegionHeartBeatReportInterval))
	keysPerSecInit := uint64(float64(keysFlow) / float64(RegionHeartBeatReportInterval))

	for storeID := range storeIDs {
		bytesPerSec := bytesPerSecInit
		keysPerSec := keysPerSecInit
		var oldItem *HotPeerStat

		hotStoreStats, ok := f.hotStoreStats[storeID]
		if ok {
			if v, isExist := hotStoreStats.Peek(region.GetID()); isExist {
				oldItem = v.(*HotPeerStat)
				// This is used for the simulator.
				if Denoising {
					interval := time.Since(oldItem.LastUpdateTime).Seconds()
					// ignore if report too fast
					if interval < minHotRegionReportInterval && !isExpiredInStore(storeID) {
						continue
					}
					bytesPerSec = uint64(float64(bytesFlow) / interval)
					keysPerSec = uint64(float64(keysFlow) / interval)
				}
			}
		}

		isExpried := isExpiredInStore(storeID)
		isLeader := region.GetLeader().GetStoreId() == storeID
		isHot := bytesPerSec >= calcHotThreshold(storeID)

		newItem := newHotPeerStat(storeID, region, kind, bytesPerSec, keysPerSec, isExpried, isLeader, isHot, oldItem)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func newHotPeerStat(storeID uint64, region *core.RegionInfo, kind FlowKind, bytesRate, keysRate uint64, isExpired, isLeader, isHot bool, oldItem *HotPeerStat) *HotPeerStat {
	newItem := &HotPeerStat{
		StoreID:        storeID,
		RegionID:       region.GetID(),
		Kind:           kind,
		BytesRate:      bytesRate,
		KeysRate:       keysRate,
		LastUpdateTime: time.Now(),
		Version:        region.GetMeta().GetRegionEpoch().GetVersion(),
		needDelete:     isExpired,
		isLeader:       isLeader,
	}

	if isExpired {
		return newItem
	}

	if oldItem != nil {
		newItem.RollingBytesRate = oldItem.RollingBytesRate
		if isHot {
			newItem.HotDegree = oldItem.HotDegree + 1
			newItem.AntiCount = hotRegionAntiCount
		} else {
			newItem.HotDegree = oldItem.HotDegree - 1
			newItem.AntiCount = oldItem.AntiCount - 1
			if newItem.AntiCount < 0 {
				newItem.needDelete = true
			}
		}
	} else {
		if !isHot {
			return nil
		}
		newItem.RollingBytesRate = NewRollingStats(rollingWindowsSize)
		newItem.AntiCount = hotRegionAntiCount
		newItem.isNew = true
	}
	newItem.RollingBytesRate.Add(float64(bytesRate))

	return newItem
}

// gets the storeIDs, including old region and new region
func (f *hotStoresStats) getAllStoreIDs(region *core.RegionInfo, kind FlowKind) map[uint64]struct{} {
	storeIDs := make(map[uint64]struct{})
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
		}
	}

	return storeIDs
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
