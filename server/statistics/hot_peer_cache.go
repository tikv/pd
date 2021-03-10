// Copyright 2019 TiKV Project Authors.
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
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	topNTTL           = 3 * RegionHeartBeatReportInterval * time.Second

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var (
	minHotThresholds = [2][dimLen]float64{
		WriteFlow: {
			byteDim: 1 * 1024,
			keyDim:  32,
		},
		ReadFlow: {
			byteDim: 8 * 1024,
			keyDim:  128,
		},
	}
)

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	mu struct {
		sync.RWMutex
		peersOfStore   map[uint64]*TopN               // storeID -> hot peers
		storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
	}
	kind FlowKind
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	c := &hotPeerCache{
		kind: kind,
	}
	c.mu.peersOfStore = make(map[uint64]*TopN)
	c.mu.storesOfRegion = make(map[uint64]map[uint64]struct{})
	return c
}

// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	f.mu.RLock()
	defer f.mu.RUnlock()
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.mu.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if item.IsNeedDelete() {
		if peers, ok := f.mu.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.mu.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
	} else {
		peers, ok := f.mu.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(dimLen, TopNN, topNTTL)
			f.mu.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		stores, ok := f.mu.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.mu.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
	}
}

func (f *hotPeerCache) collectRegionMetricsLocked(byteRate, keyRate float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	if f.kind == ReadFlow {
		readByteHist.Observe(byteRate)
		readKeyHist.Observe(keyRate)
	}
	if f.kind == WriteFlow {
		writeByteHist.Observe(byteRate)
		writeKeyHist.Observe(keyRate)
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo) (ret []*HotPeerStat) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bytes := float64(f.getRegionBytesLocked(region))
	keys := float64(f.getRegionKeysLocked(region))

	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)

	f.collectRegionMetricsLocked(byteRate, keyRate, interval)
	// old region is in the front and new region is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}

	var tmpItem *HotPeerStat
	storeIDs := f.getAllStoreIDsLocked(region)
	justTransferLeader := f.justTransferLeaderLocked(region)
	for _, storeID := range storeIDs {
		isExpired := f.isRegionExpiredLocked(region, storeID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStatLocked(region.GetID(), storeID)
		if isExpired && oldItem != nil { // it may has been moved to other store, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator and test. Ignore if report too fast.
		if !isExpired && Denoising && interval < HotRegionReportMinInterval {
			continue
		}

		thresholds := f.calcHotThresholdsLocked(storeID)

		newItem := &HotPeerStat{
			StoreID:            storeID,
			RegionID:           region.GetID(),
			Kind:               f.kind,
			ByteRate:           byteRate,
			KeyRate:            keyRate,
			LastUpdateTime:     time.Now(),
			needDelete:         isExpired,
			isLeader:           region.GetLeader().GetStoreId() == storeID,
			justTransferLeader: justTransferLeader,
			interval:           interval,
			peers:              peers,
			thresholds:         thresholds,
		}

		if oldItem == nil {
			if tmpItem != nil { // use the tmpItem cached from the store where this region was in before
				oldItem = tmpItem
			} else { // new item is new peer after adding replica
				for _, storeID := range storeIDs {
					oldItem = f.getOldHotPeerStatLocked(region.GetID(), storeID)
					if oldItem != nil {
						break
					}
				}
			}
		}

		newItem = f.updateHotPeerStatLocked(newItem, oldItem, bytes, keys, time.Duration(interval)*time.Second)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	log.Debug("region heartbeat info",
		zap.String("type", f.kind.String()),
		zap.Uint64("region", region.GetID()),
		zap.Uint64("leader", region.GetLeader().GetStoreId()),
		zap.Uint64s("peers", peers),
	)
	return ret
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeersLocked(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithPeerLocked(region, region.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for storeID, peers := range f.mu.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholdsLocked(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getRegionBytesLocked(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		x := region.GetBytesWritten()
		return x
	case ReadFlow:
		y := region.GetBytesRead()
		return y
	}
	return 0
}

func (f *hotPeerCache) getRegionKeysLocked(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetKeysWritten()
	case ReadFlow:
		return region.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStatLocked(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.mu.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isRegionExpiredLocked(region *core.RegionInfo, storeID uint64) bool {
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholdsLocked(storeID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.mu.peersOfStore[storeID]
	if !ok || tn.Len() < TopNN {
		return minThresholds
	}
	ret := [dimLen]float64{
		byteDim: tn.GetTopNMin(byteDim).(*HotPeerStat).GetByteRate(),
		keyDim:  tn.GetTopNMin(keyDim).(*HotPeerStat).GetKeyRate(),
	}
	for k := 0; k < dimLen; k++ {
		ret[k] = math.Max(ret[k]*HotThresholdRatio, minThresholds[k])
	}
	return ret
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDsLocked(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(region.GetPeers()))
	// old stores
	ids, ok := f.mu.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
			ret = append(ret, storeID)
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
			ret = append(ret, peer.GetStoreId())
		}
	}

	return ret
}
func (f *hotPeerCache) isOldColdPeerLocked(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.peers {
			if id == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.mu.storesOfRegion[oldItem.RegionID]
		if ok {
			for id := range ids {
				if id == storeID {
					return false
				}
			}
		}
		return true
	}
	return isOldPeer() && noInCache()
}

func (f *hotPeerCache) justTransferLeaderLocked(region *core.RegionInfo) bool {
	ids, ok := f.mu.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStatLocked(region.GetID(), storeID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != region.GetLeader().GetStoreId()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithAnyPeersLocked(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeerLocked(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeerLocked(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.mu.peersOfStore[storeID]; ok {
		if stat := peers.Get(region.GetID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval*time.Second)
}

func (f *hotPeerCache) updateHotPeerStatLocked(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
	if newItem.needDelete {
		return newItem
	}

	if oldItem == nil {
		if interval == 0 {
			return nil
		}
		isHot := bytes/interval.Seconds() >= newItem.thresholds[byteDim] || keys/interval.Seconds() >= newItem.thresholds[keyDim]
		if !isHot {
			return nil
		}
		if interval.Seconds() >= RegionHeartBeatReportInterval {
			newItem.HotDegree = 1
			newItem.AntiCount = hotRegionAntiCount
		}
		newItem.isNew = true
		newItem.rollingByteRate = newDimStat(byteDim)
		newItem.rollingKeyRate = newDimStat(keyDim)
		newItem.rollingByteRate.Add(bytes, interval)
		newItem.rollingKeyRate.Add(keys, interval)
		if newItem.rollingKeyRate.isFull() {
			newItem.clearLastAverage()
		}
		return newItem
	}

	newItem.rollingByteRate = oldItem.rollingByteRate
	newItem.rollingKeyRate = oldItem.rollingKeyRate

	if newItem.justTransferLeader {
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		// skip the first heartbeat interval after transfer leader
		return newItem
	}

	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)

	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeerLocked(oldItem, newItem.StoreID) {
			if newItem.isFullAndHot() {
				newItem.HotDegree = 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.needDelete = true
			}
		} else {
			if newItem.isFullAndHot() {
				newItem.HotDegree = oldItem.HotDegree + 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.HotDegree = oldItem.HotDegree - 1
				newItem.AntiCount = oldItem.AntiCount - 1
				if newItem.AntiCount <= 0 {
					newItem.needDelete = true
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}
