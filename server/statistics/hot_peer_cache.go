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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
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
	kind           FlowKind
	peersOfStore   map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
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
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
		item.Log("region heartbeat delete from cache", log.Debug)
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(dimLen, TopNN, topNTTL)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		stores, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
		item.Log("region heartbeat update", log.Debug)
	}
}

func (f *hotPeerCache) collectRegionMetrics(byteRate, keyRate float64, interval uint64) {
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

	bytes := float64(f.getRegionBytes(region))
	keys := float64(f.getRegionKeys(region))

	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)

	f.collectRegionMetrics(byteRate, keyRate, interval)
	// old region is in the front and new region is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}

	var tmpItem *HotPeerStat
	storeIDs := f.getAllStoreIDs(region)
	justTransferLeader := f.justTransferLeader(region)
	for _, storeID := range storeIDs {
		isExpired := f.isRegionExpired(region, storeID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
		if isExpired && oldItem != nil { // it may has been moved to other store, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator and test. Ignore if report too fast.
		if !isExpired && Denoising && interval < HotRegionReportMinInterval {
			continue
		}

		thresholds := f.calcHotThresholds(storeID)

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
			source:             direct,
		}
		// todo diff with branch
		if oldItem == nil {
			if tmpItem != nil && tmpItem.AntiCount > 0 { // use the tmpItem cached from the store where this region was in before
				newItem.source = inherit
				oldItem = tmpItem
				tmpItem = nil
			} else { // new item is new peer after adding replica
				for _, storeID := range storeIDs {
					oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
					if oldItem != nil && oldItem.allowAdopt {
						newItem.source = adopt
						break
					}
				}
			}
		}

		newItem = f.updateHotPeerStat(newItem, oldItem, bytes, keys, time.Duration(interval)*time.Second)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithPeer(region, region.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholds(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getRegionBytes(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetBytesWritten()
	case ReadFlow:
		return region.GetBytesRead()
	}
	return 0
}

func (f *hotPeerCache) getRegionKeys(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetKeysWritten()
	case ReadFlow:
		return region.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isRegionExpired(region *core.RegionInfo, storeID uint64) bool {
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.peersOfStore[storeID]
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
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(region.GetPeers()))
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
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
func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.peers {
			if id == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.storesOfRegion[oldItem.RegionID]
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

func (f *hotPeerCache) justTransferLeader(region *core.RegionInfo) bool {
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
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

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(region.GetID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
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
			newItem.allowAdopt = true
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

	if newItem.source == adopt {
		newItem.rollingByteRate = oldItem.rollingByteRate.Clone()
		newItem.rollingKeyRate = oldItem.rollingKeyRate.Clone()
		newItem.allowAdopt = false
	} else {
		newItem.rollingByteRate = oldItem.rollingByteRate
		newItem.rollingKeyRate = oldItem.rollingKeyRate
		newItem.allowAdopt = oldItem.allowAdopt
	}

	if newItem.justTransferLeader {
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		newItem.lastTransferLeaderTime = time.Now()
		return newItem
	}

	newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)

	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeer(oldItem, newItem.StoreID) {
			if newItem.isFullAndHot() {
				newItem.HotDegree = 1
				newItem.allowAdopt = true
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.needDelete = true
			}
		} else {
			if newItem.isFullAndHot() {
				newItem.HotDegree = oldItem.HotDegree + 1
				newItem.AntiCount = hotRegionAntiCount
				newItem.allowAdopt = true
			} else {
				newItem.HotDegree = oldItem.HotDegree - 1
				newItem.AntiCount = oldItem.AntiCount - 1
				if newItem.AntiCount <= 0 {
					newItem.needDelete = true
				} else {
					newItem.allowAdopt = true
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}
