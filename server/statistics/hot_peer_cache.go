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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	// WriteReportInterval indicates the interval between write interval
	WriteReportInterval = RegionHeartBeatReportInterval
	// ReadReportInterval indicates the interval between read stats report
	ReadReportInterval = StoreHeartBeatReportInterval

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var minHotThresholds = [RegionStatCount]float64{
	RegionWriteBytes: 1 * 1024,
	RegionWriteKeys:  32,
	RegionWriteQuery: 32,
	RegionReadBytes:  8 * 1024,
	RegionReadKeys:   128,
	RegionReadQuery:  128,
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind               FlowKind
	peersOfStore       map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion     map[uint64]map[uint64]struct{} // regionID -> storeIDs
	regionsOfStore     map[uint64]map[uint64]struct{} // storeID -> regionIDs
	inheritItem        map[uint64]*HotPeerStat        // regionID -> HotPeerStat
	topNTTL            time.Duration
	reportIntervalSecs int
}

// NewHotPeerCache creates a hotPeerCache
func NewHotPeerCache(kind FlowKind) *hotPeerCache {
	c := &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
		regionsOfStore: make(map[uint64]map[uint64]struct{}),
		inheritItem:    make(map[uint64]*HotPeerStat),
	}
	if kind == WriteFlow {
		c.reportIntervalSecs = WriteReportInterval
	} else {
		c.reportIntervalSecs = ReadReportInterval
	}
	c.topNTTL = time.Duration(3*c.reportIntervalSecs) * time.Second
	return c
}

// TODO: rename RegionStats as PeerStats
// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree && !peer.inCold {
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
		f.putInheritItem(item)
		f.removeItem(item)
		item.Log("region heartbeat delete from cache", log.Debug)
	} else {
		f.putItem(item)
		item.Log("region heartbeat update", log.Debug)
	}
}

func (f *hotPeerCache) collectPeerMetrics(loads []float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	// TODO: use unified metrics. (keep backward compatibility at the same time)
	for _, k := range f.kind.RegionStats() {
		switch k {
		case RegionReadBytes:
			readByteHist.Observe(loads[int(k)])
		case RegionReadKeys:
			readKeyHist.Observe(loads[int(k)])
		case RegionWriteBytes:
			writeByteHist.Observe(loads[int(k)])
		case RegionWriteKeys:
			writeKeyHist.Observe(loads[int(k)])
		case RegionWriteQuery:
			writeQueryHist.Observe(loads[int(k)])
		case RegionReadQuery:
			readQueryHist.Observe(loads[int(k)])
		}
	}
}

// CollectExpiredItems collects expired items, mark them as needDelete and puts them into inherit items
func (f *hotPeerCache) CollectExpiredItems(region *core.RegionInfo) []*HotPeerStat {
	regionID := region.GetID()
	items := make([]*HotPeerStat, 0)
	for _, storeID := range f.getAllStoreIDs(region) {
		if region.GetStorePeer(storeID) == nil {
			item := f.getOldHotPeerStat(regionID, storeID)
			if item != nil {
				item.needDelete = true
				items = append(items, item)
			}
		}
	}
	return items
}

// CheckPeerFlow checks the flow information of a peer.
// Notice: CheckPeerFlow couldn't be used concurrently.
// CheckPeerFlow will update oldItem's rollingLoads into newItem, thus we should use write lock here.
func (f *hotPeerCache) CheckPeerFlow(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	interval := peer.GetInterval()
	if Denoising && interval < HotRegionReportMinInterval {
		return nil
	}
	storeID := peer.GetStoreID()
	deltaLoads := peer.GetLoads()
	f.collectPeerMetrics(deltaLoads, interval)
	loads := make([]float64, len(deltaLoads))
	for i := range deltaLoads {
		loads[i] = deltaLoads[i] / float64(interval)
	}
	justTransferLeader := f.justTransferLeader(region)
	oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
	thresholds := f.calcHotThresholds(storeID)
	regionPeers := region.GetPeers()
	peers := make([]uint64, 0, len(regionPeers))
	for _, peer := range regionPeers {
		peers = append(peers, peer.StoreId)
	}
	newItem := &HotPeerStat{
		StoreID:            storeID,
		RegionID:           region.GetID(),
		Kind:               f.kind,
		Loads:              loads,
		LastUpdateTime:     time.Now(),
		needDelete:         false,
		isLeader:           region.GetLeader().GetStoreId() == storeID,
		justTransferLeader: justTransferLeader,
		interval:           interval,
		peers:              peers,
		thresholds:         thresholds,
	}
	if oldItem == nil {
		inheritItem := f.takeInheritItem(region.GetID())
		if inheritItem != nil {
			oldItem = inheritItem
		} else {
			for _, storeID := range f.getAllStoreIDs(region) {
				oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
				if oldItem != nil {
					break
				}
			}
		}
	}
	return f.updateHotPeerStat(newItem, oldItem, deltaLoads, time.Duration(interval)*time.Second)
}

// CheckColdPeer checks the collect the un-heartbeat peer and maintain it.
func (f *hotPeerCache) CheckColdPeer(storeID uint64, reportRegions map[uint64]struct{}, interval uint64) (ret []*HotPeerStat) {
	if Denoising && interval < HotRegionReportMinInterval {
		return
	}
	previousHotStat, ok := f.regionsOfStore[storeID]
	if !ok {
		return
	}
	for regionID := range previousHotStat {
		if _, ok := reportRegions[regionID]; !ok {
			oldItem := f.getOldHotPeerStat(regionID, storeID)
			if oldItem == nil {
				continue
			}
			newItem := &HotPeerStat{
				StoreID:  storeID,
				RegionID: regionID,
				Kind:     f.kind,
				// use oldItem.thresholds to make the newItem won't affect the threshold
				Loads:              oldItem.thresholds,
				LastUpdateTime:     time.Now(),
				needDelete:         false,
				isLeader:           oldItem.isLeader,
				justTransferLeader: oldItem.justTransferLeader,
				interval:           interval,
				peers:              oldItem.peers,
				thresholds:         oldItem.thresholds,
				inCold:             true,
			}
			deltaLoads := make([]float64, RegionStatCount)
			for i, loads := range oldItem.thresholds {
				deltaLoads[i] = loads * float64(interval)
			}
			stat := f.updateHotPeerStat(newItem, oldItem, deltaLoads, time.Duration(interval)*time.Second)
			if stat != nil {
				ret = append(ret, stat)
			}
		}
	}
	return
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholds(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[ByteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[KeyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[ByteDim])
	}
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64) []float64 {
	statKinds := f.kind.RegionStats()
	mins := make([]float64, len(statKinds))
	for i, k := range statKinds {
		mins[i] = minHotThresholds[k]
	}
	tn, ok := f.peersOfStore[storeID]
	if !ok || tn.Len() < TopNN {
		return mins
	}
	ret := make([]float64, len(statKinds))
	for i := range ret {
		ret[i] = math.Max(tn.GetTopNMin(i).(*HotPeerStat).GetLoad(statKinds[i])*HotThresholdRatio, mins[i])
	}
	return ret
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	regionPeers := region.GetPeers()
	ret := make([]uint64, 0, len(regionPeers))
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
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, time.Duration(f.reportIntervalSecs)*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	regionStats := f.kind.RegionStats()
	if oldItem == nil {
		return f.updateNewHotPeerStat(newItem, deltaLoads, interval)
	}

	newItem.rollingLoads = oldItem.rollingLoads

	if newItem.justTransferLeader {
		newItem.lastTransferLeaderTime = time.Now()
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		// For write stat, as the stat is send by region heartbeat, the first heartbeat will be skipped.
		// For read stat, as the stat is send by store heartbeat, the first heartbeat won't be skipped.
		if newItem.Kind == WriteFlow {
			inheritItemDegree(newItem, oldItem)
			return newItem
		}
	} else {
		newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	}

	for i, k := range regionStats {
		newItem.rollingLoads[i].Add(deltaLoads[k], interval)
	}

	isFull := newItem.rollingLoads[0].isFull() // The intervals of dims are the same, so it is only necessary to determine whether any of them
	if !isFull {
		// not update hot degree and anti count
		inheritItemDegree(newItem, oldItem)
	} else {
		// If item is inCold, it means the pd didn't recv this item in the store heartbeat,
		// thus we make it colder
		if newItem.inCold {
			coldItem(newItem, oldItem)
		} else {
			if f.isOldColdPeer(oldItem, newItem.StoreID) {
				if newItem.isFullAndHot() {
					initItemDegree(newItem)
				} else {
					newItem.needDelete = true
				}
			} else {
				if newItem.isFullAndHot() {
					hotItem(newItem, oldItem)
				} else {
					coldItem(newItem, oldItem)
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}

func (f *hotPeerCache) updateNewHotPeerStat(newItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	regionStats := f.kind.RegionStats()
	if interval == 0 {
		return nil
	}
	isHot := slice.AnyOf(regionStats, func(i int) bool {
		return deltaLoads[regionStats[i]]/interval.Seconds() >= newItem.thresholds[i]
	})
	if !isHot {
		return nil
	}
	if interval.Seconds() >= float64(f.reportIntervalSecs) {
		initItemDegree(newItem)
	}
	newItem.isNew = true
	newItem.rollingLoads = make([]*dimStat, len(regionStats))
	for i, k := range regionStats {
		ds := newDimStat(k, time.Duration(newItem.hotStatReportInterval())*time.Second)
		ds.Add(deltaLoads[k], interval)
		if ds.isFull() {
			ds.clearLastAverage()
		}
		newItem.rollingLoads[i] = ds
	}
	return newItem
}

func (f *hotPeerCache) putInheritItem(item *HotPeerStat) {
	f.inheritItem[item.RegionID] = item
}

func (f *hotPeerCache) takeInheritItem(regionID uint64) *HotPeerStat {
	item, ok := f.inheritItem[regionID]
	if !ok {
		return nil
	}
	if item != nil {
		delete(f.inheritItem, regionID)
		return item
	}
	return nil
}

func (f *hotPeerCache) putItem(item *HotPeerStat) {
	peers, ok := f.peersOfStore[item.StoreID]
	if !ok {
		peers = NewTopN(DimLen, TopNN, f.topNTTL)
		f.peersOfStore[item.StoreID] = peers
	}
	peers.Put(item)
	stores, ok := f.storesOfRegion[item.RegionID]
	if !ok {
		stores = make(map[uint64]struct{})
		f.storesOfRegion[item.RegionID] = stores
	}
	stores[item.StoreID] = struct{}{}
	regions, ok := f.regionsOfStore[item.StoreID]
	if !ok {
		regions = make(map[uint64]struct{})
		f.regionsOfStore[item.StoreID] = regions
	}
	regions[item.RegionID] = struct{}{}
}

func (f *hotPeerCache) removeItem(item *HotPeerStat) {
	if peers, ok := f.peersOfStore[item.StoreID]; ok {
		peers.Remove(item.RegionID)
	}
	if stores, ok := f.storesOfRegion[item.RegionID]; ok {
		delete(stores, item.StoreID)
	}
	if regions, ok := f.regionsOfStore[item.StoreID]; ok {
		delete(regions, item.RegionID)
	}
}

func coldItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	if newItem.AntiCount <= 0 {
		newItem.needDelete = true
	}
}

func hotItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree + 1
	newItem.AntiCount = hotRegionAntiCount
	if newItem.Kind == ReadFlow {
		newItem.AntiCount = hotRegionAntiCount * (RegionHeartBeatReportInterval / StoreHeartBeatReportInterval)
	}
}

func initItemDegree(item *HotPeerStat) {
	item.HotDegree = 1
	item.AntiCount = hotRegionAntiCount
	if item.Kind == ReadFlow {
		item.AntiCount = hotRegionAntiCount * (RegionHeartBeatReportInterval / StoreHeartBeatReportInterval)
	}
}

func inheritItemDegree(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree
	newItem.AntiCount = oldItem.AntiCount
}
