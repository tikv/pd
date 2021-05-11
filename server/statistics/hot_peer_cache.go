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
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	// WriteReportInterval indicates the interval between write interval
	WriteReportInterval = RegionHeartBeatReportInterval
	// ReadReportInterval indicates the interval between read stats report
	// TODO: use StoreHeartBeatReportInterval in future
	ReadReportInterval = RegionHeartBeatReportInterval

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var minHotThresholds = [RegionStatCount]float64{
	RegionWriteBytes: 1 * 1024,
	RegionWriteKeys:  32,
	RegionReadBytes:  8 * 1024,
	RegionReadKeys:   128,
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind               FlowKind
	peersOfStore       map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion     map[uint64]map[uint64]struct{} // regionID -> storeIDs
	inheritItem        map[uint64]*HotPeerStat        // regionID -> HotPeerStat
	topNTTL            time.Duration
	reportIntervalSecs int
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	c := &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
		inheritItem:    make(map[uint64]*HotPeerStat),
	}
	if kind == WriteFlow {
		c.reportIntervalSecs = WriteReportInterval
	} else {
		c.reportIntervalSecs = ReadReportInterval
	}
	c.topNTTL = 3 * time.Duration(c.reportIntervalSecs) * time.Second
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
		item.Log("region heartbeat update", log.Debug)
	}
}

// TODO: remove it in future
func (f *hotPeerCache) collectRegionMetrics(loads []float64, interval uint64) {
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
		}
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo) (ret []*HotPeerStat) {
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	{
		// TODO: collect metrics by peer stat
		deltaLoads := f.getFlowDeltaLoads(region)
		loads := make([]float64, len(deltaLoads))
		for i := range deltaLoads {
			loads[i] = deltaLoads[i] / float64(interval)
		}
		f.collectRegionMetrics(loads, interval)
	}
	regionID := region.GetID()
	storeIDs := f.getAllStoreIDs(region)
	for _, storeID := range storeIDs {
		peer := region.GetStorePeer(storeID)
		var item *HotPeerStat
		if peer != nil {
			peerInfo := core.NewPeerInfo(peer,
				region.GetBytesWritten(),
				region.GetKeysWritten(),
				region.GetBytesRead(),
				region.GetKeysRead())
			item = f.CheckPeerFlow(peerInfo, region, interval)
		} else {
			item = f.markExpiredItem(regionID, storeID)
		}
		if item != nil {
			ret = append(ret, item)
		}
	}
	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}
	log.Debug("region heartbeat info",
		zap.String("type", f.kind.String()),
		zap.Uint64("region", region.GetID()),
		zap.Uint64("leader", region.GetLeader().GetStoreId()),
		zap.Uint64s("peers", peers),
	)
	return ret
}

// CheckPeerFlow checks the flow information of a peer.
// Notice: CheckPeerFlow couldn't be used concurrently.
func (f *hotPeerCache) CheckPeerFlow(peer *core.PeerInfo, region *core.RegionInfo, interval uint64) *HotPeerStat {
	storeID := peer.GetStoreID()
	deltaLoads := f.getFlowDeltaLoads(peer)
	loads := make([]float64, len(deltaLoads))
	for i := range deltaLoads {
		loads[i] = deltaLoads[i] / float64(interval)
	}
	justTransferLeader := f.justTransferLeader(region)
	// transfer read leader or remove write peer
	isExpired := f.isPeerExpired(peer, region)
	oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
	if isExpired && oldItem != nil {
		f.putInheritItem(oldItem)
	}
	if !isExpired && Denoising && interval < HotRegionReportMinInterval {
		return nil
	}
	thresholds := f.calcHotThresholds(storeID)
	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}
	newItem := &HotPeerStat{
		StoreID:            storeID,
		RegionID:           region.GetID(),
		Kind:               f.kind,
		Loads:              loads,
		LastUpdateTime:     time.Now(),
		needDelete:         isExpired,
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

func (f *hotPeerCache) isPeerExpired(peer *core.PeerInfo, region *core.RegionInfo) bool {
	storeID := peer.GetStoreID()
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	//TODO: make readFlow isPeerExpired condition as same as the writeFlow
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
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
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, time.Duration(f.reportIntervalSecs)*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	if newItem.needDelete {
		return newItem
	}

	regionStats := f.kind.RegionStats()

	if oldItem == nil {
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
			newItem.HotDegree = 1
			newItem.AntiCount = hotRegionAntiCount
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

	newItem.rollingLoads = oldItem.rollingLoads

	// TODO: don't inherit hot degree after transfer leader when we report peer by store heartbeat.
	if newItem.justTransferLeader {
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		newItem.lastTransferLeaderTime = time.Now()
		return newItem
	}

	newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime

	for i, k := range regionStats {
		newItem.rollingLoads[i].Add(deltaLoads[k], interval)
	}

	isFull := newItem.rollingLoads[0].isFull() // The intervals of dims are the same, so it is only necessary to determine whether any of them
	if !isFull {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeer(oldItem, newItem.StoreID) {
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

func (f *hotPeerCache) markExpiredItem(regionID, storeID uint64) *HotPeerStat {
	item := f.getOldHotPeerStat(regionID, storeID)
	item.needDelete = true
	return item
}

func (f *hotPeerCache) getFlowDeltaLoads(stat core.FlowStat) []float64 {
	ret := make([]float64, RegionStatCount)
	for k := RegionStatKind(0); k < RegionStatCount; k++ {
		switch k {
		case RegionReadBytes:
			ret[k] = float64(stat.GetBytesRead())
		case RegionReadKeys:
			ret[k] = float64(stat.GetKeysRead())
		case RegionWriteBytes:
			ret[k] = float64(stat.GetBytesWritten())
		case RegionWriteKeys:
			ret[k] = float64(stat.GetKeysWritten())
		}
	}
	return ret
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
