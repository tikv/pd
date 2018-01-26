// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
)

var (
	errRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	errRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
)

type clusterInfo struct {
	sync.RWMutex

	id              core.IDAllocator
	kv              *core.KV
	meta            *metapb.Cluster
	stores          *core.StoresInfo
	regions         *core.RegionsInfo
	activeRegions   int
	writeStatistics cache.Cache
	readStatistics  cache.Cache
}

func newClusterInfo(id core.IDAllocator) *clusterInfo {
	return &clusterInfo{
		id:              id,
		stores:          core.NewStoresInfo(),
		regions:         core.NewRegionsInfo(),
		writeStatistics: cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
		readStatistics:  cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
	}
}

// Return nil if cluster is not bootstrapped.
func loadClusterInfo(id core.IDAllocator, kv *core.KV) (*clusterInfo, error) {
	c := newClusterInfo(id)
	c.kv = kv

	c.meta = &metapb.Cluster{}
	ok, err := kv.LoadMeta(c.meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := kv.LoadStores(c.stores, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v stores cost %v", c.stores.GetStoreCount(), time.Since(start))

	start = time.Now()
	if err := kv.LoadRegions(c.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v regions cost %v", c.regions.GetRegionCount(), time.Since(start))

	return c, nil
}

func (c *clusterInfo) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (c *clusterInfo) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Errorf("failed to alloc peer: %v", err)
		return nil, errors.Trace(err)
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (c *clusterInfo) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *clusterInfo) getMeta() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *clusterInfo) putMeta(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

func (c *clusterInfo) putMetaLocked(meta *metapb.Cluster) error {
	if c.kv != nil {
		if err := c.kv.SaveMeta(meta); err != nil {
			return errors.Trace(err)
		}
	}
	c.meta = meta
	return nil
}

// GetStore searches for a store by ID.
func (c *clusterInfo) GetStore(storeID uint64) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStore(storeID)
}

func (c *clusterInfo) putStore(store *core.StoreInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(store.Clone())
}

func (c *clusterInfo) putStoreLocked(store *core.StoreInfo) error {
	if c.kv != nil {
		if err := c.kv.SaveStore(store.Store); err != nil {
			return errors.Trace(err)
		}
	}
	c.stores.SetStore(store)
	return nil
}

// BlockStore stops balancer from selecting the store.
func (c *clusterInfo) BlockStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	return errors.Trace(c.stores.BlockStore(storeID))
}

// UnblockStore allows balancer to select the store.
func (c *clusterInfo) UnblockStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()
	c.stores.UnblockStore(storeID)
}

// GetStores returns all stores in the cluster.
func (c *clusterInfo) GetStores() []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStores()
}

func (c *clusterInfo) getMetaStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetMetaStores()
}

func (c *clusterInfo) getStoreCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStoreCount()
}

func (c *clusterInfo) getStoresWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStoresWriteStat()
}

func (c *clusterInfo) getStoresReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStoresReadStat()
}

// GetRegions searches for a region by ID.
func (c *clusterInfo) GetRegion(regionID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetRegion(regionID)
}

func (c *clusterInfo) isNeedUpdateWriteStatCache(region *core.RegionInfo, hotRegionThreshold uint64) (bool, *core.RegionStat) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := c.writeStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.WrittenBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.WrittenBytes < hotRegionThreshold {
		if !isExist {
			return false, nil
		}
		if v.AntiCount <= 0 {
			return true, nil
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	return true, newItem
}

func (c *clusterInfo) isNeedUpdateReadStatCache(region *core.RegionInfo, hotRegionThreshold uint64) (bool, *core.RegionStat) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := c.readStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.ReadBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.ReadBytes < hotRegionThreshold {
		if !isExist {
			return false, nil
		}
		if v.AntiCount <= 0 {
			return true, nil
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	return true, newItem
}

// RegionWriteStats returns hot region's write stats.
func (c *clusterInfo) RegionWriteStats() []*core.RegionStat {
	elements := c.writeStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

// RegionReadStats returns hot region's read stats.
func (c *clusterInfo) RegionReadStats() []*core.RegionStat {
	elements := c.readStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

// IsRegionHot checks if a region is in hot state.
func (c *clusterInfo) IsRegionHot(id uint64) bool {
	c.RLock()
	defer c.RUnlock()
	if stat, ok := c.writeStatistics.Peek(id); ok {
		return stat.(*core.RegionStat).HotDegree >= hotRegionLowThreshold
	}
	return false
}

func (c *clusterInfo) searchRegion(regionKey []byte) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.SearchRegion(regionKey)
}

func (c *clusterInfo) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region.Clone())
}

func (c *clusterInfo) putRegionLocked(region *core.RegionInfo) error {
	if c.kv != nil {
		if err := c.kv.SaveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.SetRegion(region)
	return nil
}

func (c *clusterInfo) getRegions() []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetRegions()
}

func (c *clusterInfo) randomRegion() *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.RandRegion()
}

func (c *clusterInfo) getMetaRegions() []*metapb.Region {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetMetaRegions()
}

func (c *clusterInfo) getRegionCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetRegionCount()
}

func (c *clusterInfo) getStoreRegionCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetStoreRegionCount(storeID)
}

func (c *clusterInfo) getStoreLeaderCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.GetStoreLeaderCount(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *clusterInfo) RandLeaderRegion(storeID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.RandLeaderRegion(storeID)
}

// RandFolowerRegion returns a random region that has a follower on the store.
func (c *clusterInfo) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.RandFollowerRegion(storeID)
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *clusterInfo) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
	for id := range region.GetStoreIds() {
		if store := c.stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetRegionStores returns all stores that contains the region's leader peer.
func (c *clusterInfo) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.GetStore(region.Leader.GetStoreId())
}

// GetRegionStores returns all stores that contains the region's follower peer.
func (c *clusterInfo) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := c.stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// isPrepared if the cluster information is collected
func (c *clusterInfo) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return float64(c.regions.Length())*collectFactor <= float64(c.activeRegions)
}

// handleStoreHeartbeat updates the store status.
func (c *clusterInfo) handleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.stores.GetStore(storeID)
	if store == nil {
		return errors.Trace(core.ErrStoreNotFound(storeID))
	}
	store.Stats = proto.Clone(stats).(*pdpb.StoreStats)
	store.LastHeartbeatTS = time.Now()

	c.stores.SetStore(store)
	return nil
}

func (c *clusterInfo) updateStoreStatus(id uint64) {
	c.stores.SetLeaderCount(id, c.regions.GetStoreLeaderCount(id))
	c.stores.SetRegionCount(id, c.regions.GetStoreRegionCount(id))
}

// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *core.RegionInfo) error {
	region = region.Clone()
	c.RLock()
	origin := c.regions.GetRegion(region.GetId())
	isWriteUpdate, writeItem := c.checkWriteStatus(region)
	isReadUpdate, readItem := c.checkReadStatus(region)
	c.RUnlock()

	// Save to KV if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		log.Infof("[region %d] Insert new region {%v}", region.GetId(), region)
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		// Region meta is stale, return an error.
		if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
			return errors.Trace(errRegionIsStale(region.Region, origin.Region))
		}
		if r.GetVersion() > o.GetVersion() {
			log.Infof("[region %d] %s, Version changed from {%d} to {%d}", region.GetId(), core.DiffRegionKeyInfo(origin, region), o.GetVersion(), r.GetVersion())
			saveKV, saveCache = true, true
		}
		if r.GetConfVer() > o.GetConfVer() {
			log.Infof("[region %d] %s, ConfVer changed from {%d} to {%d}", region.GetId(), core.DiffRegionPeersInfo(origin, region), o.GetConfVer(), r.GetConfVer())
			saveKV, saveCache = true, true
		}
		if region.Leader.GetId() != origin.Leader.GetId() {
			log.Infof("[region %d] Leader changed from {%v} to {%v}", region.GetId(), origin.GetPeer(origin.Leader.GetId()), region.GetPeer(region.Leader.GetId()))
			if origin.Leader.GetId() == 0 {
				isNew = true
			}
			saveCache = true
		}
		if len(region.DownPeers) > 0 || len(region.PendingPeers) > 0 {
			saveCache = true
		}
		if len(origin.DownPeers) > 0 || len(origin.PendingPeers) > 0 {
			saveCache = true
		}
	}

	if saveKV && c.kv != nil {
		if err := c.kv.SaveRegion(region.Region); err != nil {
			// Not successfully saved to kv is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			log.Errorf("[region %d] fail to save region %v: %v", region.GetId(), region, err)
		}
	}
	if !isWriteUpdate && !isReadUpdate && !saveCache && !isNew {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	if isNew {
		c.activeRegions++
	}

	if saveCache {
		c.regions.SetRegion(region)

		// Update related stores.
		if origin != nil {
			for _, p := range origin.Peers {
				c.updateStoreStatus(p.GetStoreId())
			}
		}
		for _, p := range region.Peers {
			c.updateStoreStatus(p.GetStoreId())
		}
	}
	key := region.GetId()
	if isWriteUpdate {
		if writeItem == nil {
			c.writeStatistics.Remove(key)
		} else {
			c.writeStatistics.Put(key, writeItem)
		}
	}
	if isReadUpdate {
		if readItem == nil {
			c.readStatistics.Remove(key)
		} else {
			c.readStatistics.Put(key, readItem)
		}
	}
	return nil
}

func (c *clusterInfo) checkWriteStatus(region *core.RegionInfo) (bool, *core.RegionStat) {
	var WrittenBytesPerSec uint64
	v, isExist := c.writeStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return false, nil
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(regionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(c.stores.TotalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	return c.isNeedUpdateWriteStatCache(region, hotRegionThreshold)
}

func (c *clusterInfo) checkReadStatus(region *core.RegionInfo) (bool, *core.RegionStat) {
	var ReadBytesPerSec uint64
	v, isExist := c.readStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Now().Sub(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return false, nil
		}
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / interval)
	} else {
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / float64(regionHeartBeatReportInterval))
	}
	region.ReadBytes = ReadBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is statLRUMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	divisor := float64(statCacheMaxLen) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(c.stores.TotalReadBytes()) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	return c.isNeedUpdateReadStatCache(region, hotRegionThreshold)
}
