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
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
	errStoreIsBlocked = func(storeID uint64) error {
		return errors.Errorf("store %v is blocked", storeID)
	}
	errRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	errRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
)

type storesInfo struct {
	stores map[uint64]*storeInfo
}

func newStoresInfo() *storesInfo {
	return &storesInfo{
		stores: make(map[uint64]*storeInfo),
	}
}

func (s *storesInfo) getStore(storeID uint64) *storeInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store.clone()
}

func (s *storesInfo) setStore(store *storeInfo) {
	s.stores[store.GetId()] = store
}

func (s *storesInfo) blockStore(storeID uint64) error {
	store, ok := s.stores[storeID]
	if !ok {
		return errStoreNotFound(storeID)
	}
	if store.isBlocked() {
		return errStoreIsBlocked(storeID)
	}
	store.block()
	return nil
}

func (s *storesInfo) unblockStore(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		log.Fatalf("store %d is unblocked, but it is not found", storeID)
	}
	store.unblock()
}

func (s *storesInfo) getStores() []*storeInfo {
	stores := make([]*storeInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.clone())
	}
	return stores
}

func (s *storesInfo) getMetaStores() []*metapb.Store {
	stores := make([]*metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, proto.Clone(store.Store).(*metapb.Store))
	}
	return stores
}

func (s *storesInfo) getStoreCount() int {
	return len(s.stores)
}

func (s *storesInfo) setLeaderCount(storeID uint64, leaderCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.status.LeaderCount = leaderCount
	}
}

func (s *storesInfo) setRegionCount(storeID uint64, regionCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.status.RegionCount = regionCount
	}
}

func (s *storesInfo) totalWrittenBytes() uint64 {
	var totalWrittenBytes uint64
	for _, s := range s.stores {
		if s.isUp() {
			totalWrittenBytes += s.status.GetBytesWritten()
		}
	}
	return totalWrittenBytes
}

// regionMap wraps a map[uint64]*RegionInfo and supports randomly pick a region.
type regionMap struct {
	m   map[uint64]*regionEntry
	ids []uint64
}

type regionEntry struct {
	*RegionInfo
	pos int
}

func newRegionMap() *regionMap {
	return &regionMap{
		m: make(map[uint64]*regionEntry),
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if entry, ok := rm.m[id]; ok {
		return entry.RegionInfo
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetId()]; ok {
		old.RegionInfo = region
		return
	}
	rm.m[region.GetId()] = &regionEntry{
		RegionInfo: region,
		pos:        len(rm.ids),
	}
	rm.ids = append(rm.ids, region.GetId())
}

func (rm *regionMap) RandomRegion() *RegionInfo {
	if rm.Len() == 0 {
		return nil
	}
	return rm.Get(rm.ids[rand.Intn(rm.Len())])
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		len := rm.Len()
		last := rm.m[rm.ids[len-1]]
		last.pos = old.pos
		rm.ids[last.pos] = last.GetId()
		delete(rm.m, id)
		rm.ids = rm.ids[:len-1]
	}
}

type regionsInfo struct {
	tree      *regionTree
	regions   *regionMap            // regionID -> regionInfo
	leaders   map[uint64]*regionMap // storeID -> regionID -> regionInfo
	followers map[uint64]*regionMap // storeID -> regionID -> regionInfo
}

func newRegionsInfo() *regionsInfo {
	return &regionsInfo{
		tree:      newRegionTree(),
		regions:   newRegionMap(),
		leaders:   make(map[uint64]*regionMap),
		followers: make(map[uint64]*regionMap),
	}
}

func (r *regionsInfo) getRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region.clone()
}

func (r *regionsInfo) setRegion(region *RegionInfo) {
	if origin := r.regions.Get(region.GetId()); origin != nil {
		r.removeRegion(origin)
	}
	r.addRegion(region)
}

func (r *regionsInfo) addRegion(region *RegionInfo) {
	// Add to tree and regions.
	r.tree.update(region.Region)
	r.regions.Put(region)

	if region.Leader == nil {
		return
	}

	// Add to leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.Leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionMap()
				r.leaders[storeID] = store
			}
			store.Put(region)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionMap()
				r.followers[storeID] = store
			}
			store.Put(region)
		}
	}
}

func (r *regionsInfo) removeRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region.Region)
	r.regions.Delete(region.GetId())

	// Remove from leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].Delete(region.GetId())
		r.followers[storeID].Delete(region.GetId())
	}
}

func (r *regionsInfo) searchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.getRegion(region.GetId())
}

func (r *regionsInfo) getRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region.clone())
	}
	return regions
}

func (r *regionsInfo) getMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.Region).(*metapb.Region))
	}
	return regions
}

func (r *regionsInfo) getRegionCount() int {
	return r.regions.Len()
}

func (r *regionsInfo) getStoreRegionCount(storeID uint64) int {
	return r.getStoreLeaderCount(storeID) + r.getStoreFollowerCount(storeID)
}

func (r *regionsInfo) getStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].Len()
}

func (r *regionsInfo) getStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].Len()
}

func (r *regionsInfo) randRegion() *RegionInfo {
	return randRegion(r.regions)
}

func (r *regionsInfo) randLeaderRegion(storeID uint64) *RegionInfo {
	return randRegion(r.leaders[storeID])
}

func (r *regionsInfo) randFollowerRegion(storeID uint64) *RegionInfo {
	return randRegion(r.followers[storeID])
}

const randomRegionMaxRetry = 10

func randRegion(regions *regionMap) *RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion()
		if region == nil {
			return nil
		}
		if len(region.DownPeers) == 0 && len(region.PendingPeers) == 0 {
			return region.clone()
		}
	}
	return nil
}

type clusterInfo struct {
	sync.RWMutex

	id      IDAllocator
	kv      *kv
	meta    *metapb.Cluster
	stores  *storesInfo
	regions *regionsInfo

	activeRegions   int
	writeStatistics *lruCache
}

func newClusterInfo(id IDAllocator) *clusterInfo {
	return &clusterInfo{
		id:              id,
		stores:          newStoresInfo(),
		regions:         newRegionsInfo(),
		writeStatistics: newLRUCache(writeStatLRUMaxLen),
	}
}

// Return nil if cluster is not bootstrapped.
func loadClusterInfo(id IDAllocator, kv *kv) (*clusterInfo, error) {
	c := newClusterInfo(id)
	c.kv = kv

	c.meta = &metapb.Cluster{}
	ok, err := kv.loadMeta(c.meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := kv.loadStores(c.stores, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v stores cost %v", c.stores.getStoreCount(), time.Since(start))

	start = time.Now()
	if err := kv.loadRegions(c.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v regions cost %v", c.regions.getRegionCount(), time.Since(start))

	return c, nil
}

func (c *clusterInfo) allocID() (uint64, error) {
	return c.id.Alloc()
}

func (c *clusterInfo) allocPeer(storeID uint64) (*metapb.Peer, error) {
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
		if err := c.kv.saveMeta(meta); err != nil {
			return errors.Trace(err)
		}
	}
	c.meta = meta
	return nil
}

func (c *clusterInfo) getStore(storeID uint64) *storeInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStore(storeID)
}

func (c *clusterInfo) putStore(store *storeInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(store.clone())
}

func (c *clusterInfo) putStoreLocked(store *storeInfo) error {
	if c.kv != nil {
		if err := c.kv.saveStore(store.Store); err != nil {
			return errors.Trace(err)
		}
	}
	c.stores.setStore(store)
	return nil
}

func (c *clusterInfo) blockStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	return errors.Trace(c.stores.blockStore(storeID))
}

func (c *clusterInfo) unblockStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()
	c.stores.unblockStore(storeID)
}

func (c *clusterInfo) getStores() []*storeInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStores()
}

func (c *clusterInfo) getMetaStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getMetaStores()
}

func (c *clusterInfo) getStoreCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStoreCount()
}

func (c *clusterInfo) getStoresWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	res := make(map[uint64]uint64)
	for _, s := range c.stores.stores {
		res[s.GetId()] = s.status.GetBytesWritten()
	}
	return res
}

func (c *clusterInfo) getRegion(regionID uint64) *RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegion(regionID)
}

// updateWriteStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (c *clusterInfo) updateWriteStatCache(region *RegionInfo, hotRegionThreshold uint64) {
	var v *RegionStat
	key := region.GetId()
	value, isExist := c.writeStatistics.peek(key)
	newItem := &RegionStat{
		RegionID:       region.GetId(),
		WrittenBytes:   region.WrittenBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		version:        region.GetRegionEpoch().GetVersion(),
		antiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.WrittenBytes < hotRegionThreshold {
		if !isExist {
			return
		}
		if v.antiCount <= 0 {
			c.writeStatistics.remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.antiCount = v.antiCount - 1
		newItem.WrittenBytes = v.WrittenBytes
	}
	c.writeStatistics.add(key, newItem)
}

func (c *clusterInfo) searchRegion(regionKey []byte) *RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.searchRegion(regionKey)
}

func (c *clusterInfo) putRegion(region *RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region.clone())
}

func (c *clusterInfo) putRegionLocked(region *RegionInfo) error {
	if c.kv != nil {
		if err := c.kv.saveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.setRegion(region)
	return nil
}

func (c *clusterInfo) getRegions() []*RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegions()
}

func (c *clusterInfo) randomRegion() *RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randRegion()
}

func (c *clusterInfo) getMetaRegions() []*metapb.Region {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getMetaRegions()
}

func (c *clusterInfo) getRegionCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegionCount()
}

func (c *clusterInfo) getStoreRegionCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getStoreRegionCount(storeID)
}

func (c *clusterInfo) getStoreLeaderCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getStoreLeaderCount(storeID)
}

func (c *clusterInfo) randLeaderRegion(storeID uint64) *RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randLeaderRegion(storeID)
}

func (c *clusterInfo) randFollowerRegion(storeID uint64) *RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randFollowerRegion(storeID)
}

func (c *clusterInfo) getRegionStores(region *RegionInfo) []*storeInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*storeInfo
	for id := range region.GetStoreIds() {
		if store := c.stores.getStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *clusterInfo) getLeaderStore(region *RegionInfo) *storeInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStore(region.Leader.GetStoreId())
}

func (c *clusterInfo) getFollowerStores(region *RegionInfo) []*storeInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*storeInfo
	for id := range region.GetFollowers() {
		if store := c.stores.getStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// isPrepared if the cluster information is collected
func (c *clusterInfo) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return float64(c.regions.regions.Len())*collectFactor <= float64(c.activeRegions)
}

// handleStoreHeartbeat updates the store status.
func (c *clusterInfo) handleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.stores.getStore(storeID)
	if store == nil {
		return errors.Trace(errStoreNotFound(storeID))
	}
	store.status.StoreStats = proto.Clone(stats).(*pdpb.StoreStats)
	store.status.LastHeartbeatTS = time.Now()

	c.stores.setStore(store)
	return nil
}

func (c *clusterInfo) updateStoreStatus(id uint64) {
	c.stores.setLeaderCount(id, c.regions.getStoreLeaderCount(id))
	c.stores.setRegionCount(id, c.regions.getStoreRegionCount(id))
}

// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *RegionInfo) error {
	region = region.clone()
	c.RLock()
	origin := c.regions.getRegion(region.GetId())
	c.RUnlock()

	// Save to KV if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		log.Infof("[region %d] Insert new region {%v}", region.GetId(), region)
		saveKV, saveCache = true, true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		// Region meta is stale, return an error.
		if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
			return errors.Trace(errRegionIsStale(region.Region, origin.Region))
		}
		if r.GetVersion() > o.GetVersion() {
			log.Infof("[region %d] %s, Version changed from {%d} to {%d}", region.GetId(), diffRegionKeyInfo(origin, region), o.GetVersion(), r.GetVersion())
			saveKV, saveCache = true, true
		}
		if r.GetConfVer() > o.GetConfVer() {
			log.Infof("[region %d] %s, ConfVer changed from {%d} to {%d}", region.GetId(), diffRegionPeersInfo(origin, region), o.GetConfVer(), r.GetConfVer())
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
		if err := c.kv.saveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}

	c.Lock()
	defer c.Unlock()

	if isNew {
		c.activeRegions++
	}

	if saveCache {
		c.regions.setRegion(region)

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

	c.updateWriteStatus(region)

	return nil
}

func (c *clusterInfo) updateWriteStatus(region *RegionInfo) {
	var WrittenBytesPerSec uint64
	v, isExist := c.writeStatistics.peek(region.GetId())
	if isExist {
		interval := time.Now().Sub(v.(*RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(regionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is writeStatLRUMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(writeStatLRUMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(c.stores.totalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotRegionMinWriteRate {
		hotRegionThreshold = hotRegionMinWriteRate
	}
	c.updateWriteStatCache(region, hotRegionThreshold)
}
