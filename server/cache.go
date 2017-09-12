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
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
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
	stores map[uint64]*core.StoreInfo
}

func newStoresInfo() *storesInfo {
	return &storesInfo{
		stores: make(map[uint64]*core.StoreInfo),
	}
}

func (s *storesInfo) getStore(storeID uint64) *core.StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store.Clone()
}

func (s *storesInfo) setStore(store *core.StoreInfo) {
	s.stores[store.GetId()] = store
}

func (s *storesInfo) blockStore(storeID uint64) error {
	store, ok := s.stores[storeID]
	if !ok {
		return errStoreNotFound(storeID)
	}
	if store.IsBlocked() {
		return errStoreIsBlocked(storeID)
	}
	store.Block()
	return nil
}

func (s *storesInfo) unblockStore(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		log.Fatalf("store %d is unblocked, but it is not found", storeID)
	}
	store.Unblock()
}

func (s *storesInfo) getStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.Clone())
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
		store.LeaderCount = leaderCount
	}
}

func (s *storesInfo) setRegionCount(storeID uint64, regionCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.RegionCount = regionCount
	}
}

func (s *storesInfo) totalWrittenBytes() uint64 {
	var totalWrittenBytes uint64
	for _, s := range s.stores {
		if s.IsUp() {
			totalWrittenBytes += s.Stats.GetBytesWritten()
		}
	}
	return totalWrittenBytes
}

func (s *storesInfo) totalReadBytes() uint64 {
	var totalReadBytes uint64
	for _, s := range s.stores {
		if s.IsUp() {
			totalReadBytes += s.Stats.GetBytesRead()
		}
	}
	return totalReadBytes
}

// regionMap wraps a map[uint64]*core.RegionInfo and supports randomly pick a region.
type regionMap struct {
	m   map[uint64]*regionEntry
	ids []uint64
}

type regionEntry struct {
	*core.RegionInfo
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

func (rm *regionMap) Get(id uint64) *core.RegionInfo {
	if rm == nil {
		return nil
	}
	if entry, ok := rm.m[id]; ok {
		return entry.RegionInfo
	}
	return nil
}

func (rm *regionMap) Put(region *core.RegionInfo) {
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

func (rm *regionMap) RandomRegion() *core.RegionInfo {
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

func (r *regionsInfo) getRegion(regionID uint64) *core.RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region.Clone()
}

func (r *regionsInfo) setRegion(region *core.RegionInfo) {
	if origin := r.regions.Get(region.GetId()); origin != nil {
		r.removeRegion(origin)
	}
	r.addRegion(region)
}

func (r *regionsInfo) addRegion(region *core.RegionInfo) {
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

func (r *regionsInfo) removeRegion(region *core.RegionInfo) {
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

func (r *regionsInfo) searchRegion(regionKey []byte) *core.RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.getRegion(region.GetId())
}

func (r *regionsInfo) getRegions() []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region.Clone())
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

func (r *regionsInfo) randRegion() *core.RegionInfo {
	return randRegion(r.regions)
}

func (r *regionsInfo) randLeaderRegion(storeID uint64) *core.RegionInfo {
	return randRegion(r.leaders[storeID])
}

func (r *regionsInfo) randFollowerRegion(storeID uint64) *core.RegionInfo {
	return randRegion(r.followers[storeID])
}

const randomRegionMaxRetry = 10

func randRegion(regions *regionMap) *core.RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion()
		if region == nil {
			return nil
		}
		if len(region.DownPeers) == 0 && len(region.PendingPeers) == 0 {
			return region.Clone()
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
	writeStatistics cache.Cache
	readStatistics  cache.Cache
}

func newClusterInfo(id IDAllocator) *clusterInfo {
	return &clusterInfo{
		id:              id,
		stores:          newStoresInfo(),
		regions:         newRegionsInfo(),
		readStatistics:  cache.NewDefaultCache(statCacheMaxLen),
		writeStatistics: cache.NewDefaultCache(statCacheMaxLen),
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
		if err := c.kv.saveMeta(meta); err != nil {
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
	return c.stores.getStore(storeID)
}

func (c *clusterInfo) putStore(store *core.StoreInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(store.Clone())
}

func (c *clusterInfo) putStoreLocked(store *core.StoreInfo) error {
	if c.kv != nil {
		if err := c.kv.saveStore(store.Store); err != nil {
			return errors.Trace(err)
		}
	}
	c.stores.setStore(store)
	return nil
}

// BlockStore stops balancer from selecting the store.
func (c *clusterInfo) BlockStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	return errors.Trace(c.stores.blockStore(storeID))
}

// UnblockStore allows balancer to select the store.
func (c *clusterInfo) UnblockStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()
	c.stores.unblockStore(storeID)
}

// GetStores returns all stores in the cluster.
func (c *clusterInfo) GetStores() []*core.StoreInfo {
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
		res[s.GetId()] = s.Stats.GetBytesWritten()
	}
	return res
}

func (c *clusterInfo) getStoresReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	res := make(map[uint64]uint64)
	for _, s := range c.stores.stores {
		res[s.GetId()] = s.Stats.GetBytesRead()
	}
	return res
}

// GetRegions searches for a region by ID.
func (c *clusterInfo) GetRegion(regionID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegion(regionID)
}

// updateWriteStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (c *clusterInfo) updateWriteStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
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
			return
		}
		if v.AntiCount <= 0 {
			c.writeStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	c.writeStatistics.Put(key, newItem)
}

// updateReadStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (c *clusterInfo) updateReadStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
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
			return
		}
		if v.AntiCount <= 0 {
			c.readStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	c.readStatistics.Put(key, newItem)
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
	return c.regions.searchRegion(regionKey)
}

func (c *clusterInfo) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region.Clone())
}

func (c *clusterInfo) putRegionLocked(region *core.RegionInfo) error {
	if c.kv != nil {
		if err := c.kv.saveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.setRegion(region)
	return nil
}

func (c *clusterInfo) getRegions() []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegions()
}

func (c *clusterInfo) randomRegion() *core.RegionInfo {
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

// RandLeaderRegion returns a random region that has leader on the store.
func (c *clusterInfo) RandLeaderRegion(storeID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randLeaderRegion(storeID)
}

// RandFolowerRegion returns a random region that has a follower on the store.
func (c *clusterInfo) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randFollowerRegion(storeID)
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *clusterInfo) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
	for id := range region.GetStoreIds() {
		if store := c.stores.getStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetRegionStores returns all stores that contains the region's leader peer.
func (c *clusterInfo) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStore(region.Leader.GetStoreId())
}

// GetRegionStores returns all stores that contains the region's follower peer.
func (c *clusterInfo) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
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
	store.Stats = proto.Clone(stats).(*pdpb.StoreStats)
	store.LastHeartbeatTS = time.Now()

	c.stores.setStore(store)
	return nil
}

func (c *clusterInfo) updateStoreStatus(id uint64) {
	c.stores.setLeaderCount(id, c.regions.getStoreLeaderCount(id))
	c.stores.setRegionCount(id, c.regions.getStoreRegionCount(id))
}

// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *core.RegionInfo) error {
	region = region.Clone()
	c.RLock()
	origin := c.regions.getRegion(region.GetId())
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
	c.updateReadStatus(region)

	return nil
}

func (c *clusterInfo) updateWriteStatus(region *core.RegionInfo) {
	var WrittenBytesPerSec uint64
	v, isExist := c.writeStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(regionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is writeStatCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(c.stores.totalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotRegionMinWriteRate {
		hotRegionThreshold = hotRegionMinWriteRate
	}
	c.updateWriteStatCache(region, hotRegionThreshold)
}

func (c *clusterInfo) updateReadStatus(region *core.RegionInfo) {
	var ReadBytesPerSec uint64
	v, isExist := c.readStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Now().Sub(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / interval)
	} else {
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / float64(regionHeartBeatReportInterval))
	}
	region.ReadBytes = ReadBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is writeStatLRUMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	divisor := float64(statCacheMaxLen) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(c.stores.totalReadBytes()) / divisor)

	if hotRegionThreshold < hotRegionMinWriteRate {
		hotRegionThreshold = hotRegionMinWriteRate
	}
	c.updateReadStatCache(region, hotRegionThreshold)
}
