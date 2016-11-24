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
	"bytes"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
	errRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	errRegionIsMerging = func(regionID uint64) error {
		return errors.Errorf("region %v is merging", regionID)
	}
	errRegionIsShutdown = func(regionID uint64) error {
		return errors.Errorf("region %v is shutdown", regionID)
	}
	errRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
)

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Trace(errRegionIsStale(region, origin))
	}

	return nil
}

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

type regionsInfo struct {
	*mergeInfo
	tree      *regionTree
	regions   map[uint64]*regionInfo
	leaders   map[uint64]map[uint64]*regionInfo
	followers map[uint64]map[uint64]*regionInfo
}

func newRegionsInfo() *regionsInfo {
	return &regionsInfo{
		mergeInfo: newMergeInfo(),
		tree:      newRegionTree(),
		regions:   make(map[uint64]*regionInfo),
		leaders:   make(map[uint64]map[uint64]*regionInfo),
		followers: make(map[uint64]map[uint64]*regionInfo),
	}
}

func (r *regionsInfo) getRegion(regionID uint64) *regionInfo {
	region, ok := r.regions[regionID]
	if !ok {
		return nil
	}
	return region.clone()
}

func (r *regionsInfo) setRegion(region *regionInfo) {
	r.removeRegion(region.GetId())
	r.addRegion(region)
}

func (r *regionsInfo) addRegion(region *regionInfo) {
	// Add to tree and regions.
	r.tree.update(region.Region)
	r.regions[region.GetId()] = region

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
				store = make(map[uint64]*regionInfo)
				r.leaders[storeID] = store
			}
			store[region.GetId()] = region
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = make(map[uint64]*regionInfo)
				r.followers[storeID] = store
			}
			store[region.GetId()] = region
		}
	}
}

func (r *regionsInfo) removeRegion(regionID uint64) {
	region, ok := r.regions[regionID]
	if !ok {
		return
	}

	// Remove from tree and regions.
	r.tree.remove(region.Region)
	delete(r.regions, region.GetId())

	// Remove from leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		delete(r.leaders[storeID], region.GetId())
		delete(r.followers[storeID], region.GetId())
	}
}

func (r *regionsInfo) searchRegion(regionKey []byte) *regionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.getRegion(region.GetId())
}

func (r *regionsInfo) getRegions() []*regionInfo {
	regions := make([]*regionInfo, 0, len(r.regions))
	for _, region := range r.regions {
		regions = append(regions, region.clone())
	}
	return regions
}

func (r *regionsInfo) getMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, len(r.regions))
	for _, region := range r.regions {
		regions = append(regions, proto.Clone(region.Region).(*metapb.Region))
	}
	return regions
}

func (r *regionsInfo) getRegionCount() int {
	return len(r.regions)
}

func (r *regionsInfo) getStoreRegionCount(storeID uint64) int {
	return r.getStoreLeaderCount(storeID) + r.getStoreFollowerCount(storeID)
}

func (r *regionsInfo) getStoreLeaderCount(storeID uint64) int {
	return len(r.leaders[storeID])
}

func (r *regionsInfo) getStoreFollowerCount(storeID uint64) int {
	return len(r.followers[storeID])
}

func (r *regionsInfo) randLeaderRegion(storeID uint64) *regionInfo {
	for _, region := range r.leaders[storeID] {
		if region.Leader == nil {
			log.Fatalf("rand leader region without leader: store %v region %v", storeID, region)
		}
		if r.isRegionMerging(region.GetId()) {
			continue
		}
		return region.clone()
	}
	return nil
}

func (r *regionsInfo) randFollowerRegion(storeID uint64) *regionInfo {
	for _, region := range r.followers[storeID] {
		if region.Leader == nil {
			log.Fatalf("rand follower region without leader: store %v region %v", storeID, region)
		}
		if r.isRegionMerging(region.GetId()) {
			continue
		}
		return region.clone()
	}
	return nil
}

func (r *regionsInfo) getMergingRegions(regionID uint64) (*regionInfo, *regionInfo) {
	fromRegionID, intoRegionID, ok := r.mergeInfo.getMergingRegions(regionID)
	if !ok {
		return nil, nil
	}
	return r.regions[fromRegionID], r.regions[intoRegionID]
}

func (r *regionsInfo) commitRegionMerge(fromRegionID uint64, intoRegion *regionInfo) {
	r.markRegionShutdown(fromRegionID)
	r.clearRegionMerge(fromRegionID)
	r.removeRegion(fromRegionID)
	r.setRegion(intoRegion)
}

type clusterInfo struct {
	sync.RWMutex

	id      IDAllocator
	kv      *kv
	meta    *metapb.Cluster
	stores  *storesInfo
	regions *regionsInfo
}

func newClusterInfo(id IDAllocator) *clusterInfo {
	return &clusterInfo{
		id:      id,
		stores:  newStoresInfo(),
		regions: newRegionsInfo(),
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

	start = time.Now()
	if err := kv.loadMergeRegions(c.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v merge regions cost %v", c.regions.getMergingRegionCount(), time.Since(start))

	start = time.Now()
	if err := kv.loadShutdownRegions(c.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v shutdown regions cost %v", c.regions.getShutdownRegionCount(), time.Since(start))

	return c, nil
}

func (c *clusterInfo) allocID() (uint64, error) {
	return c.id.Alloc()
}

func (c *clusterInfo) allocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		return nil, errors.Trace(err)
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
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

func (c *clusterInfo) getRegion(regionID uint64) *regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegion(regionID)
}

func (c *clusterInfo) searchRegion(regionKey []byte) *regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.searchRegion(regionKey)
}

func (c *clusterInfo) putRegion(region *regionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region.clone())
}

func (c *clusterInfo) putRegionLocked(region *regionInfo) error {
	if c.kv != nil {
		if err := c.kv.saveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.setRegion(region)
	return nil
}

func (c *clusterInfo) getRegions() []*regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegions()
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

func (c *clusterInfo) randLeaderRegion(storeID uint64) *regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randLeaderRegion(storeID)
}

func (c *clusterInfo) randFollowerRegion(storeID uint64) *regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.randFollowerRegion(storeID)
}

func (c *clusterInfo) isRegionMerging(regionID uint64) bool {
	c.RLock()
	defer c.RUnlock()
	return c.regions.isRegionMerging(regionID)
}

func (c *clusterInfo) isRegionShutdown(regionID uint64) bool {
	c.RLock()
	defer c.RUnlock()
	return c.regions.isRegionShutdown(regionID)
}

func (c *clusterInfo) getMergingRegions(regionID uint64) (*regionInfo, *regionInfo) {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getMergingRegions(regionID)
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

	store.stats.StoreStats = proto.Clone(stats).(*pdpb.StoreStats)
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.TotalRegionCount = c.regions.getRegionCount()
	store.stats.LeaderRegionCount = c.regions.getStoreLeaderCount(storeID)

	c.stores.setStore(store)
	return nil
}

// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *regionInfo) error {
	c.Lock()
	defer c.Unlock()

	if c.regions.isRegionShutdown(region.GetId()) {
		return errors.Trace(errRegionIsShutdown(region.GetId()))
	}

	region = region.clone()
	origin := c.regions.getRegion(region.GetId())

	// Region does not exist, add it.
	if origin == nil {
		return c.putRegionLocked(region)
	}

	r := region.GetRegionEpoch()
	o := origin.GetRegionEpoch()

	// Region meta is stale, return an error.
	if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
		return errors.Trace(errRegionIsStale(region.Region, origin.Region))
	}

	// Region version is updated, we need to commit or abort region merge.
	if r.GetVersion() > o.GetVersion() && c.regions.isRegionMerging(region.GetId()) {
		return c.handleRegionMergeHeartbeat(region)
	}

	// Region meta is updated, update kv and cache.
	if r.GetVersion() > o.GetVersion() || r.GetConfVer() > o.GetConfVer() {
		return c.putRegionLocked(region)
	}

	// Region meta is the same, update cache only.
	c.regions.setRegion(region)
	return nil
}

func (c *clusterInfo) handleRegionSplit(region *metapb.Region) (uint64, []uint64, error) {
	c.RLock()
	defer c.RUnlock()

	_, err := c.checkRegionAvailable(region)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	newRegionID, err := c.allocID()
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	newPeerIDs := make([]uint64, len(region.GetPeers()))
	for i := 0; i < len(newPeerIDs); i++ {
		if newPeerIDs[i], err = c.allocID(); err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	return newRegionID, newPeerIDs, nil
}

func (c *clusterInfo) handleRegionMerge(region *metapb.Region) (*metapb.Region, error) {
	c.Lock()
	defer c.Unlock()

	// Check from region.
	fromRegion, err := c.checkRegionAvailable(region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Check into region.
	intoRegion := c.regions.searchRegion(fromRegion.GetEndKey())
	if intoRegion == nil {
		return nil, errors.New("neighbor region is not available")
	}
	if c.regions.isRegionMerging(intoRegion.GetId()) {
		return nil, errors.Trace(errRegionIsMerging(intoRegion.GetId()))
	}

	// Mark from region and into region.
	if c.kv != nil {
		if err := c.kv.markRegionMerge(fromRegion.GetId(), intoRegion.GetId()); err != nil {
			return nil, errors.Trace(err)
		}
	}
	c.regions.markRegionMerge(fromRegion.GetId(), intoRegion.GetId())

	return intoRegion.Region, nil
}

// Helper function for internal use.
func (c *clusterInfo) handleRegionMergeHeartbeat(region *regionInfo) error {
	fromRegion, intoRegion := c.regions.getMergingRegions(region.GetId())
	if fromRegion == nil || intoRegion == nil {
		log.Fatalf("there are no regions merging: region %v", region)
	}

	// Only into region can commit region merge.
	if region.GetId() == intoRegion.GetId() {
		r := region.GetRegionEpoch()
		o := intoRegion.GetRegionEpoch()
		if r.GetVersion() == o.GetVersion()+1 &&
			regionRangeCover(region, fromRegion, intoRegion) {
			// Commit region merge.
			if c.kv != nil {
				err := c.kv.commitRegionMerge(fromRegion.GetId(), region.Region)
				if err != nil {
					return errors.Trace(err)
				}
			}
			c.regions.commitRegionMerge(fromRegion.GetId(), region)
			return nil
		}
	}

	// Abort region merge.
	if c.kv != nil {
		if err := c.kv.clearRegionMerge(fromRegion.GetId()); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.clearRegionMerge(fromRegion.GetId())
	return c.innerPutRegion(region)
}

func regionRangeCover(newRegion, fromRegion, intoRegion *regionInfo) bool {
	start, end := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Equal(start, fromRegion.GetStartKey()) && bytes.Equal(end, intoRegion.GetEndKey()) {
		return true
	}
	if bytes.Equal(start, intoRegion.GetStartKey()) && bytes.Equal(end, fromRegion.GetEndKey()) {
		return true
	}
	return false
}

// Helper function for internal use.
func (c *clusterInfo) checkRegionAvailable(region *metapb.Region) (*regionInfo, error) {
	origin := c.regions.getRegion(region.GetId())
	if origin == nil {
		return nil, errors.Trace(errRegionNotFound(region.GetId()))
	}
	if c.regions.isRegionMerging(region.GetId()) {
		return nil, errors.Trace(errRegionIsMerging(region.GetId()))
	}

	// We need to make sure that region epoch are the same.
	r := region.GetRegionEpoch()
	o := origin.GetRegionEpoch()
	if r.GetVersion() != o.GetVersion() || r.GetConfVer() != o.GetConfVer() {
		return nil, errors.Trace(errRegionIsStale(region, origin.Region))
	}

	return origin, nil
}
