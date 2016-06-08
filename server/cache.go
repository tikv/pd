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
	"math/rand"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	statsd "gopkg.in/alexcesaro/statsd.v2"
)

const (
	defaultBtreeDegree = 64
	maxRandRegionTime  = 500 * time.Millisecond
)

type searchKeyItem struct {
	region *metapb.Region
}

var _ btree.Item = &searchKeyItem{}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (s *searchKeyItem) Less(other btree.Item) bool {
	left := s.region.GetStartKey()
	right := other.(*searchKeyItem).region.GetStartKey()
	return bytes.Compare(left, right) > 0
}

func containPeer(region *metapb.Region, peer *metapb.Peer) bool {
	for _, p := range region.GetPeers() {
		if p.GetId() == peer.GetId() {
			return true
		}
	}

	return false
}

func leaderPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}

	return nil
}

func cloneRegion(r *metapb.Region) *metapb.Region {
	return proto.Clone(r).(*metapb.Region)
}

func checkStaleRegion(region *metapb.Region, checkRegion *metapb.Region) error {
	epoch := region.GetRegionEpoch()
	checkEpoch := checkRegion.GetRegionEpoch()

	if checkEpoch.GetVersion() >= epoch.GetVersion() &&
		checkEpoch.GetConfVer() >= epoch.GetConfVer() {
		return nil
	}

	return errors.Errorf("stale epoch %s, now %s", epoch, checkEpoch)
}

func keyInRegion(regionKey []byte, region *metapb.Region) bool {
	return bytes.Compare(regionKey, region.GetStartKey()) >= 0 &&
		(len(region.GetEndKey()) == 0 || bytes.Compare(regionKey, region.GetEndKey()) < 0)
}

type leaders struct {
	// store id -> region id -> struct{}
	storeRegions map[uint64]map[uint64]struct{}
	// region id -> store id
	regionStores map[uint64]uint64
}

func (l *leaders) remove(regionID uint64) {
	storeID, ok := l.regionStores[regionID]
	if !ok {
		return
	}

	l.removeStoreRegion(storeID, regionID)
	delete(l.regionStores, regionID)
}

func (l *leaders) removeStoreRegion(regionID uint64, storeID uint64) {
	storeRegions, ok := l.storeRegions[storeID]
	if ok {
		delete(storeRegions, regionID)
		if len(storeRegions) == 0 {
			delete(l.storeRegions, storeID)
		}
	}
}

func (l *leaders) update(regionID uint64, storeID uint64) {
	storeRegions, ok := l.storeRegions[storeID]
	if !ok {
		storeRegions = make(map[uint64]struct{})
		l.storeRegions[storeID] = storeRegions
	}
	storeRegions[regionID] = struct{}{}

	if lastStoreID, ok := l.regionStores[regionID]; ok && lastStoreID != storeID {
		l.removeStoreRegion(regionID, lastStoreID)
	}

	l.regionStores[regionID] = storeID
}

// RegionsInfo is regions cache info.
type RegionsInfo struct {
	sync.RWMutex

	// region id -> RegionInfo
	regions map[uint64]*metapb.Region
	// search key -> region id
	searchRegions *btree.BTree

	leaders *leaders
}

func newRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		regions:       make(map[uint64]*metapb.Region),
		searchRegions: btree.New(defaultBtreeDegree),
		leaders: &leaders{
			storeRegions: make(map[uint64]map[uint64]struct{}),
			regionStores: make(map[uint64]uint64),
		},
	}
}

// GetRegion gets the region by regionKey. Return nil if not found.
func (r *RegionsInfo) GetRegion(regionKey []byte) *metapb.Region {
	r.RLock()
	region := r.getRegion(regionKey)
	r.RUnlock()

	if region == nil {
		return nil
	}

	if keyInRegion(regionKey, region) {
		return cloneRegion(region)
	}

	return nil
}

func (r *RegionsInfo) getRegion(regionKey []byte) *metapb.Region {
	startSearchItem := &searchKeyItem{
		region: &metapb.Region{
			StartKey: regionKey,
		},
	}

	var searchItem *searchKeyItem
	r.searchRegions.AscendGreaterOrEqual(startSearchItem, func(i btree.Item) bool {
		searchItem = i.(*searchKeyItem)
		return false
	})

	if searchItem == nil {
		return nil
	}

	return searchItem.region
}

func (r *RegionsInfo) addRegion(region *metapb.Region) {
	item := &searchKeyItem{
		region: region,
	}

	oldItem := r.searchRegions.ReplaceOrInsert(item)
	if oldItem != nil {
		log.Fatalf("addRegion for already existed region in searchRegions - %v", region)
	}

	_, ok := r.regions[region.GetId()]
	if ok {
		log.Fatalf("addRegion for already existed region in regions - %v", region)
	}

	r.regions[region.GetId()] = region
}

func (r *RegionsInfo) updateRegion(region *metapb.Region) {
	item := &searchKeyItem{
		region: region,
	}

	oldItem := r.searchRegions.ReplaceOrInsert(item)
	if oldItem == nil {
		log.Fatalf("updateRegion for none existed region - %v", region)
	}

	r.regions[region.GetId()] = region
}

func (r *RegionsInfo) removeRegion(region *metapb.Region) {
	item := &searchKeyItem{
		region: region,
	}
	regionID := region.GetId()

	oldItem := r.searchRegions.Delete(item)
	if oldItem == nil {
		log.Fatalf("removeRegion for none existed region - %v", region)
	}

	delete(r.regions, region.GetId())

	r.leaders.remove(regionID)
}

// HeartbeatResp is the response after heartbeat handling.
// If PutRegion is not nil, we should update it in etcd,
// if RemoveRegion is not nil, we should remove it in etcd.
type HeartbeatResp struct {
	PutRegion    *metapb.Region
	RemoveRegion *metapb.Region
}

func (r *RegionsInfo) heartbeatVersion(region *metapb.Region) (bool, *metapb.Region, error) {
	// For split, we should handle heartbeat carefully.
	// E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
	// after split, region 1 and 2 will do heartbeat independently.
	startKey := region.GetStartKey()
	endKey := region.GetEndKey()

	searchRegion := r.getRegion(startKey)
	if searchRegion == nil {
		// Find no region for start key, insert directly.
		r.addRegion(region)
		return true, nil, nil
	}

	searchStartKey := searchRegion.GetStartKey()
	searchEndKey := searchRegion.GetEndKey()

	if bytes.Equal(startKey, searchStartKey) && bytes.Equal(endKey, searchEndKey) {
		// we are the same, must check epoch here.
		if err := checkStaleRegion(searchRegion, region); err != nil {
			return false, nil, errors.Trace(err)
		}

		// TODO: If we support merge regions, we should check the detail epoch version.
		return false, nil, nil
	}

	if len(searchEndKey) > 0 && bytes.Compare(startKey, searchEndKey) >= 0 {
		// No range covers [start, end) now, insert directly.
		r.addRegion(region)
		return true, nil, nil
	}

	// overlap, remove old, insert new.
	// E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
	// is overlapped with origin [a, c).
	epoch := region.GetRegionEpoch()
	searchEpoch := searchRegion.GetRegionEpoch()
	if epoch.GetVersion() <= searchEpoch.GetVersion() ||
		epoch.GetConfVer() < searchEpoch.GetConfVer() {
		return false, nil, errors.Errorf("region %s has wrong epoch compared with %s", region, searchRegion)
	}

	r.removeRegion(searchRegion)
	r.addRegion(region)
	return true, searchRegion, nil
}

func (r *RegionsInfo) heartbeatConfVer(region *metapb.Region) (bool, error) {
	// ConfVer is handled after Version, so here
	// we must get the region by ID.
	curRegion := r.regions[region.GetId()]
	if err := checkStaleRegion(curRegion, region); err != nil {
		return false, errors.Trace(err)
	}

	if region.GetRegionEpoch().GetConfVer() > curRegion.GetRegionEpoch().GetConfVer() {
		// ConfChanged, update
		r.updateRegion(region)
		return true, nil
	}

	return false, nil
}

// Heartbeat handles heartbeat for the region.
func (r *RegionsInfo) Heartbeat(region *metapb.Region, leaderPeer *metapb.Peer) (*HeartbeatResp, error) {
	r.Lock()
	defer r.Unlock()

	versionUpdated, removeRegion, err := r.heartbeatVersion(region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	confVerUpdated, err := r.heartbeatConfVer(region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	regionID := region.GetId()
	storeID := leaderPeer.GetStoreId()
	r.leaders.update(regionID, storeID)

	resp := &HeartbeatResp{
		RemoveRegion: removeRegion,
	}

	if versionUpdated || confVerUpdated {
		resp.PutRegion = region
	}

	return resp, nil
}

// randRegion random selects a region from region cache.
func (r *RegionsInfo) randRegion(storeID uint64) *metapb.Region {
	r.RLock()
	defer r.RUnlock()

	storeRegions, ok := r.leaders.storeRegions[storeID]
	if !ok {
		return nil
	}

	start := time.Now()
	idx, randIdx, randRegionID := 0, rand.Intn(len(storeRegions)), uint64(0)
	for regionID := range storeRegions {
		if idx == randIdx {
			randRegionID = regionID
			break
		}

		idx++
	}

	// TODO: if costs too much time, we may refactor the rand region way.
	if cost := time.Now().Sub(start); cost > maxRandRegionTime {
		log.Warnf("select region %d in %d regions for store %d too slow, cost %s", randRegionID, len(storeRegions), storeID, cost)
	}

	region, ok := r.regions[randRegionID]
	if ok {
		return cloneRegion(region)
	}

	return nil
}

// StoreInfo is store cache info.
type StoreInfo struct {
	store *metapb.Store

	// store capacity info.
	stats *pdpb.StoreStats
}

func (s *StoreInfo) clone() *StoreInfo {
	return &StoreInfo{
		store: proto.Clone(s.store).(*metapb.Store),
		stats: proto.Clone(s.stats).(*pdpb.StoreStats),
	}
}

// usedRatio is the used capacity ratio of storage capacity.
func (s *StoreInfo) usedRatio() float64 {
	if s.stats.GetCapacity() == 0 {
		return 0
	}

	return float64(s.stats.GetCapacity()-s.stats.GetAvailable()) / float64(s.stats.GetCapacity())
}

// ClusterInfo is cluster cache info.
type ClusterInfo struct {
	sync.RWMutex

	meta        *metapb.Cluster
	stores      map[uint64]*StoreInfo
	regions     *RegionsInfo
	clusterRoot string

	idAlloc IDAllocator

	stats *statsd.Client
}

func newClusterInfo(clusterRoot string) *ClusterInfo {
	cluster := &ClusterInfo{
		clusterRoot: clusterRoot,
		stores:      make(map[uint64]*StoreInfo),
		regions:     newRegionsInfo(),
	}

	// create a Mute stats, can' fail.
	stats, _ := statsd.New(statsd.Mute(true))
	cluster.stats = stats

	return cluster
}

func (c *ClusterInfo) addStore(store *metapb.Store) {
	c.Lock()
	defer c.Unlock()

	storeInfo := &StoreInfo{
		store: store,
		stats: &pdpb.StoreStats{},
	}

	c.stores[store.GetId()] = storeInfo
}

func (c *ClusterInfo) updateStoreStatus(stats *pdpb.StoreStats) bool {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store, ok := c.stores[storeID]
	if !ok {
		return false
	}

	store.stats = stats
	return true
}

func (c *ClusterInfo) removeStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.stores, storeID)
}

func (c *ClusterInfo) getStore(storeID uint64) *StoreInfo {
	c.RLock()
	defer c.RUnlock()

	store, ok := c.stores[storeID]
	if !ok {
		return nil
	}

	return store.clone()
}

func (c *ClusterInfo) getStores() []*StoreInfo {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*StoreInfo, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, store.clone())
	}

	return stores
}

func (c *ClusterInfo) getMetaStores() []metapb.Store {
	c.RLock()
	defer c.RUnlock()

	stores := make([]metapb.Store, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, *proto.Clone(store.store).(*metapb.Store))
	}

	return stores
}

func (c *ClusterInfo) setMeta(meta *metapb.Cluster) {
	c.Lock()
	defer c.Unlock()

	c.meta = meta
}

func (c *ClusterInfo) getMeta() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()

	return proto.Clone(c.meta).(*metapb.Cluster)
}
