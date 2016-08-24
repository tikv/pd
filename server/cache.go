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
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
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

	return errors.Errorf("epoch %s is staler than %s", checkEpoch, epoch)
}

func getFollowerPeers(region *metapb.Region, leader *metapb.Peer) map[uint64]*metapb.Peer {
	followerPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		if peer.GetId() == leader.GetId() {
			continue
		}
		followerPeers[storeID] = peer
	}
	return followerPeers
}

func getExcludedStores(region *metapb.Region) map[uint64]struct{} {
	excludedStores := make(map[uint64]struct{}, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		excludedStores[storeID] = struct{}{}
	}
	return excludedStores
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

// regionsInfo is regions cache info.
type regionsInfo struct {
	sync.RWMutex

	// region id -> RegionInfo
	regions map[uint64]*metapb.Region
	// search key -> region id
	searchRegions *btree.BTree

	leaders *leaders
}

func newRegionsInfo() *regionsInfo {
	return &regionsInfo{
		regions:       make(map[uint64]*metapb.Region),
		searchRegions: btree.New(defaultBtreeDegree),
		leaders: &leaders{
			storeRegions: make(map[uint64]map[uint64]struct{}),
			regionStores: make(map[uint64]uint64),
		},
	}
}

// getRegion gets the region and leader peer by regionKey.
func (r *regionsInfo) getRegion(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	r.RLock()
	defer r.RUnlock()

	region := r.innerGetRegion(regionKey)
	if region == nil {
		return nil, nil
	}

	if !keyInRegion(regionKey, region) {
		return nil, nil
	}

	regionID := region.GetId()
	leaderStoreID, ok := r.leaders.regionStores[regionID]
	if ok {
		return cloneRegion(region), leaderPeer(region, leaderStoreID)
	}

	return cloneRegion(region), nil
}

// getRegionByID gets the region and leader peer by regionID.
func (r *regionsInfo) getRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	r.RLock()
	defer r.RUnlock()

	region, ok := r.regions[regionID]
	if !ok {
		return nil, nil
	}

	leaderStoreID, ok := r.leaders.regionStores[regionID]
	if ok {
		return cloneRegion(region), leaderPeer(region, leaderStoreID)
	}

	return cloneRegion(region), nil
}

// getRegions gets all the regions, returns nil if not found.
func (r *regionsInfo) getRegions() []*metapb.Region {
	r.RLock()
	defer r.RUnlock()

	regions := make([]*metapb.Region, 0, len(r.regions))
	for _, region := range r.regions {
		regions = append(regions, cloneRegion(region))
	}

	return regions
}

func (r *regionsInfo) innerGetRegion(regionKey []byte) *metapb.Region {
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

func (r *regionsInfo) addRegion(region *metapb.Region) {
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

func (r *regionsInfo) updateRegion(region *metapb.Region) {
	item := &searchKeyItem{
		region: region,
	}

	oldItem := r.searchRegions.ReplaceOrInsert(item)
	if oldItem == nil {
		log.Fatalf("updateRegion for none existed region - %v", region)
	}

	r.regions[region.GetId()] = region
}

func (r *regionsInfo) removeRegion(region *metapb.Region) {
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

func (r *regionsInfo) heartbeatVersion(region *metapb.Region) (bool, *metapb.Region, error) {
	// For split, we should handle heartbeat carefully.
	// E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
	// after split, region 1 and 2 will do heartbeat independently.
	startKey := region.GetStartKey()
	endKey := region.GetEndKey()

	searchRegion := r.innerGetRegion(startKey)
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

func (r *regionsInfo) heartbeatConfVer(region *metapb.Region) (*pdpb.ChangePeer, bool, error) {
	// ConfVer is handled after Version, so here
	// we must get the region by ID.
	cacheRegion := r.regions[region.GetId()]
	if err := checkStaleRegion(cacheRegion, region); err != nil {
		return nil, false, errors.Trace(err)
	}

	if region.GetRegionEpoch().GetConfVer() > cacheRegion.GetRegionEpoch().GetConfVer() {
		// ConfChanged, update
		r.updateRegion(region)
		return r.getChangePeer(cacheRegion, region), true, nil
	}

	return nil, false, nil
}

func (r *regionsInfo) getChangePeer(region *metapb.Region, curRegion *metapb.Region) *pdpb.ChangePeer {
	if region == nil || curRegion == nil {
		return nil
	}

	// Get remove peer.
	for _, peer := range region.GetPeers() {
		// Current region does not have region peer,
		// so it is removing the region peer now.
		if !containPeer(curRegion, peer) {
			return &pdpb.ChangePeer{
				ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
				Peer:       peer,
			}
		}
	}

	// Get add peer.
	for _, peer := range curRegion.GetPeers() {
		// Current region has region peer, but region does not have,
		// so it is adding the region peer now.
		if !containPeer(region, peer) {
			return &pdpb.ChangePeer{
				ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
				Peer:       peer,
			}
		}
	}

	return nil
}

// heartbeatResp is the response after heartbeat handling.
// If putRegion is not nil, we should update it in etcd,
// if removeRegion is not nil, we should remove it in etcd.
type heartbeatResp struct {
	putRegion    *metapb.Region
	removeRegion *metapb.Region
}

// heartbeat handles heartbeat for the region.
func (r *regionsInfo) heartbeat(region *metapb.Region, leaderPeer *metapb.Peer) (*heartbeatResp, *pdpb.ChangePeer, error) {
	r.Lock()
	defer r.Unlock()

	versionUpdated, removeRegion, err := r.heartbeatVersion(region)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	changePeer, confVerUpdated, err := r.heartbeatConfVer(region)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	regionID := region.GetId()
	storeID := leaderPeer.GetStoreId()
	r.leaders.update(regionID, storeID)

	resp := &heartbeatResp{
		removeRegion: removeRegion,
	}

	if versionUpdated || confVerUpdated {
		resp.putRegion = region
	}

	return resp, changePeer, nil
}

func (r *regionsInfo) leaderRegionCount(storeID uint64) int {
	r.RLock()
	defer r.RUnlock()

	return len(r.leaders.storeRegions[storeID])
}

func (r *regionsInfo) regionCount() int {
	r.RLock()
	defer r.RUnlock()

	return len(r.regions)
}

// randLeaderRegion selects a leader region from region cache randomly.
func (r *regionsInfo) randLeaderRegion(storeID uint64) *metapb.Region {
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

	// TODO: if costs too much time, we may refactor the rand leader region way.
	cost := time.Now().Sub(start)
	if cost > maxRandRegionTime {
		log.Warnf("select leader region %d in %d regions for store %d too slow, cost %s", randRegionID, len(storeRegions), storeID, cost)
	}
	randRegionDuration.WithLabelValues("leader").Observe(cost.Seconds())

	region, ok := r.regions[randRegionID]
	if ok {
		return cloneRegion(region)
	}

	return nil
}

// randRegion selects a region from region cache randomly.
func (r *regionsInfo) randRegion(storeID uint64) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	r.RLock()
	defer r.RUnlock()

	var (
		region   *metapb.Region
		leader   *metapb.Peer
		follower *metapb.Peer
	)

	start := time.Now()
	for _, rg := range r.regions {
		for _, peer := range rg.GetPeers() {
			if peer.GetStoreId() == storeID {
				// Check whether it is leader region of this store.
				regionID := rg.GetId()
				leaderStoreID, ok := r.leaders.regionStores[regionID]
				if ok {
					if leaderStoreID != storeID {
						region = cloneRegion(rg)
						follower = peer
						leader = leaderPeer(region, leaderStoreID)
						break
					}
				}
			}
		}
	}

	// TODO: if costs too much time, we may refactor the rand region way.
	cost := time.Now().Sub(start)
	if cost > maxRandRegionTime {
		log.Warnf("select region %d in %d regions for store %d too slow, cost %s", region.GetId(), len(r.regions), storeID, cost)
	}
	randRegionDuration.WithLabelValues("follower").Observe(cost.Seconds())

	return region, leader, follower
}

// StoreStatus is store status info.
type StoreStatus struct {
	// store capacity info.
	Stats *pdpb.StoreStats `json:"stats"`

	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`

	LeaderRegionCount int `json:"leader_region_count"`

	TotalRegionCount int `json:"total_region_count"`

	Scores []int `json:"scores"`
}

func (s *StoreStatus) clone() *StoreStatus {
	return &StoreStatus{
		Stats:             proto.Clone(s.Stats).(*pdpb.StoreStats),
		LastHeartbeatTS:   s.LastHeartbeatTS,
		LeaderRegionCount: s.LeaderRegionCount,
		TotalRegionCount:  s.TotalRegionCount,
	}
}

// storeInfo is store cache info.
type storeInfo struct {
	store *metapb.Store

	stats *StoreStatus
}

func (s *storeInfo) clone() *storeInfo {
	return &storeInfo{
		store: proto.Clone(s.store).(*metapb.Store),
		stats: s.stats.clone(),
	}
}

// leaderRatio is the leader region ratio of storage regions.
func (s *storeInfo) leaderRatio() float64 {
	if s.stats.TotalRegionCount == 0 {
		return 0
	}

	return float64(s.stats.LeaderRegionCount) / float64(s.stats.TotalRegionCount)
}

// usedRatio is the used capacity ratio of storage capacity.
func (s *storeInfo) usedRatio() float64 {
	if s.stats.Stats.GetCapacity() == 0 {
		return 0
	}

	return float64(s.stats.Stats.GetCapacity()-s.stats.Stats.GetAvailable()) / float64(s.stats.Stats.GetCapacity())
}

func (s *storeInfo) downSeconds() uint64 {
	return uint64(time.Since(s.stats.LastHeartbeatTS).Seconds())
}

// clusterInfo is cluster cache info.
type clusterInfo struct {
	sync.RWMutex

	meta        *metapb.Cluster
	stores      map[uint64]*storeInfo
	regions     *regionsInfo
	clusterRoot string

	idAlloc IDAllocator
}

func newClusterInfo(clusterRoot string) *clusterInfo {
	cluster := &clusterInfo{
		clusterRoot: clusterRoot,
		stores:      make(map[uint64]*storeInfo),
		regions:     newRegionsInfo(),
	}

	return cluster
}

func (c *clusterInfo) addStore(store *metapb.Store) {
	c.Lock()
	defer c.Unlock()

	storeInfo := &storeInfo{
		store: store,
		stats: &StoreStatus{},
	}

	storeID := store.GetId()
	c.stores[storeID] = storeInfo
}

func (c *clusterInfo) updateStoreStatus(stats *pdpb.StoreStats) bool {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store, ok := c.stores[storeID]
	if !ok {
		return false
	}

	store.stats.Stats = stats
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.LeaderRegionCount = c.regions.leaderRegionCount(storeID)
	store.stats.TotalRegionCount = c.regions.regionCount()
	return true
}

func (c *clusterInfo) removeStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.stores, storeID)
}

func (c *clusterInfo) getStore(storeID uint64) *storeInfo {
	c.RLock()
	defer c.RUnlock()

	store, ok := c.stores[storeID]
	if !ok {
		return nil
	}

	return store.clone()
}

func (c *clusterInfo) getStores() []*storeInfo {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*storeInfo, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, store.clone())
	}

	return stores
}

func (c *clusterInfo) getMetaStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*metapb.Store, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, proto.Clone(store.store).(*metapb.Store))
	}

	return stores
}

func (c *clusterInfo) setMeta(meta *metapb.Cluster) {
	c.Lock()
	defer c.Unlock()

	c.meta = meta
}

func (c *clusterInfo) getMeta() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()

	return proto.Clone(c.meta).(*metapb.Cluster)
}
