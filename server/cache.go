package server

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const defaultBtreeDegree = 64

type searchKey []byte

type searchKeyItem struct {
	key    searchKey
	region *metapb.Region
}

// Less compares whether searchKey is less than other.
func (s searchKey) Less(other searchKey) bool {
	return bytes.Compare(s, other) < 0
}

var _ btree.Item = &searchKeyItem{}

// Less returns true if the key is less than the other item key.
func (s *searchKeyItem) Less(other btree.Item) bool {
	return s.key.Less(other.(*searchKeyItem).key)
}

func cloneRegion(r *metapb.Region) *metapb.Region {
	if r == nil {
		return nil
	}

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

func encodeRegionSearchKey(searchKey []byte) string {
	return string(append([]byte{'z'}, searchKey...))
}

func encodeRegionStartKey(region *metapb.Region) string {
	startKey := region.GetStartKey()
	return string(append([]byte{'z'}, startKey...))
}

func encodeRegionEndKey(region *metapb.Region) string {
	endKey := region.GetEndKey()

	if len(endKey) == 0 {
		return "\xFF"
	}

	return string(append([]byte{'z'}, endKey...))
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
	region := r.getRegion(regionKey)
	if region == nil {
		return nil
	}

	if keyInRegion(regionKey, region) {
		return cloneRegion(region)
	}

	return nil
}

func (r *RegionsInfo) getRegion(regionKey []byte) *metapb.Region {
	// We must use the next region key for search,
	// e,g, we have two regions 1, 2, and key ranges are ["", "abc"), ["abc", +infinite),
	// if we use "abc" to search the region, the first key >= "abc" may be
	// region 1, not region 2. So we use the next region key for search.
	nextRegionKey := append(regionKey, 0x00)

	startSearchItem := &searchKeyItem{key: searchKey(encodeRegionSearchKey(nextRegionKey))}

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
		key:    searchKey(encodeRegionEndKey(region)),
		region: region,
	}

	oldItem := r.searchRegions.ReplaceOrInsert(item)
	if oldItem != nil && bytes.Equal(oldItem.(*searchKeyItem).key, item.key) {
		log.Fatalf("addRegion for already existed region - %v", region)
	}

	r.regions[region.GetId()] = region
}

func (r *RegionsInfo) updataRegion(region *metapb.Region) {
	item := &searchKeyItem{
		key:    searchKey(encodeRegionEndKey(region)),
		region: region,
	}

	oldItem := r.searchRegions.ReplaceOrInsert(item)
	if oldItem == nil {
		log.Fatalf("updataRegion for none existed region - %v", region)
	}

	r.regions[region.GetId()] = region
}

func (r *RegionsInfo) removeRegion(region *metapb.Region) {
	item := &searchKeyItem{
		key:    searchKey(encodeRegionEndKey(region)),
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
	startKey := encodeRegionStartKey(region)
	endKey := encodeRegionEndKey(region)

	searchRegion := r.getRegion(region.GetStartKey())
	if searchRegion == nil {
		// Find no region for start key, insert directly.
		r.addRegion(region)
		return true, nil, nil
	}

	searchStartKey := encodeRegionStartKey(searchRegion)
	searchEndKey := encodeRegionEndKey(searchRegion)

	if startKey == searchStartKey && endKey == searchEndKey {
		// we are the same, must check epoch here.
		if err := checkStaleRegion(searchRegion, region); err != nil {
			return false, nil, errors.Trace(err)
		}

		// TODO: If we support merge regions, we should check the detail epoch version.
		return false, nil, nil
	}

	if searchStartKey >= endKey {
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
		r.updataRegion(region)
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

	idx, randIdx, randRegionID := 0, rand.Intn(len(storeRegions)), uint64(0)
	for regionID := range storeRegions {
		if idx == randIdx {
			randRegionID = regionID
			break
		}

		idx++
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
}

func newClusterInfo(clusterRoot string) *ClusterInfo {
	cluster := &ClusterInfo{
		clusterRoot: clusterRoot,
		stores:      make(map[uint64]*StoreInfo),
	}
	cluster.regions = newRegionsInfo()
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

func (c *ClusterInfo) getStores() map[uint64]*StoreInfo {
	c.RLock()
	defer c.RUnlock()

	stores := make(map[uint64]*StoreInfo, len(c.stores))
	for key, store := range c.stores {
		stores[key] = store.clone()
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
