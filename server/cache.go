package server

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const defaultBtreeDegree = 64

type searchKey []byte

type searchKeyItem struct {
	key searchKey
	id  uint64
}

// Less compares two searchKeys.
func (s searchKey) Less(other searchKey) bool {
	return bytes.Compare(s, other) < 0
}

var _ btree.Item = &searchKeyItem{}

// Less returns true if the region's encoded end key is less than the item key.
func (s *searchKeyItem) Less(than btree.Item) bool {
	return s.key.Less(than.(*searchKeyItem).key)
}

// RegionInfo is region cache info.
type RegionInfo struct {
	region *metapb.Region
	// leader peer
	peer *metapb.Peer
}

func (r *RegionInfo) clone() *RegionInfo {
	return &RegionInfo{
		region: proto.Clone(r.region).(*metapb.Region),
		peer:   proto.Clone(r.peer).(*metapb.Peer),
	}
}

// RegionsInfo is regions cache info.
type RegionsInfo struct {
	sync.RWMutex

	cluster *ClusterInfo

	// region id -> RegionInfo
	regions map[uint64]*RegionInfo
	// search key -> region id
	searchRegions *btree.BTree
	// store id -> region id -> struct{}
	storeLeaderRegions map[uint64]map[uint64]struct{}
}

func newRegionsInfo(cluster *ClusterInfo) *RegionsInfo {
	return &RegionsInfo{
		cluster:            cluster,
		regions:            make(map[uint64]*RegionInfo),
		searchRegions:      btree.New(defaultBtreeDegree),
		storeLeaderRegions: make(map[uint64]map[uint64]struct{}),
	}
}

func (r *RegionsInfo) delSearchRegion(delRegionKey []byte) {
	if len(delRegionKey) == 0 {
		return
	}

	r.Lock()
	defer r.Unlock()

	searchItem := &searchKeyItem{
		key: searchKey(delRegionKey),
	}
	r.searchRegions.Delete(searchItem)
}

func (r *RegionsInfo) getRegion(startKey []byte, endKey []byte) *RegionInfo {
	r.RLock()
	defer r.RUnlock()

	startSearchItem := &searchKeyItem{key: searchKey(startKey)}
	endSearchItem := &searchKeyItem{key: searchKey(endKey)}

	var searchItem *searchKeyItem
	r.searchRegions.AscendRange(startSearchItem, endSearchItem, func(i btree.Item) bool {
		searchItem = i.(*searchKeyItem)
		return false
	})
	if searchItem == nil {
		return nil
	}

	regionID := searchItem.id
	region, ok := r.regions[regionID]
	if ok {
		return region.clone()
	}

	return nil
}

// randRegion random selects a region from region cache.
func (r *RegionsInfo) randRegion(storeID uint64) *RegionInfo {
	r.RLock()
	defer r.RUnlock()

	storeRegions, ok := r.storeLeaderRegions[storeID]
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
		return region.clone()
	}

	return nil
}

func (r *RegionsInfo) upsertRegion(region *metapb.Region, leaderPeer *metapb.Peer) {
	r.Lock()
	defer r.Unlock()

	skipRegionCache, skipStoreRegionCache, skipSearchRegionCache := false, false, false

	regionID := region.GetId()
	cacheRegion, regionExist := r.regions[regionID]
	if regionExist {
		// If region epoch and leader peer has not been changed, set `skipRegionCache = true`.
		if cacheRegion.region.GetRegionEpoch().GetVersion() == region.GetRegionEpoch().GetVersion() &&
			cacheRegion.region.GetRegionEpoch().GetConfVer() == region.GetRegionEpoch().GetConfVer() &&
			cacheRegion.peer.GetId() == leaderPeer.GetId() {
			skipRegionCache = true
		}

		// If region startKey and endKey has not been changed, set `skipSearchRegionCache = true`.
		if bytes.Equal(cacheRegion.region.GetStartKey(), region.GetStartKey()) &&
			bytes.Equal(cacheRegion.region.GetEndKey(), region.GetEndKey()) {
			skipSearchRegionCache = true
		}

		oldLeaderPeer := cacheRegion.peer
		if oldLeaderPeer.GetId() == leaderPeer.GetId() {
			// If region leader peer has not been changed, set `skipStoreRegionCache = true`.
			skipStoreRegionCache = true
		} else {
			// If region leader peer has been changed, remove old region from store cache.
			storeID := oldLeaderPeer.GetStoreId()
			storeRegions, storeExist := r.storeLeaderRegions[storeID]
			if storeExist {
				delete(storeRegions, regionID)
				if len(storeRegions) == 0 {
					delete(r.storeLeaderRegions, storeID)
				}
			}
		}
	}

	if !skipRegionCache {
		r.regions[regionID] = &RegionInfo{
			region: region,
			peer:   leaderPeer,
		}
	}

	if !skipStoreRegionCache {
		storeID := leaderPeer.GetStoreId()
		store, ok := r.storeLeaderRegions[storeID]
		if !ok {
			store = make(map[uint64]struct{})
			r.storeLeaderRegions[storeID] = store
		}
		store[regionID] = struct{}{}
	}

	if !skipSearchRegionCache {
		searchItem := &searchKeyItem{
			key: searchKey(makeRegionSearchKey(r.cluster.clusterRoot, region.GetEndKey())),
			id:  region.GetId(),
		}
		r.searchRegions.ReplaceOrInsert(searchItem)
	}
}

func (r *RegionsInfo) removeRegion(regionID uint64) {
	r.Lock()
	defer r.Unlock()

	cacheRegion, ok := r.regions[regionID]
	if ok {
		storeID := cacheRegion.peer.GetStoreId()
		storeRegions, ok := r.storeLeaderRegions[storeID]
		if ok {
			delete(storeRegions, regionID)
			if len(storeRegions) == 0 {
				delete(r.storeLeaderRegions, storeID)
			}
		}

		delete(r.regions, regionID)

		searchItem := &searchKeyItem{
			key: searchKey(makeRegionSearchKey(r.cluster.clusterRoot, cacheRegion.region.GetEndKey())),
			id:  cacheRegion.region.GetId(),
		}
		r.searchRegions.Delete(searchItem)
	}
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
	cluster.regions = newRegionsInfo(cluster)
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
