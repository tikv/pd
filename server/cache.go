package server

import (
	"math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type storeIDMap map[uint64]struct{}

func newStoreIDMap(storeIDs ...uint64) storeIDMap {
	storeMap := make(storeIDMap, len(storeIDs))
	for _, id := range storeIDs {
		storeMap[id] = struct{}{}
	}
	return storeMap
}

// RegionInfo is region cache info.
type RegionInfo struct {
	region *metapb.Region
	// leader peer
	peer *metapb.Peer
}

// RegionsInfo is regions cache info.
type RegionsInfo struct {
	sync.RWMutex

	// region id -> RegionInfo
	leaderRegions map[uint64]*RegionInfo
	// store id -> regionid -> struct{}
	storeLeaderRegions map[uint64]map[uint64]struct{}
}

func newRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		leaderRegions:      make(map[uint64]*RegionInfo),
		storeLeaderRegions: make(map[uint64]map[uint64]struct{}),
	}
}

// randRegionPeer random selects a leader peer in regions.
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

	return r.leaderRegions[randRegionID]
}

func (r *RegionsInfo) addRegion(region *metapb.Region, leaderPeer *metapb.Peer) {
	r.Lock()
	defer r.Unlock()

	regionID := region.GetId()
	cacheRegion, regionExist := r.leaderRegions[regionID]
	if regionExist {
		// If region leader has been changed, then remove old region from store cache.
		oldLeaderPeer := cacheRegion.peer
		if oldLeaderPeer.GetId() != leaderPeer.GetId() {
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

	r.leaderRegions[regionID] = &RegionInfo{
		region: region,
		peer:   leaderPeer,
	}

	storeID := leaderPeer.GetStoreId()
	store, ok := r.storeLeaderRegions[storeID]
	if !ok {
		store = make(map[uint64]struct{})
		r.storeLeaderRegions[storeID] = store
	}
	store[regionID] = struct{}{}
}

func (r *RegionsInfo) removeRegion(regionID uint64) {
	r.Lock()
	defer r.Unlock()

	cacheRegion, ok := r.leaderRegions[regionID]
	if ok {
		storeID := cacheRegion.peer.GetStoreId()
		storeRegions, ok := r.storeLeaderRegions[storeID]
		if ok {
			delete(storeRegions, regionID)
			if len(storeRegions) == 0 {
				delete(r.storeLeaderRegions, storeID)
			}
		}

		delete(r.leaderRegions, regionID)
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

// fractionUsed is the used fraction of storage capacity.
func (s *StoreInfo) fractionUsed() float64 {
	if s.stats.GetCapacity() == 0 {
		return 0
	}

	return float64(s.stats.GetCapacity()-s.stats.GetAvailable()) / float64(s.stats.GetCapacity())
}

// ClusterInfo is cluster cache info.
type ClusterInfo struct {
	sync.RWMutex

	meta    *metapb.Cluster
	stores  map[uint64]*StoreInfo
	regions *RegionsInfo
}

func newClusterInfo() *ClusterInfo {
	return &ClusterInfo{
		stores:  make(map[uint64]*StoreInfo),
		regions: newRegionsInfo(),
	}
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
