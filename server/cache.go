package server

import (
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
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

// StoreInfo is store cache info.
type StoreInfo struct {
	store *metapb.Store
	// region id -> leader peer
	regions map[uint64]*metapb.Peer
}

// ClusterInfo is cluster cache info.
type ClusterInfo struct {
	sync.RWMutex

	meta    *metapb.Cluster
	stores  map[uint64]*StoreInfo
	regions map[uint64]*RegionInfo
}

func newClusterInfo() *ClusterInfo {
	return &ClusterInfo{
		stores:  make(map[uint64]*StoreInfo),
		regions: make(map[uint64]*RegionInfo),
	}
}

func (c *ClusterInfo) addStore(store *metapb.Store) {
	c.Lock()
	defer c.Unlock()

	storeInfo := &StoreInfo{
		store:   store,
		regions: make(map[uint64]*metapb.Peer),
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

	store, _ := c.stores[storeID]
	return store
}

func (c *ClusterInfo) getStores() map[uint64]*StoreInfo {
	c.RLock()
	defer c.RUnlock()

	return c.stores
}

func (c *ClusterInfo) addRegion(region *metapb.Region, leaderPeer *metapb.Peer) {
	c.Lock()
	defer c.Unlock()

	regionID := region.GetId()
	cacheRegion, ok := c.regions[regionID]
	if ok {
		// If region leader has been changed, then remove old region from store cache.
		oldLeaderPeer := cacheRegion.peer
		if oldLeaderPeer.GetId() != leaderPeer.GetId() {
			storeID := oldLeaderPeer.GetStoreId()
			store, ok := c.stores[storeID]
			if ok {
				delete(store.regions, regionID)
			}
		}
	}

	c.regions[regionID] = &RegionInfo{
		region: region,
		peer:   leaderPeer,
	}

	if store, ok := c.stores[leaderPeer.GetStoreId()]; ok {
		store.regions[regionID] = leaderPeer
	}
}

func (c *ClusterInfo) removeRegion(regionID uint64) {
	c.Lock()
	defer c.Unlock()

	cacheRegion, ok := c.regions[regionID]
	if ok {
		storeID := cacheRegion.peer.GetStoreId()
		store, ok := c.stores[storeID]
		if ok {
			delete(store.regions, regionID)
		}

		delete(c.regions, regionID)
	}
}

func (c *ClusterInfo) setMeta(meta *metapb.Cluster) {
	c.Lock()
	defer c.Unlock()

	c.meta = meta
}

func (c *ClusterInfo) getMeta() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()

	return c.meta
}
