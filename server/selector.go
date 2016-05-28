package server

import (
	"github.com/pingcap/kvproto/pkg/metapb"
)

// Selector is an interface to select regions and stores for leader-transfer/rebalance.
type Selector interface {
	// Select selects one pair regions to do leader transfer.
	SelectTransferRegion(cluster *ClusterInfo, excluded map[uint64]struct{}) (*RegionInfo, *metapb.Peer)
	// SelectGoodPeer selects one region peer to be transferred to.
	SelectGoodPeer(cluster *ClusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer
	// SelectBadRegion selects one region to be transferred.
	SelectBadRegion(cluster *ClusterInfo, excluded map[uint64]struct{}) *RegionInfo
	// SelectGoodStore selects one store to be rebalanced to.
	SelectGoodStore(stores map[uint64]*StoreInfo, excluded map[uint64]struct{}) *StoreInfo
	// SelectBadStore selects one store to be rebalanced.
	SelectBadStore(stores map[uint64]*StoreInfo, excluded map[uint64]struct{}) *StoreInfo
}

const (
	// If the used fraction of one storage is greater than this value,
	// it will never be used as a selected target and it should be rebalanced.
	maxCapacityUsedFraction = 0.6
)

var (
	_ Selector = &regionCapacitySelector{}
)

type regionCapacitySelector struct {
}

func (rs *regionCapacitySelector) SelectBadStore(stores map[uint64]*StoreInfo, excluded map[uint64]struct{}) *StoreInfo {
	var badStore *StoreInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if store.usedFraction() <= maxCapacityUsedFraction {
			continue
		}

		if badStore == nil {
			badStore = store
			continue
		}

		if store.usedFraction() > badStore.usedFraction() {
			badStore = store
		}
	}

	return badStore
}

func (rs *regionCapacitySelector) SelectGoodStore(stores map[uint64]*StoreInfo, excluded map[uint64]struct{}) *StoreInfo {
	var goodStore *StoreInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if store.usedFraction() > maxCapacityUsedFraction {
			continue
		}

		if goodStore == nil {
			goodStore = store
			continue
		}

		if store.usedFraction() < goodStore.usedFraction() {
			goodStore = store
		}
	}

	return goodStore
}

func (rs *regionCapacitySelector) SelectBadRegion(cluster *ClusterInfo, excluded map[uint64]struct{}) *RegionInfo {
	stores := cluster.getStores()
	store := rs.SelectBadStore(stores, excluded)
	if store == nil {
		return nil
	}

	// Random select one bad region leader peer from bad store.
	storeID := store.store.GetId()
	return cluster.regions.randRegion(storeID)
}

func (rs *regionCapacitySelector) SelectGoodPeer(cluster *ClusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make(map[uint64]*StoreInfo, len(peers))
	for storeID := range peers {
		stores[storeID] = cluster.getStore(storeID)
	}

	store := rs.SelectGoodStore(stores, nil)
	if store == nil {
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (rs *regionCapacitySelector) SelectTransferRegion(cluster *ClusterInfo, excluded map[uint64]struct{}) (*RegionInfo, *metapb.Peer) {
	// First select one bad store region from cluster info.
	region := rs.SelectBadRegion(cluster, excluded)
	if region == nil {
		return nil, nil
	}

	leaderPeer := region.peer
	if leaderPeer == nil {
		return nil, nil
	}

	// Second select one good store to do region leader transfer.
	followerPeers := make(map[uint64]*metapb.Peer)
	for _, peer := range region.region.GetPeers() {
		if peer.GetId() == leaderPeer.GetId() {
			continue
		}

		storeID := peer.GetStoreId()
		followerPeers[storeID] = peer
	}

	peer := rs.SelectGoodPeer(cluster, followerPeers)
	if peer == nil {
		return nil, nil
	}

	return region, peer
}
