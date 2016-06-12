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
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

// Balancer is an interface to select store regions for auto-balance.
type Balancer interface {
	Balance(cluster *clusterInfo) (*balanceOperator, error)
}

const (
	// If the used ratio of one storage is greater than this value,
	// it should be rebalanced.
	minCapacityUsedRatio = 0.4
	// If the used ratio of one storage is greater than this value,
	// it will never be used as a selected target.
	maxCapacityUsedRatio = 0.9
)

var (
	_ Balancer = &defaultBalancer{}
	_ Balancer = &capacityBalancer{}
)

type capacityBalancer struct {
	minCapacityUsedRatio float64
	maxCapacityUsedRatio float64
}

func newCapacityBalancer(minRatio float64, maxRatio float64) *capacityBalancer {
	return &capacityBalancer{
		minCapacityUsedRatio: minRatio,
		maxCapacityUsedRatio: maxRatio,
	}
}

func (cb *capacityBalancer) selectFromStore(stores []*storeInfo, useFilter bool) *storeInfo {
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if useFilter {
			if store.usedRatio() <= cb.minCapacityUsedRatio {
				continue
			}
		}

		if resultStore == nil {
			resultStore = store
			continue
		}

		if store.usedRatio() > resultStore.usedRatio() {
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) selectToStore(stores []*storeInfo, excluded map[uint64]struct{}) *storeInfo {
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if store.usedRatio() >= cb.maxCapacityUsedRatio {
			continue
		}

		if resultStore == nil {
			resultStore = store
			continue
		}

		if store.usedRatio() < resultStore.usedRatio() {
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer) {
	store := cb.selectFromStore(stores, true)
	if store == nil {
		return nil, nil
	}

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region := cluster.regions.randRegion(storeID)

	// If region peer count is less than max peer count, no need to do capacity balance.
	if len(region.GetPeers()) < int(cluster.getMeta().GetMaxPeerCount()) {
		return nil, nil
	}

	leaderPeer := leaderPeer(region, storeID)
	return region, leaderPeer
}

func (cb *capacityBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := cb.selectToStore(stores, nil)
	if store == nil {
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := cb.selectToStore(stores, excluded)
	if store == nil {
		log.Warn("to store cannot be found to add peer")
		return nil, nil
	}

	peerID, err := cluster.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peer := &metapb.Peer{
		Id:      proto.Uint64(peerID),
		StoreId: proto.Uint64(store.store.GetId()),
	}

	return peer, nil
}

func (cb *capacityBalancer) selectRemovePeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) (*metapb.Peer, error) {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := cb.selectFromStore(stores, false)
	if store == nil {
		log.Warn("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	// Firstly, select one balance region from cluster info.
	stores := cluster.getStores()
	region, oldLeader := cb.selectBalanceRegion(cluster, stores)
	if region == nil || oldLeader == nil {
		log.Warn("region cannot be found to do balance")
		return nil, nil
	}

	// Secondly, select one region peer to do leader transfer.
	followerPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	excludedStores := make(map[uint64]struct{}, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		excludedStores[storeID] = struct{}{}

		if peer.GetId() == oldLeader.GetId() {
			continue
		}

		followerPeers[storeID] = peer
	}

	newLeader := cb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do balance")
		return nil, nil
	}

	// Thirdly, select one store to add new peer.
	newPeer, err := cb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil
	}

	leaderTransferOperator := newTransferLeaderOperator(oldLeader, newLeader, maxWaitCount)
	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(oldLeader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), nil
}

// defaultBalancer is used for default config change, like add/remove peer.
type defaultBalancer struct {
	*capacityBalancer
	region *metapb.Region
	leader *metapb.Peer
}

func newDefaultBalancer(region *metapb.Region, leader *metapb.Peer) *defaultBalancer {
	return &defaultBalancer{
		region: region,
		leader: leader,
		// TODO: we should use capacity used ratio configuration later.
		capacityBalancer: newCapacityBalancer(minCapacityUsedRatio, maxCapacityUsedRatio),
	}
}

func (db *defaultBalancer) addPeer(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	excludedStores := make(map[uint64]struct{}, len(db.region.GetPeers()))
	for _, peer := range db.region.GetPeers() {
		storeID := peer.GetStoreId()
		excludedStores[storeID] = struct{}{}
	}

	peer, err := db.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		log.Warnf("find no store to add peer for region %v", db.region)
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(peer)
	return newBalanceOperator(db.region, newOnceOperator(addPeerOperator)), nil
}

func (db *defaultBalancer) removePeer(cluster *clusterInfo) (*balanceOperator, error) {
	followerPeers := make(map[uint64]*metapb.Peer, len(db.region.GetPeers()))
	for _, peer := range db.region.GetPeers() {
		if peer.GetId() == db.leader.GetId() {
			continue
		}

		storeID := peer.GetStoreId()
		followerPeers[storeID] = peer
	}

	peer, err := db.selectRemovePeer(cluster, followerPeers)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		log.Warnf("find no store to remove peer for region %v", db.region)
		return nil, nil
	}

	removePeerOperator := newRemovePeerOperator(peer)
	return newBalanceOperator(db.region, newOnceOperator(removePeerOperator)), nil
}

func (db *defaultBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	clusterMeta := cluster.getMeta()

	regionID := db.region.GetId()
	peerCount := len(db.region.GetPeers())
	maxPeerCount := int(clusterMeta.GetMaxPeerCount())

	if peerCount == maxPeerCount {
		log.Infof("region %d peer count equals %d, no need to change peer", regionID, maxPeerCount)
		return nil, nil
	} else if peerCount < maxPeerCount {
		log.Infof("region %d peer count %d < %d, need to add peer", regionID, peerCount, maxPeerCount)
		return db.addPeer(cluster)
	} else {
		log.Infof("region %d peer count %d > %d, need to remove peer", regionID, peerCount, maxPeerCount)
		return db.removePeer(cluster)
	}
}
