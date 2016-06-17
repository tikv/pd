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

// calculate the score, higher score region will be selected as balance from store,
// and lower score region will be balance to store.
// TODO: we should adjust the weight of used ratio and leader score in futher,
// now it is a little naive.
func (cb *capacityBalancer) score(store *storeInfo, regionCount int) float64 {
	usedRatioScore := store.usedRatio()
	leaderScore := store.leaderScore(regionCount)
	score := usedRatioScore*0.6 + leaderScore*0.4
	log.Infof("capacity balancer store %d, used ratio score: %v, leader score: %v [region count: %d], score: %v",
		store.store.GetId(), usedRatioScore, leaderScore, regionCount, score)
	return score
}

func (cb *capacityBalancer) selectFromStore(stores []*storeInfo, regionCount int, useFilter bool) *storeInfo {
	score := 0.0
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

		currScore := cb.score(store, regionCount)
		if resultStore == nil {
			resultStore = store
			score = currScore
			continue
		}

		if currScore > score {
			score = currScore
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) selectToStore(stores []*storeInfo, excluded map[uint64]struct{}, regionCount int) *storeInfo {
	score := 0.0
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

		currScore := cb.score(store, regionCount)
		if resultStore == nil {
			resultStore = store
			score = currScore
			continue
		}

		if currScore < score {
			score = currScore
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, bool) {
	store := cb.selectFromStore(stores, cluster.regions.regionCount(), true)
	if store == nil {
		log.Warn("from store cannot be found to select balance region")
		return nil, nil, false
	}

	var (
		region *metapb.Region
		leader *metapb.Peer
	)

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region = cluster.regions.randLeaderRegion(storeID)
	if region == nil {
		log.Warnf("random leader region is nil, store %d", storeID)
		region, leader = cluster.regions.randRegion(storeID)
		return region, leader, false
	}

	// If region peer count is not equal to max peer count, no need to do capacity balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d", len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil, false
	}

	leader = leaderPeer(region, storeID)
	return region, leader, true
}

func (cb *capacityBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := cb.selectToStore(stores, nil, cluster.regions.regionCount())
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := cb.selectToStore(stores, excluded, cluster.regions.regionCount())
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

	store := cb.selectFromStore(stores, cluster.regions.regionCount(), false)
	if store == nil {
		log.Warn("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

func (cb *capacityBalancer) doLeaderBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, leader *metapb.Peer) (*balanceOperator, bool, error) {
	followerPeers, excludedStores := getFollowerPeers(region, leader)
	newLeader := cb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do balance, try to do follower peer balance")
		return nil, false, nil
	}

	// Select one store to add new peer.
	newPeer, err := cb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, true, nil
	}

	leaderTransferOperator := newTransferLeaderOperator(leader, newLeader, maxWaitCount)
	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(leader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), true, nil
}

func (cb *capacityBalancer) doFollowerBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, leader *metapb.Peer) (*balanceOperator, error) {
	followerPeers, excludedStores := getFollowerPeers(region, leader)

	newPeer, err := cb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil
	}

	oldPeer, err := cb.selectRemovePeer(cluster, followerPeers)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if oldPeer == nil {
		log.Warnf("find no store to remove peer for region %v", region)
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(oldPeer)
	return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	// Select one balance region from cluster info.
	stores := cluster.getStores()
	region, oldLeader, isLeaderBalance := cb.selectBalanceRegion(cluster, stores)
	if region == nil || oldLeader == nil {
		log.Warn("region cannot be found to do balance")
		return nil, nil
	}

	if isLeaderBalance {
		ops, ok, err := cb.doLeaderBalance(cluster, stores, region, oldLeader)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			// If we cannot find a region peer to do leader transfer,
			// then we should balance a follower peer instead of leader peer.
			return cb.doFollowerBalance(cluster, stores, region, oldLeader)
		}

		return ops, nil
	}

	return cb.doFollowerBalance(cluster, stores, region, oldLeader)
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
		log.Debugf("region %d peer count equals %d, no need to change peer", regionID, maxPeerCount)
		return nil, nil
	} else if peerCount < maxPeerCount {
		log.Debugf("region %d peer count %d < %d, need to add peer", regionID, peerCount, maxPeerCount)
		return db.addPeer(cluster)
	} else {
		log.Debugf("region %d peer count %d > %d, need to remove peer", regionID, peerCount, maxPeerCount)
		return db.removePeer(cluster)
	}
}
