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
	// If the used ratio of one storage is less than this value,
	// it will never be used as a from store.
	minCapacityUsedRatio = 0.4
	// If the used ratio of one storage is greater than this value,
	// it will never be used as a to store.
	maxCapacityUsedRatio = 0.9

	// If the sending snapshot count of one storage is greater than this value,
	// it will never be used as a from store.
	maxSnapSendingCount = uint32(3)
	// If the receiving snapshot count of one storage is greater than this value,
	// it will never be used as a to store.
	maxSnapReceivingCount = uint32(3)

	defaultDiffScoreFraction = 0.1
)

var (
	_ Balancer = &defaultBalancer{}
	_ Balancer = &resourceBalancer{}
)

type resourceBalancer struct {
	filters []Filter
}

func newResourceBalancer(minRatio float64, maxRatio float64) *resourceBalancer {
	rb := &resourceBalancer{}
	rb.addFilter(newCapacityFilter(minRatio, maxRatio))
	rb.addFilter(newSnapCountFilter(maxSnapSendingCount, maxSnapReceivingCount))

	return rb
}

func (rb *resourceBalancer) addFilter(filter Filter) {
	rb.filters = append(rb.filters, filter)
}

func (rb *resourceBalancer) filterFromStore(store *storeInfo, args ...interface{}) bool {
	for _, filter := range rb.filters {
		if filter.FilterFromStore(store, args) {
			return true
		}
	}

	return false
}

func (rb *resourceBalancer) filterToStore(store *storeInfo, args ...interface{}) bool {
	for _, filter := range rb.filters {
		if filter.FilterToStore(store, args) {
			return true
		}
	}

	return false
}

// calculate the score, higher score region will be selected as balance from store,
// and lower score region will be balance to store. The score range is [0,100].
// TODO: we should adjust the weight of used ratio and leader score in futher,
// now it is a little naive.
func (rb *resourceBalancer) score(store *storeInfo, regionCount int) int {
	usedRatioScore := store.usedRatioScore()
	leaderScore := store.leaderScore(regionCount)
	score := int(float64(usedRatioScore)*0.6 + float64(leaderScore)*0.4)
	return score
}

// checkScore checks whether the new store score and old store score are valid.
func (rb *resourceBalancer) checkScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer) bool {
	regionCount := cluster.regions.regionCount()
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return false
	}

	oldStoreScore := rb.score(oldStore, regionCount)
	newStoreScore := rb.score(newStore, regionCount)

	// If the diff score is in defaultScoreFraction range, then we will do nothing.
	diffScore := oldStoreScore - newStoreScore
	if diffScore <= int(float64(oldStoreScore)*defaultDiffScoreFraction) {
		log.Debugf("check score failed - diff score is too small - old peer: %v, new peer: %v, old store score: %d, new store score :%d, diif score: %d",
			oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return false
	}

	return true
}

func (rb *resourceBalancer) selectFromStore(stores []*storeInfo, regionCount int, useFilter bool) *storeInfo {
	score := 0
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if useFilter {
			if rb.filterFromStore(store) {
				continue
			}
		}

		currScore := rb.score(store, regionCount)
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

func (rb *resourceBalancer) selectToStore(stores []*storeInfo, excluded map[uint64]struct{}, regionCount int) *storeInfo {
	score := 0
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if rb.filterToStore(store) {
			continue
		}

		currScore := rb.score(store, regionCount)
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

// selectBalanceRegion tries to select a store leader region to do balance and returns true, but if we cannot find any,
// we try to find a store follower region and returns false.
func (rb *resourceBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer, bool) {
	store := rb.selectFromStore(stores, cluster.regions.regionCount(), true)
	if store == nil {
		log.Warn("from store cannot be found to select balance region")
		return nil, nil, nil, false
	}

	var (
		region   *metapb.Region
		leader   *metapb.Peer
		follower *metapb.Peer
	)

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region = cluster.regions.randLeaderRegion(storeID)
	if region == nil {
		log.Warnf("random leader region is nil, store %d", storeID)
		region, leader, follower = cluster.regions.randRegion(storeID)
		return region, leader, follower, false
	}

	leader = leaderPeer(region, storeID)
	return region, leader, nil, true
}

func (rb *resourceBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := rb.selectToStore(stores, nil, cluster.regions.regionCount())
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (rb *resourceBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := rb.selectToStore(stores, excluded, cluster.regions.regionCount())
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

func (rb *resourceBalancer) selectRemovePeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) (*metapb.Peer, error) {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := rb.selectFromStore(stores, cluster.regions.regionCount(), false)
	if store == nil {
		log.Warn("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

func (rb *resourceBalancer) doLeaderBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, leader *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	followerPeers, _ := getFollowerPeers(region, leader)
	newLeader := rb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do balance, try to do follower peer balance")
		return nil, nil
	}

	if !rb.checkScore(cluster, leader, newPeer) {
		return nil, nil
	}

	leaderTransferOperator := newTransferLeaderOperator(leader, newLeader, maxWaitCount)
	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(leader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), nil
}

func (rb *resourceBalancer) doFollowerBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, follower *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	if !rb.checkScore(cluster, follower, newPeer) {
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(follower)
	return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

func (rb *resourceBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, follower, isLeaderBalance := rb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil {
		log.Warn("region cannot be found to do balance")
		return nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do capacity balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do balance",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil
	}

	_, excludedStores := getFollowerPeers(region, leader)

	// Select one store to add new peer.
	newPeer, err := rb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil
	}

	if isLeaderBalance {
		ops, err := rb.doLeaderBalance(cluster, stores, region, leader, newPeer)
		return ops, errors.Trace(err)
	}

	return rb.doFollowerBalance(cluster, stores, region, follower, newPeer)
}

// defaultBalancer is used for default config change, like add/remove peer.
type defaultBalancer struct {
	*resourceBalancer
	region *metapb.Region
	leader *metapb.Peer
}

func newDefaultBalancer(region *metapb.Region, leader *metapb.Peer, minRatio float64, maxRatio float64) *defaultBalancer {
	return &defaultBalancer{
		region:           region,
		leader:           leader,
		resourceBalancer: newResourceBalancer(minRatio, maxRatio),
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
	peerCount := len(db.region.GetPeers())
	maxPeerCount := int(clusterMeta.GetMaxPeerCount())

	if peerCount == maxPeerCount {
		return nil, nil
	} else if peerCount < maxPeerCount {
		return db.addPeer(cluster)
	} else {
		return db.removePeer(cluster)
	}
}
