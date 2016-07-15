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

var (
	_ Balancer = &defaultBalancer{}
	_ Balancer = &resourceBalancer{}
)

// leaderScore is the leader peer count score of store, the score range is [0,100].
func leaderScore(leaderCount int, regionCount int) int {
	if regionCount == 0 {
		return 0
	}

	return leaderCount * 100 / regionCount
}

type resourceBalancer struct {
	balanceFilters        []Filter
	leaderTransferFilters []Filter

	cfg *BalanceConfig
}

func newResourceBalancer(cfg *BalanceConfig) *resourceBalancer {
	rb := &resourceBalancer{cfg: cfg}

	// Add resource filters to check whether the store can only do balance.
	rb.balanceFilters = append(rb.balanceFilters, newCapacityFilter(cfg))
	rb.balanceFilters = append(rb.balanceFilters, newSnapCountFilter(cfg))

	// Add leader transfer filters to check whether the store can only do leader transfer.
	rb.leaderTransferFilters = append(rb.leaderTransferFilters, newLeaderCountFilter(cfg))

	return rb
}

func (rb *resourceBalancer) filterFromStore(store *storeInfo, filters []Filter, args ...interface{}) bool {
	for _, filter := range filters {
		if filter.FilterFromStore(store, args) {
			return true
		}
	}

	return false
}

func (rb *resourceBalancer) filterToStore(store *storeInfo, filters []Filter, args ...interface{}) bool {
	for _, filter := range filters {
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
func (rb *resourceBalancer) score(store *storeInfo, leaderCount int, regionCount int) int {
	usedRatioScore := store.usedRatioScore()
	leaderScore := leaderScore(leaderCount, regionCount)
	return int(float64(usedRatioScore)*0.6 + float64(leaderScore)*0.4)
}

// checkScore checks whether the new store score and old store score are valid.
func (rb *resourceBalancer) checkScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer, isLeaderPeer bool) bool {
	regionCount := cluster.regions.regionCount()
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return false
	}

	// We should check the diff score of pre-balance `from store` and post balance `to store`.
	// If isLeaderPeer is true, we should calculate the `to store` score with added leader region count.
	var oldStoreScore, newStoreScore int
	oldStoreScore = rb.score(oldStore, oldStore.stats.LeaderRegionCount, regionCount)
	if isLeaderPeer {
		newStoreScore = rb.score(newStore, newStore.stats.LeaderRegionCount+1, regionCount)
	} else {
		newStoreScore = rb.score(newStore, newStore.stats.LeaderRegionCount, regionCount)
	}

	// If the diff score is in defaultScoreFraction range, then we will do nothing.
	diffScore := oldStoreScore - newStoreScore
	if diffScore <= int(float64(oldStoreScore)*rb.cfg.MaxDiffScoreFraction) {
		log.Debugf("check score failed - diff score is too small - old peer: %v, new peer: %v, old store score: %d, new store score: %d, diif score: %d",
			oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return false
	}

	return true
}

func (rb *resourceBalancer) selectFromStore(stores []*storeInfo, regionCount int, filters []Filter) *storeInfo {
	score := 0
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if rb.filterFromStore(store, filters) {
			continue
		}

		currScore := rb.score(store, store.stats.LeaderRegionCount, regionCount)
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

func (rb *resourceBalancer) selectToStore(stores []*storeInfo, excluded map[uint64]struct{}, regionCount int, filters []Filter) *storeInfo {
	score := 0
	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if rb.filterToStore(store, filters) {
			continue
		}

		currScore := rb.score(store, store.stats.LeaderRegionCount, regionCount)
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
	store := rb.selectFromStore(stores, cluster.regions.regionCount(), rb.balanceFilters)
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

// selectLeaderTransferRegion tries to select a store leader region to do leader transfer.
func (rb *resourceBalancer) selectLeaderTransferRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	store := rb.selectFromStore(stores, cluster.regions.regionCount(), rb.leaderTransferFilters)
	if store == nil {
		log.Warn("from store cannot be found to select leader transfer region")
		return nil, nil, nil
	}

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region := cluster.regions.randLeaderRegion(storeID)
	if region == nil {
		return nil, nil, nil
	}

	leader := leaderPeer(region, storeID)
	if leader == nil {
		return nil, nil, nil
	}

	followerPeers, _ := getFollowerPeers(region, leader)
	newLeader := rb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do leader transfer")
		return nil, nil, nil
	}

	return region, leader, newLeader
}

func (rb *resourceBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := rb.selectToStore(stores, nil, cluster.regions.regionCount(), nil)
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (rb *resourceBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := rb.selectToStore(stores, excluded, cluster.regions.regionCount(), rb.balanceFilters)
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

	store := rb.selectFromStore(stores, cluster.regions.regionCount(), nil)
	if store == nil {
		log.Warn("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

func (rb *resourceBalancer) doLeaderBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, leader *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	regionID := region.GetId()

	// If cluster max peer count config is 1, we cannot do leader transfer,
	// only need to add new peer and remove leader peer.
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		addPeerOperator := newAddPeerOperator(regionID, newPeer)
		removePeerOperator := newRemovePeerOperator(regionID, leader)
		if !rb.checkScore(cluster, leader, newPeer, true) {
			return nil, nil
		}
		return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
	}

	if !rb.checkScore(cluster, leader, newPeer, false) {
		return nil, nil
	}

	followerPeers, _ := getFollowerPeers(region, leader)
	newLeader := rb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do balance, try to do follower peer balance")
		return nil, nil
	}

	leaderTransferOperator := newTransferLeaderOperator(regionID, leader, newLeader, int(rb.cfg.MaxTransferWaitCount))
	addPeerOperator := newAddPeerOperator(regionID, newPeer)
	removePeerOperator := newRemovePeerOperator(regionID, leader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), nil
}

func (rb *resourceBalancer) doFollowerBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, follower *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	if !rb.checkScore(cluster, follower, newPeer, false) {
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), follower)
	return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

func (rb *resourceBalancer) doBalance(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, follower, isLeaderBalance := rb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil {
		log.Warn("region cannot be found to do balance")
		return nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do balance.
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

func (rb *resourceBalancer) doLeaderTransfer(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, newLeader := rb.selectLeaderTransferRegion(cluster, stores)
	if region == nil || leader == nil || newLeader == nil {
		log.Warn("region cannot be found to do leader transfer")
		return nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do leader transfer.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do leader transfer",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil
	}

	if !rb.checkScore(cluster, leader, newLeader, true) {
		return nil, nil
	}

	regionID := region.GetId()
	leaderTransferOperator := newTransferLeaderOperator(regionID, leader, newLeader, int(rb.cfg.MaxTransferWaitCount))
	return newBalanceOperator(region, leaderTransferOperator), nil
}

// Balance tries to select a store to do balance.
// The priority of balance type is:
// doBalance:
// 1 do leader balance.
// 2 do follower balance.
// doLeaderTransfer:
// 1 do leader transfer.
func (rb *resourceBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	op, err := rb.doBalance(cluster)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if op != nil {
		return op, nil
	}

	return rb.doLeaderTransfer(cluster)
}

// defaultBalancer is used for default config change, like add/remove peer.
type defaultBalancer struct {
	*resourceBalancer
	region *metapb.Region
	leader *metapb.Peer
}

func newDefaultBalancer(region *metapb.Region, leader *metapb.Peer, cfg *BalanceConfig) *defaultBalancer {
	return &defaultBalancer{
		region:           region,
		leader:           leader,
		resourceBalancer: newResourceBalancer(cfg),
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

	addPeerOperator := newAddPeerOperator(db.region.GetId(), peer)
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

	removePeerOperator := newRemovePeerOperator(db.region.GetId(), peer)
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
