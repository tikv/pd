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

var (
	_ Balancer = &capacityBalancer{}
)

func selectFromStore(stores []*storeInfo, excluded map[uint64]struct{}, filters []Filter, st scoreType) *storeInfo {
	score := 0
	scorer := newScorer(st)
	if scorer == nil {
		return nil
	}

	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if filterFromStore(store, filters) {
			continue
		}

		currScore := scorer.Score(store)
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

func selectToStore(stores []*storeInfo, excluded map[uint64]struct{}, filters []Filter, st scoreType) *storeInfo {
	score := 0
	scorer := newScorer(st)
	if scorer == nil {
		return nil
	}

	var resultStore *storeInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if _, ok := excluded[store.store.GetId()]; ok {
			continue
		}

		if filterToStore(store, filters) {
			continue
		}

		currScore := scorer.Score(store)
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

type capacityBalancer struct {
	filters []Filter
	st      scoreType

	cfg *BalanceConfig
}

func newCapacityBalancer(cfg *BalanceConfig) *capacityBalancer {
	cb := &capacityBalancer{cfg: cfg, st: capacityScore}
	cb.filters = append(cb.filters, newCapacityFilter(cfg))
	cb.filters = append(cb.filters, newSnapCountFilter(cfg))
	return cb
}

// checkScore checks whether the new store score and old store score are valid.
func (cb *capacityBalancer) checkScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer) bool {
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return false
	}

	// We should check the diff score of pre-balance `from store` and post balance `to store`.
	scorer := newScorer(cb.st)
	oldStoreScore := scorer.Score(oldStore)
	newStoreScore := scorer.Score(newStore)

	// Check whether the diff score is in MaxDiffScoreFraction range.
	diffScore := oldStoreScore - newStoreScore
	if diffScore <= int(float64(oldStoreScore)*cb.cfg.MaxDiffScoreFraction) {
		log.Debugf("check score failed - diff score is too small - old peer: %v, new peer: %v, old store score: %d, new store score: %d, diif score: %d",
			oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return false
	}

	return true
}

// selectBalanceRegion tries to select a store leader region to do balance and returns true, but if we cannot find any,
// we try to find a store follower region and returns false.
func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer, bool) {
	store := selectFromStore(stores, cluster.getUnknownStores(), cb.filters, cb.st)
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

func (cb *capacityBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectToStore(stores, cluster.getUnknownStores(), nil, cb.st)
	if store == nil {
		log.Warn("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := selectToStore(stores, excluded, cb.filters, cb.st)
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

func (cb *capacityBalancer) doLeaderBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, leader *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	if !cb.checkScore(cluster, leader, newPeer) {
		return nil, nil
	}

	regionID := region.GetId()

	// If cluster max peer count config is 1, we cannot do leader transfer,
	// only need to add new peer and remove leader peer.
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		addPeerOperator := newAddPeerOperator(regionID, newPeer)
		removePeerOperator := newRemovePeerOperator(regionID, leader)
		return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
	}

	followerPeers, _ := getFollowerPeers(region, leader)
	newLeader := cb.selectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do balance, try to do follower peer balance")
		return nil, nil
	}

	leaderTransferOperator := newTransferLeaderOperator(regionID, leader, newLeader, int(cb.cfg.MaxTransferWaitCount))
	addPeerOperator := newAddPeerOperator(regionID, newPeer)
	removePeerOperator := newRemovePeerOperator(regionID, leader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), nil
}

func (cb *capacityBalancer) doFollowerBalance(cluster *clusterInfo, stores []*storeInfo, region *metapb.Region, follower *metapb.Peer, newPeer *metapb.Peer) (*balanceOperator, error) {
	if !cb.checkScore(cluster, follower, newPeer) {
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), follower)
	return newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

func (cb *capacityBalancer) doBalance(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, follower, isLeaderBalance := cb.selectBalanceRegion(cluster, stores)
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
	excludedStores = mergeMap(excludedStores, cluster.getUnknownStores())

	// Select one store to add new peer.
	newPeer, err := cb.selectAddPeer(cluster, stores, excludedStores)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil
	}

	if isLeaderBalance {
		ops, err := cb.doLeaderBalance(cluster, stores, region, leader, newPeer)
		return ops, errors.Trace(err)
	}

	return cb.doFollowerBalance(cluster, stores, region, follower, newPeer)
}

// Balance tries to select a store region to do balance.
// The priority of balance type is:
// doBalance:
// 1 do leader balance.
// 2 do follower balance.
func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*balanceOperator, error) {
	op, err := cb.doBalance(cluster)
	return op, errors.Trace(err)
}
