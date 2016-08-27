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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	_ Balancer = &capacityBalancer{}
	_ Balancer = &replicaBalancer{}
	_ Balancer = &leaderBalancer{}
)

// Balancer is an interface to select store regions for auto-balance.
type Balancer interface {
	// Balance selects one store to do balance.
	Balance(cluster *clusterInfo) (*score, *balanceOperator, error)
	// ScoreType returns score type.
	ScoreType() scoreType
}

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
	cb.filters = append(cb.filters, newStateFilter(cfg))
	cb.filters = append(cb.filters, newCapacityFilter(cfg))
	cb.filters = append(cb.filters, newSnapCountFilter(cfg))
	return cb
}

func (cb *capacityBalancer) ScoreType() scoreType {
	return cb.st
}

func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	store := selectFromStore(stores, nil, cb.filters, cb.st)
	if store == nil {
		log.Debug("from store cannot be found to select balance region")
		return nil, nil, nil
	}

	storeID := store.store.GetId()
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		region := cluster.regions.randLeaderRegion(storeID)
		if region == nil {
			return nil, nil, nil
		}

		leader := leaderPeer(region, storeID)
		return region, leader, leader
	}

	// Random select one follower region from store.
	return cluster.regions.randRegion(storeID)
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := selectToStore(stores, excluded, cb.filters, cb.st)
	if store == nil {
		log.Debug("to store cannot be found to add peer")
		return nil, nil
	}

	peerID, err := cluster.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: store.store.GetId(),
	}

	return peer, nil
}

func (cb *capacityBalancer) selectRemovePeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) (*metapb.Peer, error) {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectFromStore(stores, nil, nil, cb.st)
	if store == nil {
		log.Debug("from store cannot be found to remove peer")
		return nil, nil
	}

	storeID := store.store.GetId()
	return peers[storeID], nil
}

// Balance tries to select a store region to do balance.
// The balance type is follower balance.
func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	stores := cluster.getStores()
	region, leader, peer := cb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil || peer == nil {
		log.Debug("region cannot be found to do balance")
		return nil, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do balance",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil, nil
	}

	// Select one store to add new peer.
	excluded := getExcludedStores(region)
	newPeer, err := cb.selectAddPeer(cluster, stores, excluded)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if newPeer == nil {
		log.Warn("new peer cannot be found to do balance")
		return nil, nil, nil
	}

	// Check and get diff score.
	score, ok := checkAndGetDiffScore(cluster, peer, newPeer, cb.st, cb.cfg)
	if !ok {
		return nil, nil, nil
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), peer)
	return score, newBalanceOperator(region, addPeerOperator, removePeerOperator), nil
}

type leaderBalancer struct {
	filters []Filter
	st      scoreType

	cfg *BalanceConfig
}

func newLeaderBalancer(cfg *BalanceConfig) *leaderBalancer {
	lb := &leaderBalancer{cfg: cfg, st: leaderScore}
	lb.filters = append(lb.filters, newStateFilter(cfg))
	lb.filters = append(lb.filters, newLeaderCountFilter(cfg))
	return lb
}

func (lb *leaderBalancer) ScoreType() scoreType {
	return lb.st
}

// selectBalanceRegion tries to select a store leader region to do balance.
func (lb *leaderBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*metapb.Region, *metapb.Peer, *metapb.Peer) {
	store := selectFromStore(stores, nil, lb.filters, lb.st)
	if store == nil {
		log.Debug("from store cannot be found to select balance region")
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

	followers := getFollowerPeers(region, leader)
	newLeader := lb.selectNewLeaderPeer(cluster, followers)
	if newLeader == nil {
		log.Warn("new leader peer cannot be found to do leader transfer")
		return nil, nil, nil
	}

	return region, leader, newLeader
}

func (lb *leaderBalancer) selectNewLeaderPeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectToStore(stores, nil, nil, lb.st)
	if store == nil {
		log.Debug("find no store to get new leader peer for region")
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

// Balance tries to select a store region to do balance.
// The balance type is leader transfer.
func (lb *leaderBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	// If cluster max peer count config is 1, we cannot do leader transfer,
	meta := cluster.getMeta()
	if meta.GetMaxPeerCount() == 1 {
		return nil, nil, nil
	}

	stores := cluster.getStores()
	region, leader, newLeader := lb.selectBalanceRegion(cluster, stores)
	if region == nil || leader == nil || newLeader == nil {
		log.Debug("region cannot be found to do leader transfer")
		return nil, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do leader transfer.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		log.Warnf("region peer count %d not equals to max peer count %d, no need to do leader transfer",
			len(region.GetPeers()), cluster.getMeta().GetMaxPeerCount())
		return nil, nil, nil
	}

	score, ok := checkAndGetDiffScore(cluster, leader, newLeader, lb.st, lb.cfg)
	if !ok {
		return nil, nil, nil
	}

	regionID := region.GetId()
	transferLeaderOperator := newTransferLeaderOperator(regionID, leader, newLeader, lb.cfg)
	return score, newBalanceOperator(region, transferLeaderOperator), nil
}

// replicaBalancer is used to balance active replica count.
type replicaBalancer struct {
	*capacityBalancer
	cfg       *BalanceConfig
	region    *metapb.Region
	leader    *metapb.Peer
	downPeers []*pdpb.PeerStats
}

func newReplicaBalancer(region *metapb.Region, leader *metapb.Peer, downPeers []*pdpb.PeerStats, cfg *BalanceConfig) *replicaBalancer {
	return &replicaBalancer{
		cfg:              cfg,
		region:           region,
		leader:           leader,
		downPeers:        downPeers,
		capacityBalancer: newCapacityBalancer(cfg),
	}
}

func (rb *replicaBalancer) addPeer(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	excluded := getExcludedStores(rb.region)
	peer, err := rb.selectAddPeer(cluster, stores, excluded)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		log.Warnf("find no store to add peer for region %v", rb.region)
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, newOnceOperator(addPeerOperator)), nil
}

func (rb *replicaBalancer) removePeer(cluster *clusterInfo, badPeers []*metapb.Peer) (*balanceOperator, error) {
	var peer *metapb.Peer

	if len(badPeers) >= 1 {
		peer = badPeers[0]
	} else {
		var err error
		followers := getFollowerPeers(rb.region, rb.leader)
		peer, err = rb.selectRemovePeer(cluster, followers)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if peer == nil {
		log.Warnf("find no store to remove peer for region %v", rb.region)
		return nil, nil
	}

	removePeerOperator := newRemovePeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, newOnceOperator(removePeerOperator)), nil
}

func containsPeer(peers []*metapb.Peer, peer *metapb.Peer) bool {
	for _, p := range peers {
		if p.GetId() == peer.GetId() {
			return true
		}
	}
	return false
}

func (rb *replicaBalancer) collectBadPeers(cluster *clusterInfo) []*metapb.Peer {
	badPeers := rb.collectTombstonePeers(cluster)
	downPeers := rb.collectDownPeers(cluster)
	for _, peer := range downPeers {
		if !containsPeer(badPeers, peer) {
			badPeers = append(badPeers, peer)
		}
	}
	return badPeers
}

func (rb *replicaBalancer) collectDownPeers(cluster *clusterInfo) []*metapb.Peer {
	downPeers := make([]*metapb.Peer, 0, len(rb.downPeers))
	for _, stats := range rb.downPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := cluster.getStore(peer.GetStoreId())
		if store == nil {
			continue
		}
		if stats.GetDownSeconds() >= rb.cfg.MaxPeerDownDuration.Seconds() {
			// Peer has been down for too long.
			downPeers = append(downPeers, peer)
		} else if store.downSeconds() >= rb.cfg.MaxStoreDownDuration.Seconds() {
			// Both peer and store are down, we should do balance.
			downPeers = append(downPeers, peer)
		}
	}
	return downPeers
}

func (rb *replicaBalancer) collectTombstonePeers(cluster *clusterInfo) []*metapb.Peer {
	tombPeers := make([]*metapb.Peer, 0)
	for _, peer := range rb.region.GetPeers() {
		store := cluster.getStore(peer.GetStoreId())
		if store == nil {
			continue
		}
		if !store.isUpState() {
			tombPeers = append(tombPeers, peer)
		}
	}
	return tombPeers
}

func (rb *replicaBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	badPeers := rb.collectBadPeers(cluster)
	peerCount := len(rb.region.GetPeers())
	maxPeerCount := int(cluster.getMeta().GetMaxPeerCount())

	if len(badPeers) > 0 {
		log.Debugf("region: %v peers: %v bad peers: %v",
			rb.region.GetId(), rb.region.GetPeers(), badPeers)
	}

	var (
		bop *balanceOperator
		err error
	)

	if peerCount-len(badPeers) < maxPeerCount {
		bop, err = rb.addPeer(cluster)
	} else if peerCount > maxPeerCount {
		bop, err = rb.removePeer(cluster, badPeers)
	}

	return nil, bop, errors.Trace(err)
}
