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

		if _, ok := excluded[store.GetId()]; ok {
			continue
		}

		if filterSource(store, filters) {
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

		if _, ok := excluded[store.GetId()]; ok {
			continue
		}

		if filterTarget(store, filters) {
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

func (cb *capacityBalancer) selectBalanceRegion(cluster *clusterInfo, stores []*storeInfo) (*regionInfo, *metapb.Peer) {
	store := selectFromStore(stores, nil, cb.filters, cb.st)
	if store == nil {
		return nil, nil
	}

	storeID := store.GetId()
	region := cluster.randFollowerRegion(storeID)
	if region == nil {
		region = cluster.randLeaderRegion(storeID)
	}
	if region == nil {
		return nil, nil
	}
	return region, region.GetStorePeer(storeID)
}

func (cb *capacityBalancer) selectAddPeer(cluster *clusterInfo, stores []*storeInfo, excluded map[uint64]struct{}) (*metapb.Peer, error) {
	store := selectToStore(stores, excluded, cb.filters, cb.st)
	if store == nil {
		return nil, nil
	}
	return cluster.allocPeer(store.GetId())
}

func (cb *capacityBalancer) selectRemovePeer(cluster *clusterInfo, peers map[uint64]*metapb.Peer) (*metapb.Peer, error) {
	stores := make([]*storeInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := selectFromStore(stores, nil, nil, cb.st)
	if store == nil {
		return nil, nil
	}

	storeID := store.GetId()
	return peers[storeID], nil
}

// Balance tries to select a store region to do balance.
// The balance type is follower balance.
func (cb *capacityBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	stores := cluster.getStores()
	region, peer := cb.selectBalanceRegion(cluster, stores)
	if region == nil || peer == nil {
		return nil, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		return nil, nil, nil
	}

	// Select one store to add new peer.
	newPeer, err := cb.selectAddPeer(cluster, stores, region.GetStoreIds())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if newPeer == nil {
		return nil, nil, nil
	}

	// Check and get diff score.
	score, ok := checkAndGetDiffScore(cluster, peer, newPeer, cb.st, cb.cfg)
	if !ok {
		return nil, nil, nil
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), peer)
	return score, newBalanceOperator(region, balanceOP, addPeerOperator, removePeerOperator), nil
}

type leaderBalancer struct {
	cfg      *BalanceConfig
	st       scoreType
	selector Selector
}

func newLeaderBalancer(cfg *BalanceConfig) *leaderBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(cfg))
	filters = append(filters, newLeaderCountFilter(cfg))

	lb := &leaderBalancer{cfg: cfg, st: leaderScore}
	lb.selector = newBalanceSelector(newScorer(lb.st), filters)
	return lb
}

func (lb *leaderBalancer) ScoreType() scoreType {
	return lb.st
}

// Balance tries to select a store region to do balance.
// The balance type is leader transfer.
func (lb *leaderBalancer) Balance(cluster *clusterInfo) (*score, *balanceOperator, error) {
	region, _, target := scheduleLeader(cluster, lb.selector)
	if region == nil {
		return nil, nil, nil
	}
	newLeader := region.GetStorePeer(target.GetId())

	score, ok := checkAndGetDiffScore(cluster, region.Leader, newLeader, lb.st, lb.cfg)
	if !ok {
		return nil, nil, nil
	}

	regionID := region.GetId()
	transferLeaderOperator := newTransferLeaderOperator(regionID, region.Leader, newLeader, lb.cfg)
	return score, newBalanceOperator(region, balanceOP, transferLeaderOperator), nil
}

// replicaBalancer is used to balance active replica count.
type replicaBalancer struct {
	*capacityBalancer
	cfg    *BalanceConfig
	region *regionInfo
}

func newReplicaBalancer(region *regionInfo, cfg *BalanceConfig) *replicaBalancer {
	return &replicaBalancer{
		cfg:              cfg,
		region:           region,
		capacityBalancer: newCapacityBalancer(cfg),
	}
}

func (rb *replicaBalancer) addPeer(cluster *clusterInfo) (*balanceOperator, error) {
	stores := cluster.getStores()
	peer, err := rb.selectAddPeer(cluster, stores, rb.region.GetStoreIds())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if peer == nil {
		return nil, nil
	}

	addPeerOperator := newAddPeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, replicaOP, newOnceOperator(addPeerOperator)), nil
}

func (rb *replicaBalancer) removePeer(cluster *clusterInfo, badPeers []*metapb.Peer) (*balanceOperator, error) {
	var peer *metapb.Peer

	if len(badPeers) >= 1 {
		peer = badPeers[0]
	} else {
		var err error
		peer, err = rb.selectRemovePeer(cluster, rb.region.GetFollowers())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if peer == nil {
		return nil, nil
	}

	removePeerOperator := newRemovePeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, replicaOP, newOnceOperator(removePeerOperator)), nil
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
	downPeers := make([]*metapb.Peer, 0, len(rb.region.DownPeers))
	for _, stats := range rb.region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := cluster.getStore(peer.GetStoreId())
		if store == nil {
			continue
		}
		if stats.GetDownSeconds() >= uint64(rb.cfg.MaxPeerDownDuration.Seconds()) {
			// Peer has been down for too long.
			downPeers = append(downPeers, peer)
		} else if store.downTime() >= rb.cfg.MaxStoreDownDuration.Duration {
			// Both peer and store are down, we should do balance.
			downPeers = append(downPeers, peer)
		}
	}
	return downPeers
}

func (rb *replicaBalancer) collectTombstonePeers(cluster *clusterInfo) []*metapb.Peer {
	var tombPeers []*metapb.Peer
	for _, peer := range rb.region.GetPeers() {
		store := cluster.getStore(peer.GetStoreId())
		if store == nil {
			continue
		}
		if !store.isUp() {
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
