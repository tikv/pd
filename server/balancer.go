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
	Balance(cluster *clusterInfo) (float64, *balanceOperator, error)
	// GetResourceKind returns resource kind.
	GetResourceKind() ResourceKind
}

type capacityBalancer struct {
	cfg      *BalanceConfig
	kind     ResourceKind
	selector Selector
}

func newCapacityBalancer(cfg *BalanceConfig) *capacityBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(cfg))
	filters = append(filters, newCapacityFilter(cfg))
	filters = append(filters, newSnapCountFilter(cfg))

	cb := &capacityBalancer{cfg: cfg, kind: storageKind}
	cb.selector = newBalanceSelector(cb.kind, filters)
	return cb
}

func (cb *capacityBalancer) GetResourceKind() ResourceKind {
	return cb.kind
}

// Balance tries to select a store region to do balance.
// The balance type is follower balance.
func (cb *capacityBalancer) Balance(cluster *clusterInfo) (float64, *balanceOperator, error) {
	region, source, target := scheduleStorage(cluster, cb.selector)
	if region == nil {
		return 0, nil, nil
	}

	// Check diff score.
	diff := source.resourceRatio(cb.kind) - target.resourceRatio(cb.kind)
	if diff < cb.cfg.MaxDiffScoreFraction {
		return 0, nil, nil
	}

	// If region peer count is not equal to max peer count, no need to do balance.
	if len(region.GetPeers()) != int(cluster.getMeta().GetMaxPeerCount()) {
		return 0, nil, nil
	}

	peer := region.GetStorePeer(source.GetId())
	newPeer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	addPeerOperator := newAddPeerOperator(region.GetId(), newPeer)
	removePeerOperator := newRemovePeerOperator(region.GetId(), peer)
	return diff, newBalanceOperator(region, balanceOP, addPeerOperator, removePeerOperator), nil
}

type leaderBalancer struct {
	cfg      *BalanceConfig
	kind     ResourceKind
	selector Selector
}

func newLeaderBalancer(cfg *BalanceConfig) *leaderBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(cfg))
	filters = append(filters, newLeaderCountFilter(cfg))

	lb := &leaderBalancer{cfg: cfg, kind: leaderKind}
	lb.selector = newBalanceSelector(lb.kind, filters)
	return lb
}

func (lb *leaderBalancer) GetResourceKind() ResourceKind {
	return lb.kind
}

// Balance tries to select a store region to do balance.
// The balance type is leader transfer.
func (lb *leaderBalancer) Balance(cluster *clusterInfo) (float64, *balanceOperator, error) {
	region, source, target := scheduleLeader(cluster, lb.selector)
	if region == nil {
		return 0, nil, nil
	}

	diff := source.resourceRatio(lb.kind) - target.resourceRatio(lb.kind)
	if diff < lb.cfg.MaxDiffScoreFraction {
		return 0, nil, nil
	}

	regionID := region.GetId()
	newLeader := region.GetStorePeer(target.GetId())
	transferLeaderOperator := newTransferLeaderOperator(regionID, region.Leader, newLeader, lb.cfg)
	return diff, newBalanceOperator(region, balanceOP, transferLeaderOperator), nil
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

func (rb *replicaBalancer) addPeer(cluster *clusterInfo) *balanceOperator {
	stores := cluster.getStores()

	filter := newExcludedFilter(nil, rb.region.GetStoreIds())
	target := rb.selector.SelectTarget(stores, filter)
	if target == nil {
		return nil
	}

	peer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocated peer: %v", err)
		return nil
	}

	addPeerOperator := newAddPeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, replicaOP, newOnceOperator(addPeerOperator))
}

func (rb *replicaBalancer) removePeer(cluster *clusterInfo, badPeers []*metapb.Peer) *balanceOperator {
	var peer *metapb.Peer

	if len(badPeers) >= 1 {
		peer = badPeers[0]
	} else {
		stores := cluster.getFollowerStores(rb.region)
		source := rb.selector.SelectSource(stores)
		if source != nil {
			peer = rb.region.GetStorePeer(source.GetId())
		}
	}

	if peer == nil {
		return nil
	}

	removePeerOperator := newRemovePeerOperator(rb.region.GetId(), peer)
	return newBalanceOperator(rb.region, replicaOP, newOnceOperator(removePeerOperator))
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

func (rb *replicaBalancer) Balance(cluster *clusterInfo) (float64, *balanceOperator, error) {
	badPeers := rb.collectBadPeers(cluster)
	peerCount := len(rb.region.GetPeers())
	maxPeerCount := int(cluster.getMeta().GetMaxPeerCount())

	if len(badPeers) > 0 {
		log.Debugf("region: %v peers: %v bad peers: %v",
			rb.region.GetId(), rb.region.GetPeers(), badPeers)
	}

	if peerCount-len(badPeers) < maxPeerCount {
		return 0, rb.addPeer(cluster), nil
	} else if peerCount > maxPeerCount {
		return 0, rb.removePeer(cluster, badPeers), nil
	}

	return 0, nil, nil
}
