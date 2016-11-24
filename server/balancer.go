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
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type leaderBalancer struct {
	opt      *scheduleOption
	selector Selector
}

func newLeaderBalancer(opt *scheduleOption) *leaderBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newLeaderCountFilter(opt))

	return &leaderBalancer{
		opt:      opt,
		selector: newBalanceSelector(leaderKind, filters),
	}
}

func (l *leaderBalancer) GetName() string {
	return "leader_balancer"
}

func (l *leaderBalancer) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *leaderBalancer) Schedule(cluster *clusterInfo) *balanceOperator {
	region, source, target := scheduleLeader(cluster, l.selector)
	if region == nil {
		return nil
	}

	diff := source.leaderRatio() - target.leaderRatio()
	if diff < l.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	newLeader := region.GetStorePeer(target.GetId())
	if newLeader == nil {
		return nil
	}

	transferLeader := newTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return newBalanceOperator(region, balanceOP, transferLeader)
}

type storageBalancer struct {
	opt      *scheduleOption
	selector Selector
}

func newStorageBalancer(opt *scheduleOption) *storageBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newRegionCountFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &storageBalancer{
		opt:      opt,
		selector: newBalanceSelector(storageKind, filters),
	}
}

func (s *storageBalancer) GetName() string {
	return "storage_balancer"
}

func (s *storageBalancer) GetResourceKind() ResourceKind {
	return storageKind
}

func (s *storageBalancer) Schedule(cluster *clusterInfo) *balanceOperator {
	region, source, target := scheduleStorage(cluster, s.selector)
	if region == nil {
		return nil
	}

	diff := source.storageRatio() - target.storageRatio()
	if diff < s.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	peer := region.GetStorePeer(source.GetId())
	newPeer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	addPeer := newAddPeerOperator(region.GetId(), newPeer)
	removePeer := newRemovePeerOperator(region.GetId(), peer)
	return newBalanceOperator(region, balanceOP, addPeer, removePeer)
}

// replicaChecker ensures region has enough replicas.
type replicaChecker struct {
	cluster  *clusterInfo
	opt      *scheduleOption
	selector Selector
}

func newReplicaChecker(cluster *clusterInfo, opt *scheduleOption) *replicaChecker {
	var filters []Filter
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &replicaChecker{
		cluster:  cluster,
		opt:      opt,
		selector: newBalanceSelector(storageKind, filters),
	}
}

func (r *replicaChecker) GetResourceKind() ResourceKind {
	return r.kind
}

func (r *replicaChecker) Check(region *regionInfo) *balanceOperator {
	badPeers := r.collectBadPeers(region)
	peerCount := len(region.GetPeers())
	maxPeerCount := int(r.cluster.getMeta().GetMaxPeerCount())

	if peerCount-len(badPeers) < maxPeerCount {
		return r.addPeer(region)
	}
	if peerCount > maxPeerCount {
		return r.removePeer(region, badPeers)
	}
	return nil
}

func (r *replicaChecker) addPeer(region *regionInfo) *balanceOperator {
	stores := r.cluster.getStores()

	filter := newExcludedFilter(nil, region.GetStoreIds())
	target := r.selector.SelectTarget(stores, filter)
	if target == nil {
		return nil
	}

	peer, err := r.cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocated peer: %v", err)
		return nil
	}

	addPeer := newAddPeerOperator(region.GetId(), peer)
	return newBalanceOperator(region, replicaOP, newOnceOperator(addPeer))
}

func (r *replicaChecker) removePeer(region *regionInfo, badPeers []*metapb.Peer) *balanceOperator {
	var peer *metapb.Peer

	if len(badPeers) >= 1 {
		peer = badPeers[0]
	} else {
		stores := r.cluster.getFollowerStores(region)
		source := r.selector.SelectSource(stores)
		if source != nil {
			peer = region.GetStorePeer(source.GetId())
		}
	}
	if peer == nil {
		return nil
	}

	removePeer := newRemovePeerOperator(region.GetId(), peer)
	return newBalanceOperator(region, replicaOP, newOnceOperator(removePeer))
}

func (r *replicaChecker) collectBadPeers(region *regionInfo) []*metapb.Peer {
	downPeers := r.collectDownPeers(region)

	var badPeers []*metapb.Peer
	for _, peer := range region.GetPeers() {
		if _, ok := downPeers[peer.GetId()]; ok {
			badPeers = append(badPeers, peer)
			continue
		}
		store := r.cluster.getStore(peer.GetStoreId())
		if store == nil || !store.isUp() {
			badPeers = append(badPeers, peer)
		}
	}

	return badPeers
}

func (r *replicaChecker) collectDownPeers(region *regionInfo) map[uint64]*metapb.Peer {
	downPeers := make(map[uint64]*metapb.Peer)
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		if stats.GetDownSeconds() > uint64(r.opt.GetMaxStoreDownTime().Seconds()) {
			downPeers[peer.GetId()] = peer
		}
	}
	return downPeers
}
