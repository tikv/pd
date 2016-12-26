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
	return "leader-balancer"
}

func (l *leaderBalancer) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *leaderBalancer) Schedule(cluster *clusterInfo) Operator {
	region, source, target := scheduleLeader(cluster, l.selector)
	if region == nil {
		return nil
	}

	diff := source.leaderRatio() - target.leaderRatio()
	if diff < l.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	return newTransferLeader(region, target.GetId())
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
	return "storage-balancer"
}

func (s *storageBalancer) GetResourceKind() ResourceKind {
	return storageKind
}

func (s *storageBalancer) Schedule(cluster *clusterInfo) Operator {
	region, source, target := scheduleStorage(cluster, s.opt, s.selector)
	if region == nil {
		return nil
	}

	diff := source.storageRatio() - target.storageRatio()
	if diff < s.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	oldPeer := region.GetStorePeer(source.GetId())
	newPeer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newTransferPeer(region, oldPeer, newPeer)
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

func (r *replicaChecker) Check(region *regionInfo) Operator {
	// If we have bad peer, we remove it first.
	for _, peer := range r.collectBadPeers(region) {
		return newRemovePeer(region, peer)
	}

	stores := r.cluster.getRegionStores(region)
	result := r.opt.GetConstraints().Match(stores)

	// If we have redundant replicas, we can remove unmatched peers.
	if len(stores) > r.opt.GetMaxReplicas() {
		for _, store := range stores {
			if _, ok := result.stores[store.GetId()]; !ok {
				return newRemovePeer(region, region.GetStorePeer(store.GetId()))
			}
		}
	}

	// Make sure all constraints will be satisfied.
	for _, matched := range result.constraints {
		if len(matched.stores) < matched.constraint.Replicas {
			constraint := newConstraintFilter(nil, matched.constraint)
			if op := r.addPeer(region, constraint); op != nil {
				return op
			}
		}
	}
	if len(stores) < r.opt.GetMaxReplicas() {
		// No matter whether we can satisfy all constraints or not,
		// we should at least ensure that the region has enough replicas.
		return r.addPeer(region)
	}

	return nil
}

func (r *replicaChecker) addPeer(region *regionInfo, filters ...Filter) Operator {
	stores := r.cluster.getStores()

	excluded := newExcludedFilter(nil, region.GetStoreIds())
	target := r.selector.SelectTarget(stores, append(filters, excluded)...)
	if target == nil {
		return nil
	}

	newPeer, err := r.cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newAddPeer(region, newPeer)
}

func (r *replicaChecker) collectBadPeers(region *regionInfo) map[uint64]*metapb.Peer {
	badPeers := r.collectDownPeers(region)
	for _, peer := range region.GetPeers() {
		store := r.cluster.getStore(peer.GetStoreId())
		if store == nil || !store.isUp() {
			badPeers[peer.GetStoreId()] = peer
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
		store := r.cluster.getStore(peer.GetStoreId())
		if store.downTime() < r.opt.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opt.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		downPeers[peer.GetStoreId()] = peer
	}
	return downPeers
}
