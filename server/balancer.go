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
	region, newLeader := scheduleTransferLeader(cluster, l.selector)
	if region == nil {
		return nil
	}

	source := cluster.getStore(region.Leader.GetStoreId())
	target := cluster.getStore(newLeader.GetStoreId())
	if source.leaderRatio()-target.leaderRatio() < l.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	return newTransferLeader(region, newLeader)
}

type storageBalancer struct {
	opt      *scheduleOption
	rep      *Replication
	selector Selector
}

func newStorageBalancer(opt *scheduleOption) *storageBalancer {
	var filters []Filter
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newRegionCountFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &storageBalancer{
		opt:      opt,
		rep:      opt.GetReplication(),
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
	// Select a peer from the store with largest storage ratio.
	region, oldPeer := scheduleRemovePeer(cluster, s.selector)
	if region == nil {
		return nil
	}

	// We don't schedule region with abnormal number of replicas.
	if len(region.GetPeers()) != s.rep.GetMaxReplicas() {
		return nil
	}

	stores := cluster.getRegionStores(region)
	source := cluster.getStore(oldPeer.GetStoreId())

	// Allocate a new peer from the store with smallest storage ratio.
	// We need to ensure the target store will not break the replication constraints.
	excluded := newExcludedFilter(nil, region.GetStoreIds())
	replication := newReplicationFilter(s.rep, stores, source)
	newPeer := scheduleAddPeer(cluster, s.selector, excluded, replication)
	if newPeer == nil {
		return nil
	}

	target := cluster.getStore(newPeer.GetStoreId())
	if source.storageRatio()-target.storageRatio() < s.opt.GetMinBalanceDiffRatio() {
		return nil
	}

	return newTransferPeer(region, oldPeer, newPeer)
}

// replicaChecker ensures region has the best replicas.
type replicaChecker struct {
	opt     *scheduleOption
	rep     *Replication
	cluster *clusterInfo
	filters []Filter
}

func newReplicaChecker(opt *scheduleOption, cluster *clusterInfo) *replicaChecker {
	var filters []Filter
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &replicaChecker{
		opt:     opt,
		rep:     opt.GetReplication(),
		cluster: cluster,
		filters: filters,
	}
}

func (r *replicaChecker) Check(region *regionInfo) Operator {
	if op := r.checkDownPeer(region); op != nil {
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		return op
	}

	if len(region.GetPeers()) < r.rep.GetMaxReplicas() {
		newPeer, _ := r.addPeer(region)
		if newPeer == nil {
			return nil
		}
		return newAddPeer(region, newPeer)
	}

	if len(region.GetPeers()) > r.rep.GetMaxReplicas() {
		oldPeer, _ := r.removePeer(region)
		if oldPeer == nil {
			return nil
		}
		return newRemovePeer(region, oldPeer)
	}

	return r.checkBetterPeer(region)
}

// addPeer returns the best peer in other stores.
func (r *replicaChecker) addPeer(region *regionInfo, filters ...Filter) (*metapb.Peer, int) {
	filters = append(filters, r.filters...)
	filters = append(filters, newExcludedFilter(nil, region.GetStoreIds()))

	var (
		maxStore *storeInfo
		maxScore int
	)

	// Find the store with maximum score.
	regionStores := r.cluster.getRegionStores(region)
	for _, store := range r.cluster.getStores() {
		if filterTarget(store, filters) {
			continue
		}
		score := r.rep.GetReplicaScore(regionStores, store)
		if maxStore == nil || compareStoreScore(store, score, maxStore, maxScore) > 0 {
			maxStore = store
			maxScore = score
		}
	}

	if maxStore == nil {
		return nil, 0
	}

	newPeer, err := r.cluster.allocPeer(maxStore.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil, 0
	}
	return newPeer, maxScore
}

// removePeer returns the worst peer in the region.
func (r *replicaChecker) removePeer(region *regionInfo, filters ...Filter) (*metapb.Peer, int) {
	filters = append(filters, r.filters...)

	var (
		minStore *storeInfo
		minScore int
	)

	// Find the store with minimum score.
	regionStores := r.cluster.getRegionStores(region)
	for _, store := range regionStores {
		if filterSource(store, filters) {
			continue
		}
		score := r.rep.GetReplicaScore(regionStores, store)
		if minStore == nil || compareStoreScore(store, score, minStore, minScore) < 0 {
			minStore = store
			minScore = score
		}
	}

	if minStore == nil {
		return nil, 0
	}
	return region.GetStorePeer(minStore.GetId()), minScore
}

// replacePeer returns the best peer to replace the region peer.
func (r *replicaChecker) replacePeer(region *regionInfo, peer *metapb.Peer) (*metapb.Peer, int) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	// Get the best peer in other stores.
	return r.addPeer(newRegion, newExcludedFilter(nil, region.GetStoreIds()))
}

func (r *replicaChecker) checkDownPeer(region *regionInfo) Operator {
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
		return newRemovePeer(region, peer)
	}
	return nil
}

func (r *replicaChecker) checkOfflinePeer(region *regionInfo) Operator {
	for _, peer := range region.GetPeers() {
		store := r.cluster.getStore(peer.GetStoreId())
		if store == nil || store.isUp() {
			continue
		}
		newPeer, _ := r.replacePeer(region, peer)
		if newPeer == nil {
			return nil
		}
		return newTransferPeer(region, peer, newPeer)
	}
	return nil
}

func (r *replicaChecker) checkBetterPeer(region *regionInfo) Operator {
	oldPeer, oldScore := r.removePeer(region)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.replacePeer(region, oldPeer)
	if newPeer == nil {
		return nil
	}
	// We can't find a better peer.
	if newScore <= oldScore {
		return nil
	}
	return newTransferPeer(region, oldPeer, newPeer)
}
