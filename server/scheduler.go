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

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() ResourceKind
	GetResourceLimit() uint64
	Prepare(cluster *clusterInfo)
	Cleanup(cluster *clusterInfo)
	Schedule(cluster *clusterInfo) Operator
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	opt     *scheduleOption
	StoreID uint64 `json:"store_id"`
}

func newGrantLeaderScheduler(opt *scheduleOption, storeID uint64) *grantLeaderScheduler {
	return &grantLeaderScheduler{opt: opt, StoreID: storeID}
}

func (s *grantLeaderScheduler) GetName() string {
	return "grant-leader-scheduler"
}

func (s *grantLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *grantLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Prepare(cluster *clusterInfo) {
	for id := range s.stores {
		cluster.blockStore(id)
	}
}

func (s *grantLeaderScheduler) Cleanup(cluster *clusterInfo) {
	for id := range s.stores {
		cluster.unblockStore(id)
	}
}

func (s *grantLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	for id := range s.stores {
		region := cluster.randFollowerRegion(id)
		if region == nil {
			continue
		}
		if _, ok := s.stores[region.Leader.StoreId]; ok {
			continue
		}
		return newTransferLeader(region, region.GetStorePeer(id))
	}
	return nil
}

type evictLeaderScheduler struct {
	stores map[uint64]struct{}
}

func newEvictLeaderScheduler(stores map[uint64]struct{}) *evictLeaderScheduler {
	return &evictLeaderScheduler{stores: stores}
}

func (s *evictLeaderScheduler) GetName() string {
	return "evict-leader-scheduler"
}

func (s *evictLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *evictLeaderScheduler) Prepare(cluster *clusterInfo) {
	for id := range s.stores {
		cluster.blockStore(id)
	}
}

func (s *evictLeaderScheduler) Cleanup(cluster *clusterInfo) {
	for id := range s.stores {
		cluster.unblockStore(id)
	}
}

func (s *evictLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	for id := range s.stores {
		region := cluster.randLeaderRegion(id)
		if region == nil {
			continue
		}
		for _, peer := range region.GetFollowers() {
			if _, ok := s.stores[peer.GetStoreId()]; ok {
				continue
			}
			return newTransferLeader(region, peer)
		}
		// TODO: no available followers, we need to transfer peers to other stores first.
	}
	return nil
}

type shuffleLeaderScheduler struct {
	opt      *scheduleOption
	selector Selector
	selected *metapb.Peer
}

func newShuffleLeaderScheduler(opt *scheduleOption) *shuffleLeaderScheduler {
	return &shuffleLeaderScheduler{
		opt:      opt,
		selector: newRandomSelector(),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return "shuffle-leader-scheduler"
}

func (s *shuffleLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *shuffleLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Prepare(cluster *clusterInfo) {}

func (s *shuffleLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (s *shuffleLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	// We shuffle leaders between stores:
	// 1. select a store randomly.
	// 2. transfer a leader from the store to another store.
	// 3. transfer a leader to the store from another store.
	// These will not change store's leader count, but swap leaders between stores.

	// Select a store and transfer a leader from it.
	if s.selected == nil {
		region, newLeader := scheduleTransferLeader(cluster, s.selector)
		if region == nil {
			return nil
		}
		// Mark the selected store.
		s.selected = region.Leader
		return newTransferLeader(region, newLeader)
	}

	// Reset the selected store.
	storeID := s.selected.GetStoreId()
	s.selected = nil

	// Transfer a leader to the selected store.
	region := cluster.randFollowerRegion(storeID)
	if region == nil {
		return nil
	}
	return newTransferLeader(region, region.GetStorePeer(storeID))
}

func newAddPeer(region *regionInfo, peer *metapb.Peer) Operator {
	addPeer := newAddPeerOperator(region.GetId(), peer)
	return newRegionOperator(region, addPeer)
}

func newRemovePeer(region *regionInfo, peer *metapb.Peer) Operator {
	removePeer := newRemovePeerOperator(region.GetId(), peer)
	return newRegionOperator(region, removePeer)
}

func newTransferPeer(region *regionInfo, oldPeer, newPeer *metapb.Peer) Operator {
	addPeer := newAddPeerOperator(region.GetId(), newPeer)
	removePeer := newRemovePeerOperator(region.GetId(), oldPeer)
	return newRegionOperator(region, addPeer, removePeer)
}

func newTransferLeader(region *regionInfo, newLeader *metapb.Peer) Operator {
	transferLeader := newTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return newRegionOperator(region, transferLeader)
}

// scheduleAddPeer schedules a new peer.
func scheduleAddPeer(cluster *clusterInfo, s Selector, filters ...Filter) *metapb.Peer {
	stores := cluster.getStores()

	target := s.SelectTarget(stores, filters...)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newPeer
}

// scheduleRemovePeer schedules a region to remove the peer.
func scheduleRemovePeer(cluster *clusterInfo, s Selector, filters ...Filter) (*regionInfo, *metapb.Peer) {
	stores := cluster.getStores()

	source := s.SelectSource(stores, filters...)
	if source == nil {
		return nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}

// scheduleTransferLeader schedules a region to transfer leader to the peer.
func scheduleTransferLeader(cluster *clusterInfo, s Selector, filters ...Filter) (*regionInfo, *metapb.Peer) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores, filters...)
	if source == nil {
		return nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil
	}

	targetStores := cluster.getFollowerStores(region)

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil
	}

	return region, region.GetStorePeer(target.GetId())
}
