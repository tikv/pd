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

import "github.com/pingcap/kvproto/pkg/metapb"

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() ResourceKind
	Schedule(cluster *clusterInfo) Operator
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	StoreID uint64 `json:"store_id"`
}

func newGrantLeaderScheduler(storeID uint64) *grantLeaderScheduler {
	return &grantLeaderScheduler{StoreID: storeID}
}

func (s *grantLeaderScheduler) GetName() string {
	return "grant-leader-scheduler"
}

func (s *grantLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *grantLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	region := cluster.randFollowerRegion(s.StoreID)
	if region == nil {
		return nil
	}
	return newTransferLeader(region, s.StoreID)
}

type shuffleLeaderScheduler struct {
	selector Selector
	source   *storeInfo
}

func newShuffleLeaderScheduler() *shuffleLeaderScheduler {
	return &shuffleLeaderScheduler{
		selector: newRandomSelector(),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return "shuffle-leader-scheduler"
}

func (s *shuffleLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *shuffleLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	// We shuffle leaders between stores:
	// 1. select a store as a source store randomly.
	// 2. transfer a leader from the store to another store.
	// 3. transfer a leader to the store from another store.
	// These will not change store's leader count, but swap leaders between stores.

	// Select a source store and transfer a leader from it.
	if s.source == nil {
		region, source, target := scheduleLeader(cluster, s.selector)
		if region == nil {
			return nil
		}
		s.source = source // Mark the source store.
		return newTransferLeader(region, target.GetId())
	}

	// Reset the source store.
	source := s.source
	s.source = nil

	// Transfer a leader to the source store.
	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		return nil
	}
	return newTransferLeader(region, source.GetId())
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

func newTransferLeader(region *regionInfo, storeID uint64) Operator {
	newLeader := region.GetStorePeer(storeID)
	if newLeader == nil {
		return nil
	}
	transferLeader := newTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return newRegionOperator(region, transferLeader)
}

// scheduleLeader schedules a region to transfer leader from the source store to the target store.
func scheduleLeader(cluster *clusterInfo, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil, nil
	}

	targetStores := cluster.getFollowerStores(region)

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}

// scheduleStorage schedules a region to transfer peer from the source store to the target store.
func scheduleStorage(cluster *clusterInfo, opt *scheduleOption, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	stores := cluster.getStores()

	source := s.SelectSource(stores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		return nil, nil, nil
	}

	if len(region.GetPeers()) != opt.GetMaxReplicas() {
		// We only schedule region with just enough replicas.
		return nil, nil, nil
	}

	excluded := newExcludedFilter(nil, region.GetStoreIds())
	target := s.SelectTarget(stores, excluded)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}
