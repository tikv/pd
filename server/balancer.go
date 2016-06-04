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
	Balance(cluster *raftCluster) (*BalanceOperator, error)
}

const (
	// If the used ratio of one storage is greater than this value,
	// it will never be used as a selected target and it should be rebalanced.
	maxCapacityUsedRatio = 0.4
)

var (
	_ Balancer = &capacityBalancer{}
)

type capacityBalancer struct {
}

func (cb *capacityBalancer) SelectFromStore(stores []*StoreInfo) *StoreInfo {
	var resultStore *StoreInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if store.usedRatio() <= maxCapacityUsedRatio {
			continue
		}

		if resultStore == nil {
			resultStore = store
			continue
		}

		if store.usedRatio() > resultStore.usedRatio() {
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) SelectToStore(stores []*StoreInfo) *StoreInfo {
	var resultStore *StoreInfo
	for _, store := range stores {
		if store == nil {
			continue
		}

		if resultStore == nil {
			resultStore = store
			continue
		}

		if store.usedRatio() < resultStore.usedRatio() {
			resultStore = store
		}
	}

	return resultStore
}

func (cb *capacityBalancer) SelectBalanceRegion(stores []*StoreInfo, regions *RegionsInfo) (*metapb.Region, *metapb.Peer) {
	store := cb.SelectFromStore(stores)
	if store == nil {
		return nil, nil
	}

	// Random select one leader region from store.
	storeID := store.store.GetId()
	region := regions.randRegion(storeID)
	leaderPeer := leaderPeer(region, storeID)
	return region, leaderPeer
}

func (cb *capacityBalancer) SelectNewLeaderPeer(cluster *ClusterInfo, peers map[uint64]*metapb.Peer) *metapb.Peer {
	stores := make([]*StoreInfo, 0, len(peers))
	for storeID := range peers {
		stores = append(stores, cluster.getStore(storeID))
	}

	store := cb.SelectToStore(stores)
	if store == nil {
		return nil
	}

	storeID := store.store.GetId()
	return peers[storeID]
}

func (cb *capacityBalancer) Balance(cluster *raftCluster) (*BalanceOperator, error) {
	// Firstly, select one balance region from cluster info.
	stores := cluster.cachedCluster.getStores()
	region, oldLeader := cb.SelectBalanceRegion(stores, cluster.cachedCluster.regions)
	if region == nil || oldLeader == nil {
		log.Warn("Region cannot be found to do balance")
		return nil, nil
	}

	// Secondly, select one region peer to do leader transfer.
	followerPeers := make(map[uint64]*metapb.Peer)
	for _, peer := range region.GetPeers() {
		if peer.GetId() == oldLeader.GetId() {
			continue
		}

		storeID := peer.GetStoreId()
		followerPeers[storeID] = peer
	}

	newLeader := cb.SelectNewLeaderPeer(cluster.cachedCluster, followerPeers)
	if newLeader == nil {
		log.Warn("New leader peer cannot be found to do balance")
		return nil, nil
	}

	// Thirdly, select one store to add new peer.
	store := cb.SelectToStore(stores)
	if store == nil {
		log.Warn("To store cannot be found to do balance")
		return nil, nil
	}

	peerID, err := cluster.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPeer := &metapb.Peer{
		Id:      proto.Uint64(peerID),
		StoreId: proto.Uint64(store.store.GetId()),
	}

	leaderTransferOperator := newTransferLeaderOperator(oldLeader, newLeader)
	addPeerOperator := newAddPeerOperator(newPeer)
	removePeerOperator := newRemovePeerOperator(oldLeader)

	return newBalanceOperator(region, leaderTransferOperator, addPeerOperator, removePeerOperator), nil
}
