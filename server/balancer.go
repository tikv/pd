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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

// Operator is the interface to do some operations.
type Operator interface {
	// Do does the operator, if finished then return true.
	Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.ChangePeer, error)
	// Check checks whether the operator has been finished.
	Check(region *metapb.Region, leader *metapb.Peer) (bool, error)
}

func containPeer(region *metapb.Region, peer *metapb.Peer) bool {
	for _, p := range region.GetPeers() {
		if p.GetId() == peer.GetId() {
			return true
		}
	}

	return false
}

func leaderPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}

	return nil
}

// BalanceOperator is used to do region balance.
type BalanceOperator struct {
	index  int
	ops    []Operator
	region *metapb.Region
}

func newBalanceOperator(region *metapb.Region, ops ...Operator) *BalanceOperator {
	return &BalanceOperator{
		index:  0,
		ops:    ops,
		region: region,
	}
}

// Check implements Operator.Check interface.
func (bo *BalanceOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if bo.index >= len(bo.ops) {
		return true, nil
	}

	err := checkStaleRegion(bo.region, region)
	if err != nil {
		return false, errors.Trace(err)
	}

	bo.region = cloneRegion(region)

	return false, nil
}

// Do implements Operator.Do interface.
func (bo *BalanceOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.ChangePeer, error) {
	// TODO: optimize it to return next operator response directly.
	ok, err := bo.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	finished, res, err := bo.ops[bo.index].Do(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if !finished {
		return false, res, nil
	}

	bo.index++
	return false, nil, nil
}

// ChangePeerOperator is used to do peer change.
type ChangePeerOperator struct {
	changePeer *pdpb.ChangePeer
}

func newAddPeerOperator(peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
			Peer:       peer,
		},
	}
}

func newRemovePeerOperator(peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
			Peer:       peer,
		},
	}
}

// Check implements Operator.Check interface.
func (co *ChangePeerOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if region == nil {
		return false, errors.New("invalid region")
	}
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	if co.changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
		if containPeer(region, leader) {
			return true, nil
		}
	} else if co.changePeer.GetChangeType() == raftpb.ConfChangeType_RemoveNode {
		if !containPeer(region, leader) {
			return true, nil
		}
	}

	return false, nil
}

// Do implements Operator.Do interface.
func (co *ChangePeerOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.ChangePeer, error) {
	ok, err := co.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	return false, co.changePeer, nil
}

// TransferLeaderOperator is used to do leader transfer.
type TransferLeaderOperator struct {
	oldLeader *metapb.Peer
	newLeader *metapb.Peer
}

func newTransferLeaderOperator(oldLeader, newLeader *metapb.Peer) *TransferLeaderOperator {
	return &TransferLeaderOperator{
		oldLeader: oldLeader,
		newLeader: newLeader,
	}
}

// Check implements Operator.Check interface.
func (lto *TransferLeaderOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	// If the leader has already been changed to new leader, we finish it.
	if leader.GetId() == lto.newLeader.GetId() {
		return true, nil
	}

	// If the old leader has been changed but not be new leader, we also finish it.
	if leader.GetId() != lto.oldLeader.GetId() {
		return true, nil
	}

	return false, nil
}

// Do implements Operator.Doop interface.
func (lto *TransferLeaderOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.ChangePeer, error) {
	ok, err := lto.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	// TODO: call admin command, maybe we can send it to a channel.
	/*
		res := &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader.Enum(),
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: lto.newLeader,
			},
		}
	*/

	return false, nil, nil
}

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
