package server

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

// Operator is the interface to do some operations.
type Operator interface {
	// Do does the operator, if finished then return true.
	Do(req *pdpb.RegionHeartbeatRequest) (bool, *pdpb.RegionHeartbeatResponse)
	// Check checks whether the operator has been finished.
	Check(req *pdpb.RegionHeartbeatRequest) bool
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
	index int
	ops   []Operator
}

func newBalanceOperator(oldLeader, newLeader, newPeer *metapb.Peer) *BalanceOperator {
	leaderTransferOperator := newTransferLeaderOperator(oldLeader, newLeader)

	addPeer := &pdpb.ChangePeer{
		ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
		Peer:       newPeer,
	}
	addPeerOperator := newChangePeerOperator(addPeer)

	removePeer := &pdpb.ChangePeer{
		ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
		Peer:       oldLeader,
	}
	removePeerOperator := newChangePeerOperator(removePeer)

	return &BalanceOperator{
		index: 0,
		ops:   []Operator{leaderTransferOperator, addPeerOperator, removePeerOperator},
	}
}

// Check implements Operator.Check interface.
func (bo *BalanceOperator) Check(req *pdpb.RegionHeartbeatRequest) bool {
	if bo.index >= len(bo.ops) {
		return true
	}

	return false
}

// Do implements Operator.Do interface.
func (bo *BalanceOperator) Do(req *pdpb.RegionHeartbeatRequest) (bool, *pdpb.RegionHeartbeatResponse) {
	// TODO: optimize it to return next operator response directly.
	if bo.Check(req) {
		return true, nil
	}

	finished, res := bo.ops[bo.index].Do(req)
	if !finished {
		return false, res
	}

	bo.index++
	return false, nil
}

// ChangePeerOperator is used to do peer change.
type ChangePeerOperator struct {
	changePeer *pdpb.ChangePeer
}

func newChangePeerOperator(changePeer *pdpb.ChangePeer) *ChangePeerOperator {
	return &ChangePeerOperator{
		changePeer: changePeer,
	}
}

// Check implements Operator.Check interface.
func (co *ChangePeerOperator) Check(req *pdpb.RegionHeartbeatRequest) bool {
	region := req.GetRegion()
	if region == nil {
		return false
	}

	leaderPeer := req.GetLeader()
	if leaderPeer == nil {
		return false
	}

	if co.changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
		if containPeer(region, leaderPeer) {
			return true
		}
	} else if co.changePeer.GetChangeType() == raftpb.ConfChangeType_RemoveNode {
		if !containPeer(region, leaderPeer) {
			return true
		}
	}

	return false
}

// Do implements Operator.Do interface.
func (co *ChangePeerOperator) Do(req *pdpb.RegionHeartbeatRequest) (bool, *pdpb.RegionHeartbeatResponse) {
	if co.Check(req) {
		return true, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: co.changePeer,
	}
	return false, res
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
func (lto *TransferLeaderOperator) Check(req *pdpb.RegionHeartbeatRequest) bool {
	leader := req.GetLeader()
	if leader == nil {
		return false
	}

	if leader.GetId() == lto.newLeader.GetId() {
		return true
	}

	return false
}

// Do implements Operator.Doop interface.
func (lto *TransferLeaderOperator) Do(req *pdpb.RegionHeartbeatRequest) (bool, *pdpb.RegionHeartbeatResponse) {
	if lto.Check(req) {
		return true, nil
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

	return false, nil
}

// Balancer is an interface to select store regions for auto-balance.
type Balancer interface {
	Balance(cluster *ClusterInfo, c *raftCluster) (*BalanceOperator, error)
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

func (cb *capacityBalancer) Balance(cluster *ClusterInfo, c *raftCluster) (*BalanceOperator, error) {
	// Firstly, select one balance region from cluster info.
	stores := cluster.getStores()
	region, oldLeader := cb.SelectBalanceRegion(stores, cluster.regions)
	if region == nil || oldLeader == nil {
		return nil, errors.New("Region cannot be found to do balance")
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

	newLeader := cb.SelectNewLeaderPeer(cluster, followerPeers)
	if newLeader == nil {
		return nil, errors.New("New leader peer cannot be found to do balance")
	}

	// Thirdly, select one store to add new peer.
	store := cb.SelectToStore(stores)
	if store == nil {
		return nil, errors.New("To store cannot be found to do balance")
	}

	peerID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPeer := &metapb.Peer{
		Id:      proto.Uint64(peerID),
		StoreId: proto.Uint64(store.store.GetId()),
	}

	return newBalanceOperator(oldLeader, newLeader, newPeer), nil
}
