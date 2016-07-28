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
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testBalancerSuite{})

type testBalancerSuite struct {
	testClusterBaseSuite

	cfg *BalanceConfig
}

func (s *testBalancerSuite) getRootPath() string {
	return "test_balancer"
}

func (s *testBalancerSuite) SetUpSuite(c *C) {
	s.cfg = newBalanceConfig()
	s.cfg.adjust()
}

func (s *testBalancerSuite) newClusterInfo(c *C) *clusterInfo {
	clusterInfo := newClusterInfo(s.getRootPath())
	clusterInfo.idAlloc = newMockIDAllocator()

	// Set cluster info.
	meta := &metapb.Cluster{
		Id:           proto.Uint64(0),
		MaxPeerCount: proto.Uint32(3),
	}
	clusterInfo.setMeta(meta)

	var (
		id   uint64
		peer *metapb.Peer
		err  error
	)

	// Add 4 stores, store id will be 1,2,3,4.
	for i := 1; i < 5; i++ {
		id, err = clusterInfo.idAlloc.Alloc()
		c.Assert(err, IsNil)

		addr := fmt.Sprintf("127.0.0.1:%d", i)
		store := s.newStore(c, id, addr)
		clusterInfo.addStore(store)
	}

	// Add 1 peer, id will be 5.
	id, err = clusterInfo.idAlloc.Alloc()
	c.Assert(err, IsNil)
	peer = s.newPeer(c, 1, id)

	// Add 1 region, id will be 6.
	id, err = clusterInfo.idAlloc.Alloc()
	c.Assert(err, IsNil)

	region := s.newRegion(c, id, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)
	clusterInfo.regions.addRegion(region)

	// Set leader store region.
	clusterInfo.regions.leaders.update(region.GetId(), peer.GetStoreId())

	stores := clusterInfo.getStores()
	c.Assert(stores, HasLen, 4)

	return clusterInfo
}

func (s *testBalancerSuite) updateStore(c *C, clusterInfo *clusterInfo, storeID uint64, capacity uint64, available uint64,
	sendingSnapCount uint32, receivingSnapCount uint32) {
	stats := &pdpb.StoreStats{
		StoreId:            proto.Uint64(storeID),
		Capacity:           proto.Uint64(capacity),
		Available:          proto.Uint64(available),
		SendingSnapCount:   proto.Uint32(sendingSnapCount),
		ReceivingSnapCount: proto.Uint32(receivingSnapCount),
	}

	ok := clusterInfo.updateStoreStatus(stats)
	c.Assert(ok, IsTrue)
}

func (s *testBalancerSuite) addRegionPeer(c *C, clusterInfo *clusterInfo, storeID uint64, region *metapb.Region, leader *metapb.Peer) {
	db := newDefaultBalancer(region, leader, s.cfg)
	_, bop, err := db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)

	peer := op.ChangePeer.GetPeer()
	c.Assert(peer.GetStoreId(), Equals, storeID)

	addRegionPeer(c, region, peer)

	clusterInfo.regions.updateRegion(region)
}

func (s *testBalancerSuite) TestDefaultBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, _ := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	// Get leader peer.
	leader := region.GetPeers()[0]

	// Test add peer.
	s.addRegionPeer(c, clusterInfo, 4, region, leader)

	// Test add another peer.
	s.addRegionPeer(c, clusterInfo, 3, region, leader)

	// Now peers count equals to max peer count, so there is nothing to do.
	db := newDefaultBalancer(region, leader, s.cfg)
	_, bop, err := db.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Mock add one more peer.
	id, err := clusterInfo.idAlloc.Alloc()
	c.Assert(err, IsNil)

	newPeer := s.newPeer(c, uint64(2), id)
	region.Peers = append(region.Peers, newPeer)

	// Test remove peer.
	db = newDefaultBalancer(region, leader, s.cfg)
	_, bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	// Now we cannot remove leader peer, so the result is peer in store 2.
	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))
}

func (s *testBalancerSuite) TestCapacityBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, _ := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0)

	// Now we have all stores with low capacity used ratio, so we need not to do balance.
	testCfg := newBalanceConfig()
	testCfg.adjust()
	testCfg.MinCapacityUsedRatio = 0.5
	testCfg.MaxCapacityUsedRatio = 0.9
	cb := newCapacityBalancer(testCfg)
	_, bop, err := cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Now region peer count is less than max peer count, so we need not to do balance.
	testCfg.MinCapacityUsedRatio = 0.1
	testCfg.MaxCapacityUsedRatio = 0.9

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Get leader peer.
	leader := region.GetPeers()[0]
	c.Assert(leader, NotNil)

	// Add two peers.
	s.addRegionPeer(c, clusterInfo, 4, region, leader)
	s.addRegionPeer(c, clusterInfo, 3, region, leader)

	// If we cannot find a `from store` to do balance, then we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 91, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 92, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 93, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 94, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.3
	testCfg.MaxCapacityUsedRatio = 0.9

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// If the store does not have follower region, we should do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Now the region is (1,3,4), the balance operators should be
	// 1) add peer: 2
	// 2) remove peer: 4
	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 60, 0, 0)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.Ops, HasLen, 2)

	op1 := bop.Ops[0].(*changePeerOperator)
	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op2 := bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

	// If the sending snapshot count of `from store` is greater than MaxSnapSendingCount,
	// we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 65, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 60, 10, 0)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, NotNil)

	op1 = bop.Ops[0].(*changePeerOperator)
	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op2 = bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(3))

	// If the receiving snapshot count of `to store` is greater than MaxReceivingSnapCount,
	// we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 10)
	s.updateStore(c, clusterInfo, 3, 100, 65, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 60, 10, 0)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// If cluster max peer count config is 1, we can only add peer and remove leader peer.
	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0)

	// Set cluster config.
	oldMeta := clusterInfo.getMeta()
	meta := &metapb.Cluster{
		Id:           proto.Uint64(0),
		MaxPeerCount: proto.Uint32(1),
	}
	clusterInfo.setMeta(meta)

	testCfg.MinCapacityUsedRatio = 0.3
	testCfg.MaxCapacityUsedRatio = 0.9
	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Set region peers to one peer.
	peers := region.GetPeers()
	region.Peers = []*metapb.Peer{leader}
	clusterInfo.regions.updateRegion(region)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, NotNil)

	op1 = bop.Ops[0].(*changePeerOperator)
	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

	op2 = bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))

	// If the diff score is too small, we should do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 85, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0)

	cb = newCapacityBalancer(testCfg)
	_, bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Reset cluster config and region peers.
	clusterInfo.setMeta(oldMeta)

	region.Peers = peers
	clusterInfo.regions.updateRegion(region)
}

// TODO: Refactor these tests, they are quite ugly now.
func (s *testBalancerSuite) TestDownStore(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, _ := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	testCfg := newBalanceConfig()
	testCfg.adjust()
	testCfg.MinCapacityUsedRatio = 0.1
	testCfg.MaxCapacityUsedRatio = 0.9
	testCfg.MaxStoreDownDuration = 1 * time.Second
	cb := newCapacityBalancer(testCfg)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 50, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 70, 0, 0)

	// Get leader peer.
	leader := region.GetPeers()[0]

	// Test add peer.
	s.addRegionPeer(c, clusterInfo, 4, region, leader)

	// Test add another peer.
	s.addRegionPeer(c, clusterInfo, 3, region, leader)

	// Make store 2 up and down.
	for i := 0; i < 3; i++ {
		// Now we should move from store 4 to 2.
		s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0)
		s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
		s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0)
		s.updateStore(c, clusterInfo, 4, 100, 50, 0, 0)

		_, bop, err := cb.Balance(clusterInfo)
		c.Assert(err, IsNil)
		c.Assert(bop.Ops, HasLen, 2)

		op0 := bop.Ops[0].(*changePeerOperator)
		c.Assert(op0.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
		c.Assert(op0.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

		op1 := bop.Ops[1].(*changePeerOperator)
		c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
		c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

		// Update store 1,3,4 and let store 2 down.
		time.Sleep(600 * time.Millisecond)
		s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0)
		s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0)
		s.updateStore(c, clusterInfo, 4, 100, 50, 0, 0)
		time.Sleep(600 * time.Millisecond)

		// Now store 2 is down, we should not do balance.
		_, bop, err = cb.Balance(clusterInfo)
		c.Assert(err, IsNil)
		c.Assert(bop, IsNil)
	}
}

func (s *testBalancerSuite) TestReplicaBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, _ := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	// Get leader peer.
	leader := region.GetPeers()[0]

	// Test add peer.
	s.addRegionPeer(c, clusterInfo, 4, region, leader)

	// Test add another peer.
	s.addRegionPeer(c, clusterInfo, 3, region, leader)

	downPeers := []*pdpb.PeerStats{
		{
			Peer:        region.GetPeers()[1],
			DownSeconds: new(uint64),
		},
	}

	// DownSeconds < s.cfg.MaxPeerDownDuration, so there is nothing to do.
	*downPeers[0].DownSeconds = 100
	rb := newReplicaBalancer(region, leader, downPeers, s.cfg)
	_, bop, err := rb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Make store 4 down.
	s.cfg.MaxStoreDownDuration = 1 * time.Second
	time.Sleep(600 * time.Millisecond)
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	time.Sleep(600 * time.Millisecond)

	// DownSeconds < s.cfg.MaxPeerDownDuration, but store 4 is down, add replica to store 2.
	rb = newReplicaBalancer(region, leader, downPeers, s.cfg)
	_, bop, err = rb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.Ops, HasLen, 1)

	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	// DownSeconds >= s.cfg.MaxPeerDownDuration, add a replica to store 2.
	*downPeers[0].DownSeconds = 60 * 60
	rb = newReplicaBalancer(region, leader, downPeers, s.cfg)
	_, bop, err = rb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.Ops, HasLen, 1)

	op, ok = bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	// Now we have enough active replicas, we can remove the down peer in store 4.
	addRegionPeer(c, region, op.ChangePeer.GetPeer())
	clusterInfo.regions.updateRegion(region)

	rb = newReplicaBalancer(region, leader, downPeers, s.cfg)
	_, bop, err = rb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.Ops, HasLen, 1)

	op, ok = bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))
}
