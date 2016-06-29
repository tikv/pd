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

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
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

func (s *testBalancerSuite) addRegionPeer(c *C, clusterInfo *clusterInfo, storeID uint64, region *metapb.Region, leaderPeer *metapb.Peer) {
	db := newDefaultBalancer(region, leaderPeer, s.cfg)
	bop, err := db.Balance(clusterInfo)
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

	region := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	// Get leader peer.
	leaderPeer := region.GetPeers()[0]

	// Test add peer.
	s.addRegionPeer(c, clusterInfo, 4, region, leaderPeer)

	// Test add another peer.
	s.addRegionPeer(c, clusterInfo, 3, region, leaderPeer)

	// Now peers count equals to max peer count, so there is nothing to do.
	db := newDefaultBalancer(region, leaderPeer, s.cfg)
	bop, err := db.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Mock add one more peer.
	id, err := clusterInfo.idAlloc.Alloc()
	c.Assert(err, IsNil)

	newPeer := s.newPeer(c, uint64(2), id)
	region.Peers = append(region.Peers, newPeer)

	// Test remove peer.
	db = newDefaultBalancer(region, leaderPeer, s.cfg)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	// Now we cannot remove leader peer, so the result is peer in store 2.
	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))
}

func (s *testBalancerSuite) TestResourceBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0)

	// Now we have all stores with low capacityUsedRatio, so we need not to do balance.
	testCfg := newBalanceConfig()
	testCfg.adjust()
	testCfg.MinCapacityUsedRatio = 0.3
	testCfg.MaxCapacityUsedRatio = 0.9
	cb := newResourceBalancer(testCfg)
	bop, err := cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Now region peer count is less than max peer count, so we need not to do balance.
	testCfg.MinCapacityUsedRatio = 0.1
	testCfg.MaxCapacityUsedRatio = 0.9
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Get leader peer.
	leaderPeer := region.GetPeers()[0]
	c.Assert(leaderPeer, NotNil)

	// Add two peers.
	s.addRegionPeer(c, clusterInfo, 4, region, leaderPeer)
	s.addRegionPeer(c, clusterInfo, 3, region, leaderPeer)

	// Reset capacityUsedRatio = 0.3 to balance region.
	// Now the region is (1,3,4), the balance operators should be
	// 1) leader transfer: 1 -> 4
	// 2) add peer: 2
	// 3) remove peer: 1
	testCfg.MinCapacityUsedRatio = 0.3
	testCfg.MaxCapacityUsedRatio = 0.9
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.Ops, HasLen, 3)

	op1 := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op1.OldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.NewLeader.GetStoreId(), Equals, uint64(4))

	op2 := bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 := bop.Ops[2].(*changePeerOperator)
	c.Assert(op3.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))

	// If the store to be balanced with leader region, the capacity of region stores exceed the max used ratio,
	// but we can find store to add new peer, we should also do leader balance.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 40, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 25, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 20, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.4
	testCfg.MaxCapacityUsedRatio = 0.7
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, NotNil)

	op1 = bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op1.OldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.NewLeader.GetStoreId(), Equals, uint64(3))

	op2 = bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 = bop.Ops[2].(*changePeerOperator)
	c.Assert(op3.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))

	// If the sending snapshot count of store is greater than maxSnapSendingCount,
	// we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 10, 10, 0)
	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 10)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.3
	testCfg.MaxCapacityUsedRatio = 0.9
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// If the region leader is to be balanced, but there is no store to do leader transfer,
	// then we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.4
	testCfg.MaxCapacityUsedRatio = 0.6
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// If the old store and new store diff score is not big enough, we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 21, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 20, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.4
	testCfg.MaxCapacityUsedRatio = 0.7
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// If the store to be balanced without leader region, but we can find store to add new peer,
	// we should do follower balance.
	s.updateStore(c, clusterInfo, 1, 100, 99, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 40, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 20, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.4
	testCfg.MaxCapacityUsedRatio = 0.7
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, NotNil)

	newOp1 := bop.Ops[0].(*changePeerOperator)
	c.Assert(newOp1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(newOp1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	newOp2 := bop.Ops[1].(*changePeerOperator)
	c.Assert(newOp2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(newOp2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(3))

	// If the old store and new store diff score is not big enough, we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 1, 0, 0)
	s.updateStore(c, clusterInfo, 2, 100, 11, 0, 0)
	s.updateStore(c, clusterInfo, 3, 100, 10, 0, 0)
	s.updateStore(c, clusterInfo, 4, 100, 20, 0, 0)

	testCfg.MinCapacityUsedRatio = 0.4
	testCfg.MaxCapacityUsedRatio = 0.7
	cb = newResourceBalancer(testCfg)
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)
}
