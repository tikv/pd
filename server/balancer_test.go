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
}

func (s *testBalancerSuite) getRootPath() string {
	return "test_balancer"
}

func (s *testBalancerSuite) SetUpSuite(c *C) {
}

func (s *testBalancerSuite) TearDownSuite(c *C) {
}

func (s *testBalancerSuite) newClusterInfo(c *C) *ClusterInfo {
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
		clusterInfo.addMetaStore(store)
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

func (s *testBalancerSuite) TestDefaultBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region := clusterInfo.regions.GetRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 10)
	s.updateStore(c, clusterInfo, 2, 100, 20)
	s.updateStore(c, clusterInfo, 3, 100, 30)
	s.updateStore(c, clusterInfo, 4, 100, 40)

	// Get leader peer.
	peer := region.GetPeers()[0]

	// Test add peer.
	db := newDefaultBalancer(region, peer)
	bop, err := db.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.ops, HasLen, 1)

	op, ok := bop.ops[0].(*OnceOperator).op.(*ChangePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.changePeer.GetPeer().GetStoreId(), Equals, uint64(4))

	// Test add another peer.
	region.Peers = append(region.Peers, op.changePeer.GetPeer())
	db = newDefaultBalancer(region, peer)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	op, ok = bop.ops[0].(*OnceOperator).op.(*ChangePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.changePeer.GetPeer().GetStoreId(), Equals, uint64(3))

	// Now peers count equals to max peer count, so there is nothing to do.
	region.Peers = append(region.Peers, op.changePeer.GetPeer())
	db = newDefaultBalancer(region, peer)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Test remove peer.
	id, err := clusterInfo.idAlloc.Alloc()
	c.Assert(err, IsNil)

	newPeer := s.newPeer(c, uint64(2), id)
	region.Peers = append(region.Peers, newPeer)

	db = newDefaultBalancer(region, peer)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	// Now we cannot remove leader peer, so the result is peer in store 2.
	op, ok = bop.ops[0].(*OnceOperator).op.(*ChangePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.changePeer.GetPeer().GetStoreId(), Equals, uint64(2))
}

func (s *testBalancerSuite) updateStore(c *C, clusterInfo *ClusterInfo, storeID uint64, capacity uint64, available uint64) {
	store := clusterInfo.getStore(storeID)
	c.Assert(store, NotNil)
	store.stats = &pdpb.StoreStats{
		StoreId:   proto.Uint64(store.store.GetId()),
		Capacity:  proto.Uint64(capacity),
		Available: proto.Uint64(available),
	}
	clusterInfo.addStore(store)
}

func (s *testBalancerSuite) TestCapacityBalancer(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region := clusterInfo.regions.GetRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 60)
	s.updateStore(c, clusterInfo, 2, 100, 70)
	s.updateStore(c, clusterInfo, 3, 100, 80)
	s.updateStore(c, clusterInfo, 4, 100, 90)

	// Now we have all stores with low capacityUsedRatio, so we need not to do balance.
	cb := newCapacityBalancer()
	cb.capacityUsedRatio = 0.4
	cb.maxCapacityUsedRatio = 0.9
	bop, err := cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)

	// Get leader peer.
	peer := region.GetPeers()[0]
	c.Assert(peer, NotNil)

	// Add two peers.
	db := newDefaultBalancer(region, peer)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	op, ok := bop.ops[0].(*OnceOperator).op.(*ChangePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.changePeer.GetPeer().GetStoreId(), Equals, uint64(4))

	region.Peers = append(region.Peers, op.changePeer.GetPeer())

	db = newDefaultBalancer(region, peer)
	bop, err = db.Balance(clusterInfo)
	c.Assert(err, IsNil)

	op, ok = bop.ops[0].(*OnceOperator).op.(*ChangePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.changePeer.GetPeer().GetStoreId(), Equals, uint64(3))

	region.Peers = append(region.Peers, op.changePeer.GetPeer())

	clusterInfo.regions.updateRegion(region)

	// Reset capacityUsedRatio = 0.3 to balance region.
	// Now the region is (1,3,4), the balance operators should be
	// 1) leader transfer: 1 -> 4
	// 2) add peer: 2
	// 3) remove peer: 1
	cb = newCapacityBalancer()
	cb.capacityUsedRatio = 0.3
	cb.maxCapacityUsedRatio = 0.9
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop.ops, HasLen, 3)

	op1 := bop.ops[0].(*TransferLeaderOperator)
	c.Assert(op1.oldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.newLeader.GetStoreId(), Equals, uint64(4))

	op2 := bop.ops[1].(*ChangePeerOperator)
	c.Assert(op2.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.changePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 := bop.ops[2].(*ChangePeerOperator)
	c.Assert(op3.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.changePeer.GetPeer().GetStoreId(), Equals, uint64(1))

	// If the region leader is to be balanced, but there is no store to balance to,
	// then we will do nothing.
	s.updateStore(c, clusterInfo, 1, 100, 10)
	s.updateStore(c, clusterInfo, 2, 100, 20)
	s.updateStore(c, clusterInfo, 3, 100, 30)
	s.updateStore(c, clusterInfo, 4, 100, 40)

	cb = newCapacityBalancer()
	cb.capacityUsedRatio = 0.4
	cb.maxCapacityUsedRatio = 0.6
	bop, err = cb.Balance(clusterInfo)
	c.Assert(err, IsNil)
	c.Assert(bop, IsNil)
}
