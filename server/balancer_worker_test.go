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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

var _ = Suite(&testBalancerWorkerSuite{})

type testBalancerWorkerSuite struct {
	testBalancerSuite

	balancerWorker *balancerWorker
}

func (s *testBalancerWorkerSuite) getRootPath() string {
	return "test_balancer_worker"
}

func (s *testBalancerWorkerSuite) TestBalancerWorker(c *C) {
	clusterInfo := s.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region := clusterInfo.regions.GetRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	s.balancerWorker = newBalancerWorker(clusterInfo,
		newCapacityBalancer(minCapacityUsedRatio, maxCapacityUsedRatio),
		defaultBalanceInterval)

	// The store id will be 1,2,3,4.
	s.updateStore(c, clusterInfo, 1, 100, 10)
	s.updateStore(c, clusterInfo, 2, 100, 20)
	s.updateStore(c, clusterInfo, 3, 100, 30)
	s.updateStore(c, clusterInfo, 4, 100, 40)

	// Now we have no region to do balance.
	ret := s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	// Get leader peer.
	leaderPeer := region.GetPeers()[0]
	c.Assert(leaderPeer, NotNil)

	// Add two peers.
	s.addRegionPeer(c, clusterInfo, 4, region, leaderPeer)
	s.addRegionPeer(c, clusterInfo, 3, region, leaderPeer)

	// Now the region is (1,3,4), the balance operators should be
	// 1) leader transfer: 1 -> 4
	// 2) add peer: 2
	// 3) remove peer: 1
	ret = s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	regionID := region.GetId()
	bop, ok := s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.ops, HasLen, 3)

	op1 := bop.ops[0].(*TransferLeaderOperator)
	c.Assert(op1.oldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.newLeader.GetStoreId(), Equals, uint64(4))

	ok, res, err := op1.Do(region, leaderPeer)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res.GetTransferLeader().GetPeer().GetStoreId(), Equals, uint64(4))

	ok, res, err = op1.Do(region, leaderPeer)
	c.Assert(err, NotNil)
	c.Assert(ok, IsFalse)
	c.Assert(res, IsNil)

	op2 := bop.ops[1].(*ChangePeerOperator)
	c.Assert(op2.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.changePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 := bop.ops[2].(*ChangePeerOperator)
	c.Assert(op3.changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.changePeer.GetPeer().GetStoreId(), Equals, uint64(1))
}
