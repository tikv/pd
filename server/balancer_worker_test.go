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
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
)

var _ = Suite(&testBalancerWorkerSuite{})

type testBalancerWorkerSuite struct {
	ts testBalancerSuite

	balancerWorker *balancerWorker
}

func (s *testBalancerWorkerSuite) getRootPath() string {
	return "test_balancer_worker"
}

func (s *testBalancerWorkerSuite) SetUpSuite(c *C) {
	s.ts.cfg = newBalanceConfig()
	s.ts.cfg.adjust()
}

func (s *testBalancerWorkerSuite) TestBalancerWorker(c *C) {
	clusterInfo := s.ts.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region, leader := clusterInfo.regions.getRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)
	c.Assert(leader, NotNil)

	s.balancerWorker = newBalancerWorker(clusterInfo, s.ts.cfg)

	// The store id will be 1,2,3,4.
	s.ts.updateStore(c, clusterInfo, 1, 100, 10, 0, 0)
	s.ts.updateStore(c, clusterInfo, 2, 100, 20, 0, 0)
	s.ts.updateStore(c, clusterInfo, 3, 100, 30, 0, 0)
	s.ts.updateStore(c, clusterInfo, 4, 100, 40, 0, 0)

	// Now we have no region to do balance.
	ret := s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	// Add two peers.
	s.ts.addRegionPeer(c, clusterInfo, 4, region, leader)
	s.ts.addRegionPeer(c, clusterInfo, 3, region, leader)

	// Now the region is (1,3,4), the balance operators should be
	// 1) leader transfer: 1 -> 4
	// 2) add peer: 2
	// 3) remove peer: 1
	ret = s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	regionID := region.GetId()
	bop, ok := s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.Ops, HasLen, 3)

	op1 := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op1.cfg.MaxTransferWaitCount, Equals, defaultMaxTransferWaitCount)
	c.Assert(op1.OldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.NewLeader.GetStoreId(), Equals, uint64(4))

	// Now we check the cfg.MaxTransferWaitCount for transferLeaderOperator.
	op1.cfg.MaxTransferWaitCount = 2

	ctx := newOpContext(nil, nil)
	ok, res, err := op1.Do(ctx, region, leader)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res.GetTransferLeader().GetPeer().GetStoreId(), Equals, uint64(4))
	c.Assert(op1.Count, Equals, 1)

	ok, res, err = op1.Do(ctx, region, leader)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res, IsNil)
	c.Assert(op1.Count, Equals, 2)

	ok, res, err = op1.Do(ctx, region, leader)
	c.Assert(err, NotNil)
	c.Assert(ok, IsFalse)
	c.Assert(res, IsNil)
	c.Assert(op1.Count, Equals, 2)

	op1.cfg.MaxTransferWaitCount = defaultMaxTransferWaitCount

	op2 := bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 := bop.Ops[2].(*changePeerOperator)
	c.Assert(op3.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))

	c.Assert(s.balancerWorker.balanceOperators, HasLen, 1)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 1)

	// Since we have already cached region balance operator, so recall doBalance will do nothing.
	ret = s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	oldBop := bop
	bop, ok = s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop, DeepEquals, oldBop)

	// Try to remove region balance operator cache, but we also have balance expire cache, so
	// we also cannot get a new balancer.
	s.balancerWorker.removeBalanceOperator(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 1)

	ret = s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)

	// Remove balance expire cache, this time we can get a new balancer now.
	s.balancerWorker.removeRegionCache(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 0)

	ret = s.balancerWorker.doBalance()
	c.Assert(ret, IsNil)

	bop, ok = s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.Ops, HasLen, 3)

	op1 = bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op1.OldLeader.GetStoreId(), Equals, uint64(1))
	c.Assert(op1.NewLeader.GetStoreId(), Equals, uint64(4))

	op2 = bop.Ops[1].(*changePeerOperator)
	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

	op3 = bop.Ops[2].(*changePeerOperator)
	c.Assert(op3.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op3.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))
}
