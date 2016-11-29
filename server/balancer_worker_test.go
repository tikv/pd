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

import . "github.com/pingcap/check"

var _ = Suite(&testBalancerWorkerSuite{})

type testBalancerWorkerSuite struct {
	ts testBalancerSuite

	balancerWorker *balancerWorker
}

func (s *testBalancerWorkerSuite) getRootPath() string {
	return "test_balancer_worker"
}

func (s *testBalancerWorkerSuite) SetUpSuite(c *C) {
	s.ts.SetUpSuite(c)
}

func (s *testBalancerWorkerSuite) TestBalancerWorker(c *C) {
	clusterInfo := s.ts.newClusterInfo(c)
	c.Assert(clusterInfo, NotNil)

	region := clusterInfo.searchRegion([]byte("a"))
	c.Assert(region.GetPeers(), HasLen, 1)

	s.balancerWorker = newBalancerWorker(clusterInfo, s.ts.opt)

	// The store id will be 1,2,3,4.
	s.ts.updateStore(c, clusterInfo, 1, 100, 50, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 2, 100, 20, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 3, 100, 30, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 4, 100, 40, 0, 0, 0)

	// Now we have no region to do balance.
	err := s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	// Add two peers.
	s.ts.addRegionPeer(c, clusterInfo, 4, region)
	s.ts.addRegionPeer(c, clusterInfo, 3, region)

	s.ts.cfg.MinLeaderCount = 1

	// Now the region is (1,3,4), leader is 1, the balance operators should be
	// leader transfer: 1 -> 3/4
	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	regionID := region.GetId()
	bop, ok := s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop.Ops, HasLen, 1)

	op := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op.OldLeader.GetStoreId(), Equals, uint64(1))
	newLeaderStoreID := op.NewLeader.GetStoreId()
	c.Assert(newLeaderStoreID, Not(Equals), uint64(1))

	ctx := newOpContext(nil, nil)
	ok, res, err := op.Do(ctx, region)
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	c.Assert(res.GetTransferLeader().GetPeer().GetStoreId(), Equals, newLeaderStoreID)
	c.Assert(op.Count, Equals, 1)

	// Since we have already cached region balance operator, so recall doBalance will do nothing.
	s.ts.updateStore(c, clusterInfo, 1, 100, 50, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 2, 100, 90, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 3, 100, 30, 0, 0, 0)
	s.ts.updateStore(c, clusterInfo, 4, 100, 40, 0, 0, 0)

	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)

	oldBop := bop
	bop, ok = s.balancerWorker.balanceOperators[regionID]
	c.Assert(ok, IsTrue)
	c.Assert(bop, DeepEquals, oldBop)

	// Try to remove region balance operator cache, but we also have balance expire cache, so
	// we also cannot get a new balancer.
	s.balancerWorker.removeBalanceOperator(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 1)

	err = s.balancerWorker.doBalance()
	c.Assert(err, IsNil)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)

	// Remove balance expire cache, this time we can get a new balancer now.
	s.balancerWorker.removeRegionCache(regionID)
	c.Assert(s.balancerWorker.balanceOperators, HasLen, 0)
	c.Assert(s.balancerWorker.regionCache.count(), Equals, 0)
}
