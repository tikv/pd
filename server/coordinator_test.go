// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type testOperator struct {
	RegionID uint64
	Kind     ResourceKind
}

func newTestOperator(regionID uint64, kind ResourceKind) Operator {
	region := newRegionInfo(&metapb.Region{Id: regionID}, nil)
	op := &testOperator{RegionID: regionID, Kind: kind}
	return newRegionOperator(region, op)
}

func (op *testOperator) GetRegionID() uint64           { return op.RegionID }
func (op *testOperator) GetResourceKind() ResourceKind { return op.Kind }
func (op *testOperator) Do(region *regionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	return nil, false
}

var _ = Suite(&testCoordinatorSuite{})

type testCoordinatorSuite struct{}

func (s *testCoordinatorSuite) TestBasic(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	_, opt := newTestScheduleConfig()
	co := newCoordinator(cluster, opt)
	l := co.limiter

	op1 := newTestOperator(1, leaderKind)
	co.addOperator(op1)
	c.Assert(l.operatorCount(op1.GetResourceKind()), Equals, uint64(1))
	c.Assert(co.getOperator(1).GetRegionID(), Equals, op1.GetRegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, regionKind)
	co.addOperator(op2)
	c.Assert(l.operatorCount(op2.GetResourceKind()), Equals, uint64(0))

	// Remove the operator manually, then we can add a new operator.
	co.removeOperator(op1)
	co.addOperator(op2)
	c.Assert(l.operatorCount(op2.GetResourceKind()), Equals, uint64(1))
	c.Assert(co.getOperator(1).GetRegionID(), Equals, op2.GetRegionID())
}

func (s *testCoordinatorSuite) TestDispatch(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	co := newCoordinator(cluster, opt)
	co.run()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	tc.addRegionStore(4, 4)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(1, 1)
	tc.addLeaderRegion(1, 2, 3, 4)

	// Transfer leader from store 4 to store 2.
	tc.updateLeaderCount(4, 4)
	tc.updateLeaderCount(3, 3)
	tc.updateLeaderCount(2, 2)
	tc.updateLeaderCount(1, 1)
	tc.addLeaderRegion(2, 4, 3, 2)

	// Wait for schedule and turn off balance.
	s.waitOperator(c, co, 1)
	checkTransferPeer(c, co.getOperator(1), 4, 1)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	s.waitOperator(c, co, 2)
	checkTransferLeader(c, co.getOperator(2), 4, 2)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)

	// Transfer peer.
	region := cluster.getRegion(1)
	resp := co.dispatch(region)
	checkAddPeerResp(c, resp, 1)
	region.Peers = append(region.Peers, resp.GetChangePeer().GetPeer())
	cluster.putRegion(region)
	resp = co.dispatch(region)
	checkRemovePeerResp(c, resp, 4)
	region.RemoveStorePeer(4)
	cluster.putRegion(region)
	c.Assert(co.dispatch(region), IsNil)

	// Transfer leader.
	region = cluster.getRegion(2)
	resp = co.dispatch(region)
	checkTransferLeaderResp(c, resp, 2)
	region.Leader = resp.GetTransferLeader().GetPeer()
	cluster.putRegion(region)
	c.Assert(co.dispatch(region), IsNil)
}

func (s *testCoordinatorSuite) TestReplica(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	// Turn off balance.
	cfg, opt := newTestScheduleConfig()
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	co := newCoordinator(cluster, opt)
	co.run()
	defer co.stop()

	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(4, 4)

	// Add peer to store 1.
	tc.addLeaderRegion(1, 2, 3)
	region := cluster.getRegion(1)
	resp := co.dispatch(region)
	checkAddPeerResp(c, resp, 1)
	region.Peers = append(region.Peers, resp.GetChangePeer().GetPeer())
	c.Assert(co.dispatch(region), IsNil)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	tc.setStoreDown(3)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: proto.Uint64(24 * 60 * 60),
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	resp = co.dispatch(region)
	checkRemovePeerResp(c, resp, 3)
	region.RemoveStorePeer(3)
	region.DownPeers = nil
	resp = co.dispatch(region)
	checkAddPeerResp(c, resp, 4)
	region.Peers = append(region.Peers, resp.GetChangePeer().GetPeer())
	c.Assert(co.dispatch(region), IsNil)

	// Remove peer from store 4.
	tc.addLeaderRegion(2, 1, 2, 3, 4)
	region = cluster.getRegion(2)
	resp = co.dispatch(region)
	checkRemovePeerResp(c, resp, 4)
	region.RemoveStorePeer(4)
	c.Assert(co.dispatch(region), IsNil)
}

func (s *testCoordinatorSuite) TestPeerState(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	co := newCoordinator(cluster, opt)
	co.run()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(4, 4)
	tc.addLeaderRegion(1, 2, 3, 4)

	// Wait for schedule.
	s.waitOperator(c, co, 1)
	checkTransferPeer(c, co.getOperator(1), 4, 1)

	region := cluster.getRegion(1)

	// Add new peer.
	resp := co.dispatch(region)
	checkAddPeerResp(c, resp, 1)
	newPeer := resp.GetChangePeer().GetPeer()
	region.Peers = append(region.Peers, newPeer)

	// If the new peer is pending, the operator will not finish.
	region.PendingPeers = append(region.PendingPeers, newPeer)
	c.Assert(co.dispatch(region), IsNil)
	c.Assert(co.getOperator(region.GetId()), NotNil)

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region.PendingPeers = nil
	resp = co.dispatch(region)
	checkRemovePeerResp(c, resp, 4)
	tc.addLeaderRegion(1, 1, 2, 3)
	region = cluster.getRegion(1)
	c.Assert(co.dispatch(region), IsNil)
}

func (s *testCoordinatorSuite) TestAddScheduler(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	cfg.ReplicaScheduleLimit = 0

	co := newCoordinator(cluster, opt)
	co.run()
	defer co.stop()

	c.Assert(co.schedulers, HasLen, 2)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	c.Assert(co.schedulers, HasLen, 0)

	// Add stores 1,2,3
	tc.addLeaderStore(1, 1)
	tc.addLeaderStore(2, 1)
	tc.addLeaderStore(3, 1)
	// Add regions 1 with leader in store 1 and followers in stores 2,3
	tc.addLeaderRegion(1, 1, 2, 3)
	// Add regions 2 with leader in store 2 and followers in stores 1,3
	tc.addLeaderRegion(2, 2, 1, 3)
	// Add regions 3 with leader in store 3 and followers in stores 1,2
	tc.addLeaderRegion(3, 3, 1, 2)

	gls := newGrantLeaderScheduler(opt, 0)
	c.Assert(co.addScheduler(gls), NotNil)
	c.Assert(co.removeScheduler(gls.GetName()), NotNil)

	gls = newGrantLeaderScheduler(opt, 1)
	c.Assert(co.addScheduler(gls), IsNil)

	// Transfer all leaders to store 1.
	s.waitOperator(c, co, 2)
	region2 := cluster.getRegion(2)
	checkTransferLeaderResp(c, co.dispatch(region2), 1)
	region2.Leader = region2.GetStorePeer(1)
	cluster.putRegion(region2)
	c.Assert(co.dispatch(region2), IsNil)

	s.waitOperator(c, co, 3)
	region3 := cluster.getRegion(3)
	checkTransferLeaderResp(c, co.dispatch(region3), 1)
	region3.Leader = region3.GetStorePeer(1)
	cluster.putRegion(region3)
	c.Assert(co.dispatch(region3), IsNil)
}

func (s *testCoordinatorSuite) waitOperator(c *C, co *coordinator, regionID uint64) {
	for i := 0; i < 20; i++ {
		if co.getOperator(regionID) != nil {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
	c.Fatal("no operator found after retry 20 times.")
}

var _ = Suite(&testScheduleLimiterSuite{})

type testScheduleLimiterSuite struct{}

func (s *testScheduleLimiterSuite) TestOperatorCount(c *C) {
	l := newScheduleLimiter()
	c.Assert(l.operatorCount(leaderKind), Equals, uint64(0))
	c.Assert(l.operatorCount(regionKind), Equals, uint64(0))

	leaderOP := newTestOperator(1, leaderKind)
	l.addOperator(leaderOP)
	c.Assert(l.operatorCount(leaderKind), Equals, uint64(1))
	l.addOperator(leaderOP)
	c.Assert(l.operatorCount(leaderKind), Equals, uint64(2))
	l.removeOperator(leaderOP)
	c.Assert(l.operatorCount(leaderKind), Equals, uint64(1))

	regionOP := newTestOperator(1, regionKind)
	l.addOperator(regionOP)
	c.Assert(l.operatorCount(regionKind), Equals, uint64(1))
	l.addOperator(regionOP)
	c.Assert(l.operatorCount(regionKind), Equals, uint64(2))
	l.removeOperator(regionOP)
	c.Assert(l.operatorCount(regionKind), Equals, uint64(1))
}

var _ = Suite(&testScheduleControllerSuite{})

type testScheduleControllerSuite struct{}

func (s *testScheduleControllerSuite) TestController(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	cfg, opt := newTestScheduleConfig()
	co := newCoordinator(cluster, opt)
	lb := newBalanceLeaderScheduler(opt)
	sc := newScheduleController(co, lb)

	for i := minScheduleInterval; sc.GetInterval() != maxScheduleInterval; i *= 2 {
		c.Assert(sc.GetInterval(), Equals, i)
		c.Assert(sc.Schedule(cluster), IsNil)
	}

	cfg.LeaderScheduleLimit = 1
	c.Assert(sc.GetResourceLimit(), Equals, uint64(1))
	cfg.LeaderScheduleLimit = 0
	c.Assert(sc.GetResourceLimit(), Equals, uint64(0))
	cfg.LeaderScheduleLimit = 2
	c.Assert(sc.GetResourceLimit(), Equals, uint64(1))

	// limit = 2
	lb.limit = 2
	op := newTestOperator(1, leaderKind)
	// count = 0
	c.Assert(sc.AllowSchedule(), IsTrue)
	sc.limiter.addOperator(op)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
	sc.limiter.addOperator(op)
	// count = 2
	c.Assert(sc.AllowSchedule(), IsFalse)
	sc.limiter.removeOperator(op)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
}

func checkAddPeerResp(c *C, resp *pdpb.RegionHeartbeatResponse, storeID uint64) {
	changePeer := resp.GetChangePeer()
	c.Assert(changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(changePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeerResp(c *C, resp *pdpb.RegionHeartbeatResponse, storeID uint64) {
	changePeer := resp.GetChangePeer()
	c.Assert(changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(changePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferLeaderResp(c *C, resp *pdpb.RegionHeartbeatResponse, storeID uint64) {
	c.Assert(resp.GetTransferLeader().GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeerResp(c *C, resp *pdpb.RegionHeartbeatResponse, sourceID, targetID uint64) {
	checkAddPeerResp(c, resp, targetID)
	checkRemovePeerResp(c, resp, sourceID)
}
