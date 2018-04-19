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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedulers"
)

func newTestOperator(regionID uint64, kind schedule.OperatorKind) *schedule.Operator {
	return schedule.NewOperator("test", regionID, kind)
}

func newTestScheduleConfig() (*ScheduleConfig, *scheduleOption) {
	cfg := NewConfig()
	cfg.adjust()
	opt := newScheduleOption(cfg)
	return &cfg.Schedule, opt
}

type testClusterInfo struct {
	*clusterInfo
}

func newTestClusterInfo(opt *scheduleOption) *testClusterInfo {
	return &testClusterInfo{clusterInfo: newClusterInfo(
		core.NewMockIDAllocator(),
		opt,
		core.NewKV(core.NewMemoryKV()),
	)}
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
}

func (c *testClusterInfo) addRegionStore(storeID uint64, regionCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.RegionCount = regionCount
	store.RegionSize = int64(regionCount) * 10
	store.Stats.Capacity = 1000 * (1 << 20)
	store.Stats.Available = store.Stats.Capacity - uint64(store.RegionSize)
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := newTestRegionMeta(regionID)
	leader, _ := c.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(core.NewRegionInfo(region, leader))
}

func (c *testClusterInfo) updateLeaderCount(storeID uint64, leaderCount int) {
	store := c.GetStore(storeID)
	store.LeaderCount = leaderCount
	store.LeaderSize = int64(leaderCount) * 10
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderStore(storeID uint64, leaderCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.LeaderCount = leaderCount
	store.LeaderSize = int64(leaderCount) * 10
	c.putStore(store)
}

func (c *testClusterInfo) setStoreDown(storeID uint64) {
	store := c.GetStore(storeID)
	store.State = metapb.StoreState_Up
	store.LastHeartbeatTS = time.Time{}
	c.putStore(store)
}

func (c *testClusterInfo) setStoreOffline(storeID uint64) {
	store := c.GetStore(storeID)
	store.State = metapb.StoreState_Offline
	c.putStore(store)
}

func (c *testClusterInfo) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerIds {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(core.NewRegionInfo(region, nil))
}

var _ = Suite(&testCoordinatorSuite{})

type testCoordinatorSuite struct{}

func (s *testCoordinatorSuite) TestBasic(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.clusterInfo.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	l := co.limiter

	op1 := newTestOperator(1, schedule.OpLeader)
	co.addOperator(op1)
	c.Assert(l.OperatorCount(op1.Kind()), Equals, uint64(1))
	c.Assert(co.getOperator(1).RegionID(), Equals, op1.RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, schedule.OpRegion)
	co.addOperator(op2)
	c.Assert(l.OperatorCount(op2.Kind()), Equals, uint64(0))

	// Remove the operator manually, then we can add a new operator.
	co.removeOperator(op1)
	co.addOperator(op2)
	c.Assert(l.OperatorCount(op2.Kind()), Equals, uint64(1))
	c.Assert(co.getOperator(1).RegionID(), Equals, op2.RegionID())
}

type mockHeartbeatStream struct {
	ch chan *pdpb.RegionHeartbeatResponse
}

func (s *mockHeartbeatStream) Send(m *pdpb.RegionHeartbeatResponse) error {
	s.ch <- m
	return nil
}

func (s *mockHeartbeatStream) Recv() *pdpb.RegionHeartbeatResponse {
	select {
	case <-time.After(time.Millisecond * 10):
		return nil
	case res := <-s.ch:
		return res
	}
}

func newMockHeartbeatStream() *mockHeartbeatStream {
	return &mockHeartbeatStream{
		ch: make(chan *pdpb.RegionHeartbeatResponse),
	}
}

func (s *testCoordinatorSuite) TestDispatch(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	tc.addRegionStore(4, 4)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(1, 1)
	tc.addLeaderRegion(1, 2, 3, 4)

	// Transfer leader from store 4 to store 2.
	tc.updateLeaderCount(4, 5)
	tc.updateLeaderCount(3, 3)
	tc.updateLeaderCount(2, 2)
	tc.updateLeaderCount(1, 1)
	tc.addLeaderRegion(2, 4, 3, 2)

	// Wait for schedule and turn off balance.
	waitOperator(c, co, 1)
	schedulers.CheckTransferPeer(c, co.getOperator(1), schedule.OpBalance, 4, 1)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	waitOperator(c, co, 2)
	schedulers.CheckTransferLeader(c, co.getOperator(2), schedule.OpBalance, 4, 2)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)

	stream := newMockHeartbeatStream()

	// Transfer peer.
	region := tc.GetRegion(1)
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 1)
	dispatchHeartbeat(c, co, region, stream)
	waitRemovePeer(c, stream, region, 4)
	region.RemoveStorePeer(4)
	dispatchHeartbeat(c, co, region, stream)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)

	// Transfer leader.
	region = tc.GetRegion(2)
	dispatchHeartbeat(c, co, region, stream)
	waitTransferLeader(c, stream, region, 2)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)
}

func dispatchHeartbeat(c *C, co *coordinator, region *core.RegionInfo, stream *mockHeartbeatStream) {
	co.hbStreams.bindStream(region.Leader.GetStoreId(), stream)
	co.cluster.putRegion(region)
	co.dispatch(region)
}

func (s *testCoordinatorSuite) TestReplica(c *C) {
	// Turn off balance.
	cfg, opt := newTestScheduleConfig()
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.stop()

	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(4, 4)

	stream := newMockHeartbeatStream()

	// Add peer to store 1.
	tc.addLeaderRegion(1, 2, 3)
	region := tc.GetRegion(1)
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 1)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	tc.setStoreDown(3)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	dispatchHeartbeat(c, co, region, stream)
	waitRemovePeer(c, stream, region, 3)
	region.DownPeers = nil
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 4)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)

	// Remove peer from store 4.
	tc.addLeaderRegion(2, 1, 2, 3, 4)
	region = tc.GetRegion(2)
	dispatchHeartbeat(c, co, region, stream)
	waitRemovePeer(c, stream, region, 4)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)

	// Remove offline peer directly when it's pending.
	tc.addLeaderRegion(3, 1, 2, 3)
	tc.setStoreOffline(3)
	region = tc.GetRegion(3)
	region.PendingPeers = []*metapb.Peer{region.GetStorePeer(3)}
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestPeerState(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(4, 4)
	tc.addLeaderRegion(1, 2, 3, 4)

	stream := newMockHeartbeatStream()

	// Wait for schedule.
	waitOperator(c, co, 1)
	schedulers.CheckTransferPeer(c, co.getOperator(1), schedule.OpBalance, 4, 1)

	region := tc.GetRegion(1)

	// Add new peer.
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 1)

	// If the new peer is pending, the operator will not finish.
	region.PendingPeers = append(region.PendingPeers, region.GetStorePeer(1))
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)
	c.Assert(co.getOperator(region.GetId()), NotNil)

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region.PendingPeers = nil
	dispatchHeartbeat(c, co, region, stream)
	waitRemovePeer(c, stream, region, 4)
	tc.addLeaderRegion(1, 1, 2, 3)
	region = tc.GetRegion(1)
	dispatchHeartbeat(c, co, region, stream)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestShouldRun(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)

	tc.LoadRegion(1, 1, 2, 3)
	tc.LoadRegion(2, 1, 2, 3)
	tc.LoadRegion(3, 1, 2, 3)
	tc.LoadRegion(4, 1, 2, 3)
	tc.LoadRegion(5, 1, 2, 3)
	c.Assert(co.shouldRun(), IsFalse)

	tbl := []struct {
		regionID  uint64
		shouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, true},
		{5, true},
	}

	for _, t := range tbl {
		r := tc.GetRegion(t.regionID)
		r.Leader = r.Peers[0]
		tc.handleRegionHeartbeat(r)
		c.Assert(co.shouldRun(), Equals, t.shouldRun)
	}
	nr := &metapb.Region{Id: 6, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	tc.handleRegionHeartbeat(newRegion)
	c.Assert(co.cluster.activeRegions, Equals, 6)

}

func (s *testCoordinatorSuite) TestAddScheduler(c *C) {
	cfg, opt := newTestScheduleConfig()
	cfg.ReplicaScheduleLimit = 0

	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()
	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.stop()

	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-hot-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("label-scheduler"), IsNil)
	c.Assert(co.schedulers, HasLen, 0)

	stream := newMockHeartbeatStream()

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

	gls, err := schedule.CreateScheduler("grant-leader", co.limiter, "0")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls), NotNil)
	c.Assert(co.removeScheduler(gls.GetName()), NotNil)

	gls, err = schedule.CreateScheduler("grant-leader", co.limiter, "1")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls), IsNil)

	// Transfer all leaders to store 1.
	waitOperator(c, co, 2)
	region2 := tc.GetRegion(2)
	dispatchHeartbeat(c, co, region2, stream)
	waitTransferLeader(c, stream, region2, 1)
	dispatchHeartbeat(c, co, region2, stream)
	waitNoResponse(c, stream)

	waitOperator(c, co, 3)
	region3 := tc.GetRegion(3)
	dispatchHeartbeat(c, co, region3, stream)
	waitTransferLeader(c, stream, region3, 1)
	dispatchHeartbeat(c, co, region3, stream)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestPersistScheduler(c *C) {
	cfg, opt := newTestScheduleConfig()
	cfg.ReplicaScheduleLimit = 0

	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()

	// Add stores 1,2
	tc.addLeaderStore(1, 1)
	tc.addLeaderStore(2, 1)

	c.Assert(co.schedulers, HasLen, 4)
	gls1, err := schedule.CreateScheduler("grant-leader", co.limiter, "1")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls1, "1"), IsNil)
	gls2, err := schedule.CreateScheduler("grant-leader", co.limiter, "2")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls2, "2"), IsNil)
	c.Assert(co.schedulers, HasLen, 6)
	fmt.Println(opt)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-hot-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("label-scheduler"), IsNil)
	c.Assert(co.schedulers, HasLen, 2)
	c.Assert(co.cluster.opt.persist(co.cluster.kv), IsNil)
	co.stop()
	// make a new coordinator for testing
	// whether the schedulers added or removed in dynamic way are recorded in opt
	_, newOpt := newTestScheduleConfig()
	_, err = schedule.CreateScheduler("adjacent-region", co.limiter)
	c.Assert(err, IsNil)
	// suppose we add a new default enable scheduler
	newOpt.AddSchedulerCfg("adjacent-region", []string{})
	c.Assert(newOpt.GetSchedulers(), HasLen, 5)
	newOpt.reload(co.cluster.kv)
	c.Assert(newOpt.GetSchedulers(), HasLen, 7)
	tc.clusterInfo.opt = newOpt

	co = newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	c.Assert(co.schedulers, HasLen, 3)
	bls, err := schedule.CreateScheduler("balance-leader", co.limiter)
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(bls), IsNil)
	brs, err := schedule.CreateScheduler("balance-region", co.limiter)
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(brs), IsNil)
	c.Assert(co.schedulers, HasLen, 5)
	c.Assert(co.removeScheduler("grant-leader-scheduler-1"), IsNil)
	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.cluster.opt.persist(co.cluster.kv), IsNil)
	co.stop()

	opt.reload(co.cluster.kv)
	tc.clusterInfo.opt = opt
	co = newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)

	co.run()
	defer co.stop()
	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.removeScheduler("grant-leader-scheduler-2"), IsNil)
	c.Assert(co.schedulers, HasLen, 3)
}

func (s *testCoordinatorSuite) TestRestart(c *C) {
	// Turn off balance, we test add replica only.
	cfg, opt := newTestScheduleConfig()
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 3)
	tc.addLeaderRegion(1, 1)
	tc.activeRegions = 1
	region := tc.GetRegion(1)

	// Add 1 replica on store 2.
	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	stream := newMockHeartbeatStream()
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 2)
	co.stop()

	// Recreate coodinator then add another replica on store 3.
	co = newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	co.run()
	dispatchHeartbeat(c, co, region, stream)
	waitAddPeer(c, stream, region, 3)
	co.stop()
}

func waitOperator(c *C, co *coordinator, regionID uint64) {
	testutil.WaitUntil(c, func(c *C) bool {
		return co.getOperator(regionID) != nil
	})
}

var _ = Suite(&testScheduleLimiterSuite{})

type testScheduleLimiterSuite struct{}

func (s *testScheduleLimiterSuite) TestOperatorCount(c *C) {
	l := schedule.NewLimiter()
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(0))
	c.Assert(l.OperatorCount(schedule.OpRegion), Equals, uint64(0))

	operators := make(map[uint64]*schedule.Operator)

	operators[1] = newTestOperator(1, schedule.OpLeader)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(1)) // 1:leader
	operators[2] = newTestOperator(2, schedule.OpLeader)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(2)) // 1:leader, 2:leader
	delete(operators, 1)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(1)) // 2:leader

	operators[1] = newTestOperator(1, schedule.OpRegion)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(schedule.OpRegion), Equals, uint64(1)) // 1:region 2:leader
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(1))
	operators[2] = newTestOperator(2, schedule.OpRegion)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(schedule.OpRegion), Equals, uint64(2)) // 1:region 2:region
	c.Assert(l.OperatorCount(schedule.OpLeader), Equals, uint64(0))
}

var _ = Suite(&testScheduleControllerSuite{})

type testScheduleControllerSuite struct{}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.Limiter
	kind    schedule.OperatorKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func (s *testScheduleControllerSuite) TestController(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	scheduler, err := schedule.CreateScheduler("balance-leader", co.limiter)
	c.Assert(err, IsNil)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   co.limiter,
		kind:      schedule.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		c.Assert(sc.GetInterval(), Equals, i)
		c.Assert(sc.Schedule(tc.clusterInfo, schedule.NewOpInfluence(nil, tc.clusterInfo)), IsNil)
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	c.Assert(sc.AllowSchedule(), IsTrue)
	op1 := newTestOperator(1, schedule.OpLeader)
	c.Assert(co.addOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
	op2 := newTestOperator(2, schedule.OpLeader)
	c.Assert(co.addOperator(op2), IsTrue)
	// count = 2
	c.Assert(sc.AllowSchedule(), IsFalse)
	co.removeOperator(op1)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)

	// add a PriorityKind operator will remove old operator
	op3 := newTestOperator(2, schedule.OpHotRegion)
	op3.SetPriorityLevel(core.HighPriority)
	c.Assert(co.addOperator(op1), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(co.addOperator(op3), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	co.removeOperator(op3)

	// add a admin operator will remove old operator
	c.Assert(co.addOperator(op2), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	op4 := newTestOperator(2, schedule.OpAdmin)
	op4.SetPriorityLevel(core.HighPriority)
	c.Assert(co.addOperator(op4), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
}

func (s *testScheduleControllerSuite) TestInterval(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	hbStreams := newHeartbeatStreams(tc.getClusterID())
	defer hbStreams.Close()

	co := newCoordinator(tc.clusterInfo, hbStreams, namespace.DefaultClassifier)
	lb, err := schedule.CreateScheduler("balance-leader", co.limiter)
	c.Assert(err, IsNil)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			c.Assert(sc.Schedule(tc.clusterInfo, schedule.NewOpInfluence(nil, tc.clusterInfo)), IsNil)
		}
		c.Assert(sc.GetInterval(), Less, time.Second*time.Duration(n/2))
	}
}

func waitAddPeer(c *C, stream *mockHeartbeatStream, region *core.RegionInfo, storeID uint64) {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetId() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	region.AddPeer(res.GetChangePeer().GetPeer())
	region.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: region.GetRegionEpoch().GetConfVer() + 1,
		Version: region.GetRegionEpoch().GetVersion(),
	}
}

func waitRemovePeer(c *C, stream *mockHeartbeatStream, region *core.RegionInfo, storeID uint64) {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetId() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_RemoveNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	region.RemoveStorePeer(storeID)
	region.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: region.GetRegionEpoch().GetConfVer() + 1,
		Version: region.GetRegionEpoch().GetVersion(),
	}
}

func waitTransferLeader(c *C, stream *mockHeartbeatStream, region *core.RegionInfo, storeID uint64) {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetId() && res.GetTransferLeader().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	region.Leader = res.GetTransferLeader().GetPeer()
}

func waitNoResponse(c *C, stream *mockHeartbeatStream) {
	testutil.WaitUntil(c, func(c *C) bool {
		res := stream.Recv()
		return res == nil
	})
}
