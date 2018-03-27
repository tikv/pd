// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
)

func newTestScheduleConfig() *MockSchedulerOptions {
	mso := newMockSchedulerOptions()
	return mso
}

func newTestReplication(mso *MockSchedulerOptions, maxReplicas int, locationLabels ...string) {
	mso.MaxReplicas = maxReplicas
	mso.LocationLabels = locationLabels
}

var _ = Suite(&testBalanceSpeedSuite{})

type testBalanceSpeedSuite struct{}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	avgScore       float64
	regionSize     int64
	diff           int
	expectedResult bool
}

func (s *testBalanceSpeedSuite) TestShouldBalance(c *C) {
	testCases := []struct {
		sourceSize   int64
		sourceWeight float64
		targetSize   int64
		targetWeight float64
		moveSize     float64
		result       bool
	}{
		{100, 1, 80, 1, 5, true},
		{100, 1, 80, 1, 15, false},
		{100, 1, 120, 2, 10, true},
		{100, 1, 180, 2, 10, false},
		{100, 0.5, 180, 1, 10, false},
		{100, 0.5, 180, 1, 5, true},
		{100, 1, 10, 0, 10, false}, // targetWeight=0
		{100, 0, 10, 0, 10, false},
		{100, 0, 500, 1, 50, true}, // sourceWeight=0
	}

	for _, t := range testCases {
		c.Assert(shouldBalance(t.sourceSize, t.sourceWeight, t.targetSize, t.targetWeight, t.moveSize), Equals, t.result)
	}
}

func (s *testBalanceSpeedSuite) TestBalanceLimit(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)
	tc.addLeaderStore(1, 10)
	tc.addLeaderStore(2, 20)
	tc.addLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(200.0/3.0)))

	tc.setStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(50.0/2.0)))
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct {
	tc *mockCluster
	lb schedule.Scheduler
}

func (s *testBalanceLeaderSchedulerSuite) SetUpTest(c *C) {
	opt := newTestScheduleConfig()
	s.tc = newMockCluster(opt)
	lb, err := schedule.CreateScheduler("balance-leader", schedule.NewLimiter())
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerSuite) schedule(operators []*schedule.Operator) []*schedule.Operator {
	return s.lb.Schedule(s.tc, schedule.NewOpInfluence(operators, s.tc))
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLimit(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 0)
	s.tc.addLeaderStore(3, 0)
	s.tc.addLeaderStore(4, 0)
	s.tc.addLeaderRegion(1, 1, 2, 3, 4)
	c.Check(s.schedule(nil), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	s.tc.updateLeaderCount(1, 16)
	c.Check(s.schedule(nil), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(1, 7)
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	s.tc.updateLeaderCount(4, 10)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(nil), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(4, 16)
	c.Check(s.schedule(nil), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestScheduleWithOpInfluence(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   14
	// Region1:    F    F    F    L
	s.tc.addLeaderStore(1, 7)
	s.tc.addLeaderStore(2, 8)
	s.tc.addLeaderStore(3, 9)
	s.tc.addLeaderStore(4, 14)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	op := s.schedule(nil)[0]
	c.Check(op, NotNil)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader differece is not greater than 5.
	c.Check(s.schedule([]*schedule.Operator{op}), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(1, 8)
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	s.tc.updateLeaderCount(4, 13)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(nil), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceFilter(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 16)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)

	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we schould consider it
	// because it still provides services
	s.tc.setStoreOffline(4)
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.setStoreDown(1)
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.setStoreBusy(2, true)
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestLeaderWeight(c *C) {
	// Stores:	1	2	3	4
	// Leaders:    10      10      10      10
	// Weight:    0.5     0.9       1       2
	// Region1:     L       F       F       F

	s.tc.addLeaderStore(1, 10)
	s.tc.addLeaderStore(2, 10)
	s.tc.addLeaderStore(3, 10)
	s.tc.addLeaderStore(4, 10)
	s.tc.updateStoreLeaderWeight(1, 0.5)
	s.tc.updateStoreLeaderWeight(2, 0.9)
	s.tc.updateStoreLeaderWeight(3, 1)
	s.tc.updateStoreLeaderWeight(4, 2)
	s.tc.addLeaderRegion(1, 1, 2, 3, 4)
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 1, 4)
	s.tc.updateLeaderCount(4, 30)
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 1, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceSelector(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 16)
	s.tc.addLeaderRegion(1, 4, 2, 3)
	s.tc.addLeaderRegion(2, 3, 1, 2)
	// store4 has max leader score, store1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.updateLeaderCount(2, 14)
	s.tc.updateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderRegion(1, 3, 2, 4)
	s.tc.addLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. No operator is created.
	c.Assert(s.schedule(nil), IsNil)
	// store4 and store1 are marked taint.
	// Now source and target are store3 and store2.
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 3, 2)

	// Stores:     1    2    3    4
	// Leaders:    9    10   10   11
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	s.tc.addLeaderStore(1, 10)
	s.tc.addLeaderStore(2, 10)
	s.tc.addLeaderStore(3, 10)
	s.tc.addLeaderStore(4, 10)
	s.tc.addLeaderRegion(1, 4, 2, 3)
	s.tc.addLeaderRegion(2, 1, 2, 3)
	// The cluster is balanced.
	c.Assert(s.schedule(nil), IsNil) // store1, store4 are marked taint.
	c.Assert(s.schedule(nil), IsNil) // store2, store3 are marked taint.

	// store3's leader drops:
	// Stores:     1    2    3    4
	// Leaders:    11   13   0    16
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	s.tc.addLeaderStore(1, 11)
	s.tc.addLeaderStore(2, 13)
	s.tc.addLeaderStore(3, 0)
	s.tc.addLeaderStore(4, 16)
	c.Assert(s.schedule(nil), IsNil)                                     // All stores are marked taint.
	CheckTransferLeader(c, s.schedule(nil)[0], schedule.OpBalance, 4, 3) // The taint store will be clear.
}

var _ = Suite(&testBalanceRegionSchedulerSuite{})

type testBalanceRegionSchedulerSuite struct{}

func (s *testBalanceRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	cache := sb.(*balanceRegionScheduler).taintStores

	opt.SetMaxReplicas(1)

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 6)
	tc.addRegionStore(2, 8)
	tc.addRegionStore(3, 8)
	tc.addRegionStore(4, 16)
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	tc.updateRegionCount(2, 6)
	cache.Remove(4)
	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 4, 2)
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)

	cache.Clear()
	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	cache := sb.(*balanceRegionScheduler).taintStores

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.addLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.addLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores,
	// so store 1 will be set in the cache and skipped next schedule.
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(cache.Exists(1), IsTrue)

	// Store 4 has smaller region score than store 2.
	tc.addLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.addLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	cache.Remove(1) // Delete store 1 from cache, or it will be skipped.
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 6)

	// Store 7 has smaller region score with store 6.
	tc.addLabelsStore(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 7)

	// If store 7 is not available, will choose store 6.
	tc.setStoreDown(7)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 6)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.addLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 6)

	// Take down 4,5,6,7
	tc.setStoreDown(4)
	tc.setStoreDown(5)
	tc.setStoreDown(6)
	tc.setStoreDown(7)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(cache.Exists(1), IsTrue)
	cache.Remove(1)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.addLabelsStore(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 5, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	tc.addLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 5, 6)

	// Store 7 has larger region score and same distinct score with store 6.
	tc.addLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 5, 6)

	// Store 1 has smaller region score and higher distinct score.
	tc.addLeaderRegion(1, 2, 3, 4, 5, 6)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.addLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.addLeaderRegion(1, 2, 3, 11, 12, 13)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 11, 6)
}

func (s *testBalanceRegionSchedulerSuite) TestStoreWeight(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)

	tc.addRegionStore(1, 10)
	tc.addRegionStore(2, 10)
	tc.addRegionStore(3, 10)
	tc.addRegionStore(4, 10)
	tc.updateStoreRegionWeight(1, 0.5)
	tc.updateStoreRegionWeight(2, 0.9)
	tc.updateStoreRegionWeight(3, 1.0)
	tc.updateStoreRegionWeight(4, 2.0)

	tc.addLeaderRegion(1, 1)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 4)

	tc.updateRegionCount(4, 30)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpBalance, 1, 3)
}

var _ = Suite(&testBalanceFailOverSuite{})

type testBalanceFailOverSuite struct{}

func (s *testBalanceFailOverSuite) TestBasic(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	fo, err := schedule.CreateScheduler("fail-over", schedule.NewLimiter())
	c.Assert(err, IsNil)

	opt.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4)
	tc.addRegionStore(2, 3)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 3.
	tc.addLeaderRegion(1, 1, 3)

	// Region has 2 peers, we need to add a new peer.
	region := tc.GetRegion(1)

	// Peer in store 3 is down, remove it.
	tc.setStoreDown(3)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	tc.PutRegion(region)
	c.Assert(fo.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	//	checkRemovePeer(c, rc.Check(region), 2)
	//	region.DownPeers = nil
	// Add peer in store 2
	peer3, _ := tc.AllocPeer(2)
	region.Peers = append(region.Peers, peer3)
	tc.PutRegion(region)
	checkRemovePeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], 3)
	region.DownPeers = nil
	tc.setStoreUp(3)
	tc.PutRegion(region)

	// Peer in store 2 is offline, transfer peer to store 4.
	tc.setStoreOffline(2)
	CheckTransferPeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 2, 4)
	// Add peer in store 4, now we have 4 replica
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], 2)
}

func (s *testBalanceFailOverSuite) TestStoreDown(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")
	fo, err := schedule.CreateScheduler("fail-over", schedule.NewLimiter())
	c.Assert(err, IsNil)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1)

	// Test Store Down
	tc.setStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	tc.PutRegion(region)
	// unHealth to removepeer
	c.Assert(fo.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)
	tc.PutRegion(region)
	checkRemovePeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], 2)
}

func (s *testBalanceFailOverSuite) TestOffline(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")
	fo, err := schedule.CreateScheduler("fail-over", schedule.NewLimiter())
	c.Assert(err, IsNil)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 3, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 5, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 10, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2)

	// TestOffilne
	tc.setStoreOffline(2)
	CheckTransferPeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 2, 3)
	region := tc.GetRegion(1)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	tc.PutRegion(region)
	// region unhealth
	c.Assert(fo.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)

	region.DownPeers = nil
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)
	CheckTransferPeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 2, 4)
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], 2)

	tc.addLeaderRegionWithWriteInfo(2, 2, 512*1024*schedule.RegionHeartBeatReportInterval, 3, 4)
	opt.HotRegionLowThreshold = 0
	CheckTransferPeer(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 2, 1)
	tc.addLeaderRegionWithReadInfo(3, 2, 512*1024*schedule.RegionHeartBeatReportInterval, 3, 4)
	CheckTransferLeader(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 2, 3)
}

func (s *testBalanceFailOverSuite) TestLowSpace(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")
	fo, err := schedule.CreateScheduler("fail-over", schedule.NewLimiter())
	c.Assert(err, IsNil)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 3)
	c.Assert(fo.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)

	// TestLowSpace
	tc.setStoreLowSpace(3)
	tc.addLeaderRegionWithReadInfo(2, 3, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 2)
	opt.HotRegionLowThreshold = 0
	CheckTransferLeader(c, fo.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpReplica, 3, 1)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) TestBasic(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	opt.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4)
	tc.addRegionStore(2, 3)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := tc.GetRegion(1)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 4)

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.setStoreDown(4)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)
	tc.setStoreUp(4)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to store 3.
	tc.updateSnapshotCount(4, 3)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.updateSnapshotCount(4, 1)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 4)

	// Test storageThresholdFilter.
	// If availableRatio < storageAvailableRatioThreshold(0.2), we can not add peer.
	tc.updateStorageRatio(4, 0.9, 0.1)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)
	tc.updateStorageRatio(4, 0.5, 0.1)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)
	// If availableRatio > storageAvailableRatioThreshold(0.2), we can add peer again.
	tc.updateStorageRatio(4, 0.7, 0.3)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)
	checkRemovePeer(c, rc.Check(region), 1)
	region.RemoveStorePeer(1)

}

func (s *testReplicaCheckerSuite) TestLostStore(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 1)

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	// now region peer in store 1,2,3.but we just have store 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.addLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)
	op := rc.Check(region)
	c.Assert(op, IsNil)
}

func (s *testReplicaCheckerSuite) TestReplacePeer(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1)
	region := tc.GetRegion(1)

	// Store 2 has different zone and smallest region score.
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 3 has different zone and smallest region score.
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)

	// Store 4 has the same zone with store 3 and larger region score.
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test healthFilter.
	tc.setStoreBusy(4, true)
	c.Assert(rc.Check(region), IsNil)
	tc.setStoreBusy(4, false)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test checkBestReplacement
	// the number of region peers equals the maxReplicas
	// but two peers in same label level
	region.RemoveStorePeer(2)
	// Transfer peer from store 4.
	CheckTransferPeer(c, rc.Check(region), schedule.OpReplica, 4, 2)

	// Store 5 has a same label score with store 4,but the region score smaller than store 4, we will choose store 5.
	tc.addLabelsStore(5, 1, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, rc.Check(region), schedule.OpReplica, 4, 5)
	// Store 5 has too many snapshots, choose store 2
	tc.updateSnapshotCount(5, 10)
	CheckTransferPeer(c, rc.Check(region), schedule.OpReplica, 4, 2)
	tc.updatePendingPeerCount(2, 30)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.addLeaderRegion(1, 1)
	region := tc.GetRegion(1)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 1,2,3 have the same zone, rack, and host.
	tc.addLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 3)

	// Store 4 has smaller region score.
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 4)

	// Store 5 has a different host.
	tc.addLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 5)

	// Store 6 has a different rack.
	tc.addLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 6)

	// Store 7 has a different zone.
	tc.addLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 7)

	// Test stateFilter.
	tc.setStoreOffline(7)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 6)
	tc.setStoreUp(7)
	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 7)

	// Add peer to store 7.
	peer7, _ := tc.AllocPeer(7)
	region.Peers = append(region.Peers, peer7)

	// Replace peer in store 1 with store 6 because it has a different rack.
	CheckTransferPeer(c, rc.Check(region), schedule.OpReplica, 1, 6)
	peer6, _ := tc.AllocPeer(6)
	region.Peers = append(region.Peers, peer6)
	checkRemovePeer(c, rc.Check(region), 1)
	region.RemoveStorePeer(1)
	c.Assert(rc.Check(region), IsNil)

	// Store 8 has the same zone and different rack with store 7.
	// Store 1 has the same zone and different rack with store 6.
	// So store 8 and store 1 are equivalent.
	tc.addLabelsStore(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	c.Assert(rc.Check(region), IsNil)

	// Store 9 has a different zone, but it is almost full.
	tc.addLabelsStore(9, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.updateStorageRatio(9, 0.9, 0.1)
	c.Assert(rc.Check(region), IsNil)

	// Store 10 has a different zone.
	// Store 2 and 6 have the same distinct score, but store 2 has larger region score.
	// So replace peer in store 2 with store 10.
	tc.addLabelsStore(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, rc.Check(region), schedule.OpReplica, 2, 10)
	peer10, _ := tc.AllocPeer(10)
	region.Peers = append(region.Peers, peer10)
	checkRemovePeer(c, rc.Check(region), 2)
	region.RemoveStorePeer(2)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore2(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 5, "zone", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.addLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.addLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.addLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.addLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 4)
	region := tc.GetRegion(1)

	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 6)
	peer6, _ := tc.AllocPeer(6)
	region.Peers = append(region.Peers, peer6)

	CheckAddPeer(c, rc.Check(region), schedule.OpReplica, 5)
	peer5, _ := tc.AllocPeer(5)
	region.Peers = append(region.Peers, peer5)

	c.Assert(rc.Check(region), IsNil)
}

var _ = Suite(&testMergeCheckerSuite{})

type testMergeCheckerSuite struct {
	cluster *mockCluster
	mc      *schedule.MergeChecker
	regions []*core.RegionInfo
}

func (s *testMergeCheckerSuite) SetUpSuite(c *C) {
	cfg := newTestScheduleConfig()
	cfg.MaxMergeRegionSize = 2
	s.cluster = newMockCluster(cfg)
	s.regions = []*core.RegionInfo{
		{
			Region: &metapb.Region{
				Id:       1,
				StartKey: []byte(""),
				EndKey:   []byte("a"),
				Peers: []*metapb.Peer{
					{Id: 101, StoreId: 1},
					{Id: 102, StoreId: 2},
				},
			},
			Leader:          &metapb.Peer{Id: 101, StoreId: 1},
			ApproximateSize: 1,
		},
		{
			Region: &metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 103, StoreId: 1},
					{Id: 104, StoreId: 4},
					{Id: 105, StoreId: 5},
				},
			},
			Leader:          &metapb.Peer{Id: 104, StoreId: 4},
			ApproximateSize: 200,
		},
		{
			Region: &metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 1},
					{Id: 107, StoreId: 5},
					{Id: 108, StoreId: 6},
				},
			},
			Leader:          &metapb.Peer{Id: 108, StoreId: 6},
			ApproximateSize: 1,
		},
		{
			Region: &metapb.Region{
				Id:       4,
				StartKey: []byte("x"),
				EndKey:   []byte(""),
				Peers: []*metapb.Peer{
					{Id: 109, StoreId: 4},
				},
			},
			Leader:          &metapb.Peer{Id: 109, StoreId: 4},
			ApproximateSize: 10,
		},
	}

	for _, region := range s.regions {
		c.Assert(s.cluster.PutRegion(region), IsNil)
	}

	s.mc = schedule.NewMergeChecker(s.cluster, namespace.DefaultClassifier)
}

func (s *testMergeCheckerSuite) TestBasic(c *C) {
	// should with same peer count
	op1, op2 := s.mc.Check(s.regions[0])
	c.Assert(op1, IsNil)
	c.Assert(op2, IsNil)
	// size should be small enough
	op1, op2 = s.mc.Check(s.regions[1])
	c.Assert(op1, IsNil)
	c.Assert(op2, IsNil)
	op1, op2 = s.mc.Check(s.regions[2])
	c.Assert(op1, NotNil)
	c.Assert(op2, NotNil)
	op1, op2 = s.mc.Check(s.regions[3])
	c.Assert(op1, IsNil)
	c.Assert(op2, IsNil)
}

func (s *testMergeCheckerSuite) checkSteps(c *C, op *schedule.Operator, steps []schedule.OperatorStep) {
	c.Assert(steps, NotNil)
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		c.Assert(op.Step(i), DeepEquals, steps[i])
	}
}

func (s *testMergeCheckerSuite) TestMatchPeers(c *C) {
	// partial store overlap not including leader
	op1, op2 := s.mc.Check(s.regions[2])
	s.checkSteps(c, op1, []schedule.OperatorStep{
		schedule.AddPeer{ToStore: 4, PeerID: 2},
		schedule.TransferLeader{FromStore: 6, ToStore: 4},
		schedule.RemovePeer{FromStore: 6},
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  false,
		},
	})
	s.checkSteps(c, op2, []schedule.OperatorStep{
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  true,
		},
	})

	// partial store overlap including leader
	s.regions[2].Leader = &metapb.Peer{Id: 106, StoreId: 1}
	s.cluster.PutRegion(s.regions[2])
	op1, op2 = s.mc.Check(s.regions[2])
	s.checkSteps(c, op1, []schedule.OperatorStep{
		schedule.AddPeer{ToStore: 4, PeerID: 3},
		schedule.RemovePeer{FromStore: 6},
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  false,
		},
	})
	s.checkSteps(c, op2, []schedule.OperatorStep{
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  true,
		},
	})

	// all store overlap
	s.regions[2].Peers = []*metapb.Peer{
		{Id: 106, StoreId: 1},
		{Id: 107, StoreId: 5},
		{Id: 108, StoreId: 4},
	}
	s.cluster.PutRegion(s.regions[2])
	op1, op2 = s.mc.Check(s.regions[2])
	s.checkSteps(c, op1, []schedule.OperatorStep{
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  false,
		},
	})
	s.checkSteps(c, op2, []schedule.OperatorStep{
		schedule.MergeRegion{
			FromRegion: s.regions[2].Region,
			ToRegion:   s.regions[1].Region,
			IsPassive:  true,
		},
	})
}

var _ = Suite(&testBalanceHotWriteRegionSchedulerSuite{})

type testBalanceHotWriteRegionSchedulerSuite struct{}

func (s *testBalanceHotWriteRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	newTestReplication(opt, 3, "zone", "host")
	tc := newMockCluster(opt)
	hb, err := schedule.CreateScheduler("hot-write-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5, 6  with region counts 3, 2, 2, 2, 0, 0.

	tc.addLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.addLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.addLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.addLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.addLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.addLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.setStoreDown(7)

	// Report store written bytes.
	tc.updateStorageWrittenBytes(1, 75*1024*1024)
	tc.updateStorageWrittenBytes(2, 45*1024*1024)
	tc.updateStorageWrittenBytes(3, 45*1024*1024)
	tc.updateStorageWrittenBytes(4, 60*1024*1024)
	tc.updateStorageWrittenBytes(5, 0)
	tc.updateStorageWrittenBytes(6, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 3, 4)
	tc.addLeaderRegionWithWriteInfo(3, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 4)
	opt.HotRegionLowThreshold = 0

	// Will transfer a hot region from store 1 to store 6, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	op := hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))
	c.Assert(op, NotNil)
	if op[0].RegionID() == 2 {
		checkTransferPeerWithLeaderTransferFrom(c, op[0], schedule.OpHotRegion, 1)
	} else {
		checkTransferPeerWithLeaderTransfer(c, op[0], schedule.OpHotRegion, 1, 6)
	}

	// After transfer a hot region from store 1 to store 5
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       6      |        2       |       4        |      512KB    |
	//|     4     |       5      |        6       |       1        |      512KB    |
	//|     5     |       3      |        4       |       5        |      512KB    |
	tc.updateStorageWrittenBytes(1, 60*1024*1024)
	tc.updateStorageWrittenBytes(2, 30*1024*1024)
	tc.updateStorageWrittenBytes(3, 60*1024*1024)
	tc.updateStorageWrittenBytes(4, 30*1024*1024)
	tc.updateStorageWrittenBytes(5, 0*1024*1024)
	tc.updateStorageWrittenBytes(6, 30*1024*1024)
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(3, 6, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 4)
	tc.addLeaderRegionWithWriteInfo(4, 5, 512*1024*schedule.RegionHeartBeatReportInterval, 6, 4)
	tc.addLeaderRegionWithWriteInfo(5, 3, 512*1024*schedule.RegionHeartBeatReportInterval, 4, 5)
	// We can find that the leader of all hot regions are on store 1,
	// so one of the leader will transfer to another store.
	checkTransferLeaderFrom(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpHotRegion, 1)

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))
}

var _ = Suite(&testBalanceHotReadRegionSchedulerSuite{})

type testBalanceHotReadRegionSchedulerSuite struct{}

func (s *testBalanceHotReadRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)
	hb, err := schedule.CreateScheduler("hot-read-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 0)

	// Report store read bytes.
	tc.updateStorageReadBytes(1, 75*1024*1024)
	tc.updateStorageReadBytes(2, 45*1024*1024)
	tc.updateStorageReadBytes(3, 45*1024*1024)
	tc.updateStorageReadBytes(4, 60*1024*1024)
	tc.updateStorageReadBytes(5, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_sotre | follower_store | follower_store |   read_bytes  |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       2      |        1       |       3        |      512KB    |
	//|     3     |       1      |        2       |       3        |      512KB    |
	tc.addLeaderRegionWithReadInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithReadInfo(2, 2, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 3)
	tc.addLeaderRegionWithReadInfo(3, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	// lower than hot read flow rate, but higher than write flow rate
	tc.addLeaderRegionWithReadInfo(11, 1, 24*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	opt.HotRegionLowThreshold = 0
	c.Assert(tc.IsRegionHot(1), IsTrue)
	c.Assert(tc.IsRegionHot(11), IsFalse)
	// check randomly pick hot region
	r := tc.RandHotRegionFromStore(2, schedule.ReadFlow)
	c.Assert(r, NotNil)
	c.Assert(r.GetId(), Equals, uint64(2))
	c.Assert(r.ReadBytes, Equals, uint64(512*1024))
	// check hot items
	stats := tc.HotCache.RegionStats(schedule.ReadFlow)
	c.Assert(len(stats), Equals, 3)
	for _, s := range stats {
		c.Assert(s.FlowBytes, Equals, uint64(512*1024))
	}
	// Will transfer a hot region leader from store 1 to store 3, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	CheckTransferLeader(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpHotRegion, 1, 3)
	// assume handle the operator
	tc.addLeaderRegionWithReadInfo(3, 3, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 2)

	// After transfer a hot region leader from store 1 to store 3
	// the tree region leader will be evenly distributed in three stores
	tc.updateStorageReadBytes(1, 60*1024*1024)
	tc.updateStorageReadBytes(2, 30*1024*1024)
	tc.updateStorageReadBytes(3, 60*1024*1024)
	tc.updateStorageReadBytes(4, 30*1024*1024)
	tc.updateStorageReadBytes(5, 30*1024*1024)
	tc.addLeaderRegionWithReadInfo(4, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithReadInfo(5, 4, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 5)

	// Now appear two read hot region in store 1 and 4
	// We will Transfer peer from 1 to 5
	checkTransferPeerWithLeaderTransfer(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))[0], schedule.OpHotRegion, 1, 5)

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc, schedule.NewOpInfluence(nil, tc))
}

func checkRemovePeer(c *C, op *schedule.Operator, storeID uint64) {
	if op.Len() == 1 {
		c.Assert(op.Step(0).(schedule.RemovePeer).FromStore, Equals, storeID)
	} else {
		c.Assert(op.Len(), Equals, 2)
		c.Assert(op.Step(0).(schedule.TransferLeader).FromStore, Equals, storeID)
		c.Assert(op.Step(1).(schedule.RemovePeer).FromStore, Equals, storeID)
	}
}

func checkTransferPeerWithLeaderTransfer(c *C, op *schedule.Operator, kind schedule.OperatorKind, sourceID, targetID uint64) {
	c.Assert(op.Len(), Equals, 3)
	CheckTransferPeer(c, op, kind, sourceID, targetID)
}

func checkTransferLeaderFrom(c *C, op *schedule.Operator, kind schedule.OperatorKind, sourceID uint64) {
	c.Assert(op.Len(), Equals, 1)
	c.Assert(op.Step(0).(schedule.TransferLeader).FromStore, Equals, sourceID)
	kind |= schedule.OpLeader
	c.Assert(op.Kind()&kind, Equals, kind)
}

func checkTransferPeerWithLeaderTransferFrom(c *C, op *schedule.Operator, kind schedule.OperatorKind, sourceID uint64) {
	c.Assert(op.Len(), Equals, 3)
	c.Assert(op.Step(2).(schedule.RemovePeer).FromStore, Equals, sourceID)
	kind |= (schedule.OpRegion | schedule.OpLeader)
	c.Assert(op.Kind()&kind, Equals, kind)
}
