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
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type testClusterInfo struct {
	*clusterInfo
}

func newTestClusterInfo(cluster *clusterInfo) *testClusterInfo {
	return &testClusterInfo{clusterInfo: cluster}
}

func (c *testClusterInfo) setStoreUp(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.status.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) setStoreDown(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.status.LastHeartbeatTS = time.Time{}
	c.putStore(store)
}

func (c *testClusterInfo) setStoreOffline(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Offline
	c.putStore(store)
}

func (c *testClusterInfo) setStoreBusy(storeID uint64, busy bool) {
	store := c.getStore(storeID)
	store.status.IsBusy = busy
	store.status.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderStore(storeID uint64, leaderCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.status.LastHeartbeatTS = time.Now()
	store.status.LeaderCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) addRegionStore(storeID uint64, regionCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.status.LastHeartbeatTS = time.Now()
	store.status.RegionCount = regionCount
	store.status.Capacity = uint64(1024)
	store.status.Available = store.status.Capacity
	c.putStore(store)
}

func (c *testClusterInfo) addLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	c.addRegionStore(storeID, regionCount)
	store := c.getStore(storeID)
	for k, v := range labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: k, Value: v})
	}
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := c.allocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.allocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(newRegionInfo(region, leader))
}

func (c *testClusterInfo) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	region := &metapb.Region{Id: regionID}
	region.Peers = []*metapb.Peer{}
	for _, id := range followerIds {
		peer, _ := c.allocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(newRegionInfo(region, nil))
}

func (c *testClusterInfo) addLeaderRegionWithWriteInfo(regionID uint64, leaderID uint64, writtenBytes uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := c.allocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.allocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := newRegionInfo(region, leader)
	r.WrittenBytes = writtenBytes
	c.updateWriteStatus(r)
	c.putRegion(r)
}

func (c *testClusterInfo) updateLeaderCount(storeID uint64, leaderCount int) {
	store := c.getStore(storeID)
	store.status.LeaderCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) updateRegionCount(storeID uint64, regionCount int) {
	store := c.getStore(storeID)
	store.status.RegionCount = regionCount
	c.putStore(store)
}

func (c *testClusterInfo) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := c.getStore(storeID)
	store.status.ApplyingSnapCount = uint32(snapshotCount)
	c.putStore(store)
}

func (c *testClusterInfo) updateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	store := c.getStore(storeID)
	store.status.Capacity = uint64(1024)
	store.status.UsedSize = uint64(float64(store.status.Capacity) * usedRatio)
	store.status.Available = uint64(float64(store.status.Capacity) * availableRatio)
	c.putStore(store)
}

func (c *testClusterInfo) updateStorageWrittenBytes(storeID uint64, BytesWritten uint64) {
	store := c.getStore(storeID)
	store.status.BytesWritten = BytesWritten
	c.putStore(store)
}

func newTestScheduleConfig() (*ScheduleConfig, *scheduleOption) {
	cfg := NewConfig()
	cfg.adjust()
	opt := newScheduleOption(cfg)
	return &cfg.Schedule, opt
}

var _ = Suite(&testBalanceSpeedSuite{})

type testBalanceSpeedSuite struct{}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	expectedResult bool
}

func (s *testBalanceSpeedSuite) TestBalanceSpeed(c *C) {
	testCases := []testBalanceSpeedCase{
		// diff >= 2
		{1, 0, false},
		{2, 0, true},
		{2, 1, false},
		{9, 0, true},
		{9, 6, true},
		{9, 8, false},
		// diff >= sqrt(10) = 3.16
		{10, 0, true},
		{10, 6, true},
		{10, 7, false},
		// diff >= sqrt(100) = 10
		{100, 89, true},
		{100, 91, false},
		// diff >= sqrt(1000) = 31.6
		{1000, 968, true},
		{1000, 969, false},
		// diff >= sqrt(10000) = 100
		{10000, 9899, true},
		{10000, 9901, false},
	}

	s.testBalanceSpeed(c, testCases, 1)
	s.testBalanceSpeed(c, testCases, 10)
	s.testBalanceSpeed(c, testCases, 100)
	s.testBalanceSpeed(c, testCases, 1000)
}

func (s *testBalanceSpeedSuite) testBalanceSpeed(c *C, tests []testBalanceSpeedCase, capaGB uint64) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	for _, t := range tests {
		tc.addLeaderStore(1, int(t.sourceCount))
		tc.addLeaderStore(2, int(t.targetCount))
		source := cluster.getStore(1)
		target := cluster.getStore(2)
		c.Assert(shouldBalance(source, target, LeaderKind), Equals, t.expectedResult)
	}

	for _, t := range tests {
		tc.addRegionStore(1, int(t.sourceCount))
		tc.addRegionStore(2, int(t.targetCount))
		source := cluster.getStore(1)
		target := cluster.getStore(2)
		c.Assert(shouldBalance(source, target, RegionKind), Equals, t.expectedResult)
	}
}

func (s *testBalanceSpeedSuite) TestBalanceLimit(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)
	tc.addLeaderStore(1, 10)
	tc.addLeaderStore(2, 20)
	tc.addLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	c.Assert(adjustBalanceLimit(cluster, LeaderKind), Equals, uint64(math.Sqrt(200.0/3.0)))

	tc.setStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	c.Assert(adjustBalanceLimit(cluster, LeaderKind), Equals, uint64(math.Sqrt(50.0/2.0)))
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct {
	cluster *clusterInfo
	tc      *testClusterInfo
	lb      *balanceLeaderScheduler
}

func (s *testBalanceLeaderSchedulerSuite) SetUpTest(c *C) {
	s.cluster = newClusterInfo(newMockIDAllocator())
	s.tc = newTestClusterInfo(s.cluster)
	_, opt := newTestScheduleConfig()
	s.lb = newBalanceLeaderScheduler(opt)
}

func (s *testBalanceLeaderSchedulerSuite) schedule() Operator {
	return s.lb.Schedule(s.cluster)
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
	// Test min balance diff (>=2).
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    2    0    0    0
	// Region1:    L    F    F    F
	s.tc.updateLeaderCount(1, 2)
	c.Check(s.schedule(), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(1, 7)
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	s.tc.updateLeaderCount(4, 10)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	// Min balance diff is 4. Now is 10-7=3.
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(4, 16)
	// Min balance diff is 4. Now is 16-7=9.
	c.Check(s.schedule(), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceFilter(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   10
	// Region1:    F    F    F    L
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 10)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)

	checkTransferLeader(c, s.schedule(), 4, 1)
	// Test stateFilter.
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.setStoreDown(1)
	checkTransferLeader(c, s.schedule(), 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.setStoreBusy(2, true)
	checkTransferLeader(c, s.schedule(), 4, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceSelector(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   10
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 10)
	s.tc.addLeaderRegion(1, 4, 2, 3)
	s.tc.addLeaderRegion(2, 3, 1, 2)
	// Average leader is 4. Select store 4 as source.
	checkTransferLeader(c, s.schedule(), 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    8    9   10
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	// Average leader is 7. Select store 1 as target.
	checkTransferLeader(c, s.schedule(), 3, 1)
}

var _ = Suite(&testBalanceRegionSchedulerSuite{})

type testBalanceRegionSchedulerSuite struct{}

func (s *testBalanceRegionSchedulerSuite) TestBalance(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	sb := newBalanceRegionScheduler(opt)

	opt.SetMaxReplicas(1)

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 6)
	tc.addRegionStore(2, 8)
	tc.addRegionStore(3, 8)
	tc.addRegionStore(4, 9)
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)
	checkTransferPeer(c, sb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	// Test min balance diff (>=2).
	c.Assert(sb.Schedule(cluster), IsNil)
	// 9 - 6 >= 2
	tc.updateRegionCount(2, 6)
	sb.cache.delete(4)
	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)

	// Test MaxReplicas.
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(cluster), IsNil)
	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(cluster), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	opt.rep = newTestReplication(3, "zone", "rack", "host")

	sb := newBalanceRegionScheduler(opt)

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.addLabelsStore(1, 6, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 5, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(3, 4, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.addLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores,
	// so store 1 will be set in the cache and skipped next schedule.
	c.Assert(sb.Schedule(cluster), IsNil)
	c.Assert(sb.cache.get(1), IsTrue)

	// Store 4 has smaller region score than store 2.
	tc.addLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.addLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	sb.cache.delete(1) // Delete store 1 from cache, or it will be skipped.
	checkTransferPeer(c, sb.Schedule(cluster), 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 1, 6)

	// Store 7 has the same region score with store 6, but in a different host.
	tc.addLabelsStore(7, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	checkTransferPeer(c, sb.Schedule(cluster), 1, 7)

	// If store 7 is not available, we wait.
	tc.setStoreDown(7)
	c.Assert(sb.Schedule(cluster), IsNil)
	c.Assert(sb.cache.get(1), IsTrue)
	tc.setStoreUp(7)
	checkTransferPeer(c, sb.Schedule(cluster), 2, 7)
	sb.cache.delete(1)
	checkTransferPeer(c, sb.Schedule(cluster), 1, 7)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.addLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	checkTransferPeer(c, sb.Schedule(cluster), 1, 7)

	// Take down 4,5,6,7
	tc.setStoreDown(4)
	tc.setStoreDown(5)
	tc.setStoreDown(6)
	tc.setStoreDown(7)
	c.Assert(sb.Schedule(cluster), IsNil)
	c.Assert(sb.cache.get(1), IsTrue)
	sb.cache.delete(1)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.addLabelsStore(9, 9, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	c.Assert(sb.Schedule(cluster), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	opt.rep = newTestReplication(5, "zone", "rack", "host")

	sb := newBalanceRegionScheduler(opt)

	tc.addLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(5, 8, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 5, 6)

	// Store 7 has smaller region score and higher distinct score.
	tc.addLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 5, 7)

	// Store 1 has smaller region score and higher distinct score.
	tc.addLeaderRegion(1, 2, 3, 4, 5, 6)
	checkTransferPeer(c, sb.Schedule(cluster), 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.addLabelsStore(11, 9, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.addLeaderRegion(1, 2, 3, 11, 12, 13)
	checkTransferPeer(c, sb.Schedule(cluster), 11, 6)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) TestBasic(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	rc := newReplicaChecker(opt, cluster)

	cfg.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4)
	tc.addRegionStore(2, 3)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 4)

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.setStoreDown(4)
	checkAddPeer(c, rc.Check(region), 3)
	tc.setStoreUp(4)
	checkAddPeer(c, rc.Check(region), 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to store 3.
	tc.updateSnapshotCount(4, 3)
	checkAddPeer(c, rc.Check(region), 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.updateSnapshotCount(4, 1)
	checkAddPeer(c, rc.Check(region), 4)

	// Test storageThresholdFilter.
	// If availableRatio < storageAvailableRatioThreshold(0.2), we can not add peer.
	tc.updateStorageRatio(4, 0.9, 0.1)
	checkAddPeer(c, rc.Check(region), 3)
	tc.updateStorageRatio(4, 0.5, 0.1)
	checkAddPeer(c, rc.Check(region), 3)
	// If availableRatio > storageAvailableRatioThreshold(0.2), we can add peer again.
	tc.updateStorageRatio(4, 0.7, 0.3)
	checkAddPeer(c, rc.Check(region), 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	checkRemovePeer(c, rc.Check(region), 1)
	region.RemoveStorePeer(1)

	// Peer in store 2 is down, remove it.
	tc.setStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkRemovePeer(c, rc.Check(region), 2)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 3 is offline, transfer peer to store 1.
	tc.setStoreOffline(3)
	checkTransferPeer(c, rc.Check(region), 3, 1)
}

func (s *testReplicaCheckerSuite) TestLostStore(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)
	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 1)
	_, opt := newTestScheduleConfig()

	rc := newReplicaChecker(opt, cluster)

	// now region peer in store 1,2,3.but we just have store 1,2
	// This happens only in recovering the PD cluster
	// should not panic
	tc.addLeaderRegion(1, 1, 2, 3)
	region := cluster.getRegion(1)
	op := rc.Check(region)
	c.Assert(op, IsNil)
}

func (s *testReplicaCheckerSuite) TestOffline(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	opt.rep = newTestReplication(3, "zone", "rack", "host")

	rc := newReplicaChecker(opt, cluster)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1)
	region := cluster.getRegion(1)

	// Store 2 has different zone and smallest region score.
	checkAddPeer(c, rc.Check(region), 2)
	peer2, _ := cluster.allocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 3 has different zone and smallest region score.
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)

	// Store 4 has the same zone with store 3 and larger region score.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test healthFilter.
	tc.setStoreBusy(4, true)
	c.Assert(rc.Check(region), IsNil)
	tc.setStoreBusy(4, false)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test offline
	// the number of region peers more than the maxReplicas
	// remove the peer
	tc.setStoreOffline(3)
	checkRemovePeer(c, rc.Check(region), 3)
	region.RemoveStorePeer(4)
	// the number of region peers equals the maxReplicas
	// Transfer peer to store 4.
	checkTransferPeer(c, rc.Check(region), 3, 4)

	// Store 5 has a different zone, we can keep it safe.
	tc.addLabelsStore(5, 5, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	checkTransferPeer(c, rc.Check(region), 3, 5)
	tc.updateSnapshotCount(5, 10)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	opt.rep = newTestReplication(3, "zone", "rack", "host")

	rc := newReplicaChecker(opt, cluster)

	tc.addLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.addLeaderRegion(1, 1)
	region := tc.getRegion(1)
	checkAddPeer(c, rc.Check(region), 2)
	peer2, _ := cluster.allocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 1,2,3 have the same zone, rack, and host.
	tc.addLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	checkAddPeer(c, rc.Check(region), 3)

	// Store 4 has smaller region score.
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	checkAddPeer(c, rc.Check(region), 4)

	// Store 5 has a different host.
	tc.addLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	checkAddPeer(c, rc.Check(region), 5)

	// Store 6 has a different rack.
	tc.addLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	checkAddPeer(c, rc.Check(region), 6)

	// Store 7 has a different zone.
	tc.addLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	checkAddPeer(c, rc.Check(region), 7)

	// Test stateFilter.
	tc.setStoreOffline(7)
	checkAddPeer(c, rc.Check(region), 6)
	tc.setStoreUp(7)
	checkAddPeer(c, rc.Check(region), 7)

	// Add peer to store 7.
	peer7, _ := cluster.allocPeer(7)
	region.Peers = append(region.Peers, peer7)

	// Replace peer in store 1 with store 6 because it has a different rack.
	checkTransferPeer(c, rc.Check(region), 1, 6)
	peer6, _ := cluster.allocPeer(6)
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
	checkTransferPeer(c, rc.Check(region), 2, 10)
	peer10, _ := cluster.allocPeer(10)
	region.Peers = append(region.Peers, peer10)
	checkRemovePeer(c, rc.Check(region), 2)
	region.RemoveStorePeer(2)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore2(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	opt.rep = newTestReplication(5, "zone", "host")

	rc := newReplicaChecker(opt, cluster)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.addLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.addLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.addLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.addLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 4)
	region := cluster.getRegion(1)

	checkAddPeer(c, rc.Check(region), 6)
	peer6, _ := cluster.allocPeer(6)
	region.Peers = append(region.Peers, peer6)

	checkAddPeer(c, rc.Check(region), 5)
	peer5, _ := cluster.allocPeer(5)
	region.Peers = append(region.Peers, peer5)

	c.Assert(rc.Check(region), IsNil)
}

func checkAddPeer(c *C, bop Operator, storeID uint64) {
	var op *changePeerOperator
	switch t := bop.(type) {
	case *changePeerOperator:
		op = t
	case *regionOperator:
		op = t.Ops[0].(*changePeerOperator)
	}
	c.Assert(op.ChangePeer.GetChangeType(), Equals, pdpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeer(c *C, bop Operator, storeID uint64) {
	var op *changePeerOperator
	switch t := bop.(type) {
	case *changePeerOperator:
		op = t
	case *regionOperator:
		if len(t.Ops) == 1 {
			op = t.Ops[0].(*changePeerOperator)
		} else {
			c.Assert(t.Ops, HasLen, 2)
			transferLeader := t.Ops[0].(*transferLeaderOperator)
			c.Assert(transferLeader.OldLeader.GetStoreId(), Equals, storeID)
			op = t.Ops[1].(*changePeerOperator)
		}
	}
	c.Assert(op, NotNil)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, pdpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeer(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator)
	c.Assert(op, NotNil)
	if len(op.Ops) == 2 {
		checkAddPeer(c, op.Ops[0], targetID)
		checkRemovePeer(c, op.Ops[1], sourceID)
	} else {
		c.Assert(op.Ops, HasLen, 3)
		checkAddPeer(c, op.Ops[0], targetID)
		transferLeader := op.Ops[1].(*transferLeaderOperator)
		c.Assert(transferLeader.OldLeader.GetStoreId(), Equals, sourceID)
		checkRemovePeer(c, op.Ops[2], sourceID)
	}
}

func checkTransferPeerWithLeaderTransfer(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator)
	c.Assert(op, NotNil)
	c.Assert(op.Ops, HasLen, 3)
	checkTransferPeer(c, bop, sourceID, targetID)
}

func checkTransferLeader(c *C, bop Operator, sourceID, targetID uint64) {
	var op *transferLeaderOperator
	switch t := bop.(type) {
	case *transferLeaderOperator:
		op = t
	case *regionOperator:
		op = t.Ops[0].(*transferLeaderOperator)
	}
	c.Assert(op, NotNil)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
	c.Assert(op.NewLeader.GetStoreId(), Equals, targetID)
}

func checkTransferLeaderFrom(c *C, bop Operator, sourceID uint64) {
	var op *transferLeaderOperator
	switch t := bop.(type) {
	case *transferLeaderOperator:
		op = t
	case *regionOperator:
		op = t.Ops[0].(*transferLeaderOperator)
	}
	c.Assert(op, NotNil)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
}

var _ = Suite(&testBalanceHotRegionSchedulerSuite{})

type testBalanceHotRegionSchedulerSuite struct{}

func (s *testBalanceHotRegionSchedulerSuite) TestBalance(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	hb := newBalanceHotRegionScheduler(opt)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 0)

	// Report store written bytes.
	tc.updateStorageWrittenBytes(1, 75*1024*1024)
	tc.updateStorageWrittenBytes(2, 45*1024*1024)
	tc.updateStorageWrittenBytes(3, 45*1024*1024)
	tc.updateStorageWrittenBytes(4, 60*1024*1024)
	tc.updateStorageWrittenBytes(5, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*regionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*regionHeartBeatReportInterval, 3, 4)
	tc.addLeaderRegionWithWriteInfo(3, 1, 512*1024*regionHeartBeatReportInterval, 2, 4)
	hotRegionLowThreshold = 0

	// Will transfer a hot region from store 1 to store 5, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	checkTransferPeerWithLeaderTransfer(c, hb.Schedule(cluster), 1, 5)

	// After transfer a hot region from store 1 to store 5
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       5      |        2       |       4        |      512KB    |
	tc.updateStorageWrittenBytes(1, 60*1024*1024)
	tc.updateStorageWrittenBytes(2, 30*1024*1024)
	tc.updateStorageWrittenBytes(3, 60*1024*1024)
	tc.updateStorageWrittenBytes(4, 30*1024*1024)
	tc.updateStorageWrittenBytes(5, 30*1024*1024)
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*regionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*regionHeartBeatReportInterval, 3, 4)
	tc.addLeaderRegionWithWriteInfo(3, 5, 512*1024*regionHeartBeatReportInterval, 2, 4)

	// We can find that the leader of all hot regions are on store 1,
	// so one of the leader will transfer to another store.
	checkTransferLeaderFrom(c, hb.Schedule(cluster), 1)
}
