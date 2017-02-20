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
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
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
	store.stats.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) setStoreDown(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.stats.LastHeartbeatTS = time.Time{}
	c.putStore(store)
}

func (c *testClusterInfo) setStoreOffline(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Offline
	c.putStore(store)
}

func (c *testClusterInfo) setStoreBusy(storeID uint64, busy bool) {
	store := c.getStore(storeID)
	store.stats.IsBusy = busy
	store.stats.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderStore(storeID uint64, leaderCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.LeaderCount = uint32(leaderCount)
	c.putStore(store)
}

func (c *testClusterInfo) addRegionStore(storeID uint64, regionCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.RegionCount = uint32(regionCount)
	store.stats.Capacity = uint64(1024)
	store.stats.Available = store.stats.Capacity
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

func (c *testClusterInfo) updateLeaderCount(storeID uint64, leaderCount int) {
	store := c.getStore(storeID)
	store.stats.LeaderCount = uint32(leaderCount)
	c.putStore(store)
}

func (c *testClusterInfo) updateRegionCount(storeID uint64, regionCount int) {
	store := c.getStore(storeID)
	store.stats.RegionCount = uint32(regionCount)
	c.putStore(store)
}

func (c *testClusterInfo) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := c.getStore(storeID)
	store.stats.ApplyingSnapCount = uint32(snapshotCount)
	c.putStore(store)
}

func (c *testClusterInfo) updateStorageRatio(storeID uint64, storageRatio float64) {
	store := c.getStore(storeID)
	store.stats.Capacity = uint64(1024)
	store.stats.Available = uint64(float64(store.stats.Capacity) * (1 - storageRatio))
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
	balanceLimit   uint64
	expectedResult bool
}

func (s *testBalanceSpeedSuite) TestBalanceSpeed(c *C) {
	testCases := []testBalanceSpeedCase{
		// diff >= 2
		{1, 0, 1, false},
		{2, 0, 1, true},
		{2, 1, 1, false},
		{9, 0, 2, true},
		{9, 6, 1, true},
		{9, 8, 1, false},
		// diff >= sqrt(10) = 3.16
		{10, 0, 2, true},
		{10, 6, 1, true},
		{10, 7, 1, false},
		// diff >= sqrt(100) = 10
		{100, 89, 2, true},
		{100, 91, 2, false},
		// diff >= sqrt(1000) = 31.6
		{1000, 968, 8, true},
		{1000, 969, 7, false},
		// diff >= sqrt(10000) = 100
		{10000, 9899, 25, true},
		{10000, 9901, 24, false},
	}

	s.testBalanceSpeed(c, testCases, 1)
	s.testBalanceSpeed(c, testCases, 10)
	s.testBalanceSpeed(c, testCases, 100)
	s.testBalanceSpeed(c, testCases, 1000)
}

func (s *testBalanceSpeedSuite) testBalanceSpeed(c *C, tests []testBalanceSpeedCase, capaGB uint64) {
	for _, t := range tests {
		sourceCount := t.sourceCount
		sourceScore := float64(t.sourceCount) / float64(capaGB)
		targetScore := float64(t.targetCount) / float64(capaGB)
		limit, ok := adjustBalanceSpeed(sourceCount, sourceScore, targetScore)
		c.Assert(limit, Equals, t.balanceLimit)
		c.Assert(ok, Equals, t.expectedResult)
	}
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct{}

func (s *testBalanceLeaderSchedulerSuite) TestBalance(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	lb := newBalanceLeaderScheduler(opt)

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 1)
	tc.addLeaderStore(2, 0)
	tc.addLeaderStore(3, 0)
	tc.addLeaderStore(4, 0)
	// Add region 1 with leader in store 1 and followers in stores 2,3,4.
	tc.addLeaderRegion(1, 1, 2, 3, 4)

	// Test min balance diff (>=2).
	c.Check(lb.Schedule(cluster), IsNil)
	// 2 - 0 >= 2
	tc.updateLeaderCount(1, 2)
	c.Check(lb.Schedule(cluster), NotNil)

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 7)
	tc.addLeaderStore(2, 8)
	tc.addLeaderStore(3, 9)
	tc.addLeaderStore(4, 10)
	// Add region 1 with leader in store 4 and followers in stores 1,2,3.
	tc.addLeaderRegion(1, 4, 1, 2, 3)

	// Test min balance diff (>=4).
	c.Check(lb.Schedule(cluster), IsNil)
	// 16 - 7 >= 4
	tc.addLeaderStore(4, 16)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	tc.setStoreDown(1)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	tc.setStoreBusy(2, true)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 3)
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

	// Store 1 has the largest storage ratio, so the balancer try to replace peer in store 1.
	tc.addLabelsStore(1, 6, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 5, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(3, 4, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.addLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores,
	// so store 1 will be set in the cache and skipped next schedule.
	c.Assert(sb.Schedule(cluster), IsNil)
	c.Assert(sb.cache.get(1), IsTrue)

	// Store 4 has smaller region ratio than store 2.
	tc.addLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 2, 4)

	// Store 5 has smaller region ratio than store 1.
	tc.addLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	sb.cache.delete(1) // Delete store 1 from cache, or it will be skipped.
	checkTransferPeer(c, sb.Schedule(cluster), 1, 5)

	// Store 6 has smaller region ratio than store 5.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 1, 6)

	// Store 7 has the same storage ratio with store 6, but in a different host.
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

	// Store 8 has smaller storage ratio than store 7, but the distinct score decrease.
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

	// Store 9 has different zone with other stores but larger storage ratio than store 1.
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

	// Store 6 has smaller ratio.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 5, 6)

	// Store 7 has smaller ratio and higher score.
	tc.addLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	checkTransferPeer(c, sb.Schedule(cluster), 5, 7)

	// Store 1 has smaller ratio and higher score.
	tc.addLeaderRegion(1, 2, 3, 4, 5, 6)
	checkTransferPeer(c, sb.Schedule(cluster), 5, 1)

	// Store 6 has smaller ratio and higher score.
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
	// If storage ratio > storageRatioThreshold, we can not add peer.
	tc.updateStorageRatio(4, 0.9)
	checkAddPeer(c, rc.Check(region), 3)
	// If storage ratio < storageRatioThreshold, we can add peer again.
	tc.updateStorageRatio(4, 0.7)
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
		DownSeconds: proto.Uint64(24 * 60 * 60),
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkRemovePeer(c, rc.Check(region), 2)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 3 is offline, transfer peer to store 1.
	tc.setStoreOffline(3)
	checkTransferPeer(c, rc.Check(region), 3, 1)
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

	// Store 2 has different zone and smallest storage ratio.
	checkAddPeer(c, rc.Check(region), 2)
	peer2, _ := cluster.allocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 3 has different zone and smallest region ratio.
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)

	// Store 4 has the same zone with store 3 and larger region ratio.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test healthFilter.
	tc.setStoreBusy(4, true)
	c.Assert(rc.Check(region), IsNil)
	tc.setStoreBusy(4, false)
	checkRemovePeer(c, rc.Check(region), 4)
	region.RemoveStorePeer(4)

	// Transfer peer to store 4.
	tc.setStoreOffline(3)
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

	// Store 4 has smaller region ratio.
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
	tc.updateStorageRatio(9, 0.9)
	c.Assert(rc.Check(region), IsNil)

	// Store 10 has a different zone.
	// Store 2 and 6 have the same distinct score, but store 2 has larger region ratio.
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
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeer(c *C, bop Operator, storeID uint64) {
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeer(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator).Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, targetID)
	op = bop.(*regionOperator).Ops[1].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, sourceID)
}

func checkTransferLeader(c *C, bop Operator, sourceID, targetID uint64) {
	op := bop.(*regionOperator).Ops[0].(*transferLeaderOperator)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
	c.Assert(op.NewLeader.GetStoreId(), Equals, targetID)
}
