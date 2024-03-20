// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/versioninfo"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testClusterInfoSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testClusterInfoSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n, "2.0.0")
	storeMetasAfterHeartbeat := make([]*metapb.Store, 0, n)
	regions := newTestRegions(n, n, np)

	for _, region := range regions {
		c.Assert(cluster.putRegion(region), IsNil)
	}
	c.Assert(cluster.core.Regions.GetRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		c.Assert(cluster.HandleStoreHeartbeat(storeStats), NotNil)

		c.Assert(cluster.putStoreLocked(store), IsNil)
		c.Assert(cluster.GetStoreCount(), Equals, i+1)

		c.Assert(store.GetLastHeartbeatTS().UnixNano(), Equals, int64(0))

		c.Assert(cluster.HandleStoreHeartbeat(storeStats), IsNil)

		s := cluster.GetStore(store.GetID())
		c.Assert(s.GetLastHeartbeatTS().UnixNano(), Not(Equals), int64(0))
		c.Assert(s.GetStoreStats(), DeepEquals, storeStats)

		storeMetasAfterHeartbeat = append(storeMetasAfterHeartbeat, s.GetMeta())
	}

	c.Assert(cluster.GetStoreCount(), Equals, int(n))

	for i, store := range stores {
		tmp := &metapb.Store{}
		ok, err := cluster.storage.LoadStore(store.GetID(), tmp)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(tmp, DeepEquals, storeMetasAfterHeartbeat[i])
	}
	hotHeartBeat := &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{
			{
				RegionId:  1,
				ReadKeys:  9999999,
				ReadBytes: 9999998,
				QueryStats: &pdpb.QueryStats{
					Get: 9999997,
				},
			},
		},
	}
	coldHeartBeat := &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{},
	}
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats := cluster.hotStat.RegionStats(statistics.Read, 3)
	c.Assert(storeStats[1], HasLen, 1)
	c.Assert(storeStats[1][0].RegionID, Equals, uint64(1))
	interval := float64(hotHeartBeat.Interval.EndTimestamp - hotHeartBeat.Interval.StartTimestamp)
	c.Assert(storeStats[1][0].Loads, HasLen, int(statistics.RegionStatCount))
	c.Assert(storeStats[1][0].Loads[statistics.RegionReadBytes], Equals, float64(hotHeartBeat.PeerStats[0].ReadBytes)/interval)
	c.Assert(storeStats[1][0].Loads[statistics.RegionReadKeys], Equals, float64(hotHeartBeat.PeerStats[0].ReadKeys)/interval)
	c.Assert(storeStats[1][0].Loads[statistics.RegionReadQuery], Equals, float64(hotHeartBeat.PeerStats[0].QueryStats.Get)/interval)
	// After cold heartbeat, we won't find region 1 peer in regionStats
	c.Assert(cluster.HandleStoreHeartbeat(coldHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 1)
	c.Assert(storeStats[1], HasLen, 0)
	// After hot heartbeat, we can find region 1 peer again
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	c.Assert(storeStats[1], HasLen, 1)
	c.Assert(storeStats[1][0].RegionID, Equals, uint64(1))
	//  after several cold heartbeats, and one hot heartbeat, we also can't find region 1 peer
	c.Assert(cluster.HandleStoreHeartbeat(coldHeartBeat), IsNil)
	c.Assert(cluster.HandleStoreHeartbeat(coldHeartBeat), IsNil)
	c.Assert(cluster.HandleStoreHeartbeat(coldHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 0)
	c.Assert(storeStats[1], HasLen, 0)
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 1)
	c.Assert(storeStats[1], HasLen, 1)
	c.Assert(storeStats[1][0].RegionID, Equals, uint64(1))
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	c.Assert(storeStats[1], HasLen, 0)
	// after 2 hot heartbeats, wo can find region 1 peer again
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	c.Assert(cluster.HandleStoreHeartbeat(hotHeartBeat), IsNil)
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	c.Assert(storeStats[1], HasLen, 1)
	c.Assert(storeStats[1][0].RegionID, Equals, uint64(1))
}

func (s *testClusterInfoSuite) TestFilterUnhealthyStore(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	stores := newTestStores(3, "2.0.0")
	for _, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		c.Assert(cluster.putStoreLocked(store), IsNil)
		c.Assert(cluster.HandleStoreHeartbeat(storeStats), IsNil)
		c.Assert(cluster.hotStat.GetRollingStoreStats(store.GetID()), NotNil)
	}

	for _, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		newStore := store.Clone(core.TombstoneStore())
		c.Assert(cluster.putStoreLocked(newStore), IsNil)
		c.Assert(cluster.HandleStoreHeartbeat(storeStats), IsNil)
		c.Assert(cluster.hotStat.GetRollingStoreStats(store.GetID()), IsNil)
	}
}

func (s *testClusterInfoSuite) TestSetOfflineStore(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 6 stores.
	for _, store := range newTestStores(6, "2.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}

	// store 1: up -> offline
	c.Assert(cluster.RemoveStore(1, false), IsNil)
	store := cluster.GetStore(1)
	c.Assert(store.IsRemoving(), IsTrue)
	c.Assert(store.IsPhysicallyDestroyed(), IsFalse)

	// store 1: set physically to true success
	c.Assert(cluster.RemoveStore(1, true), IsNil)
	store = cluster.GetStore(1)
	c.Assert(store.IsRemoving(), IsTrue)
	c.Assert(store.IsPhysicallyDestroyed(), IsTrue)

	// store 2:up -> offline & physically destroyed
	c.Assert(cluster.RemoveStore(2, true), IsNil)
	// store 2: set physically destroyed to false failed
	c.Assert(cluster.RemoveStore(2, false), NotNil)
	c.Assert(cluster.RemoveStore(2, true), IsNil)

	// store 3: up to offline
	c.Assert(cluster.RemoveStore(3, false), IsNil)
	c.Assert(cluster.RemoveStore(3, false), IsNil)

	cluster.checkStores()
	// store 1,2,3 should be to tombstone
	for storeID := uint64(1); storeID <= 3; storeID++ {
		c.Assert(cluster.GetStore(storeID).IsRemoved(), IsTrue)
	}
	// test bury store
	for storeID := uint64(0); storeID <= 4; storeID++ {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsUp() {
			c.Assert(cluster.BuryStore(storeID, false), NotNil)
		} else {
			c.Assert(cluster.BuryStore(storeID, false), IsNil)
		}
	}
}

func (s *testClusterInfoSuite) TestSetOfflineWithReplica(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}

	c.Assert(cluster.RemoveStore(2, false), IsNil)
	// should be failed since no enough store to accommodate the extra replica.
	err = cluster.RemoveStore(3, false)
	c.Assert(strings.Contains(err.Error(), string(errs.ErrStoresNotEnough.RFCCode())), IsTrue)
	c.Assert(cluster.RemoveStore(3, false), NotNil)
	// should be success since physically-destroyed is true.
	c.Assert(cluster.RemoveStore(3, true), IsNil)
}

func addEvictLeaderScheduler(cluster *RaftCluster, storeID uint64) (evictScheduler schedule.Scheduler, err error) {
	args := []string{fmt.Sprintf("%d", storeID)}
	evictScheduler, err = schedule.CreateScheduler(schedulers.EvictLeaderType, cluster.GetOperatorController(), cluster.storage, schedule.ConfigSliceDecoder(schedulers.EvictLeaderType, args))
	if err != nil {
		return
	}
	if err = cluster.AddScheduler(evictScheduler, args...); err != nil {
		return
	} else if err = cluster.opt.Persist(cluster.GetStorage()); err != nil {
		return
	}
	return
}

func (s *testClusterInfoSuite) TestSetOfflineStoreWithEvictLeader(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// Put 3 stores.
	for _, store := range newTestStores(3, "2.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	_, err = addEvictLeaderScheduler(cluster, 1)

	c.Assert(err, IsNil)
	c.Assert(cluster.RemoveStore(2, false), IsNil)

	// should be failed since there is only 1 store left and it is the evict-leader store.
	err = cluster.RemoveStore(3, false)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), string(errs.ErrNoStoreForRegionLeader.RFCCode())), IsTrue)
	c.Assert(cluster.RemoveScheduler(schedulers.EvictLeaderName), IsNil)
	c.Assert(cluster.RemoveStore(3, false), IsNil)
}

func (s *testClusterInfoSuite) TestForceBuryStore(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	// Put 2 stores.
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	c.Assert(cluster.BuryStore(uint64(1), true), IsNil)
	c.Assert(cluster.BuryStore(uint64(2), true), NotNil)
	c.Assert(errors.ErrorEqual(cluster.BuryStore(uint64(3), true), errs.ErrStoreNotFound.FastGenByArgs(uint64(3))), IsTrue)
}

func (s *testClusterInfoSuite) TestReuseAddress(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	// store 1: up
	// store 2: offline
	c.Assert(cluster.RemoveStore(2, false), IsNil)
	// store 3: offline and physically destroyed
	c.Assert(cluster.RemoveStore(3, true), IsNil)
	// store 4: tombstone
	c.Assert(cluster.RemoveStore(4, true), IsNil)
	c.Assert(cluster.BuryStore(4, false), IsNil)

	for id := uint64(1); id <= 4; id++ {
		storeInfo := cluster.GetStore(id)
		storeID := storeInfo.GetID() + 1000
		newStore := &metapb.Store{
			Id:         storeID,
			Address:    storeInfo.GetAddress(),
			State:      metapb.StoreState_Up,
			Version:    storeInfo.GetVersion(),
			DeployPath: getTestDeployPath(storeID),
		}

		if storeInfo.IsPhysicallyDestroyed() || storeInfo.IsRemoved() {
			// try to start a new store with the same address with store which is physically destryed or tombstone should be success
			c.Assert(cluster.PutStore(newStore), IsNil)
		} else {
			c.Assert(cluster.PutStore(newStore), NotNil)
		}
	}
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func (s *testClusterInfoSuite) TestUpStore(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 5 stores.
	for _, store := range newTestStores(5, "5.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}

	// set store 1 offline
	c.Assert(cluster.RemoveStore(1, false), IsNil)
	// up a offline store should be success.
	c.Assert(cluster.UpStore(1), IsNil)

	// set store 2 offline and physically destroyed
	c.Assert(cluster.RemoveStore(2, true), IsNil)
	c.Assert(cluster.UpStore(2), NotNil)

	// bury store 2
	cluster.checkStores()
	// store is tombstone
	err = cluster.UpStore(2)
	c.Assert(errors.ErrorEqual(err, errs.ErrStoreRemoved.FastGenByArgs(2)), IsTrue)

	// store 3 is up
	c.Assert(cluster.UpStore(3), IsNil)

	// store 4 not exist
	err = cluster.UpStore(10)
	c.Assert(errors.ErrorEqual(err, errs.ErrStoreNotFound.FastGenByArgs(4)), IsTrue)
}

func (s *testClusterInfoSuite) TestRemovingProcess(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.SetPrepared()

	// Put 5 stores.
	stores := newTestStores(5, "5.0.0")
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	regions := newTestRegions(100, 5, 1)
	var regionInStore1 []*core.RegionInfo
	for _, region := range regions {
		if region.GetPeers()[0].GetStoreId() == 1 {
			region = region.Clone(core.SetApproximateSize(100))
			regionInStore1 = append(regionInStore1, region)
		}
		c.Assert(cluster.putRegion(region), IsNil)
	}
	c.Assert(len(regionInStore1), Equals, 20)
	cluster.progressManager = progress.NewManager()
	cluster.RemoveStore(1, false)
	cluster.checkStores()
	process := "removing-1"
	// no region moving
	p, l, cs, err := cluster.progressManager.Status(process)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.0)
	c.Assert(l, Equals, math.MaxFloat64)
	c.Assert(cs, Equals, 0.0)
	i := 0
	// simulate region moving by deleting region from store 1
	for _, region := range regionInStore1 {
		if i >= 5 {
			break
		}
		cluster.DropCacheRegion(region.GetID())
		i++
	}
	cluster.checkStores()
	p, l, cs, err = cluster.progressManager.Status(process)
	c.Assert(err, IsNil)
	// In above we delete 5 region from store 1, the total count of region in store 1 is 20.
	// process = 5 / 20 = 0.25
	c.Assert(p, Equals, 0.25)
	// Each region is 100MB, we use more than 1s to move 5 region.
	// speed = 5 * 100MB / 20s = 25MB/s
	c.Assert(cs, Equals, 25.0)
	// left second = 15 * 100MB / 25s = 60s
	c.Assert(l, Equals, 60.0)
}

func (s *testClusterInfoSuite) TestDeleteStoreUpdatesClusterVersion(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 3 new 4.0.9 stores.
	for _, store := range newTestStores(3, "4.0.9") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	c.Assert(cluster.GetClusterVersion(), Equals, "4.0.9")

	// Upgrade 2 stores to 5.0.0.
	for _, store := range newTestStores(2, "5.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	c.Assert(cluster.GetClusterVersion(), Equals, "4.0.9")

	// Bury the other store.
	c.Assert(cluster.RemoveStore(3, true), IsNil)
	cluster.checkStores()
	c.Assert(cluster.GetClusterVersion(), Equals, "5.0.0")
}

func (s *testClusterInfoSuite) TestRegionHeartbeatHotStat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	newTestStores(4, "2.0.0")
	peers := []*metapb.Peer{
		{
			Id:      1,
			StoreId: 1,
		},
		{
			Id:      2,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
	}
	leader := &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regionMeta := &metapb.Region{
		Id:          1,
		Peers:       peers,
		StartKey:    []byte{byte(1)},
		EndKey:      []byte{byte(1 + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}
	region := core.NewRegionInfo(regionMeta, leader, core.WithInterval(&pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}),
		core.SetWrittenBytes(30000*10),
		core.SetWrittenKeys(300000*10))
	err = cluster.processRegionHeartbeat(region)
	c.Assert(err, IsNil)
	// wait HotStat to update items
	time.Sleep(1 * time.Second)
	stats := cluster.hotStat.RegionStats(statistics.Write, 0)
	c.Assert(stats[1], HasLen, 1)
	c.Assert(stats[2], HasLen, 1)
	c.Assert(stats[3], HasLen, 1)
	newPeer := &metapb.Peer{
		Id:      4,
		StoreId: 4,
	}
	region = region.Clone(core.WithRemoveStorePeer(2), core.WithAddPeer(newPeer))
	err = cluster.processRegionHeartbeat(region)
	c.Assert(err, IsNil)
	// wait HotStat to update items
	time.Sleep(1 * time.Second)
	stats = cluster.hotStat.RegionStats(statistics.Write, 0)
	c.Assert(stats[1], HasLen, 1)
	c.Assert(stats[2], HasLen, 0)
	c.Assert(stats[3], HasLen, 1)
	c.Assert(stats[4], HasLen, 1)
}

func (s *testClusterInfoSuite) TestBucketHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// case1: region is not exist
	buckets := &metapb.Buckets{
		RegionId: 1,
		Version:  1,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	c.Assert(cluster.processReportBuckets(buckets), NotNil)

	// case2: bucket can be processed after the region update.
	stores := newTestStores(3, "2.0.0")
	n, np := uint64(2), uint64(2)
	regions := newTestRegions(n, n, np)
	for _, store := range stores {
		c.Assert(cluster.putStoreLocked(store), IsNil)
	}

	c.Assert(cluster.processRegionHeartbeat(regions[0]), IsNil)
	c.Assert(cluster.processRegionHeartbeat(regions[1]), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets(), IsNil)
	c.Assert(cluster.processReportBuckets(buckets), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets(), DeepEquals, buckets)

	// case3: the bucket version is same.
	c.Assert(cluster.processReportBuckets(buckets), IsNil)
	// case4: the bucket version is changed.
	newBuckets := &metapb.Buckets{
		RegionId: 1,
		Version:  3,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	c.Assert(cluster.processReportBuckets(newBuckets), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets(), DeepEquals, newBuckets)

	// case5: region update should inherit buckets.
	newRegion := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	cluster.storeConfigManager = config.NewTestStoreConfigManager(nil)
	config := cluster.storeConfigManager.GetStoreConfig()
	config.Coprocessor.EnableRegionBucket = true
	c.Assert(cluster.processRegionHeartbeat(newRegion), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys(), HasLen, 2)

	// case6: disable region bucket in
	config.Coprocessor.EnableRegionBucket = false
	newRegion2 := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	c.Assert(cluster.processRegionHeartbeat(newRegion2), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets(), IsNil)
	c.Assert(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys(), HasLen, 0)
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3, "2.0.0")
	regions := newTestRegions(n, n, np)

	for _, store := range stores {
		c.Assert(cluster.putStoreLocked(store), IsNil)
	}

	for i, region := range regions {
		// region does not exist.
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is updated.
		region = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Add a down peer.
		region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        region.GetPeers()[rand.Intn(len(region.GetPeers()))],
				DownSeconds: 42,
			},
		}))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Remove peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Change leader.
		region = region.Clone(core.WithLeader(region.GetPeers()[1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Change ApproximateSize.
		region = region.Clone(core.SetApproximateSize(144))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Change ApproximateKeys.
		region = region.Clone(core.SetApproximateKeys(144000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Change bytes written.
		region = region.Clone(core.SetWrittenBytes(24000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions.RegionsInfo, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		c.Assert(cluster.GetStoreRegionCount(id), Equals, count)
	}

	for _, region := range cluster.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cluster.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].GetMeta())
	}

	for _, region := range regions {
		for _, store := range cluster.GetRegionStores(region) {
			c.Assert(region.GetStorePeer(store.GetID()), NotNil)
		}
		for _, store := range cluster.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetID())
			c.Assert(peer.GetId(), Not(Equals), region.GetLeader().GetId())
		}
	}

	for _, store := range cluster.core.Stores.GetStores() {
		c.Assert(store.GetLeaderCount(), Equals, cluster.core.Regions.GetStoreLeaderCount(store.GetID()))
		c.Assert(store.GetRegionCount(), Equals, cluster.core.Regions.GetStoreRegionCount(store.GetID()))
		c.Assert(store.GetLeaderSize(), Equals, cluster.core.Regions.GetStoreLeaderRegionSize(store.GetID()))
		c.Assert(store.GetRegionSize(), Equals, cluster.core.Regions.GetStoreRegionSize(store.GetID()))
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := storage.LoadRegion(region.GetID(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, region.GetMeta())
		}

		// Check overlap with stale version
		overlapRegion := regions[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewRegionID(10000),
			core.WithDecVersion(),
		)
		c.Assert(cluster.processRegionHeartbeat(overlapRegion), NotNil)
		region := &metapb.Region{}
		ok, err := storage.LoadRegion(regions[n-1].GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, regions[n-1].GetMeta())
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, regions[n-2].GetMeta())
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)

		// Check overlap
		overlapRegion = regions[n-1].Clone(
			core.WithStartKey(regions[n-2].GetStartKey()),
			core.WithNewRegionID(regions[n-1].GetID()+1),
		)
		c.Assert(cluster.processRegionHeartbeat(overlapRegion), IsNil)
		region = &metapb.Region{}
		ok, err = storage.LoadRegion(regions[n-1].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, overlapRegion.GetMeta())
	}
}

func (s *testClusterInfoSuite) TestRegionFlowChanged(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	processRegions := func(regions []*core.RegionInfo) {
		for _, r := range regions {
			cluster.processRegionHeartbeat(r)
		}
	}
	regions = core.SplitRegions(regions)
	processRegions(regions)
	// update region
	region := regions[0]
	regions[0] = region.Clone(core.SetReadBytes(1000))
	processRegions(regions)
	newRegion := cluster.GetRegion(region.GetID())
	c.Assert(newRegion.GetBytesRead(), Equals, uint64(1000))
}

func (s *testClusterInfoSuite) TestRegionSizeChanged(c *C) {
	_, opt, err := newTestScheduleConfig()
<<<<<<< HEAD
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
=======
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager,
		cluster.storeConfigManager)
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))
	region := newTestRegions(1, 3, 3)[0]
	cluster.opt.GetMaxMergeRegionKeys()
	curMaxMergeSize := int64(cluster.opt.GetMaxMergeRegionSize())
	curMaxMergeKeys := int64(cluster.opt.GetMaxMergeRegionKeys())
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize-1),
		core.SetApproximateKeys(curMaxMergeKeys-1),
		core.SetFromHeartbeat(true),
	)
	cluster.processRegionHeartbeat(region)
	regionID := region.GetID()
	c.Assert(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion), IsTrue)
	// Test ApproximateSize and ApproximateKeys change.
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize+1),
		core.SetApproximateKeys(curMaxMergeKeys+1),
		core.SetFromHeartbeat(true),
	)
	cluster.processRegionHeartbeat(region)
	c.Assert(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion), IsFalse)
	// Test MaxMergeRegionSize and MaxMergeRegionKeys change.
	cluster.opt.SetMaxMergeRegionSize((uint64(curMaxMergeSize + 2)))
	cluster.opt.SetMaxMergeRegionKeys((uint64(curMaxMergeKeys + 2)))
	cluster.processRegionHeartbeat(region)
	c.Assert(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion), IsTrue)
	cluster.opt.SetMaxMergeRegionSize((uint64(curMaxMergeSize)))
	cluster.opt.SetMaxMergeRegionKeys((uint64(curMaxMergeKeys)))
	cluster.processRegionHeartbeat(region)
	c.Assert(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion), IsFalse)
}

func (s *testClusterInfoSuite) TestConcurrentReportBucket(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	heartbeatRegions(c, cluster, regions)
	c.Assert(cluster.GetRegion(0), NotNil)

	bucket1 := &metapb.Buckets{RegionId: 0, Version: 3}
	bucket2 := &metapb.Buckets{RegionId: 0, Version: 2}
	var wg sync.WaitGroup
	wg.Add(1)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat", "return(true)"), IsNil)
	go func() {
		defer wg.Done()
		cluster.processReportBuckets(bucket1)
	}()
	time.Sleep(100 * time.Millisecond)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat"), IsNil)
	c.Assert(cluster.processReportBuckets(bucket2), IsNil)
	wg.Wait()
	c.Assert(cluster.GetRegion(0).GetBuckets(), DeepEquals, bucket1)
}

func (s *testClusterInfoSuite) TestConcurrentRegionHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	regions = core.SplitRegions(regions)
	heartbeatRegions(c, cluster, regions)

	// Merge regions manually
	source, target := regions[0], regions[1]
	target.GetMeta().StartKey = []byte{}
	target.GetMeta().EndKey = []byte{}
	source.GetMeta().GetRegionEpoch().Version++
	if source.GetMeta().GetRegionEpoch().GetVersion() > target.GetMeta().GetRegionEpoch().GetVersion() {
		target.GetMeta().GetRegionEpoch().Version = source.GetMeta().GetRegionEpoch().GetVersion()
	}
	target.GetMeta().GetRegionEpoch().Version++

	var wg sync.WaitGroup
	wg.Add(1)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat", "return(true)"), IsNil)
	go func() {
		defer wg.Done()
		cluster.processRegionHeartbeat(source)
	}()
	time.Sleep(100 * time.Millisecond)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat"), IsNil)
	c.Assert(cluster.processRegionHeartbeat(target), IsNil)
	wg.Wait()
	checkRegion(c, cluster.GetRegionByKey([]byte{}), target)
}

func (s *testClusterInfoSuite) TestRegionLabelIsolationLevel(c *C) {
	_, opt, err := newTestScheduleConfig()
	cfg := opt.GetReplicationConfig()
	cfg.LocationLabels = []string{"zone"}
	opt.SetReplicationConfig(cfg)
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	for i := uint64(1); i <= 4; i++ {
		var labels []*metapb.StoreLabel
		if i == 4 {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", 3)}, {Key: "engine", Value: "tiflash"}}
		} else {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", i)}}
		}
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("127.0.0.1:%d", i),
			State:   metapb.StoreState_Up,
			Labels:  labels,
		}
		c.Assert(cluster.putStoreLocked(core.NewStoreInfo(store)), IsNil)
	}

	peers := make([]*metapb.Peer, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		peer := &metapb.Peer{
			Id: i + 4,
		}
		peer.StoreId = i
		if i == 8 {
			peer.Role = metapb.PeerRole_Learner
		}
		peers = append(peers, peer)
	}
	region := &metapb.Region{
		Id:       9,
		Peers:    peers,
		StartKey: []byte{byte(1)},
		EndKey:   []byte{byte(2)},
	}
	r := core.NewRegionInfo(region, peers[0])
	c.Assert(cluster.putRegion(r), IsNil)

	cluster.updateRegionsLabelLevelStats([]*core.RegionInfo{r})
	counter := cluster.labelLevelStats.GetLabelCounter()
	c.Assert(counter["none"], Equals, 0)
	c.Assert(counter["zone"], Equals, 1)
}

func heartbeatRegions(c *C, cluster *RaftCluster, regions []*core.RegionInfo) {
	// Heartbeat and check region one by one.
	for _, r := range regions {
		c.Assert(cluster.processRegionHeartbeat(r), IsNil)

		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionByKey([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, r := range regions {
		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionByKey([]byte{end - 1}), r)
			result := cluster.GetRegionByKey([]byte{end + 1})
			c.Assert(result.GetID(), Not(Equals), r.GetID())
		}
	}
}

func (s *testClusterInfoSuite) TestHeartbeatSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// 1: [nil, nil)
	region1 := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionByKey([]byte("foo")), region1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	region2 := core.NewRegionInfo(&metapb.Region{Id: 2, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region2), IsNil)
	checkRegion(c, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, nil) is missing before r1's heartbeat.
	c.Assert(cluster.GetRegionByKey([]byte("z")), IsNil)

	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionByKey([]byte("z")), region1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	region3 := core.NewRegionInfo(&metapb.Region{Id: 3, StartKey: []byte("m"), EndKey: []byte("q"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.processRegionHeartbeat(region1), IsNil)
	checkRegion(c, cluster.GetRegionByKey([]byte("z")), region1)
	checkRegion(c, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, q) is missing before r3's heartbeat.
	c.Assert(cluster.GetRegionByKey([]byte("n")), IsNil)
	c.Assert(cluster.processRegionHeartbeat(region3), IsNil)
	checkRegion(c, cluster.GetRegionByKey([]byte("n")), region3)
}

func (s *testClusterInfoSuite) TestRegionSplitAndMerge(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = core.MergeRegions(regions)
		} else {
			regions = core.SplitRegions(regions)
		}
		heartbeatRegions(c, cluster, regions)
	}
}

func (s *testClusterInfoSuite) TestOfflineAndMerge(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}
<<<<<<< HEAD
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
=======
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager,
		cluster.storeConfigManager)
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))

	// Put 4 stores.
	for _, store := range newTestStores(4, "5.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}

	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		}, {
			Id:      5,
			StoreId: 2,
		}, {
			Id:      6,
			StoreId: 3,
		},
	}
	origin := core.NewRegionInfo(
		&metapb.Region{
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			Id:          1,
			Peers:       peers}, peers[0])
	regions := []*core.RegionInfo{origin}

	// store 1: up -> offline
	c.Assert(cluster.RemoveStore(1, false), IsNil)
	store := cluster.GetStore(1)
	c.Assert(store.IsRemoving(), IsTrue)

	// Split.
	n := 7
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
	}
<<<<<<< HEAD
	heartbeatRegions(c, cluster, regions)
	c.Assert(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), HasLen, len(regions))
=======
	heartbeatRegions(re, cluster, regions)
	re.Len(cluster.GetRegionStatsByType(statistics.OfflinePeer), len(regions))
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
<<<<<<< HEAD
		heartbeatRegions(c, cluster, regions)
		c.Assert(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), HasLen, len(regions))
=======
		heartbeatRegions(re, cluster, regions)
		re.Len(cluster.GetRegionStatsByType(statistics.OfflinePeer), len(regions))
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))
	}
}

func (s *testClusterInfoSuite) TestSyncConfig(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(s.ctx, opt)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		c.Assert(tc.putStoreLocked(s), IsNil)
	}
	c.Assert(tc.getUpStores(), HasLen, 5)

	testdata := []struct {
		whiteList     []string
		maxRegionSize uint64
		updated       bool
	}{{
		whiteList:     []string{},
		maxRegionSize: uint64(144),
		updated:       false,
	}, {
		whiteList:     []string{"127.0.0.1:5"},
		maxRegionSize: uint64(10),
		updated:       true,
	}}

	for _, v := range testdata {
		tc.storeConfigManager = config.NewTestStoreConfigManager(v.whiteList)
		c.Assert(tc.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(144))
		c.Assert(syncConfig(tc.storeConfigManager, tc.GetStores()), Equals, v.updated)
		c.Assert(tc.GetStoreConfig().GetRegionMaxSize(), Equals, v.maxRegionSize)
	}
}

func (s *testClusterInfoSuite) TestUpdateStorePendingPeerCount(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(s.ctx, opt)
	tc.RaftCluster.coordinator = newCoordinator(s.ctx, tc.RaftCluster, nil)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		c.Assert(tc.putStoreLocked(s), IsNil)
	}
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
		{
			Id:      4,
			StoreId: 4,
		},
	}
	origin := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[:3]}, peers[0], core.WithPendingPeers(peers[1:3]))
	c.Assert(tc.processRegionHeartbeat(origin), IsNil)
	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.RaftCluster, c)
	newRegion := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[1:]}, peers[1], core.WithPendingPeers(peers[3:4]))
	c.Assert(tc.processRegionHeartbeat(newRegion), IsNil)
	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.RaftCluster, c)
}

func (s *testClusterInfoSuite) TestTopologyWeight(c *C) {
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3", "h4"}

	var stores []*core.StoreInfo
	var testStore *core.StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := core.NewStoreInfoWithLabel(storeID, 1, storeLabels)
				if i == 0 && j == 0 && k == 0 {
					testStore = store
				}
				stores = append(stores, store)
			}
		}
	}

	c.Assert(1.0/3/3/4, Equals, getStoreTopoWeight(testStore, stores, labels, 3))
}

func (s *testClusterInfoSuite) TestTopologyWeight1(c *C) {
	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, 1, map[string]string{"dc": "dc2", "zone": "zone2", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, 1, map[string]string{"dc": "dc3", "zone": "zone3", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store5 := core.NewStoreInfoWithLabel(5, 1, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store6 := core.NewStoreInfoWithLabel(6, 1, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5, store6}

	c.Assert(1.0/3, Equals, getStoreTopoWeight(store2, stores, labels, 3))
	c.Assert(1.0/3/4, Equals, getStoreTopoWeight(store1, stores, labels, 3))
	c.Assert(1.0/3/4, Equals, getStoreTopoWeight(store6, stores, labels, 3))
}

func (s *testClusterInfoSuite) TestTopologyWeight2(c *C) {
	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, 1, map[string]string{"dc": "dc2"})
	store3 := core.NewStoreInfoWithLabel(3, 1, map[string]string{"dc": "dc3"})
	store4 := core.NewStoreInfoWithLabel(4, 1, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host1"})
	store5 := core.NewStoreInfoWithLabel(5, 1, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host1"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5}

	c.Assert(1.0/3, Equals, getStoreTopoWeight(store2, stores, labels, 3))
	c.Assert(1.0/3/3, Equals, getStoreTopoWeight(store1, stores, labels, 3))
}

func (s *testClusterInfoSuite) TestTopologyWeight3(c *C) {
	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, 1, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, 1, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, 1, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host4"})
	store5 := core.NewStoreInfoWithLabel(5, 1, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host5"})
	store6 := core.NewStoreInfoWithLabel(6, 1, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host6"})

	store7 := core.NewStoreInfoWithLabel(7, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host7"})
	store8 := core.NewStoreInfoWithLabel(8, 1, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host8"})
	store9 := core.NewStoreInfoWithLabel(9, 1, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host9"})
	store10 := core.NewStoreInfoWithLabel(10, 1, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host10"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5, store6, store7, store8, store9, store10}

	c.Assert(1.0/5/2, Equals, getStoreTopoWeight(store7, stores, labels, 5))
	c.Assert(1.0/5/4, Equals, getStoreTopoWeight(store8, stores, labels, 5))
	c.Assert(1.0/5/4, Equals, getStoreTopoWeight(store9, stores, labels, 5))
	c.Assert(1.0/5/2, Equals, getStoreTopoWeight(store10, stores, labels, 5))
}

func (s *testClusterInfoSuite) TestTopologyWeight4(c *C) {
	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, 1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, 1, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, 1, map[string]string{"dc": "dc2", "zone": "zone1", "host": "host4"})

	stores := []*core.StoreInfo{store1, store2, store3, store4}

	c.Assert(1.0/3/2, Equals, getStoreTopoWeight(store1, stores, labels, 3))
	c.Assert(1.0/3, Equals, getStoreTopoWeight(store3, stores, labels, 3))
	c.Assert(1.0/3, Equals, getStoreTopoWeight(store4, stores, labels, 3))
}

func (s *testClusterInfoSuite) TestCalculateStoreSize1(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
<<<<<<< HEAD
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
=======
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager,
		cluster.storeConfigManager)
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%3 == 0 {
			// zone 1 has 1, 4, 7, 10
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone1"})
		} else if i%3 == 1 {
			// zone 2 has 2, 5, 8
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone2"})
		} else {
			// zone 3 has 3, 6, 9
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone3"})
		}
		labels = append(labels, []*metapb.StoreLabel{
			{
				Key:   "rack",
				Value: fmt.Sprintf("rack-%d", i%2+1),
			},
			{
				Key:   "host",
				Value: fmt.Sprintf("host-%d", i),
			},
		}...)
		s := store.Clone(core.SetStoreLabels(labels))
		c.Assert(cluster.PutStore(s.GetMeta()), IsNil)
	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone1", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone1"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone2", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone2"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone3", StartKey: []byte(""), EndKey: []byte(""), Role: "follower", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone3"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)
	cluster.ruleManager.DeleteRule("pd", "default")

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		c.Assert(cluster.putRegion(region), IsNil)
	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)
	// 100 * 100 * 2 (placement rule) / 4 (host) * 0.9 = 4500
	c.Assert(cluster.getThreshold(stores, store), Equals, 4500.0)

	cluster.opt.SetPlacementRuleEnabled(false)
	cluster.opt.SetLocationLabels([]string{"zone", "rack", "host"})
	// 30000 (total region size) / 3 (zone) / 4 (host) * 0.9 = 2250
	c.Assert(cluster.getThreshold(stores, store), Equals, 2250.0)
}

func (s *testClusterInfoSuite) TestCalculateStoreSize2(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	opt.SetMaxReplicas(3)
<<<<<<< HEAD
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
=======
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager,
		cluster.storeConfigManager)
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%2 == 0 {
			// dc 1 has 1, 3, 5, 7, 9
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc1"})
			if i%4 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic1"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic2"})
			}
		} else {
			// dc 2 has 2, 4, 6, 8, 10
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc2"})
			if i%3 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic3"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic4"})
			}
		}
		labels = append(labels, []*metapb.StoreLabel{{Key: "rack", Value: "r1"}, {Key: "host", Value: "h1"}}...)
		s := store.Clone(core.SetStoreLabels(labels))
		c.Assert(cluster.PutStore(s.GetMeta()), IsNil)
	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "dc1", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc1"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "logic3", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic3"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "logic4", StartKey: []byte(""), EndKey: []byte(""), Role: "learner", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic4"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)
	cluster.ruleManager.DeleteRule("pd", "default")

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		c.Assert(cluster.putRegion(region), IsNil)
	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)

	// 100 * 100 * 4 (total region size) / 2 (dc) / 2 (logic) / 3 (host) * 0.9 = 3000
	c.Assert(cluster.getThreshold(stores, store), Equals, 3000.0)
}

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

func (s *testStoresInfoSuite) TestStores(c *C) {
	n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n, "2.0.0")

	for i, store := range stores {
		id := store.GetID()
		c.Assert(cache.GetStore(id), IsNil)
		c.Assert(cache.PauseLeaderTransfer(id), NotNil)
		cache.SetStore(store)
		c.Assert(cache.GetStore(id), DeepEquals, store)
		c.Assert(cache.GetStoreCount(), Equals, i+1)
		c.Assert(cache.PauseLeaderTransfer(id), IsNil)
		c.Assert(cache.GetStore(id).AllowLeaderTransfer(), IsFalse)
		c.Assert(cache.PauseLeaderTransfer(id), NotNil)
		cache.ResumeLeaderTransfer(id)
		c.Assert(cache.GetStore(id).AllowLeaderTransfer(), IsTrue)
	}
	c.Assert(cache.GetStoreCount(), Equals, int(n))

	for _, store := range cache.GetStores() {
		c.Assert(store, DeepEquals, stores[store.GetID()-1])
	}
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1].GetMeta())
	}

	c.Assert(cache.GetStoreCount(), Equals, int(n))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testRegionsInfoSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testRegionsInfoSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	regions := newTestRegions(n, n, np)
	_, opts, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opts, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cache := tc.core.Regions.RegionsInfo

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.GetRegionByKey(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.SetRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.GetRegionByKey(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])
		// previous region
		if i == 0 {
			c.Assert(cache.GetPrevRegionByKey(regionKey), IsNil)
		} else {
			checkRegion(c, cache.GetPrevRegionByKey(regionKey), regions[i-1])
		}
		// Update leader to peer np-1.
		newRegion := region.Clone(core.WithLeader(region.GetPeers()[np-1]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegion(c, cache.GetRegionByKey(regionKey), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.GetRegionByKey(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		newRegion = region.Clone(core.WithLeader(region.GetPeers()[0]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.GetRegionByKey(regionKey), newRegion)
	}

	for i := uint64(0); i < n; i++ {
		region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
		c.Assert(region.GetLeader().GetStoreId(), Equals, i)

		region = tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
		c.Assert(region.GetLeader().GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapRegion := regions[n-1].Clone(core.WithStartKey(regions[n-2].GetStartKey()))
	cache.SetRegion(overlapRegion)
	c.Assert(cache.GetRegion(n-2), IsNil)
	c.Assert(cache.GetRegion(n-1), NotNil)

	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount(i); j++ {
			region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			cache.SetRegion(newRegion)
		}
		c.Assert(tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy), IsNil)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := core.NewTestRegionInfo([]byte{}, []byte{})
	origin := core.NewTestRegionInfo([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)

	// (1, 0) v.s. (0, 0)
	region.GetRegionEpoch().Version++
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)

	// (1, 1) v.s. (0, 0)
	region.GetRegionEpoch().ConfVer++
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)

	// (0, 1) v.s. (0, 0)
	region.GetRegionEpoch().Version--
	c.Assert(checkStaleRegion(origin.GetMeta(), region.GetMeta()), IsNil)
	c.Assert(checkStaleRegion(region.GetMeta(), origin.GetMeta()), NotNil)
}

var _ = Suite(&testGetStoresSuite{})

type testGetStoresSuite struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *RaftCluster
}

func (s *testGetStoresSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testGetStoresSuite) SetUpSuite(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	s.cluster = cluster

	stores := newTestStores(200, "2.0.0")

	for _, store := range stores {
		c.Assert(s.cluster.putStoreLocked(store), IsNil)
	}
}

func (s *testGetStoresSuite) BenchmarkGetStores(c *C) {
	for i := 0; i < c.N; i++ {
		// Logic to benchmark
		s.cluster.core.Stores.GetStores()
	}
}

type testCluster struct {
	*RaftCluster
}

func newTestScheduleConfig() (*config.ScheduleConfig, *config.PersistOptions, error) {
	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	if err := cfg.Adjust(nil, false); err != nil {
		return nil, nil, err
	}
	opt := config.NewPersistOptions(cfg)
	opt.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version2_0))
	return &cfg.Schedule, opt, nil
}

func newTestCluster(ctx context.Context, opt *config.PersistOptions) *testCluster {
	rc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	storage := storage.NewStorageWithMemoryBackend()
	rc.regionLabeler, _ = labeler.NewRegionLabeler(ctx, storage, time.Second*5)

	return &testCluster{RaftCluster: rc}
}

func newTestRaftCluster(
	ctx context.Context,
	id id.Allocator,
	opt *config.PersistOptions,
	s storage.Storage,
	basicCluster *core.BasicCluster,
) *RaftCluster {
	rc := &RaftCluster{serverCtx: ctx}
	rc.InitCluster(id, opt, s, basicCluster)
	rc.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), rc, opt)
	if opt.IsPlacementRulesEnabled() {
		err := rc.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}
	return rc
}

// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:            i,
			Address:       fmt.Sprintf("127.0.0.1:%d", i),
			StatusAddress: fmt.Sprintf("127.0.0.1:%d", i),
			State:         metapb.StoreState_Up,
			Version:       version,
			DeployPath:    getTestDeployPath(i),
			NodeState:     metapb.NodeState_Serving,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

// Create n regions (0..n) of m stores (0..m).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, m, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % m
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte{byte(i)},
			EndKey:      []byte{byte(i + 1)},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0], core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
	}
	return regions
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
	}
}

func checkRegion(c *C, a *core.RegionInfo, b *core.RegionInfo) {
	c.Assert(a, DeepEquals, b)
	c.Assert(a.GetMeta(), DeepEquals, b.GetMeta())
	c.Assert(a.GetLeader(), DeepEquals, b.GetLeader())
	c.Assert(a.GetPeers(), DeepEquals, b.GetPeers())
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		c.Assert(a.GetDownPeers(), DeepEquals, b.GetDownPeers())
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		c.Assert(a.GetPendingPeers(), DeepEquals, b.GetPendingPeers())
	}
}

func checkRegionsKV(c *C, s storage.Storage, regions []*core.RegionInfo) {
	if s != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := s.LoadRegion(region.GetID(), &meta)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(&meta, DeepEquals, region.GetMeta())
		}
	}
}

func checkRegions(c *C, cache *core.RegionsInfo, regions []*core.RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCount[peer.StoreId]++
			if peer.Id == region.GetLeader().Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.GetLeader(peer.StoreId, region), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.GetFollower(peer.StoreId, region), region)
			}
		}
	}

	c.Assert(cache.GetRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.GetStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.GetStoreLeaderCount(id), Equals, count)
	}
	for id, count := range followerCount {
		c.Assert(cache.GetStoreFollowerCount(id), Equals, count)
	}

	for _, region := range cache.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].GetMeta())
	}
}

func checkPendingPeerCount(expect []int, cluster *RaftCluster, c *C) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		c.Assert(s.GetPendingPeerCount(), Equals, e)
	}
}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}

	return nil
}
<<<<<<< HEAD
=======

func newTestOperator(regionID uint64, regionEpoch *metapb.RegionEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewTestOperator(regionID, regionEpoch, kind, steps...)
}

func (c *testCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	id, err := c.AllocID()
	if err != nil {
		return nil, err
	}
	return &metapb.Peer{Id: id, StoreId: storeID}, nil
}

func (c *testCluster) addRegionStore(storeID uint64, regionCount int, regionSizes ...uint64) error {
	var regionSize uint64
	if len(regionSizes) == 0 {
		regionSize = uint64(regionCount) * 10
	} else {
		regionSize = regionSizes[0]
	}

	stats := &pdpb.StoreStats{}
	stats.Capacity = 100 * units.GiB
	stats.UsedSize = regionSize * units.MiB
	stats.Available = stats.Capacity - stats.UsedSize
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionSize)),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	c.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderRegion(regionID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) error {
	region := newTestRegionMeta(regionID)
	leader, _ := c.AllocPeer(leaderStoreID)
	region.Peers = []*metapb.Peer{leader}
	for _, followerStoreID := range followerStoreIDs {
		peer, _ := c.AllocPeer(followerStoreID)
		region.Peers = append(region.Peers, peer)
	}
	regionInfo := core.NewRegionInfo(region, leader, core.SetApproximateSize(10), core.SetApproximateKeys(10))
	return c.putRegion(regionInfo)
}

func (c *testCluster) updateLeaderCount(storeID uint64, leaderCount int) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderStore(storeID uint64, leaderCount int) error {
	stats := &pdpb.StoreStats{}
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	c.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(typeutil.ZeroTime),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.OfflineStore(false))
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) LoadRegion(regionID uint64, followerStoreIDs ...uint64) error {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerStoreIDs {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	return c.putRegion(core.NewRegionInfo(region, nil))
}

func TestBasic(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()

	re.NoError(tc.addLeaderRegion(1, 1))

	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
	re.Equal(op1.RegionID(), oc.GetOperator(1).RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op2)
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	// Remove the operator manually, then we can add a new operator.
	re.True(oc.RemoveOperator(op1))
	op3 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op3)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion))
	re.Equal(op3.RegionID(), oc.GetOperator(1).RegionID())
}

func TestDispatch(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	co.GetPrepareChecker().SetPrepared()
	// Transfer peer from store 4 to store 1.
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addRegionStore(3, 30))
	re.NoError(tc.addRegionStore(2, 20))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	// Transfer leader from store 4 to store 2.
	re.NoError(tc.updateLeaderCount(4, 50))
	re.NoError(tc.updateLeaderCount(3, 50))
	re.NoError(tc.updateLeaderCount(2, 20))
	re.NoError(tc.updateLeaderCount(1, 10))
	re.NoError(tc.addLeaderRegion(2, 4, 3, 2))

	go co.RunUntilStop()

	// Wait for schedule and turn off balance.
	waitOperator(re, co, 1)
	sc := co.GetSchedulersController()
	operatorutil.CheckTransferPeer(re, co.GetOperatorController().GetOperator(1), operator.OpKind(0), 4, 1)
	re.NoError(sc.RemoveScheduler(schedulers.BalanceRegionName))
	waitOperator(re, co, 2)
	operatorutil.CheckTransferLeader(re, co.GetOperatorController().GetOperator(2), operator.OpKind(0), 4, 2)
	re.NoError(sc.RemoveScheduler(schedulers.BalanceLeaderName))

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	region := tc.GetRegion(1).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitRemovePeer(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Transfer leader.
	region = tc.GetRegion(2).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitTransferLeader(re, stream, region, 2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func dispatchHeartbeat(co *schedule.Coordinator, region *core.RegionInfo, stream hbstream.HeartbeatStream) error {
	co.GetHeartbeatStreams().BindStream(region.GetLeader().GetStoreId(), stream)
	if err := co.GetCluster().(*RaftCluster).putRegion(region.Clone()); err != nil {
		return err
	}
	co.GetOperatorController().Dispatch(region, operator.DispatchFromHeartBeat, nil)
	return nil
}

func TestCollectMetricsConcurrent(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(
			tc.GetBasicCluster(),
			tc.GetOpts(),
			nil,
			tc.storeConfigManager)
	}, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()

	// Make sure there are no problem when concurrent write and read
	var wg sync.WaitGroup
	count := 10
	wg.Add(count + 1)
	for i := 0; i <= count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				re.NoError(tc.addRegionStore(uint64(i%5), rand.Intn(200)))
			}
		}(i)
	}
	sc := co.GetSchedulersController()
	for i := 0; i < 1000; i++ {
		co.CollectHotSpotMetrics()
		sc.CollectSchedulerMetrics()
		co.GetCluster().(*RaftCluster).collectClusterMetrics()
	}
	co.ResetHotSpotMetrics()
	sc.ResetSchedulerMetrics()
	co.GetCluster().(*RaftCluster).resetClusterMetrics()
	wg.Wait()
}

func TestCollectMetrics(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(
			tc.GetBasicCluster(),
			tc.GetOpts(),
			nil,
			tc.storeConfigManager)
	}, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	count := 10
	for i := 0; i <= count; i++ {
		for k := 0; k < 200; k++ {
			item := &statistics.HotPeerStat{
				StoreID:   uint64(i % 5),
				RegionID:  uint64(i*1000 + k),
				Loads:     []float64{10, 20, 30},
				HotDegree: 10,
				AntiCount: statistics.HotRegionAntiCount, // for write
			}
			tc.hotStat.HotCache.Update(item, statistics.Write)
		}
	}
	sc := co.GetSchedulersController()
	for i := 0; i < 1000; i++ {
		co.CollectHotSpotMetrics()
		sc.CollectSchedulerMetrics()
		co.GetCluster().(*RaftCluster).collectClusterMetrics()
	}
	stores := co.GetCluster().GetStores()
	regionStats := co.GetCluster().RegionWriteStats()
	status1 := statistics.CollectHotPeerInfos(stores, regionStats)
	status2 := statistics.GetHotStatus(stores, co.GetCluster().GetStoresLoads(), regionStats, statistics.Write, co.GetCluster().GetSchedulerConfig().IsTraceRegionFlow())
	for _, s := range status2.AsLeader {
		s.Stats = nil
	}
	for _, s := range status2.AsPeer {
		s.Stats = nil
	}
	re.Equal(status1, status2)
	co.ResetHotSpotMetrics()
	sc.ResetSchedulerMetrics()
	co.GetCluster().(*RaftCluster).resetClusterMetrics()
}

func prepare(setCfg func(*config.ScheduleConfig), setTc func(*testCluster), run func(*schedule.Coordinator), re *require.Assertions) (*testCluster, *schedule.Coordinator, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, opt, err := newTestScheduleConfig()
	re.NoError(err)
	if setCfg != nil {
		setCfg(cfg)
	}
	tc := newTestCluster(ctx, opt)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, tc.meta.GetId(), tc, true /* need to run */)
	if setTc != nil {
		setTc(tc)
	}
	co := schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	if run != nil {
		run(co)
	}
	return tc, co, func() {
		co.Stop()
		co.GetSchedulersController().Wait()
		co.GetWaitGroup().Wait()
		hbStreams.Close()
		cancel()
	}
}

func checkRegionAndOperator(re *require.Assertions, tc *testCluster, co *schedule.Coordinator, regionID uint64, expectAddOperator int) {
	ops := co.GetCheckerController().CheckRegion(tc.GetRegion(regionID))
	if ops == nil {
		re.Equal(0, expectAddOperator)
	} else {
		re.Equal(expectAddOperator, co.GetOperatorController().AddWaitingOperator(ops...))
	}
}

func TestCheckRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(nil, nil, nil, re)
	hbStreams, opt := co.GetHeartbeatStreams(), tc.opt
	defer cleanup()

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	checkRegionAndOperator(re, tc, co, 1, 1)
	operatorutil.CheckAddPeer(re, co.GetOperatorController().GetOperator(1), operator.OpReplica, 1)
	checkRegionAndOperator(re, tc, co, 1, 0)

	r := tc.GetRegion(1)
	p := &metapb.Peer{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}
	r = r.Clone(
		core.WithAddPeer(p),
		core.WithPendingPeers(append(r.GetPendingPeers(), p)),
	)
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 0)

	tc = newTestCluster(ctx, opt)
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 0)
	r = r.Clone(core.WithPendingPeers(nil))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 1)
	op := co.GetOperatorController().GetOperator(1)
	re.Equal(1, op.Len())
	re.Equal(uint64(1), op.Step(0).(operator.PromoteLearner).ToStore)
	checkRegionAndOperator(re, tc, co, 1, 0)
}

func TestCheckRegionWithScheduleDeny(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	region := tc.GetRegion(1)
	re.NotNil(region)
	// test with label schedule=deny
	labelerManager := tc.GetRegionLabeler()
	labelerManager.SetLabelRule(&labeler.LabelRule{
		ID:       "schedulelabel",
		Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: labeler.KeyRange,
		Data:     []interface{}{map[string]interface{}{"start_key": "", "end_key": ""}},
	})

	// should allow to do rule checker
	re.True(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 1, 1)

	// should not allow to merge
	tc.opt.SetSplitMergeInterval(time.Duration(0))
	re.NoError(tc.addLeaderRegion(2, 2, 3, 4))
	re.NoError(tc.addLeaderRegion(3, 2, 3, 4))
	region = tc.GetRegion(2)
	re.True(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 2, 0)
	// delete label rule, should allow to do merge
	labelerManager.DeleteLabelRule("schedulelabel")
	re.False(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 2, 2)
}

func TestCheckerIsBusy(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0 // ensure replica checker is busy
		cfg.MergeScheduleLimit = 10
	}, nil, nil, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 0))
	num := 1 + typeutil.MaxUint64(tc.opt.GetReplicaScheduleLimit(), tc.opt.GetMergeScheduleLimit())
	var operatorKinds = []operator.OpKind{
		operator.OpReplica, operator.OpRegion | operator.OpMerge,
	}
	for i, operatorKind := range operatorKinds {
		for j := uint64(0); j < num; j++ {
			regionID := j + uint64(i+1)*num
			re.NoError(tc.addLeaderRegion(regionID, 1))
			switch operatorKind {
			case operator.OpReplica:
				op := newTestOperator(regionID, tc.GetRegion(regionID).GetRegionEpoch(), operatorKind)
				re.Equal(1, co.GetOperatorController().AddWaitingOperator(op))
			case operator.OpRegion | operator.OpMerge:
				if regionID%2 == 1 {
					ops, err := operator.CreateMergeRegionOperator("merge-region", co.GetCluster(), tc.GetRegion(regionID), tc.GetRegion(regionID-1), operator.OpMerge)
					re.NoError(err)
					re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
				}
			}
		}
	}
	checkRegionAndOperator(re, tc, co, num, 0)
}

func TestMergeRegionCancelOneOperator(t *testing.T) {
	re := require.New(t)
	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	source := core.NewRegionInfo(
		&metapb.Region{
			Id:       1,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
		},
		nil,
	)
	target := core.NewRegionInfo(
		&metapb.Region{
			Id:       2,
			StartKey: []byte("a"),
			EndKey:   []byte("t"),
		},
		nil,
	)
	re.NoError(tc.putRegion(source))
	re.NoError(tc.putRegion(target))

	// Cancel source region.
	ops, err := operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	re.NoError(err)
	re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
	// Cancel source operator.
	co.GetOperatorController().RemoveOperator(co.GetOperatorController().GetOperator(source.GetID()))
	re.Len(co.GetOperatorController().GetOperators(), 0)

	// Cancel target region.
	ops, err = operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	re.NoError(err)
	re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
	// Cancel target operator.
	co.GetOperatorController().RemoveOperator(co.GetOperatorController().GetOperator(target.GetID()))
	re.Len(co.GetOperatorController().GetOperators(), 0)
}

func TestReplica(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off balance.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(4, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to store 1.
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	region := tc.GetRegion(1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	re.NoError(tc.setStoreDown(3))
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region = region.Clone(
		core.WithDownPeers(append(region.GetDownPeers(), downPeer)),
	)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 4)
	region = region.Clone(core.WithDownPeers(nil))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Remove peer from store 4.
	re.NoError(tc.addLeaderRegion(2, 1, 2, 3, 4))
	region = tc.GetRegion(2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitRemovePeer(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Remove offline peer directly when it's pending.
	re.NoError(tc.addLeaderRegion(3, 1, 2, 3))
	re.NoError(tc.setStoreOffline(3))
	region = tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func TestCheckCache(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off replica scheduling.
		cfg.ReplicaScheduleLimit = 0
	}, nil, nil, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 0))
	re.NoError(tc.addRegionStore(2, 0))
	re.NoError(tc.addRegionStore(3, 0))

	// Add a peer with two replicas.
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/break-patrol", `return`))

	// case 1: operator cannot be created due to replica-schedule-limit restriction
	co.GetWaitGroup().Add(1)
	co.PatrolRegions()
	re.Len(co.GetCheckerController().GetWaitingRegions(), 1)

	// cancel the replica-schedule-limit restriction
	cfg := tc.GetScheduleConfig()
	cfg.ReplicaScheduleLimit = 10
	tc.SetScheduleConfig(cfg)
	co.GetWaitGroup().Add(1)
	co.PatrolRegions()
	oc := co.GetOperatorController()
	re.Len(oc.GetOperators(), 1)
	re.Empty(co.GetCheckerController().GetWaitingRegions())

	// case 2: operator cannot be created due to store limit restriction
	oc.RemoveOperator(oc.GetOperator(1))
	tc.SetStoreLimit(1, storelimit.AddPeer, 0)
	co.GetWaitGroup().Add(1)
	co.PatrolRegions()
	re.Len(co.GetCheckerController().GetWaitingRegions(), 1)

	// cancel the store limit restriction
	tc.SetStoreLimit(1, storelimit.AddPeer, 10)
	time.Sleep(time.Second)
	co.GetWaitGroup().Add(1)
	co.PatrolRegions()
	re.Len(oc.GetOperators(), 1)
	re.Empty(co.GetCheckerController().GetWaitingRegions())

	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/break-patrol"))
}

func TestPeerState(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()

	// Transfer peer from store 4 to store 1.
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addRegionStore(2, 10))
	re.NoError(tc.addRegionStore(3, 10))
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(re, co, 1)
	operatorutil.CheckTransferPeer(re, co.GetOperatorController().GetOperator(1), operator.OpKind(0), 4, 1)

	region := tc.GetRegion(1).Clone()

	// Add new peer.
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)

	// If the new peer is pending, the operator will not finish.
	region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), region.GetStorePeer(1))))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
	re.NotNil(co.GetOperatorController().GetOperator(region.GetID()))

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region = region.Clone(core.WithPendingPeers(nil))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitRemovePeer(re, stream, region, 4)
	re.NoError(tc.addLeaderRegion(1, 1, 2, 3))
	region = tc.GetRegion(1).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func TestShouldRun(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	tc.RaftCluster.coordinator = co
	defer cleanup()

	re.NoError(tc.addLeaderStore(1, 5))
	re.NoError(tc.addLeaderStore(2, 2))
	re.NoError(tc.addLeaderStore(3, 0))
	re.NoError(tc.addLeaderStore(4, 0))
	re.NoError(tc.LoadRegion(1, 1, 2, 3))
	re.NoError(tc.LoadRegion(2, 1, 2, 3))
	re.NoError(tc.LoadRegion(3, 1, 2, 3))
	re.NoError(tc.LoadRegion(4, 1, 2, 3))
	re.NoError(tc.LoadRegion(5, 1, 2, 3))
	re.NoError(tc.LoadRegion(6, 2, 1, 4))
	re.NoError(tc.LoadRegion(7, 2, 1, 4))
	re.False(co.ShouldRun())
	re.Equal(2, tc.GetStoreRegionCount(4))

	testCases := []struct {
		regionID  uint64
		ShouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		// store4 needs Collect two region
		{6, false},
		{7, true},
	}

	for _, testCase := range testCases {
		r := tc.GetRegion(testCase.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		re.NoError(tc.processRegionHeartbeat(nr))
		re.Equal(testCase.ShouldRun, co.ShouldRun())
	}
	nr := &metapb.Region{Id: 6, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	re.Error(tc.processRegionHeartbeat(newRegion))
	re.Equal(7, co.GetPrepareChecker().GetSum())
}

func TestShouldRunWithNonLeaderRegions(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	tc.RaftCluster.coordinator = co
	defer cleanup()

	re.NoError(tc.addLeaderStore(1, 10))
	re.NoError(tc.addLeaderStore(2, 0))
	re.NoError(tc.addLeaderStore(3, 0))
	for i := 0; i < 10; i++ {
		re.NoError(tc.LoadRegion(uint64(i+1), 1, 2, 3))
	}
	re.False(co.ShouldRun())
	re.Equal(10, tc.GetStoreRegionCount(1))

	testCases := []struct {
		regionID  uint64
		ShouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		{6, false},
		{7, false},
		{8, false},
		{9, true},
	}

	for _, testCase := range testCases {
		r := tc.GetRegion(testCase.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		re.NoError(tc.processRegionHeartbeat(nr))
		re.Equal(testCase.ShouldRun, co.ShouldRun())
	}
	nr := &metapb.Region{Id: 9, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	re.Error(tc.processRegionHeartbeat(newRegion))
	re.Equal(9, co.GetPrepareChecker().GetSum())

	// Now, after server is prepared, there exist some regions with no leader.
	re.Equal(uint64(0), tc.GetRegion(10).GetLeader().GetStoreId())
}

func TestAddScheduler(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	sc := co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), len(config.DefaultSchedulers))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceLeaderName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.HotRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceWitnessName))
	re.NoError(sc.RemoveScheduler(schedulers.TransferWitnessLeaderName))
	re.Empty(sc.GetSchedulerNames())

	stream := mockhbstream.NewHeartbeatStream()

	// Add stores 1,2,3
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))
	re.NoError(tc.addLeaderStore(3, 1))
	// Add regions 1 with leader in store 1 and followers in stores 2,3
	re.NoError(tc.addLeaderRegion(1, 1, 2, 3))
	// Add regions 2 with leader in store 2 and followers in stores 1,3
	re.NoError(tc.addLeaderRegion(2, 2, 1, 3))
	// Add regions 3 with leader in store 3 and followers in stores 1,2
	re.NoError(tc.addLeaderRegion(3, 3, 1, 2))

	oc := co.GetOperatorController()

	// test ConfigJSONDecoder create
	bl, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err := bl.EncodeConfig()
	re.NoError(err)
	data := make(map[string]interface{})
	err = json.Unmarshal(conf, &data)
	re.NoError(err)
	batch := data["batch"].(float64)
	re.Equal(4, int(batch))
	gls, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"0"}), sc.RemoveScheduler)
	re.NoError(err)
	re.NotNil(sc.AddScheduler(gls))
	re.NotNil(sc.RemoveScheduler(gls.GetName()))

	gls, err = schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}), sc.RemoveScheduler)
	re.NoError(err)
	re.NoError(sc.AddScheduler(gls))

	hb, err := schedulers.CreateScheduler(schedulers.HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err = hb.EncodeConfig()
	re.NoError(err)
	data = make(map[string]interface{})
	re.NoError(json.Unmarshal(conf, &data))
	re.Contains(data, "enable-for-tiflash")
	re.Equal("true", data["enable-for-tiflash"].(string))

	// Transfer all leaders to store 1.
	waitOperator(re, co, 2)
	region2 := tc.GetRegion(2)
	re.NoError(dispatchHeartbeat(co, region2, stream))
	region2 = waitTransferLeader(re, stream, region2, 1)
	re.NoError(dispatchHeartbeat(co, region2, stream))
	waitNoResponse(re, stream)

	waitOperator(re, co, 3)
	region3 := tc.GetRegion(3)
	re.NoError(dispatchHeartbeat(co, region3, stream))
	region3 = waitTransferLeader(re, stream, region3, 1)
	re.NoError(dispatchHeartbeat(co, region3, stream))
	waitNoResponse(re, stream)
}

func TestPersistScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()
	defaultCount := len(config.DefaultSchedulers)
	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))

	sc := co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), defaultCount)
	oc := co.GetOperatorController()
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}), sc.RemoveScheduler)
	re.NoError(err)
	re.NoError(sc.AddScheduler(gls1, "1"))
	evict, err := schedulers.CreateScheduler(schedulers.EvictLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.EvictLeaderType, []string{"2"}), sc.RemoveScheduler)
	re.NoError(err)
	re.NoError(sc.AddScheduler(evict, "2"))
	re.Len(sc.GetSchedulerNames(), defaultCount+2)
	sches, _, err := storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, defaultCount+2)

	// remove 5 schedulers
	re.NoError(sc.RemoveScheduler(schedulers.BalanceLeaderName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.HotRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceWitnessName))
	re.NoError(sc.RemoveScheduler(schedulers.TransferWitnessLeaderName))
	re.Len(sc.GetSchedulerNames(), defaultCount-3)
	re.NoError(co.GetCluster().GetPersistOptions().Persist(storage))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	// make a new coordinator for testing
	// whether the schedulers added or removed in dynamic way are recorded in opt
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	_, err = schedulers.CreateScheduler(schedulers.ShuffleRegionType, oc, storage, schedulers.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	// suppose we add a new default enable scheduler
	config.DefaultSchedulers = append(config.DefaultSchedulers, config.SchedulerConfig{Type: "shuffle-region"})
	defer func() {
		config.DefaultSchedulers = config.DefaultSchedulers[:len(config.DefaultSchedulers)-1]
	}()
	re.Len(newOpt.GetSchedulers(), defaultCount)
	re.NoError(newOpt.Reload(storage))
	// only remains 3 items with independent config.
	sches, _, err = storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, 3)

	// option have 6 items because the default scheduler do not remove.
	re.Len(newOpt.GetSchedulers(), defaultCount+3)
	re.NoError(newOpt.Persist(storage))
	tc.RaftCluster.opt = newOpt

	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	sc = co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), 3)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	// suppose restart PD again
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))
	tc.RaftCluster.opt = newOpt
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	sc = co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), 3)
	bls, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	re.NoError(sc.AddScheduler(bls))
	brs, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	re.NoError(sc.AddScheduler(brs))
	re.Len(sc.GetSchedulerNames(), defaultCount)

	// the scheduler option should contain 6 items
	// the `hot scheduler` are disabled
	re.Len(co.GetCluster().GetPersistOptions().GetSchedulers(), defaultCount+3)
	re.NoError(sc.RemoveScheduler(schedulers.GrantLeaderName))
	// the scheduler that is not enable by default will be completely deleted
	re.Len(co.GetCluster().GetPersistOptions().GetSchedulers(), defaultCount+2)
	re.Len(sc.GetSchedulerNames(), 4)
	re.NoError(co.GetCluster().GetPersistOptions().Persist(co.GetCluster().GetStorage()))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(co.GetCluster().GetStorage()))
	tc.RaftCluster.opt = newOpt
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)

	co.Run()
	sc = co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), defaultCount-1)
	re.NoError(sc.RemoveScheduler(schedulers.EvictLeaderName))
	re.Len(sc.GetSchedulerNames(), defaultCount-2)
}

func TestRemoveScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()

	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))
	defaultCount := len(config.DefaultSchedulers)
	sc := co.GetSchedulersController()
	re.Len(sc.GetSchedulerNames(), defaultCount)
	oc := co.GetOperatorController()
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}), sc.RemoveScheduler)
	re.NoError(err)
	re.NoError(sc.AddScheduler(gls1, "1"))
	re.Len(sc.GetSchedulerNames(), defaultCount+1)
	sches, _, err := storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, defaultCount+1)

	// remove all schedulers
	re.NoError(sc.RemoveScheduler(schedulers.BalanceLeaderName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.HotRegionName))
	re.NoError(sc.RemoveScheduler(schedulers.GrantLeaderName))
	re.NoError(sc.RemoveScheduler(schedulers.BalanceWitnessName))
	re.NoError(sc.RemoveScheduler(schedulers.TransferWitnessLeaderName))
	// all removed
	sches, _, err = storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Empty(sches)
	re.Empty(sc.GetSchedulerNames())
	re.NoError(co.GetCluster().GetPersistOptions().Persist(co.GetCluster().GetStorage()))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()

	// suppose restart PD again
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(tc.storage))
	tc.RaftCluster.opt = newOpt
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	re.Empty(sc.GetSchedulerNames())
	// the option remains default scheduler
	re.Len(co.GetCluster().GetPersistOptions().GetSchedulers(), defaultCount)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
}

func TestRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off balance, we test add replica only.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addLeaderRegion(1, 1))
	region := tc.GetRegion(1)
	co.GetPrepareChecker().Collect(region)

	// Add 1 replica on store 2.
	stream := mockhbstream.NewHeartbeatStream()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 2)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()

	// Recreate coordinator then add another replica on store 3.
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.GetPrepareChecker().Collect(region)
	co.Run()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 3)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitPromoteLearner(re, stream, region, 3)
}

func TestPauseScheduler(t *testing.T) {
	re := require.New(t)

	_, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	sc := co.GetSchedulersController()
	_, err := sc.IsSchedulerAllowed("test")
	re.Error(err)
	sc.PauseOrResumeScheduler(schedulers.BalanceLeaderName, 60)
	paused, _ := sc.IsSchedulerPaused(schedulers.BalanceLeaderName)
	re.True(paused)
	pausedAt, err := sc.GetPausedSchedulerDelayAt(schedulers.BalanceLeaderName)
	re.NoError(err)
	resumeAt, err := sc.GetPausedSchedulerDelayUntil(schedulers.BalanceLeaderName)
	re.NoError(err)
	re.Equal(int64(60), resumeAt-pausedAt)
	allowed, _ := sc.IsSchedulerAllowed(schedulers.BalanceLeaderName)
	re.False(allowed)
}

func BenchmarkPatrolRegion(b *testing.B) {
	re := require.New(b)

	mergeLimit := uint64(4100)
	regionNum := 10000

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.MergeScheduleLimit = mergeLimit
	}, nil, nil, re)
	defer cleanup()

	tc.opt.SetSplitMergeInterval(time.Duration(0))
	for i := 1; i < 4; i++ {
		if err := tc.addRegionStore(uint64(i), regionNum, 96); err != nil {
			return
		}
	}
	for i := 0; i < regionNum; i++ {
		if err := tc.addLeaderRegion(uint64(i), 1, 2, 3); err != nil {
			return
		}
	}

	listen := make(chan int)
	go func() {
		oc := co.GetOperatorController()
		listen <- 0
		for {
			if oc.OperatorCount(operator.OpMerge) == mergeLimit {
				co.Stop()
				return
			}
		}
	}()
	<-listen

	co.GetWaitGroup().Add(1)
	b.ResetTimer()
	co.PatrolRegions()
}

func waitOperator(re *require.Assertions, co *schedule.Coordinator, regionID uint64) {
	testutil.Eventually(re, func() bool {
		return co.GetOperatorController().GetOperator(regionID) != nil
	})
}

func TestOperatorCount(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	re.Equal(uint64(0), oc.OperatorCount(operator.OpLeader))
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	{
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 1:leader
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op2)
		re.Equal(uint64(2), oc.OperatorCount(operator.OpLeader)) // 1:leader, 2:leader
		re.True(oc.RemoveOperator(op1))
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 2:leader
	}

	{
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion)) // 1:region 2:leader
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion)
		op2.SetPriorityLevel(constant.High)
		oc.AddWaitingOperator(op2)
		re.Equal(uint64(2), oc.OperatorCount(operator.OpRegion)) // 1:region 2:region
		re.Equal(uint64(0), oc.OperatorCount(operator.OpLeader))
	}
}

func TestStoreOverloaded(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	lb, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, tc.storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	opt := tc.GetOpts()
	re.NoError(tc.addRegionStore(4, 100))
	re.NoError(tc.addRegionStore(3, 100))
	re.NoError(tc.addRegionStore(2, 100))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	start := time.Now()
	{
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Len(ops, 1)
		op1 := ops[0]
		re.NotNil(op1)
		re.True(oc.AddOperator(op1))
		re.True(oc.RemoveOperator(op1))
	}
	for {
		time.Sleep(time.Millisecond * 10)
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		if time.Since(start) > time.Second {
			break
		}
		re.Empty(ops)
	}

	// reset all stores' limit
	// scheduling one time needs 1/10 seconds
	opt.SetAllStoresLimit(storelimit.AddPeer, 600)
	opt.SetAllStoresLimit(storelimit.RemovePeer, 600)
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Len(ops, 1)
		op := ops[0]
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	// sleep 1 seconds to make sure that the token is filled up
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Greater(len(ops), 0)
	}
}

func TestStoreOverloadedWithReplace(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	lb, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, tc.storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)

	re.NoError(tc.addRegionStore(4, 100))
	re.NoError(tc.addRegionStore(3, 100))
	re.NoError(tc.addRegionStore(2, 100))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))
	re.NoError(tc.addLeaderRegion(2, 1, 3, 4))
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	region = tc.GetRegion(2).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 1})
	re.True(oc.AddOperator(op1))
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 2})
	op2.SetPriorityLevel(constant.High)
	re.True(oc.AddOperator(op2))
	op3 := newTestOperator(1, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 3})
	re.False(oc.AddOperator(op3))
	ops, _ := lb.Schedule(tc, false /* dryRun */)
	re.Empty(ops)
	// sleep 2 seconds to make sure that token is filled up
	time.Sleep(2 * time.Second)
	ops, _ = lb.Schedule(tc, false /* dryRun */)
	re.Greater(len(ops), 0)
}

func TestDownStoreLimit(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	rc := co.GetCheckerController().GetRuleChecker()

	tc.addRegionStore(1, 100)
	tc.addRegionStore(2, 100)
	tc.addRegionStore(3, 100)
	tc.addLeaderRegion(1, 1, 2, 3)

	region := tc.GetRegion(1)
	tc.setStoreDown(1)
	tc.SetStoreLimit(1, storelimit.RemovePeer, 1)

	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        region.GetStorePeer(1),
			DownSeconds: 24 * 60 * 60,
		},
	}), core.SetApproximateSize(1))
	tc.putRegion(region)
	for i := uint64(1); i < 20; i++ {
		tc.addRegionStore(i+3, 100)
		op := rc.Check(region)
		re.NotNil(op)
		re.True(oc.AddOperator(op))
		oc.RemoveOperator(op)
	}

	region = region.Clone(core.SetApproximateSize(100))
	tc.putRegion(region)
	for i := uint64(20); i < 25; i++ {
		tc.addRegionStore(i+3, 100)
		op := rc.Check(region)
		re.NotNil(op)
		re.True(oc.AddOperator(op))
		oc.RemoveOperator(op)
	}
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedulers.Scheduler
	limit   uint64
	counter *operator.Controller
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func TestController(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	scheduler, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := schedulers.NewScheduleController(tc.ctx, co.GetCluster(), co.GetOperatorController(), lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		re.Equal(i, sc.GetInterval())
		re.Empty(sc.Schedule(false))
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	{
		re.True(sc.AllowSchedule(false))
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		// count = 2
		re.False(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
	}

	op11 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	// add a PriorityKind operator will remove old operator
	{
		op3 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpHotRegion)
		op3.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op11))
		re.False(sc.AllowSchedule(false))
		re.Equal(1, oc.AddWaitingOperator(op3))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op3))
	}

	// add a admin operator will remove old operator
	{
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		re.False(sc.AllowSchedule(false))
		op4 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
		op4.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op4))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op4))
	}

	// test wrong region id.
	{
		op5 := newTestOperator(3, &metapb.RegionEpoch{}, operator.OpHotRegion)
		re.Equal(0, oc.AddWaitingOperator(op5))
	}

	// test wrong region epoch.
	re.True(oc.RemoveOperator(op11))
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		re.Equal(0, oc.AddWaitingOperator(op6))
	}
	epoch.Version--
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op6))
		re.True(oc.RemoveOperator(op6))
	}
}

func TestInterval(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	lb, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, co.GetOperatorController(), storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	sc := schedulers.NewScheduleController(tc.ctx, co.GetCluster(), co.GetOperatorController(), lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.SetInterval(schedulers.MinScheduleInterval)
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			re.Empty(sc.Schedule(false))
		}
		re.Less(sc.GetInterval(), time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddLearnerNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithAddPeer(res.GetChangePeer().GetPeer()),
		core.WithIncConfVer(),
	)
}

func waitPromoteLearner(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	// Remove learner than add voter.
	return region.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithAddPeer(res.GetChangePeer().GetPeer()),
	)
}

func waitRemovePeer(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_RemoveNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithIncConfVer(),
	)
}

func waitTransferLeader(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			if res.GetRegionId() == region.GetID() {
				for _, peer := range append(res.GetTransferLeader().GetPeers(), res.GetTransferLeader().GetPeer()) {
					if peer.GetStoreId() == storeID {
						return true
					}
				}
			}
		}
		return false
	})
	return region.Clone(
		core.WithLeader(region.GetStorePeer(storeID)),
	)
}

func waitNoResponse(re *require.Assertions, stream mockhbstream.HeartbeatStream) {
	testutil.Eventually(re, func() bool {
		res := stream.Recv()
		return res == nil
	})
}
>>>>>>> 40eaa35f2 (statistics: get region info via core cluster inside RegionStatistics (#6804))
