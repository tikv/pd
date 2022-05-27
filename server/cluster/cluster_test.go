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
		RegionId: 0,
		Version:  1,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	c.Assert(cluster.processReportBuckets(buckets), NotNil)

	// case2: bucket can be processed after the region update.
	stores := newTestStores(3, "2.0.0")
	n, np := uint64(1), uint64(1)
	regions := newTestRegions(n, n, np)
	for _, store := range stores {
		c.Assert(cluster.putStoreLocked(store), IsNil)
	}

	c.Assert(cluster.processRegionHeartbeat(regions[0]), IsNil)
	c.Assert(cluster.GetRegion(uint64(0)).GetBuckets(), IsNil)
	c.Assert(cluster.processReportBuckets(buckets), IsNil)
	c.Assert(cluster.GetRegion(uint64(0)).GetBuckets(), DeepEquals, buckets)

	// case3: the bucket version is same.
	c.Assert(cluster.processReportBuckets(buckets), IsNil)
	// case4: the bucket version is changed.
	buckets.Version = 3
	c.Assert(cluster.processReportBuckets(buckets), IsNil)
	c.Assert(cluster.GetRegion(uint64(0)).GetBuckets(), DeepEquals, buckets)

	//case5: region update should inherit buckets.
	newRegion := regions[0].Clone(core.WithIncConfVer())
	c.Assert(cluster.processRegionHeartbeat(newRegion), IsNil)
	c.Assert(cluster.GetRegion(uint64(0)).GetBuckets(), NotNil)
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
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is updated.
		region = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.processRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
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
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Remove peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Change leader.
		region = region.Clone(core.WithLeader(region.GetPeers()[1]))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateSize.
		region = region.Clone(core.SetApproximateSize(144))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateKeys.
		region = region.Clone(core.SetApproximateKeys(144000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes written.
		region = region.Clone(core.SetWrittenBytes(24000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
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
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

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
	heartbeatRegions(c, cluster, regions)
	c.Assert(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), HasLen, len(regions))

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cluster, regions)
		c.Assert(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), HasLen, len(regions))
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

	c.Assert(getStoreTopoWeight(testStore, stores, labels), Equals, 1.0/3/3/4)
}

func (s *testClusterInfoSuite) TestCalculateStoreSize1(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)

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
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)

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
	cache := tc.core.Regions

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
