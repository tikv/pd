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
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n, "2.0.0")
	storeMetasAfterHeartbeat := make([]*metapb.Store, 0, n)
	regions := newTestRegions(n, np)

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
}

func (s *testClusterInfoSuite) TestFilterUnhealthyStore(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

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
		c.Assert(cluster.storesStats.GetRollingStoreStats(store.GetID()), NotNil)
	}

	for _, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
		c.Assert(cluster.putStoreLocked(newStore), IsNil)
		c.Assert(cluster.HandleStoreHeartbeat(storeStats), IsNil)
		c.Assert(cluster.storesStats.GetRollingStoreStats(store.GetID()), IsNil)
	}
}

func (s *testClusterInfoSuite) TestSetStoreState(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	// Store 3 and 4 offline normally.
	for _, id := range []uint64{3, 4} {
		c.Assert(cluster.RemoveStore(id), IsNil)
		c.Assert(cluster.BuryStore(id, false), IsNil)
	}
	// Change the status of 3 directly back to Up.
	c.Assert(cluster.SetStoreState(3, metapb.StoreState_Up), IsNil)
	// Update store 1 2 3
	for _, store := range newTestStores(3, "3.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	// Since the store 4 version is too low, setting it to Up should fail.
	c.Assert(cluster.SetStoreState(4, metapb.StoreState_Up), NotNil)
}

func (s *testClusterInfoSuite) TestDeleteStoreUpdatesClusterVersion(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

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
	c.Assert(cluster.RemoveStore(3), IsNil)
	c.Assert(cluster.BuryStore(3, false), IsNil)
	c.Assert(cluster.GetClusterVersion(), Equals, "5.0.0")
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3, "2.0.0")
	regions := newTestRegions(n, np)

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

		// Change keys written.
		region = region.Clone(core.SetWrittenKeys(240))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		c.Assert(cluster.processRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change keys read.
		region = region.Clone(core.SetReadKeys(1080))
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
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
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

	// do not trace the flow changes
	cluster.traceRegionFlow = false
	processRegions([]*core.RegionInfo{region})
	newRegion = cluster.GetRegion(region.GetID())
	c.Assert(region.GetBytesRead(), Equals, uint64(0))
	c.Assert(newRegion.GetBytesRead(), Not(Equals), uint64(0))
}

func (s *testClusterInfoSuite) TestConcurrentRegionHeartbeat(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

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
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

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
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

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

func (s *testClusterInfoSuite) TestUpdateStorePendingPeerCount(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
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

type testRegionsInfoSuite struct{}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	regions := newTestRegions(n, np)
	_, opts, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestRaftCluster(mockid.NewIDAllocator(), opts, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	cache := tc.core.Regions

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.AddRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.SearchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])
		// previous region
		if i == 0 {
			c.Assert(cache.SearchPrevRegion(regionKey), IsNil)
		} else {
			checkRegion(c, cache.SearchPrevRegion(regionKey), regions[i-1])
		}
		// Update leader to peer np-1.
		newRegion := region.Clone(core.WithLeader(region.GetPeers()[np-1]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegion(c, cache.SearchRegion(regionKey), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		newRegion = region.Clone(core.WithLeader(region.GetPeers()[0]))
		regions[i] = newRegion
		cache.AddRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.SearchRegion(regionKey), newRegion)
	}

	for i := uint64(0); i < n; i++ {
		region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, opt.HealthRegion(tc))
		c.Assert(region.GetLeader().GetStoreId(), Equals, i)

		region = tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, opt.HealthRegion(tc))
		c.Assert(region.GetLeader().GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapRegion := regions[n-1].Clone(core.WithStartKey(regions[n-2].GetStartKey()))
	cache.AddRegion(overlapRegion)
	c.Assert(cache.GetRegion(n-2), IsNil)
	c.Assert(cache.GetRegion(n-1), NotNil)

	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount(i); j++ {
			region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, opt.HealthRegion(tc))
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			cache.SetRegion(newRegion)
		}
		c.Assert(tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, opt.HealthRegion(tc)), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, opt.HealthRegion(tc)), IsNil)
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
	cluster *RaftCluster
}

func (s *testGetStoresSuite) SetUpSuite(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
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
	if err := cfg.Adjust(nil); err != nil {
		return nil, nil, err
	}
	opt := config.NewPersistOptions(cfg)
	opt.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version2_0))
	return &cfg.Schedule, opt, nil
}

func newTestCluster(opt *config.PersistOptions) *testCluster {
	storage := core.NewStorage(kv.NewMemoryKV())
	rc := newTestRaftCluster(mockid.NewIDAllocator(), opt, storage, core.NewBasicCluster())
	rc.ruleManager = placement.NewRuleManager(storage)
	if opt.IsPlacementRulesEnabled() {
		err := rc.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	return &testCluster{RaftCluster: rc}
}

func newTestRaftCluster(id id.Allocator, opt *config.PersistOptions, storage *core.Storage, basicCluster *core.BasicCluster) *RaftCluster {
	rc := &RaftCluster{ctx: context.TODO()}
	rc.InitCluster(id, opt, storage, basicCluster)
	return rc
}

// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:         i,
			Address:    fmt.Sprintf("127.0.0.1:%d", i),
			State:      metapb.StoreState_Up,
			Version:    version,
			DeployPath: fmt.Sprintf("test/store%d", i),
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte{byte(i)},
			EndKey:      []byte{byte(i + 1)},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
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

func checkRegionsKV(c *C, s *core.Storage, regions []*core.RegionInfo) {
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
