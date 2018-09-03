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
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
)

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.WithStack(ErrRegionIsStale(region, origin))
	}

	return nil
}

// Create n stores (0..n).
func newTestStores(n uint64) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) TestStores(c *C) {
	n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n)

	for i, store := range stores {
		id := store.GetId()
		c.Assert(cache.GetStore(id), IsNil)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.SetStore(store)
		c.Assert(cache.GetStore(id), DeepEquals, store)
		c.Assert(cache.GetStoreCount(), Equals, i+1)
		c.Assert(cache.BlockStore(id), IsNil)
		c.Assert(cache.GetStore(id).IsBlocked(), IsTrue)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.UnblockStore(id)
		c.Assert(cache.GetStore(id).IsBlocked(), IsFalse)
	}
	c.Assert(cache.GetStoreCount(), Equals, int(n))

	for _, store := range cache.GetStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1])
	}
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1].Store)
	}

	c.Assert(cache.GetStoreCount(), Equals, int(n))

	bytesWritten := uint64(8 * 1024 * 1024)
	bytesRead := uint64(128 * 1024 * 1024)
	store := cache.GetStore(1)

	store.Stats.BytesWritten = bytesWritten
	store.Stats.BytesRead = bytesRead
	store.Stats.Interval = &pdpb.TimeInterval{EndTimestamp: 10, StartTimestamp: 0}
	cache.SetStore(store)
	c.Assert(cache.TotalBytesWriteRate(), Equals, float64(bytesWritten/10))
	c.Assert(cache.TotalBytesReadRate(), Equals, float64(bytesRead/10))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct{}

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
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	cache := core.NewRegionsInfo()
	regions := newTestRegions(n, np)

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
		region := cache.RandLeaderRegion(i, core.HealthRegion())
		c.Assert(region.GetLeader().GetStoreId(), Equals, i)

		region = cache.RandFollowerRegion(i, core.HealthRegion())
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
			region := cache.RandLeaderRegion(i, core.HealthRegion())
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			cache.SetRegion(newRegion)
		}
		c.Assert(cache.RandLeaderRegion(i, core.HealthRegion()), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(cache.RandFollowerRegion(i, core.HealthRegion()), IsNil)
	}
}

func checkRegion(c *C, a *core.RegionInfo, b *core.RegionInfo) {
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

func checkRegionsKV(c *C, kv *core.KV, regions []*core.RegionInfo) {
	if kv != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := kv.LoadRegion(region.GetID(), &meta)
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
				checkRegion(c, cache.GetLeader(peer.StoreId, region.GetID()), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.GetFollower(peer.StoreId, region.GetID()), region)
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

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) TestLoadClusterInfo(c *C) {
	server, cleanup := mustRunTestServer(c)
	defer cleanup()

	kv := server.kv
	_, opt := newTestScheduleConfig()

	// Cluster is not bootstrapped.
	cluster, err := loadClusterInfo(server.idAlloc, kv, opt)
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(kv.SaveMeta(meta), IsNil)
	stores := mustSaveStores(c, kv, n)
	regions := mustSaveRegions(c, kv, n)

	cluster, err = loadClusterInfo(server.idAlloc, kv, opt)
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(cluster.getMeta(), DeepEquals, meta)
	c.Assert(cluster.getStoreCount(), Equals, n)
	for _, store := range cluster.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(cluster.getRegionCount(), Equals, n)
	for _, region := range cluster.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	_, opt := newTestScheduleConfig()
	cluster := newClusterInfo(core.NewMockIDAllocator(), opt, core.NewKV(core.NewMemoryKV()))

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		c.Assert(cluster.putRegion(region), IsNil)
	}
	c.Assert(cluster.getRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetId(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		c.Assert(cluster.handleStoreHeartbeat(storeStats), NotNil)

		c.Assert(cluster.putStore(store), IsNil)
		c.Assert(cluster.getStoreCount(), Equals, i+1)

		c.Assert(store.LastHeartbeatTS.IsZero(), IsTrue)

		c.Assert(cluster.handleStoreHeartbeat(storeStats), IsNil)

		s := cluster.GetStore(store.GetId())
		c.Assert(s.LastHeartbeatTS.IsZero(), IsFalse)
		c.Assert(s.Stats, DeepEquals, storeStats)
	}

	c.Assert(cluster.getStoreCount(), Equals, int(n))

	for _, store := range stores {
		tmp := &metapb.Store{}
		ok, err := cluster.kv.LoadStore(store.GetId(), tmp)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(tmp, DeepEquals, store.Store)
	}
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	_, opt := newTestScheduleConfig()
	cluster := newClusterInfo(core.NewMockIDAllocator(), opt, core.NewKV(core.NewMemoryKV()))

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3)
	regions := newTestRegions(n, np)

	for _, store := range stores {
		cluster.putStore(store)
	}

	for i, region := range regions {
		// region does not exist.
		c.Assert(cluster.handleRegionHeartbeat(region.Clone()), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cluster.handleRegionHeartbeat(region.Clone()), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])

		// region is updated.
		checkRegionsKV(c, cluster.kv, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		c.Assert(cluster.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Add a down peer.
		region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        region.GetPeers()[rand.Intn(len(region.GetPeers()))],
				DownSeconds: 42,
			},
		}))
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Remove  peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		c.Assert(cluster.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.kv, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		c.Assert(cluster.getStoreRegionCount(id), Equals, count)
	}

	for _, region := range cluster.getRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cluster.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].GetMeta())
	}

	for _, region := range regions {
		for _, store := range cluster.GetRegionStores(region) {
			c.Assert(region.GetStorePeer(store.GetId()), NotNil)
		}
		for _, store := range cluster.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetId())
			c.Assert(peer.GetId(), Not(Equals), region.GetLeader().GetId())
		}
	}

	for _, store := range cluster.core.Stores.GetStores() {
		c.Assert(store.LeaderCount, Equals, cluster.core.Regions.GetStoreLeaderCount(store.GetId()))
		c.Assert(store.RegionCount, Equals, cluster.core.Regions.GetStoreRegionCount(store.GetId()))
		c.Assert(store.LeaderSize, Equals, cluster.core.Regions.GetStoreLeaderRegionSize(store.GetId()))
		c.Assert(store.RegionSize, Equals, cluster.core.Regions.GetStoreRegionSize(store.GetId()))
	}

	// Test with kv.
	if kv := cluster.kv; kv != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := kv.LoadRegion(region.GetID(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, region.GetMeta())
		}

		// check overlap
		overlapRegion := regions[n-1].Clone(
			core.WithStartKey(regions[n-2].GetStartKey()),
			core.WithNewRegionID(regions[n-1].GetID()+1),
		)
		c.Assert(cluster.handleRegionHeartbeat(overlapRegion), IsNil)
		region := &metapb.Region{}
		ok, err := kv.LoadRegion(regions[n-1].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = kv.LoadRegion(regions[n-2].GetID(), region)
		c.Assert(ok, IsFalse)
		c.Assert(err, IsNil)
		ok, err = kv.LoadRegion(overlapRegion.GetID(), region)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
		c.Assert(region, DeepEquals, overlapRegion.GetMeta())
	}
}

func heartbeatRegions(c *C, cluster *clusterInfo, regions []*metapb.Region) {
	// Heartbeat and check region one by one.
	for _, region := range regions {
		r := core.NewRegionInfo(region, nil)

		c.Assert(cluster.handleRegionHeartbeat(r), IsNil)

		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.searchRegion(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.searchRegion([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, region := range regions {
		r := core.NewRegionInfo(region, nil)

		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.searchRegion(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.searchRegion([]byte{end - 1}), r)
			result := cluster.searchRegion([]byte{end + 1})
			c.Assert(result.GetID(), Not(Equals), r.GetID())
		}
	}
}

func (s *testClusterInfoSuite) TestHeartbeatSplit(c *C) {
	_, opt := newTestScheduleConfig()
	cluster := newClusterInfo(core.NewMockIDAllocator(), opt, nil)

	// 1: [nil, nil)
	region1 := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.handleRegionHeartbeat(region1.Clone()), IsNil)
	checkRegion(c, cluster.searchRegion([]byte("foo")), region1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	region2 := core.NewRegionInfo(&metapb.Region{Id: 2, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.handleRegionHeartbeat(region2.Clone()), IsNil)
	checkRegion(c, cluster.searchRegion([]byte("a")), region2)
	// [m, nil) is missing before r1's heartbeat.
	c.Assert(cluster.searchRegion([]byte("z")), IsNil)

	c.Assert(cluster.handleRegionHeartbeat(region1.Clone()), IsNil)
	checkRegion(c, cluster.searchRegion([]byte("z")), region1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	region3 := core.NewRegionInfo(&metapb.Region{Id: 3, StartKey: []byte("m"), EndKey: []byte("q"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	c.Assert(cluster.handleRegionHeartbeat(region1.Clone()), IsNil)
	checkRegion(c, cluster.searchRegion([]byte("z")), region1)
	checkRegion(c, cluster.searchRegion([]byte("a")), region2)
	// [m, q) is missing before r3's heartbeat.
	c.Assert(cluster.searchRegion([]byte("n")), IsNil)
	c.Assert(cluster.handleRegionHeartbeat(region3.Clone()), IsNil)
	checkRegion(c, cluster.searchRegion([]byte("n")), region3)
}

func (s *testClusterInfoSuite) TestRegionSplitAndMerge(c *C) {
	_, opt := newTestScheduleConfig()
	cluster := newClusterInfo(core.NewMockIDAllocator(), opt, nil)

	regions := []*metapb.Region{
		{
			Id:          1,
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}

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
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	stores := newTestStores(5)
	for _, s := range stores {
		tc.putStore(s)
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
	tc.handleRegionHeartbeat(origin)
	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.clusterInfo, c)
	newRegion := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[1:]}, peers[1], core.WithPendingPeers(peers[3:4]))
	tc.handleRegionHeartbeat(newRegion)
	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.clusterInfo, c)
}

func checkPendingPeerCount(expect []int, cluster *clusterInfo, c *C) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		c.Assert(s.PendingPeerCount, Equals, e)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := core.NewRegion([]byte{}, []byte{})
	origin := core.NewRegion([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region, origin), IsNil)
	c.Assert(checkStaleRegion(origin, region), IsNil)

	// (1, 0) v.s. (0, 0)
	region.RegionEpoch.Version++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (1, 1) v.s. (0, 0)
	region.RegionEpoch.ConfVer++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (0, 1) v.s. (0, 0)
	region.RegionEpoch.Version--
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)
}

func mustSaveStores(c *C, kv *core.KV, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(kv.SaveStore(store), IsNil)
	}

	return stores
}

func mustSaveRegions(c *C, kv *core.KV, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := newTestRegionMeta(uint64(i))
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(kv.SaveRegion(region), IsNil)
	}

	return regions
}
