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
	"sync/atomic"

	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

// Create n stores (0..n).
func newTestStores(n uint64) []*storeInfo {
	stores := make([]*storeInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, newStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) Test(c *C) {
	n := uint64(10)
	cache := newStoresInfo()
	stores := newTestStores(n)

	for i := uint64(0); i < n; i++ {
		c.Assert(cache.getStore(i), IsNil)
		cache.setStore(stores[i])
		c.Assert(cache.getStore(i), DeepEquals, stores[i])
		c.Assert(cache.getStoreCount(), Equals, int(i+1))
	}

	for _, store := range cache.getStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	for _, store := range cache.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()].Store)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*regionInfo {
	regions := make([]*regionInfo, 0, n)
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
			Id:       i,
			Peers:    peers,
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		regions = append(regions, newRegionInfo(region, peers[0]))
	}
	return regions
}

func checkRegion(c *C, a *regionInfo, b *regionInfo) {
	c.Assert(a.Region, DeepEquals, b.Region)
	c.Assert(a.Leader, DeepEquals, b.Leader)
}

func checkRegions(c *C, cache *regionsInfo, regions []*regionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.Peers {
			regionCount[peer.StoreId]++
			if peer.Id == region.Leader.Id {
				leaderCount[peer.StoreId]++
			}
		}
	}

	c.Assert(cache.getRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.getStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.getStoreLeaderCount(id), Equals, count)
	}

	for _, region := range cache.getRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	n, np := uint64(3), uint64(3)
	cache := newClusterInfo(newMockIDAllocator())
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		cache.addRegion(region)
	}
	c.Assert(cache.getRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{StoreId: store.GetId()}
		c.Assert(cache.handleStoreHeartbeat(storeStats), NotNil)

		cache.setStore(store)
		c.Assert(cache.getStoreCount(), Equals, int(i+1))

		stats := store.stats
		startTS := stats.StartTS
		c.Assert(stats.StartTS.IsZero(), IsFalse)
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsTrue)
		c.Assert(stats.TotalRegionCount, Equals, 0)
		c.Assert(stats.LeaderRegionCount, Equals, 0)

		c.Assert(cache.handleStoreHeartbeat(storeStats), IsNil)

		stats = cache.getStore(store.GetId()).stats
		c.Assert(stats.StartTS, Equals, startTS)
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsFalse)
		c.Assert(stats.TotalRegionCount, Equals, int(n))
		c.Assert(stats.LeaderRegionCount, Equals, 1)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	n, np := uint64(3), uint64(3)
	cache := newClusterInfo(newMockIDAllocator())
	regions := newTestRegions(n, np)

	for i, region := range regions {
		// region does not exist.
		_, err := cache.handleRegionHeartbeat(region)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is the same, not updated.
		_, err = cache.handleRegionHeartbeat(region)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		epoch := region.clone().GetRegionEpoch()

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		_, err = cache.handleRegionHeartbeat(region)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is stale (Version).
		stale := region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: epoch.GetConfVer() + 1,
		}
		_, err = cache.handleRegionHeartbeat(stale)
		c.Assert(err, NotNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
			ConfVer: epoch.GetConfVer() + 1,
		}
		_, err = cache.handleRegionHeartbeat(region)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is stale (ConfVer).
		stale = region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		_, err = cache.handleRegionHeartbeat(stale)
		c.Assert(err, NotNil)
		checkRegions(c, cache.regions, regions[0:i+1])
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := newRegion([]byte{}, []byte{})
	origin := newRegion([]byte{}, []byte{})
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

var _ = Suite(&testClusterCacheSuite{})

type testClusterCacheSuite struct {
	testClusterBaseSuite
}

func (s *testClusterCacheSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = newTestServer(c)
	s.client = s.svr.client

	go s.svr.Run()
}

func (s *testClusterCacheSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterCacheSuite) TestCache(c *C) {
	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := rpcConnect(leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)

	req := s.newBootstrapRequest(c, clusterID, "127.0.0.1:1")
	store1 := req.Bootstrap.Store

	_, err = s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stats := &pdpb.StoreStats{
		StoreId:            store1.GetId(),
		Capacity:           100,
		Available:          50,
		SendingSnapCount:   1,
		ReceivingSnapCount: 1,
	}

	c.Assert(cluster.cachedCluster.handleStoreHeartbeat(stats), IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, uint32(3))

	cacheStore := cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.Store, DeepEquals, store1)
	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)
	c.Assert(cluster.cachedCluster.regions.searchRegions.length(), Equals, 1)
	c.Assert(cluster.cachedCluster.regions.leaders.storeRegions, HasLen, 0)
	c.Assert(cluster.cachedCluster.regions.leaders.regionStores, HasLen, 0)

	// Add another store.
	store2 := s.newStore(c, 0, "127.0.0.1:2")
	err = cluster.putStore(store2)
	c.Assert(err, IsNil)

	stats = &pdpb.StoreStats{
		StoreId:            store2.GetId(),
		Capacity:           100,
		Available:          50,
		SendingSnapCount:   1,
		ReceivingSnapCount: 1,
	}

	c.Assert(cluster.cachedCluster.handleStoreHeartbeat(stats), IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, uint32(3))

	cacheStore = cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.Store, DeepEquals, store1)
	cacheStore = cluster.cachedCluster.getStore(store2.GetId())
	c.Assert(cacheStore.Store, DeepEquals, store2)
	cacheStores := cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, leader := cluster.getRegion(regionKey)
	c.Assert(leader, IsNil)
	c.Assert(region.Peers, HasLen, 1)

	cacheRegions := cluster.cachedCluster.regions
	c.Assert(cacheRegions.regions, HasLen, 1)
	c.Assert(cacheRegions.searchRegions.length(), Equals, 1)
	c.Assert(cacheRegions.leaders.storeRegions, HasLen, 0)
	c.Assert(cacheRegions.leaders.regionStores, HasLen, 0)

	leader = region.GetPeers()[0]
	res := heartbeatRegion(c, conn, clusterID, 0, region, leader)
	c.Assert(res.GetPeer(), NotNil)
	c.Assert(res.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(leader.GetId(), Not(Equals), res.GetPeer().GetId())

	cacheStores = cluster.cachedCluster.getStores()
	c.Assert(cacheStores, HasLen, 2)
	c.Assert(cacheRegions.regions, HasLen, 1)
	cacheRegion := cacheRegions.regions[region.GetId()]
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store1.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store1.GetId())

	// Add another peer.
	region.Peers = append(region.Peers, res.GetPeer())
	region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1

	res = heartbeatRegion(c, conn, clusterID, 0, region, leader)
	c.Assert(res, IsNil)

	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)

	oldRegionID := region.GetId()
	cacheRegion = cacheRegions.regions[oldRegionID]
	region, _ = cluster.getRegion(regionKey)
	c.Assert(region.GetPeers(), HasLen, 2)
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store1.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store1.GetId())

	// Test change leader peer.
	newLeader := region.GetPeers()[1]
	c.Assert(leader.GetId(), Not(Equals), newLeader.GetId())

	// There is no store to add peer, so the return res is nil.
	res = heartbeatRegion(c, conn, clusterID, 0, region, newLeader)
	c.Assert(res, IsNil)

	region, leader = cluster.getRegion(regionKey)
	c.Assert(leader, DeepEquals, newLeader)
	c.Assert(region.GetPeers(), HasLen, 2)
	c.Assert(cacheRegion, DeepEquals, region)

	c.Assert(cluster.cachedCluster.getStoreCount(), Equals, 2)
	c.Assert(cacheRegions.regions, HasLen, 1)
	c.Assert(cacheRegions.leaders.storeRegions, Not(HasKey), store1.GetId())
	c.Assert(cacheRegions.leaders.storeRegions, HasKey, store2.GetId())

	c.Assert(cacheRegions.leaders.storeRegions[store2.GetId()], HasKey, region.GetId())
	c.Assert(cacheRegions.leaders.regionStores[region.GetId()], Equals, store2.GetId())

	region, leader = cluster.getRegion(regionKey)
	c.Assert(leader, DeepEquals, newLeader)
	c.Assert(cacheRegion, DeepEquals, region)

	s.svr.cluster.stop()

	// Check GetStores.
	stores := map[uint64]*metapb.Store{
		store1.GetId(): store1,
		store2.GetId(): store2,
	}

	cluster = s.svr.GetRaftCluster()
	c.Assert(cluster, IsNil)

	allStores := s.svr.cluster.GetStores()
	c.Assert(allStores, HasLen, 2)
	for _, store := range allStores {
		c.Assert(stores, HasKey, store.GetId())
	}
}

func randRegions(count int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, count)
	for i := 0; i < count; i++ {
		peers := make([]*metapb.Peer, 0, 3)
		for j := 0; j < 3; j++ {
			peer := &metapb.Peer{StoreId: uint64(rand.Intn(count))}
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:       uint64(i),
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, region)
	}
	return regions
}

func checkStoreRegionCount(c *C, r *regionsInfo, regions []*metapb.Region) {
	stores := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			stores[peer.GetStoreId()]++
		}
	}
	for id, count := range stores {
		c.Assert(r.getStoreRegionCount(id), Equals, count)
	}
}

func (s *testClusterCacheSuite) TestStoreRegionCount(c *C) {
	count := 10
	addRegions := randRegions(count)
	updateRegions := randRegions(count)

	r := newRegionsInfo()

	var regions []*metapb.Region
	for _, region := range addRegions {
		r.addRegion(region)
		regions = append(regions, region)
		checkStoreRegionCount(c, r, regions)
	}
	checkStoreRegionCount(c, r, addRegions)

	for i, region := range updateRegions {
		r.updateRegion(region)
		regions[i] = region
		checkStoreRegionCount(c, r, regions)
	}
	checkStoreRegionCount(c, r, updateRegions)

	for len(regions) > 0 {
		r.removeRegion(regions[0])
		regions = regions[1:]
		checkStoreRegionCount(c, r, regions)
	}
}

// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{
		base: 0,
	}
}

func (alloc *mockIDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

func (s *testClusterCacheSuite) TestIDAlloc(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())

	id, err := cluster.idAlloc.Alloc()
	c.Assert(err, IsNil)
	c.Assert(id, Greater, uint64(0))
}
