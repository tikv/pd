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
	"net"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var _ = Suite(&testClusterCacheSuite{})

type testClusterCacheSuite struct {
	testClusterBaseSuite
}

func (s *testClusterCacheSuite) getRootPath() string {
	return "test_cluster_cache"
}

func (s *testClusterCacheSuite) SetUpSuite(c *C) {
	s.svr = newTestServer(c, s.getRootPath())
	s.client = newEtcdClient(c)
	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()
}

func (s *testClusterCacheSuite) TearDownSuite(c *C) {
	s.svr.Close()
	s.client.Close()
}

func (s *testClusterCacheSuite) TestCache(c *C) {
	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)

	req := s.newBootstrapRequest(c, clusterID, "127.0.0.1:1")
	store1 := req.Bootstrap.Store

	_, err = s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, uint32(3))

	cacheStore := cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.store, DeepEquals, store1)
	c.Assert(cluster.cachedCluster.regions.regions, HasLen, 1)
	c.Assert(cluster.cachedCluster.regions.searchRegions.Len(), Equals, 1)
	c.Assert(cluster.cachedCluster.regions.leaders.storeRegions, HasLen, 0)
	c.Assert(cluster.cachedCluster.regions.leaders.regionStores, HasLen, 0)

	// Add another store.
	store2 := s.newStore(c, 0, "127.0.0.1:2")
	err = cluster.putStore(store2)
	c.Assert(err, IsNil)

	// Check cachedCluster.
	c.Assert(cluster.cachedCluster.getMeta().GetId(), Equals, clusterID)
	c.Assert(cluster.cachedCluster.getMeta().GetMaxPeerCount(), Equals, uint32(3))

	cacheStore = cluster.cachedCluster.getStore(store1.GetId())
	c.Assert(cacheStore.store, DeepEquals, store1)
	cacheStore = cluster.cachedCluster.getStore(store2.GetId())
	c.Assert(cacheStore.store, DeepEquals, store2)
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
	c.Assert(cacheRegions.searchRegions.Len(), Equals, 1)
	c.Assert(cacheRegions.leaders.storeRegions, HasLen, 0)
	c.Assert(cacheRegions.leaders.regionStores, HasLen, 0)

	leader = region.GetPeers()[0]
	res := heartbeatRegion(c, conn, clusterID, 0, region, leader)
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

	oldRegion := cloneRegion(region)
	changePeer := cacheRegions.getChangePeer(oldRegion, region)
	c.Assert(changePeer, IsNil)

	// Add another peer.
	region.Peers = append(region.Peers, res.GetPeer())
	region.RegionEpoch.ConfVer = proto.Uint64(region.GetRegionEpoch().GetConfVer() + 1)

	changePeer = cacheRegions.getChangePeer(oldRegion, region)
	c.Assert(changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(changePeer.GetPeer(), DeepEquals, res.GetPeer())

	changePeer = cacheRegions.getChangePeer(region, oldRegion)
	c.Assert(changePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(changePeer.GetPeer(), DeepEquals, res.GetPeer())

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

	c.Assert(cluster.cachedCluster.stores, HasLen, 2)
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

	cluster, err = s.svr.GetRaftCluster()
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	allStores := s.svr.cluster.GetStores()
	c.Assert(allStores, HasLen, 2)
	for _, store := range allStores {
		c.Assert(stores, HasKey, store.GetId())
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
	cluster := newClusterInfo("/pd")
	cluster.idAlloc = newMockIDAllocator()

	id, err := cluster.idAlloc.Alloc()
	c.Assert(err, IsNil)
	c.Assert(id, Greater, uint64(0))
}
