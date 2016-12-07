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

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&testClusterSuite{})

type testClusterBaseSuite struct {
	client  *clientv3.Client
	svr     *Server
	cleanup cleanUpFunc
}

type testClusterSuite struct {
	testClusterBaseSuite
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = newTestServer(c)
	s.client = s.svr.client

	go s.svr.Run()
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterBaseSuite) allocID(c *C) uint64 {
	id, err := s.svr.idAlloc.Alloc()
	c.Assert(err, IsNil)
	return id
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func (s *testClusterBaseSuite) newPeer(c *C, storeID uint64, peerID uint64) *metapb.Peer {
	c.Assert(storeID, Greater, uint64(0))

	if peerID == 0 {
		peerID = s.allocID(c)
	}

	return &metapb.Peer{
		StoreId: storeID,
		Id:      peerID,
	}
}

func (s *testClusterBaseSuite) newStore(c *C, storeID uint64, addr string) *metapb.Store {
	if storeID == 0 {
		storeID = s.allocID(c)
	}

	return &metapb.Store{
		Id:      storeID,
		Address: addr,
	}
}

func (s *testClusterBaseSuite) newRegion(c *C, regionID uint64, startKey []byte,
	endKey []byte, peers []*metapb.Peer, epoch *metapb.RegionEpoch) *metapb.Region {
	if regionID == 0 {
		regionID = s.allocID(c)
	}

	if epoch == nil {
		epoch = &metapb.RegionEpoch{
			ConfVer: initEpochConfVer,
			Version: initEpochVersion,
		}
	}

	for _, peer := range peers {
		peerID := peer.GetId()
		c.Assert(peerID, Greater, uint64(0))
	}

	return &metapb.Region{
		Id:          regionID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: epoch,
		Peers:       peers,
	}
}

func (s *testClusterSuite) TestBootstrap(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := rpcConnect(leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := s.svr.clusterID

	// IsBootstrapped returns false.
	req := s.newIsBootstrapRequest(clusterID)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.IsBootstrapped, NotNil)
	c.Assert(resp.IsBootstrapped.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	s.bootstrapCluster(c, conn, clusterID, storeAddr)

	// IsBootstrapped returns true.
	req = s.newIsBootstrapRequest(clusterID)
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.IsBootstrapped, NotNil)
	c.Assert(resp.IsBootstrapped.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	req = s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.Bootstrap, IsNil)
	c.Assert(resp.Header.Error, NotNil)
	c.Assert(resp.Header.Error.Bootstrapped, NotNil)
}

func (s *testClusterBaseSuite) newIsBootstrapRequest(clusterID uint64) *pdpb.Request {
	req := &pdpb.Request{
		Header:         newRequestHeader(clusterID),
		CmdType:        pdpb.CommandType_IsBootstrapped,
		IsBootstrapped: &pdpb.IsBootstrappedRequest{},
	}

	return req
}

func (s *testClusterBaseSuite) newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.Request {
	store := s.newStore(c, 0, storeAddr)
	peer := s.newPeer(c, store.GetId(), 0)
	region := s.newRegion(c, 0, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)

	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_Bootstrap,
		Bootstrap: &pdpb.BootstrapRequest{
			Store:  store,
			Region: region,
		},
	}

	return req
}

// helper function to check and bootstrap.
func (s *testClusterBaseSuite) bootstrapCluster(c *C, conn net.Conn, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.Bootstrap, NotNil)
}

func (s *testClusterBaseSuite) tryBootstrapCluster(c *C, conn net.Conn, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	if resp.Bootstrap == nil {
		c.Assert(resp.Header.Error, NotNil)
		c.Assert(resp.Header.Error.Bootstrapped, NotNil)
	}
}

func (s *testClusterBaseSuite) getStore(c *C, conn net.Conn, clusterID uint64, storeID uint64) *metapb.Store {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetStore,
		GetStore: &pdpb.GetStoreRequest{
			StoreId: storeID,
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetStore, NotNil)
	c.Assert(resp.GetStore.GetStore().GetId(), Equals, uint64(storeID))

	return resp.GetStore.GetStore()
}

func (s *testClusterBaseSuite) getRegion(c *C, conn net.Conn, clusterID uint64, regionKey []byte) *metapb.Region {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetRegion,
		GetRegion: &pdpb.GetRegionRequest{
			RegionKey: regionKey,
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetRegion, NotNil)
	c.Assert(resp.GetRegion.GetRegion(), NotNil)

	return resp.GetRegion.GetRegion()
}

func (s *testClusterBaseSuite) getRegionByID(c *C, conn net.Conn, clusterID uint64, regionID uint64) *metapb.Region {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_GetRegionByID,
		GetRegionById: &pdpb.GetRegionByIDRequest{
			RegionId: regionID,
		},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetRegionById.GetRegion(), NotNil)

	return resp.GetRegionById.GetRegion()
}

func (s *testClusterBaseSuite) getRaftCluster(c *C) *RaftCluster {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	return cluster
}

func (s *testClusterBaseSuite) getClusterConfig(c *C, conn net.Conn, clusterID uint64) *metapb.Cluster {
	req := &pdpb.Request{
		Header:           newRequestHeader(clusterID),
		CmdType:          pdpb.CommandType_GetClusterConfig,
		GetClusterConfig: &pdpb.GetClusterConfigRequest{},
	}

	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetClusterConfig, NotNil)
	c.Assert(resp.GetClusterConfig.GetCluster(), NotNil)

	return resp.GetClusterConfig.GetCluster()
}

func (s *testClusterSuite) TestGetPutConfig(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := rpcConnect(leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	s.tryBootstrapCluster(c, conn, clusterID, storeAddr)

	// Get region.
	region := s.getRegion(c, conn, clusterID, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := s.getRegionByID(c, conn, clusterID, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	// Get store.
	storeID := peer.GetStoreId()
	store := s.getStore(c, conn, clusterID, storeID)
	c.Assert(store.GetAddress(), Equals, storeAddr)

	// Update store.
	store.Address = "127.0.0.1:1"
	s.testPutStore(c, conn, clusterID, store)

	// Remove store.
	s.testRemoveStore(c, conn, clusterID, store)

	// Update cluster config.
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_PutClusterConfig,
		PutClusterConfig: &pdpb.PutClusterConfigRequest{
			Cluster: &metapb.Cluster{
				Id:           clusterID,
				MaxPeerCount: 5,
			},
		},
	}
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.PutClusterConfig, NotNil)
	meta := s.getClusterConfig(c, conn, clusterID)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func putStore(c *C, conn net.Conn, clusterID uint64, store *metapb.Store) *pdpb.Response {
	req := &pdpb.Request{
		Header:   newRequestHeader(clusterID),
		CmdType:  pdpb.CommandType_PutStore,
		PutStore: &pdpb.PutStoreRequest{Store: store},
	}
	sendRequest(c, conn, 0, req)
	_, resp := recvResponse(c, conn)
	return resp
}

func (s *testClusterSuite) testPutStore(c *C, conn net.Conn, clusterID uint64, store *metapb.Store) {
	// Update store.
	resp := putStore(c, conn, clusterID, store)
	c.Assert(resp.PutStore, NotNil)
	updatedStore := s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	resp = putStore(c, conn, clusterID, store)
	c.Assert(resp.PutStore, NotNil)

	// Put new store with a duplicated address when old store is up will fail.
	resp = putStore(c, conn, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(resp.PutStore, IsNil)

	// Put new store with a duplicated address when old store is offline will fail.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
	resp = putStore(c, conn, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(resp.PutStore, IsNil)

	// Put new store with a duplicated address when old store is tombstone is OK.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
	resp = putStore(c, conn, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(resp.PutStore, NotNil)

	// Put a new store.
	resp = putStore(c, conn, clusterID, s.newStore(c, 0, "127.0.0.1:12345"))
	c.Assert(resp.PutStore, NotNil)

	// Put an existed store with duplicated address with other old stores.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
	resp = putStore(c, conn, clusterID, s.newStore(c, store.GetId(), "127.0.0.1:12345"))
	c.Assert(resp.PutStore, IsNil)
}

func (s *testClusterSuite) resetStoreState(c *C, storeID uint64, state metapb.StoreState) {
	cluster := s.svr.GetRaftCluster().cachedCluster
	c.Assert(cluster, NotNil)
	store := cluster.getStore(storeID)
	c.Assert(store, NotNil)
	store.State = state
	cluster.putStore(store)
}

func (s *testClusterSuite) testRemoveStore(c *C, conn net.Conn, clusterID uint64, store *metapb.Store) {
	cluster := s.getRaftCluster(c)

	// When store is up:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, conn, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ force should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), true)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, conn, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
		// Case 3: BuryStore w/o force should fail.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, NotNil)
	}

	// When store is offline:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, conn, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, conn, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	// When store is tombstone:
	{
		// Case 1: RemoveStore should should fail;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, NotNil)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, conn, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	// Put after removed should return tombstone error.
	resp := putStore(c, conn, clusterID, store)
	c.Assert(resp.PutStore, IsNil)
	c.Assert(resp.Header.Error.IsTombstone, NotNil)

	// Update after removed should return tombstone error.
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_StoreHeartbeat,
		StoreHeartbeat: &pdpb.StoreHeartbeatRequest{
			Stats: &pdpb.StoreStats{StoreId: store.GetId()},
		},
	}
	sendRequest(c, conn, 0, req)
	_, resp = recvResponse(c, conn)
	c.Assert(resp.StoreHeartbeat, IsNil)
	c.Assert(resp.Header.Error.IsTombstone, NotNil)
}

func (s *testClusterSuite) testCheckStores(c *C, conn net.Conn, clusterID uint64) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	store := s.newStore(c, 0, "127.0.0.1:11111")
	resp := putStore(c, conn, clusterID, store)
	c.Assert(resp.PutStore, NotNil)

	// store is up w/o region peers will not be buried.
	cluster.checkStores()
	tmpStore := s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	// Add a region peer to store.
	leader := &metapb.Peer{StoreId: store.GetId()}
	region := s.newRegion(c, 0, []byte{'a'}, []byte{'b'}, []*metapb.Peer{leader}, nil)
	regionInfo := newRegionInfo(region, leader)
	_, err := cluster.handleRegionHeartbeat(regionInfo)
	c.Assert(err, IsNil)
	c.Assert(cluster.cachedCluster.getStoreRegionCount(store.GetId()), Equals, 1)

	// store is up w/ region peers will not be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	err = cluster.RemoveStore(store.GetId())
	c.Assert(err, IsNil)
	removedStore := s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)

	// store is offline w/ region peers will not be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	// Clear store's region peers.
	leader.StoreId = 0
	region.Peers = []*metapb.Peer{leader}
	_, err = cluster.handleRegionHeartbeat(regionInfo)
	c.Assert(err, IsNil)
	c.Assert(cluster.cachedCluster.getStoreRegionCount(store.GetId()), Equals, 0)

	// store is offline w/o region peers will be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, conn, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Tombstone)
}

// Make sure PD will not panic if it start and stop again and again.
func (s *testClusterSuite) TestClosedChannel(c *C) {
	svr, cleanup := newTestServer(c)
	defer cleanup()
	go svr.Run()

	leader := mustGetLeader(c, svr.client, svr.getLeaderPath())

	conn, err := rpcConnect(leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := svr.clusterID
	storeAddr := "127.0.0.1:0"
	s.tryBootstrapCluster(c, conn, clusterID, storeAddr)

	cluster := svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()

	err = svr.createRaftCluster()
	c.Assert(err, IsNil)

	cluster = svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()
}

func (s *testClusterSuite) TestGetPDMembers(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := rpcConnect(leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	clusterID := uint64(0)
	req := &pdpb.Request{
		Header:       newRequestHeader(clusterID),
		CmdType:      pdpb.CommandType_GetPDMembers,
		GetPdMembers: &pdpb.GetPDMembersRequest{},
	}

	sendRequest(c, conn, 0, req)
	id, resp := recvResponse(c, conn)
	c.Assert(id, Equals, clusterID)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetPdMembers, NotNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(len(resp.GetPdMembers.Members), Not(Equals), 0)
}
