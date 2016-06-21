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
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

var _ = Suite(&testClusterWorkerSuite{})

type mockRaftStore struct {
	sync.Mutex

	s *testClusterWorkerSuite

	listener net.Listener

	store *metapb.Store

	// peerID -> Peer
	peers map[uint64]*metapb.Peer
}

func (s *mockRaftStore) addPeer(c *C, peer *metapb.Peer) {
	s.Lock()
	defer s.Unlock()

	c.Assert(s.peers, Not(HasKey), peer.GetId())
	s.peers[peer.GetId()] = peer
}

func (s *mockRaftStore) removePeer(c *C, peer *metapb.Peer) {
	s.Lock()
	defer s.Unlock()

	c.Assert(s.peers, HasKey, peer.GetId())
	delete(s.peers, peer.GetId())
}

func addRegionPeer(c *C, region *metapb.Region, peer *metapb.Peer) {
	found := false
	for _, p := range region.Peers {
		if p.GetId() == peer.GetId() || p.GetStoreId() == peer.GetStoreId() {
			found = true
			break
		}
	}

	c.Assert(found, IsFalse)
	region.Peers = append(region.Peers, peer)
}

func removeRegionPeer(c *C, region *metapb.Region, peer *metapb.Peer) {
	found := false
	peers := make([]*metapb.Peer, 0, len(region.Peers))
	for _, p := range region.Peers {
		if p.GetId() == peer.GetId() {
			c.Assert(p.GetStoreId(), Equals, peer.GetStoreId())
			found = true
			continue
		}

		peers = append(peers, p)
	}

	c.Assert(found, IsTrue)
	region.Peers = peers
}

type testClusterWorkerSuite struct {
	testClusterBaseSuite

	clusterID uint64

	storeLock sync.Mutex
	// storeID -> mockRaftStore
	stores map[uint64]*mockRaftStore

	regionLeaderLock sync.Mutex
	// regionID -> Peer
	regionLeaders map[uint64]metapb.Peer
}

func (s *testClusterWorkerSuite) clearRegionLeader(c *C, regionID uint64) {
	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	delete(s.regionLeaders, regionID)
}

func (s *testClusterWorkerSuite) chooseRegionLeader(c *C, region *metapb.Region) *metapb.Peer {
	// Randomly select a peer in the region as the leader.
	peer := region.Peers[rand.Intn(len(region.Peers))]

	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	s.regionLeaders[region.GetId()] = *peer
	return peer
}

func (s *testClusterWorkerSuite) bootstrap(c *C) *mockRaftStore {
	req := s.newBootstrapRequest(c, s.clusterID, "127.0.0.1:0")
	store := req.Bootstrap.Store
	region := req.Bootstrap.Region

	_, err := s.svr.bootstrapCluster(req.Bootstrap)
	c.Assert(err, IsNil)

	raftStore := s.newMockRaftStore(c, store)
	c.Assert(region.Peers, HasLen, 1)
	raftStore.addPeer(c, region.Peers[0])
	return raftStore
}

func (s *testClusterWorkerSuite) newMockRaftStore(c *C, metaStore *metapb.Store) *mockRaftStore {
	if metaStore == nil {
		metaStore = s.newStore(c, 0, "127.0.0.1:0")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)

	addr := l.Addr().String()
	metaStore.Address = proto.String(addr)
	store := &mockRaftStore{
		s:        s,
		listener: l,
		store:    metaStore,
		peers:    make(map[uint64]*metapb.Peer),
	}

	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	err = cluster.putStore(metaStore)
	c.Assert(err, IsNil)

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.stores[metaStore.GetId()] = store
	return store
}

func (s *testClusterWorkerSuite) getRootPath() string {
	return "test_cluster_worker"
}

func (s *testClusterWorkerSuite) SetUpTest(c *C) {
	s.clusterID = 0

	s.stores = make(map[uint64]*mockRaftStore)

	s.svr = newTestServer(c, s.getRootPath())
	s.svr.cfg.nextRetryDelay = 50 * time.Millisecond

	s.client = newEtcdClient(c)

	s.regionLeaders = make(map[uint64]metapb.Peer)

	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()

	mustGetLeader(c, s.client, s.svr.getLeaderPath())

	// Build raft cluster with 5 stores.
	s.bootstrap(c)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)

	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	err = cluster.putConfig(&metapb.Cluster{
		Id:           proto.Uint64(s.clusterID),
		MaxPeerCount: proto.Uint32(5),
	})
	c.Assert(err, IsNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.svr.Close()
	s.client.Close()
}

func (s *testClusterWorkerSuite) checkRegionPeerCount(c *C, regionKey []byte, expectCount int) *metapb.Region {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	region, err := cluster.getRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.Peers, HasLen, expectCount)
	return region
}

func (s *testClusterWorkerSuite) checkChangePeerRes(c *C, res *pdpb.ChangePeer, tp raftpb.ConfChangeType, region *metapb.Region) {
	c.Assert(res, NotNil)
	c.Assert(res.GetChangeType(), Equals, tp)
	peer := res.GetPeer()
	c.Assert(peer, NotNil)

	store, ok := s.stores[peer.GetStoreId()]
	c.Assert(ok, IsTrue)

	if tp == raftpb.ConfChangeType_AddNode {
		c.Assert(store.peers, Not(HasKey), peer.GetId())
		store.addPeer(c, peer)
		addRegionPeer(c, region, peer)
	} else if tp == raftpb.ConfChangeType_RemoveNode {
		c.Assert(store.peers, HasKey, peer.GetId())
		store.removePeer(c, peer)
		removeRegionPeer(c, region, peer)
	} else {
		c.Fatalf("invalid conf change type, %v", tp)
	}
}

func (s *testClusterWorkerSuite) askSplit(c *C, conn net.Conn, msgID uint64, r *metapb.Region) (uint64, []uint64) {
	req := &pdpb.Request{
		Header:  newRequestHeader(s.clusterID),
		CmdType: pdpb.CommandType_AskSplit.Enum(),
		AskSplit: &pdpb.AskSplitRequest{
			Region: r,
		},
	}
	sendRequest(c, conn, msgID, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_AskSplit)
	askResp := resp.GetAskSplit()
	c.Assert(askResp, NotNil)
	c.Assert(askResp.GetNewRegionId(), Not(Equals), 0)
	c.Assert(askResp.GetNewPeerIds(), HasLen, len(r.Peers))
	return askResp.GetNewRegionId(), askResp.GetNewPeerIds()
}

func updateRegionRange(r *metapb.Region, start, end []byte) {
	r.StartKey = start
	r.EndKey = end
	r.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: proto.Uint64(r.GetRegionEpoch().GetConfVer()),
		Version: proto.Uint64(r.GetRegionEpoch().GetVersion() + 1),
	}
}

func splitRegion(c *C, old *metapb.Region, splitKey []byte, newRegionID uint64, newPeerIDs []uint64) *metapb.Region {
	var peers []*metapb.Peer
	c.Assert(len(old.Peers), Equals, len(newPeerIDs))
	for i, peer := range old.Peers {
		peers = append(peers, &metapb.Peer{
			Id:      proto.Uint64(newPeerIDs[i]),
			StoreId: proto.Uint64(peer.GetStoreId()),
		})
	}
	newRegion := &metapb.Region{
		Id:          proto.Uint64(newRegionID),
		RegionEpoch: proto.Clone(old.RegionEpoch).(*metapb.RegionEpoch),
		Peers:       peers,
	}
	updateRegionRange(newRegion, splitKey, old.EndKey)
	updateRegionRange(old, old.StartKey, splitKey)
	return newRegion
}

func heartbeatRegion(c *C, conn net.Conn, clusterID uint64, msgID uint64, region *metapb.Region, leader *metapb.Peer) *pdpb.ChangePeer {
	req := &pdpb.Request{
		Header:  newRequestHeader(clusterID),
		CmdType: pdpb.CommandType_RegionHeartbeat.Enum(),
		RegionHeartbeat: &pdpb.RegionHeartbeatRequest{
			Leader: leader,
			Region: region,
		},
	}
	sendRequest(c, conn, msgID, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_RegionHeartbeat)
	return resp.GetRegionHeartbeat().GetChangePeer()
}

func (s *testClusterWorkerSuite) heartbeatStore(c *C, conn net.Conn, msgID uint64, stats *pdpb.StoreStats) *pdpb.StoreHeartbeatResponse {
	req := &pdpb.Request{
		Header:  newRequestHeader(s.clusterID),
		CmdType: pdpb.CommandType_StoreHeartbeat.Enum(),
		StoreHeartbeat: &pdpb.StoreHeartbeatRequest{
			Stats: stats,
		},
	}
	sendRequest(c, conn, msgID, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_StoreHeartbeat)
	return resp.GetStoreHeartbeat()
}

func mustGetRegion(c *C, cluster *RaftCluster, key []byte, expect *metapb.Region) {
	r, err := cluster.getRegion(key)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
}

func checkSearchRegions(c *C, cluster *RaftCluster, keys ...[]byte) {
	cacheRegions := cluster.cachedCluster.regions
	c.Assert(cacheRegions.searchRegions.Len(), Equals, len(keys))

	for _, key := range keys {
		mockRegion := &metapb.Region{StartKey: key}
		item := &searchKeyItem{
			region: mockRegion,
		}

		getItem := cacheRegions.searchRegions.Get(item)
		c.Assert(getItem, NotNil)
	}
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit(c *C) {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	meta := cluster.GetConfig()
	meta.MaxPeerCount = proto.Uint32(1)
	err = cluster.putConfig(meta)
	c.Assert(err, IsNil)

	leaderPD := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := net.Dial("tcp", leaderPD.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// split 1 to 1: [nil, m) 2: [m, nil), sync 1 first
	r1, err := cluster.getRegion([]byte("a"))
	c.Assert(err, IsNil)
	checkSearchRegions(c, cluster, []byte{})

	r2ID, r2PeerIDs := s.askSplit(c, conn, 0, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)

	leaderPeer1 := s.chooseRegionLeader(c, r1)

	resp := heartbeatRegion(c, conn, s.clusterID, 0, r1, leaderPeer1)
	c.Assert(resp, IsNil)
	checkSearchRegions(c, cluster, []byte{})

	mustGetRegion(c, cluster, []byte("a"), r1)
	// [m, nil) is missing before r2's heartbeat.
	mustGetRegion(c, cluster, []byte("z"), nil)

	leaderPeer2 := s.chooseRegionLeader(c, r2)
	resp = heartbeatRegion(c, conn, s.clusterID, 0, r2, leaderPeer2)
	c.Assert(resp, IsNil)
	checkSearchRegions(c, cluster, []byte{}, []byte("m"))

	mustGetRegion(c, cluster, []byte("z"), r2)

	// split 2 to 2: [m, q) 3: [q, nil), sync 3 first
	r3ID, r3PeerIDs := s.askSplit(c, conn, 0, r2)
	r3 := splitRegion(c, r2, []byte("q"), r3ID, r3PeerIDs)

	leaderPeer3 := s.chooseRegionLeader(c, r3)

	resp = heartbeatRegion(c, conn, s.clusterID, 0, r3, leaderPeer3)
	c.Assert(resp, IsNil)
	checkSearchRegions(c, cluster, []byte{}, []byte("q"))

	mustGetRegion(c, cluster, []byte("z"), r3)
	mustGetRegion(c, cluster, []byte("a"), r1)
	// [m, q) is missing before r2's heartbeat.
	mustGetRegion(c, cluster, []byte("n"), nil)

	resp = heartbeatRegion(c, conn, s.clusterID, 0, r2, leaderPeer2)
	c.Assert(resp, IsNil)
	checkSearchRegions(c, cluster, []byte{}, []byte("m"), []byte("q"))

	mustGetRegion(c, cluster, []byte("n"), r2)
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit2(c *C) {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)
	r1, err := cluster.getRegion([]byte("a"))
	c.Assert(err, IsNil)
	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()
	leaderPeer := s.chooseRegionLeader(c, r1)

	// Set MaxPeerCount to 10.
	meta := cluster.GetConfig()
	meta.MaxPeerCount = proto.Uint32(10)
	err = cluster.putConfig(meta)
	c.Assert(err, IsNil)

	// Add Peers util all stores are used up.
	for {
		resp := heartbeatRegion(c, conn, s.clusterID, 0, r1, leaderPeer)
		if resp == nil {
			break
		}
		s.checkChangePeerRes(c, resp, raftpb.ConfChangeType_AddNode, r1)
	}

	// Split.
	r2ID, r2PeerIDs := s.askSplit(c, conn, 0, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	resp := heartbeatRegion(c, conn, s.clusterID, 0, r2, leaderPeer2)
	c.Assert(resp, IsNil)

	mustGetRegion(c, cluster, []byte("m"), r2)
}

func (s *testClusterWorkerSuite) TestHeartbeatChangePeer(c *C) {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	meta := cluster.GetConfig()
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, err := cluster.getRegion(regionKey)
	c.Assert(err, IsNil)
	c.Assert(region.Peers, HasLen, 1)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	leaderPeer := s.chooseRegionLeader(c, region)
	c.Logf("[leaderPeer]:%v, [region]:%v", leaderPeer, region)

	// Add 4 peers.
	for i := 0; i < 4; i++ {
		resp := heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)
		// Check RegionHeartbeat response.
		s.checkChangePeerRes(c, resp, raftpb.ConfChangeType_AddNode, region)
		c.Logf("[add peer][region]:%v", region)

		// Update region epoch and check region info.
		region.RegionEpoch.ConfVer = proto.Uint64(region.GetRegionEpoch().GetConfVer() + 1)
		heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)
		// Check region peer count.
		region = s.checkRegionPeerCount(c, regionKey, i+2)
	}

	region = s.checkRegionPeerCount(c, regionKey, 5)

	// Remove 2 peers.
	err = cluster.putConfig(&metapb.Cluster{
		Id:           proto.Uint64(s.clusterID),
		MaxPeerCount: proto.Uint32(3),
	})
	c.Assert(err, IsNil)

	// Remove 2 peers
	for i := 0; i < 2; i++ {
		resp := heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)
		// Check RegionHeartbeat response.
		s.checkChangePeerRes(c, resp, raftpb.ConfChangeType_RemoveNode, region)

		// Update region epoch and check region info.
		region.RegionEpoch.ConfVer = proto.Uint64(region.GetRegionEpoch().GetConfVer() + 1)
		heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)

		// Check region peer count.
		region = s.checkRegionPeerCount(c, regionKey, 4-i)
	}

	region = s.checkRegionPeerCount(c, regionKey, 3)
}

func (s *testClusterWorkerSuite) TestHeartbeatSplitAddPeer(c *C) {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	meta := cluster.GetConfig()
	meta.MaxPeerCount = proto.Uint32(2)
	err = cluster.putConfig(meta)
	c.Assert(err, IsNil)

	leaderPD := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := net.Dial("tcp", leaderPD.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	r1, err := cluster.getRegion([]byte("a"))
	c.Assert(err, IsNil)
	leaderPeer1 := s.chooseRegionLeader(c, r1)

	// First sync, pd-server will return a AddPeer.
	resp := heartbeatRegion(c, conn, s.clusterID, 0, r1, leaderPeer1)
	// Apply the AddPeer ConfChange, but with no sync.
	s.checkChangePeerRes(c, resp, raftpb.ConfChangeType_AddNode, r1)
	// Split 1 to 1: [nil, m) 2: [m, nil).
	r2ID, r2PeerIDs := s.askSplit(c, conn, 0, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)

	// Sync r1 with both ConfVer and Version updated.
	resp = heartbeatRegion(c, conn, s.clusterID, 0, r1, leaderPeer1)
	c.Assert(resp, IsNil)

	mustGetRegion(c, cluster, []byte("a"), r1)
	mustGetRegion(c, cluster, []byte("z"), nil)

	// Sync r2.
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	resp = heartbeatRegion(c, conn, s.clusterID, 0, r2, leaderPeer2)
	c.Assert(resp, IsNil)
}

func (s *testClusterWorkerSuite) TestStoreHeartbeat(c *C) {
	cluster, err := s.svr.GetRaftCluster()
	c.Assert(err, IsNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := net.Dial("tcp", leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// Mock a store stats.
	storeID := stores[0].GetId()
	stats := &pdpb.StoreStats{
		StoreId:     proto.Uint64(storeID),
		Capacity:    proto.Uint64(100),
		Available:   proto.Uint64(50),
		RegionCount: proto.Uint32(1),
	}

	resp := s.heartbeatStore(c, conn, 0, stats)
	c.Assert(resp, NotNil)

	store := cluster.cachedCluster.getStore(storeID)
	c.Assert(stats, DeepEquals, store.stats.stats)
}
