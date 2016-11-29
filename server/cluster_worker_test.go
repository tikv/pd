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
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
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
	metaStore.Address = addr
	store := &mockRaftStore{
		s:        s,
		listener: l,
		store:    metaStore,
		peers:    make(map[uint64]*metapb.Peer),
	}

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	err = cluster.putStore(metaStore)
	c.Assert(err, IsNil)

	stats := &pdpb.StoreStats{
		StoreId:            metaStore.GetId(),
		Capacity:           100,
		Available:          50,
		SendingSnapCount:   1,
		ReceivingSnapCount: 1,
	}

	c.Assert(cluster.cachedCluster.handleStoreHeartbeat(stats), IsNil)

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.stores[metaStore.GetId()] = store
	return store
}

func (s *testClusterWorkerSuite) SetUpTest(c *C) {
	s.stores = make(map[uint64]*mockRaftStore)

	s.svr, s.cleanup = newTestServer(c)
	s.svr.cfg.nextRetryDelay = 50 * time.Millisecond
	s.svr.scheduleOpt.constraints = newConstraints(5, nil)

	s.client = s.svr.client
	s.clusterID = s.svr.clusterID

	s.regionLeaders = make(map[uint64]metapb.Peer)

	go s.svr.Run()

	mustWaitLeader(c, []*Server{s.svr})

	// Build raft cluster with 5 stores.
	s.bootstrap(c)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	err := cluster.putConfig(&metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: 5,
	})
	c.Assert(err, IsNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.cleanup()
}

func (s *testClusterWorkerSuite) checkRegionPeerCount(c *C, regionKey []byte, expectCount int) *metapb.Region {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	region, _ := cluster.getRegion(regionKey)
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
		CmdType: pdpb.CommandType_AskSplit,
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
		ConfVer: r.GetRegionEpoch().GetConfVer(),
		Version: r.GetRegionEpoch().GetVersion() + 1,
	}
}

func splitRegion(c *C, old *metapb.Region, splitKey []byte, newRegionID uint64, newPeerIDs []uint64) *metapb.Region {
	var peers []*metapb.Peer
	c.Assert(len(old.Peers), Equals, len(newPeerIDs))
	for i, peer := range old.Peers {
		peers = append(peers, &metapb.Peer{
			Id:      newPeerIDs[i],
			StoreId: peer.GetStoreId(),
		})
	}
	newRegion := &metapb.Region{
		Id:          newRegionID,
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
		CmdType: pdpb.CommandType_RegionHeartbeat,
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
		CmdType: pdpb.CommandType_StoreHeartbeat,
		StoreHeartbeat: &pdpb.StoreHeartbeatRequest{
			Stats: stats,
		},
	}
	sendRequest(c, conn, msgID, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_StoreHeartbeat)
	return resp.GetStoreHeartbeat()
}

func (s *testClusterWorkerSuite) reportSplit(c *C, conn net.Conn, msgID uint64, left *metapb.Region, right *metapb.Region) *pdpb.ReportSplitResponse {
	req := &pdpb.Request{
		Header:  newRequestHeader(s.clusterID),
		CmdType: pdpb.CommandType_ReportSplit,
		ReportSplit: &pdpb.ReportSplitRequest{
			Left:  left,
			Right: right,
		},
	}
	sendRequest(c, conn, msgID, req)
	_, resp := recvResponse(c, conn)
	c.Assert(resp.GetCmdType(), Equals, pdpb.CommandType_ReportSplit)
	return resp.GetReportSplit()
}

func mustGetRegion(c *C, cluster *RaftCluster, key []byte, expect *metapb.Region) {
	r, _ := cluster.getRegion(key)
	c.Assert(r, DeepEquals, expect)
}

func checkSearchRegions(c *C, cluster *RaftCluster, keys ...[]byte) {
	cacheRegions := cluster.cachedCluster.regions
	c.Assert(cacheRegions.tree.length(), Equals, len(keys))

	for _, key := range keys {
		getItem := cacheRegions.tree.search(key)
		c.Assert(getItem, NotNil)
	}
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	s.svr.scheduleOpt.constraints = newConstraints(1, nil)

	leaderPD := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := rpcConnect(leaderPD.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// split 1 to 1: [nil, m) 2: [m, nil), sync 1 first
	r1, _ := cluster.getRegion([]byte("a"))
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
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.getRegion([]byte("a"))
	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := rpcConnect(leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()
	leaderPeer := s.chooseRegionLeader(c, r1)

	// Set MaxPeerCount to 10.
	meta := cluster.GetConfig()
	meta.MaxPeerCount = 10
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
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	meta := cluster.GetConfig()
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, _ := cluster.getRegion(regionKey)
	c.Assert(region.Peers, HasLen, 1)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := rpcConnect(leaderPd.GetAddr())
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
		region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1
		heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)
		// Check region peer count.
		region = s.checkRegionPeerCount(c, regionKey, i+2)
	}

	region = s.checkRegionPeerCount(c, regionKey, 5)

	// Remove 2 peers.
	s.svr.scheduleOpt.constraints = newConstraints(3, nil)

	// Remove 2 peers
	for i := 0; i < 2; i++ {
		resp := heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)
		// Check RegionHeartbeat response.
		s.checkChangePeerRes(c, resp, raftpb.ConfChangeType_RemoveNode, region)

		// Update region epoch and check region info.
		region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1
		heartbeatRegion(c, conn, s.clusterID, 0, region, leaderPeer)

		// Check region peer count.
		region = s.checkRegionPeerCount(c, regionKey, 4-i)
	}

	region = s.checkRegionPeerCount(c, regionKey, 3)
}

func (s *testClusterWorkerSuite) TestHeartbeatSplitAddPeer(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	s.svr.scheduleOpt.constraints = newConstraints(2, nil)

	leaderPD := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := rpcConnect(leaderPD.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	r1, _ := cluster.getRegion([]byte("a"))
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
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := rpcConnect(leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// Mock a store stats.
	storeID := stores[0].GetId()
	stats := &pdpb.StoreStats{
		StoreId:     storeID,
		Capacity:    100,
		Available:   50,
		RegionCount: 1,
	}

	resp := s.heartbeatStore(c, conn, 0, stats)
	c.Assert(resp, NotNil)

	store := cluster.cachedCluster.getStore(storeID)
	c.Assert(stats, DeepEquals, store.stats.StoreStats)
}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	conn, err := rpcConnect(leaderPd.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	// Mock a report split request.
	peer := s.newPeer(c, 999, 0)
	left := s.newRegion(c, 0, []byte("aaa"), []byte("bbb"), []*metapb.Peer{peer}, nil)
	right := s.newRegion(c, 0, []byte("bbb"), []byte("ccc"), []*metapb.Peer{peer}, nil)

	resp := s.reportSplit(c, conn, 0, left, right)
	c.Assert(resp, NotNil)

	regionID := left.GetId()
	value, ok := cluster.coordinator.histories.get(regionID)
	c.Assert(ok, IsTrue)

	op := value.(*splitOperator)
	c.Assert(op.Left, DeepEquals, left)
	c.Assert(op.Right, DeepEquals, right)
	c.Assert(op.Origin.GetId(), Equals, regionID)
	c.Assert(op.Origin.GetRegionEpoch(), IsNil)
	c.Assert(op.Origin.GetStartKey(), BytesEquals, left.GetStartKey())
	c.Assert(op.Origin.GetEndKey(), BytesEquals, right.GetEndKey())
	c.Assert(op.Origin.GetPeers(), HasLen, 1)
	c.Assert(op.Origin.GetPeers()[0], DeepEquals, peer)
}
