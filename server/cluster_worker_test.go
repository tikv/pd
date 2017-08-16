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
	"golang.org/x/net/context"
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
	regionLeaders    map[uint64]metapb.Peer
	heartbeatClients map[uint64]*regionHeartbeatClient
}

type regionHeartbeatClient struct {
	stream pdpb.PD_RegionHeartbeatClient
	respCh chan *pdpb.RegionHeartbeatResponse
}

func newRegionheartbeatClient(c *C, grpcClient pdpb.PDClient) *regionHeartbeatClient {
	stream, err := grpcClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	ch := make(chan *pdpb.RegionHeartbeatResponse)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				return
			}
			ch <- res
		}
	}()
	return &regionHeartbeatClient{
		stream: stream,
		respCh: ch,
	}
}

func (c *regionHeartbeatClient) close() {
	c.stream.CloseSend()
}

func (c *regionHeartbeatClient) SendRecv(msg *pdpb.RegionHeartbeatRequest, timeout time.Duration) *pdpb.RegionHeartbeatResponse {
	c.stream.Send(msg)
	select {
	case <-time.After(timeout):
		return nil
	case res := <-c.respCh:
		return res
	}
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
	store := req.Store
	region := req.Region

	_, err := s.svr.bootstrapCluster(req)
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
	s.svr.scheduleOpt.SetMaxReplicas(1)

	err := s.svr.Run()
	c.Assert(err, IsNil)

	s.client = s.svr.client
	s.clusterID = s.svr.clusterID

	s.regionLeaders = make(map[uint64]metapb.Peer)

	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())

	// Build raft cluster with 5 stores.
	s.bootstrap(c)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	err = cluster.putConfig(&metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: 5,
	})
	c.Assert(err, IsNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	s.heartbeatClients = make(map[uint64]*regionHeartbeatClient)
	for _, store := range stores {
		s.heartbeatClients[store.GetId()] = newRegionheartbeatClient(c, s.grpcPDClient)
	}
}

func (s *testClusterWorkerSuite) runHeartbeatReceiver(c *C) (pdpb.PD_RegionHeartbeatClient, chan *pdpb.RegionHeartbeatResponse) {
	client, err := s.grpcPDClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	ch := make(chan *pdpb.RegionHeartbeatResponse)
	go func() {
		for {
			res, err := client.Recv()
			if err != nil {
				return
			}
			ch <- res
		}
	}()
	return client, ch
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.cleanup()
	for _, client := range s.heartbeatClients {
		client.close()
	}
}

func (s *testClusterWorkerSuite) checkRegionPeerCount(c *C, regionKey []byte, expectCount int) *metapb.Region {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	region, _ := cluster.GetRegionByKey(regionKey)
	c.Assert(region.Peers, HasLen, expectCount)
	return region
}

func (s *testClusterWorkerSuite) checkChangePeerRes(c *C, res *pdpb.ChangePeer, tp pdpb.ConfChangeType, region *metapb.Region) {
	c.Assert(res, NotNil)
	c.Assert(res.GetChangeType(), Equals, tp)
	peer := res.GetPeer()
	c.Assert(peer, NotNil)

	store, ok := s.stores[peer.GetStoreId()]
	c.Assert(ok, IsTrue)

	if tp == pdpb.ConfChangeType_AddNode {
		c.Assert(store.peers, Not(HasKey), peer.GetId())
		store.addPeer(c, peer)
		addRegionPeer(c, region, peer)
	} else if tp == pdpb.ConfChangeType_RemoveNode {
		c.Assert(store.peers, HasKey, peer.GetId())
		store.removePeer(c, peer)
		removeRegionPeer(c, region, peer)
	} else {
		c.Fatalf("invalid conf change type, %v", tp)
	}
}

func (s *testClusterWorkerSuite) askSplit(c *C, r *metapb.Region) (uint64, []uint64) {
	req := &pdpb.AskSplitRequest{
		Header: newRequestHeader(s.clusterID),
		Region: r,
	}
	askResp, err := s.grpcPDClient.AskSplit(context.Background(), req)
	c.Assert(err, IsNil)
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

func (s *testClusterWorkerSuite) heartbeatRegion(c *C, clusterID uint64, region *metapb.Region, leader *metapb.Peer, expectNil bool) *pdpb.RegionHeartbeatResponse {
	req := &pdpb.RegionHeartbeatRequest{
		Header: newRequestHeader(clusterID),
		Leader: leader,
		Region: region,
	}

	timeout := time.Millisecond * 500
	if expectNil {
		timeout = time.Millisecond * 100
	}

	heartbeatClient := s.heartbeatClients[leader.GetStoreId()]
	return heartbeatClient.SendRecv(req, timeout)
}

func (s *testClusterWorkerSuite) heartbeatStore(c *C, stats *pdpb.StoreStats) *pdpb.StoreHeartbeatResponse {
	req := &pdpb.StoreHeartbeatRequest{
		Header: newRequestHeader(s.clusterID),
		Stats:  stats,
	}
	resp, err := s.grpcPDClient.StoreHeartbeat(context.Background(), req)
	c.Assert(err, IsNil)
	return resp
}

func (s *testClusterWorkerSuite) reportSplit(c *C, left *metapb.Region, right *metapb.Region) *pdpb.ReportSplitResponse {
	req := &pdpb.ReportSplitRequest{
		Header: newRequestHeader(s.clusterID),
		Left:   left,
		Right:  right,
	}
	resp, err := s.grpcPDClient.ReportSplit(context.Background(), req)
	c.Assert(err, IsNil)
	return resp
}

func mustGetRegion(c *C, cluster *RaftCluster, key []byte, expect *metapb.Region) {
	r, _ := cluster.GetRegionByKey(key)
	c.Assert(r, DeepEquals, expect)
}

func checkSearchRegions(c *C, cluster *RaftCluster, keys ...[]byte) {
	cluster.cachedCluster.RLock()
	defer cluster.cachedCluster.RUnlock()

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

	// split 1 to 1: [nil, m) 2: [m, nil), sync 1 first
	r1, _ := cluster.GetRegionByKey([]byte("a"))
	checkSearchRegions(c, cluster, []byte{})

	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)

	leaderPeer1 := s.chooseRegionLeader(c, r1)

	s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1, false)
	checkSearchRegions(c, cluster, []byte{})

	mustGetRegion(c, cluster, []byte("a"), r1)
	// [m, nil) is missing before r2's heartbeat.
	mustGetRegion(c, cluster, []byte("z"), nil)

	leaderPeer2 := s.chooseRegionLeader(c, r2)
	s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2, true)
	checkSearchRegions(c, cluster, []byte{}, []byte("m"))

	mustGetRegion(c, cluster, []byte("z"), r2)

	// split 2 to 2: [m, q) 3: [q, nil), sync 3 first
	r3ID, r3PeerIDs := s.askSplit(c, r2)
	r3 := splitRegion(c, r2, []byte("q"), r3ID, r3PeerIDs)

	leaderPeer3 := s.chooseRegionLeader(c, r3)

	s.heartbeatRegion(c, s.clusterID, r3, leaderPeer3, true)
	checkSearchRegions(c, cluster, []byte{}, []byte("q"))

	mustGetRegion(c, cluster, []byte("z"), r3)
	mustGetRegion(c, cluster, []byte("a"), r1)
	// [m, q) is missing before r2's heartbeat.
	mustGetRegion(c, cluster, []byte("n"), nil)

	s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2, true)
	checkSearchRegions(c, cluster, []byte{}, []byte("m"), []byte("q"))

	mustGetRegion(c, cluster, []byte("n"), r2)
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit2(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(5)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.GetRegionByKey([]byte("a"))
	//	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())
	leaderPeer := s.chooseRegionLeader(c, r1)

	// Set MaxPeerCount to 10.
	meta := cluster.GetConfig()
	meta.MaxPeerCount = 10
	err := cluster.putConfig(meta)
	c.Assert(err, IsNil)

	// Add Peers util all stores are used up.
	for {
		resp := s.heartbeatRegion(c, s.clusterID, r1, leaderPeer, false)
		if resp == nil {
			break
		}
		s.checkChangePeerRes(c, resp.GetChangePeer(), pdpb.ConfChangeType_AddNode, r1)
	}

	// Split.
	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)
	leaderPeer2 := s.chooseRegionLeader(c, r2)

	resp := s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2, true)
	c.Assert(resp, IsNil)

	mustGetRegion(c, cluster, []byte("m"), r2)
}

func (s *testClusterWorkerSuite) TestHeartbeatChangePeer(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(5)

	opt := s.svr.scheduleOpt

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	meta := cluster.GetConfig()
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, _ := cluster.GetRegionByKey(regionKey)
	c.Assert(region.Peers, HasLen, 1)

	//	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	leaderPeer := s.chooseRegionLeader(c, region)
	c.Logf("[leaderPeer]:%v, [region]:%v", leaderPeer, region)

	// Add 4 peers.
	for i := 0; i < 4; i++ {
		resp := s.heartbeatRegion(c, s.clusterID, region, leaderPeer, false)
		c.Assert(resp, NotNil)
		// Check RegionHeartbeat response.
		s.checkChangePeerRes(c, resp.GetChangePeer(), pdpb.ConfChangeType_AddNode, region)
		c.Logf("[add peer][region]:%v", region)

		// Update region epoch and check region info.
		region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1
		s.heartbeatRegion(c, s.clusterID, region, leaderPeer, false)

		// Check region peer count.
		region = s.checkRegionPeerCount(c, regionKey, i+2)
	}

	region = s.checkRegionPeerCount(c, regionKey, 5)

	opt.SetMaxReplicas(3)

	// Remove 2 peers
	peerCount := 5
	for i := 0; i < 10; i++ {
		resp := s.heartbeatRegion(c, s.clusterID, region, leaderPeer, false)
		if resp == nil {
			continue
		}
		if resp.GetTransferLeader() != nil {
			leaderPeer = resp.GetTransferLeader().GetPeer()
			continue
		}

		// Check RegionHeartbeat response.
		s.checkChangePeerRes(c, resp.GetChangePeer(), pdpb.ConfChangeType_RemoveNode, region)

		// Update region epoch and check region info.
		region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1
		s.heartbeatRegion(c, s.clusterID, region, leaderPeer, false)

		// Check region peer count.
		peerCount--
		region = s.checkRegionPeerCount(c, regionKey, peerCount)
		if peerCount == 3 {
			return
		}
	}
	c.Fatal("peerCount not decrease to 3 after retry 10 times")
}

func (s *testClusterWorkerSuite) TestHeartbeatSplitAddPeer(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(2)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.GetRegionByKey([]byte("a"))
	leaderPeer1 := s.chooseRegionLeader(c, r1)

	// First sync, pd-server will return a AddPeer.
	resp := s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1, false)
	// Apply the AddPeer ConfChange, but with no sync.
	s.checkChangePeerRes(c, resp.GetChangePeer(), pdpb.ConfChangeType_AddNode, r1)
	// Split 1 to 1: [nil, m) 2: [m, nil).
	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)

	// Sync r1 with both ConfVer and Version updated.
	resp = s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1, true)
	c.Assert(resp, IsNil)

	mustGetRegion(c, cluster, []byte("a"), r1)
	mustGetRegion(c, cluster, []byte("z"), nil)

	// Sync r2.
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	resp = s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2, true)
	c.Assert(resp, IsNil)
}

func (s *testClusterWorkerSuite) TestStoreHeartbeat(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	// Mock a store stats.
	storeID := stores[0].GetId()
	stats := &pdpb.StoreStats{
		StoreId:     storeID,
		Capacity:    100,
		Available:   50,
		RegionCount: 1,
	}

	resp := s.heartbeatStore(c, stats)
	c.Assert(resp, NotNil)

	store := cluster.cachedCluster.getStore(storeID)
	c.Assert(stats, DeepEquals, store.status.StoreStats)
}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	// Mock a report split request.
	peer := s.newPeer(c, 999, 0)
	left := s.newRegion(c, 2, []byte("aaa"), []byte("bbb"), []*metapb.Peer{peer}, nil)
	right := s.newRegion(c, 1, []byte("bbb"), []byte("ccc"), []*metapb.Peer{peer}, nil)

	resp := s.reportSplit(c, left, right)
	c.Assert(resp, NotNil)

	regionID := right.GetId()
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
