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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockid"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&testClusterSuite{})

type baseCluster struct {
	svr          *Server
	grpcPDClient pdpb.PDClient
}

type testClusterSuite struct {
	baseCluster
}

type testErrorKV struct {
	core.KVBase
}

func (kv *testErrorKV) Save(key, value string) error {
	return errors.New("save failed")
}

func mustNewGrpcClient(c *C, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure())

	c.Assert(err, IsNil)
	return pdpb.NewPDClient(conn)
}

func (s *baseCluster) allocID(c *C) uint64 {
	id, err := s.svr.idAlloc.Alloc()
	c.Assert(err, IsNil)
	return id
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func (s *baseCluster) newPeer(c *C, storeID uint64, peerID uint64) *metapb.Peer {
	c.Assert(storeID, Greater, uint64(0))

	if peerID == 0 {
		peerID = s.allocID(c)
	}

	return &metapb.Peer{
		StoreId: storeID,
		Id:      peerID,
	}
}

func (s *baseCluster) newStore(c *C, storeID uint64, addr string) *metapb.Store {
	if storeID == 0 {
		storeID = s.allocID(c)
	}

	return &metapb.Store{
		Id:      storeID,
		Address: addr,
	}
}

func (s *baseCluster) newRegion(c *C, regionID uint64, startKey []byte,
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
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	defer cleanup()
	clusterID := s.svr.clusterID

	// IsBootstrapped returns false.
	req := s.newIsBootstrapRequest(clusterID)
	resp, err := s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	s.bootstrapCluster(c, clusterID, storeAddr)

	// IsBootstrapped returns true.
	req = s.newIsBootstrapRequest(clusterID)
	resp, err = s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	reqBoot := s.newBootstrapRequest(c, clusterID, storeAddr)
	respBoot, err := s.grpcPDClient.Bootstrap(context.Background(), reqBoot)
	c.Assert(err, IsNil)
	c.Assert(respBoot.GetHeader().GetError(), NotNil)
	c.Assert(respBoot.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_ALREADY_BOOTSTRAPPED)
}

func (s *baseCluster) newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	req := &pdpb.IsBootstrappedRequest{
		Header: newRequestHeader(clusterID),
	}

	return req
}

func (s *baseCluster) newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.BootstrapRequest {
	store := s.newStore(c, 0, storeAddr)
	peer := s.newPeer(c, store.GetId(), 0)
	region := s.newRegion(c, 0, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)

	req := &pdpb.BootstrapRequest{
		Header: newRequestHeader(clusterID),
		Store:  store,
		Region: region,
	}

	return req
}

// helper function to check and bootstrap.
func (s *baseCluster) bootstrapCluster(c *C, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	_, err := s.grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func (s *baseCluster) getStore(c *C, clusterID uint64, storeID uint64) *metapb.Store {
	req := &pdpb.GetStoreRequest{
		Header:  newRequestHeader(clusterID),
		StoreId: storeID,
	}
	resp, err := s.grpcPDClient.GetStore(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetStore().GetId(), Equals, storeID)

	return resp.GetStore()
}

func (s *baseCluster) getRegion(c *C, clusterID uint64, regionKey []byte) *metapb.Region {
	req := &pdpb.GetRegionRequest{
		Header:    newRequestHeader(clusterID),
		RegionKey: regionKey,
	}

	resp, err := s.grpcPDClient.GetRegion(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *baseCluster) getRegionByID(c *C, clusterID uint64, regionID uint64) *metapb.Region {
	req := &pdpb.GetRegionByIDRequest{
		Header:   newRequestHeader(clusterID),
		RegionId: regionID,
	}

	resp, err := s.grpcPDClient.GetRegionByID(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *baseCluster) getRaftCluster(c *C) *RaftCluster {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	return cluster
}

func (s *baseCluster) getClusterConfig(c *C, clusterID uint64) *metapb.Cluster {
	req := &pdpb.GetClusterConfigRequest{
		Header: newRequestHeader(clusterID),
	}

	resp, err := s.grpcPDClient.GetClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCluster(), NotNil)

	return resp.GetCluster()
}

func (s *testClusterSuite) TestGetPutConfig(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	defer cleanup()
	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, storeAddr))
	c.Assert(err, IsNil)

	// Get region.
	region := s.getRegion(c, clusterID, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := s.getRegionByID(c, clusterID, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	// Get store.
	storeID := peer.GetStoreId()
	store := s.getStore(c, clusterID, storeID)

	// Update store.
	store.Address = "127.0.0.1:1"
	s.testPutStore(c, clusterID, store)

	// Remove store.
	s.testRemoveStore(c, clusterID, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: newRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := s.grpcPDClient.PutClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	meta := s.getClusterConfig(c, clusterID)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func putStore(c *C, grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	req := &pdpb.PutStoreRequest{
		Header: newRequestHeader(clusterID),
		Store:  store,
	}
	resp, err := grpcPDClient.PutStore(context.Background(), req)
	return resp, err
}

func (s *baseCluster) testPutStore(c *C, clusterID uint64, store *metapb.Store) {
	// Update store.
	_, err := putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	updatedStore := s.getStore(c, clusterID, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	_, err = putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)

	// Put new store with a duplicated address when old store is up will fail.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is offline will fail.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is tombstone is OK.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, IsNil)

	// Put a new store.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, "127.0.0.1:12345"))
	c.Assert(err, IsNil)

	// Put an existed store with duplicated address with other old stores.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, store.GetId(), "127.0.0.1:12345"))
	c.Assert(err, NotNil)
}

func (s *baseCluster) resetStoreState(c *C, storeID uint64, state metapb.StoreState) {
	raftCluster := s.svr.GetRaftCluster()
	raftCluster.RLock()
	defer raftCluster.RUnlock()
	cluster := raftCluster.cachedCluster
	c.Assert(cluster, NotNil)
	store := cluster.GetStore(storeID)
	c.Assert(store, NotNil)
	newStore := store.Clone(core.SetStoreState(state))
	c.Assert(cluster.putStore(newStore), IsNil)
}

func (s *baseCluster) testRemoveStore(c *C, clusterID uint64, store *metapb.Store) {
	cluster := s.getRaftCluster(c)

	// When store is up:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ force should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), true)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
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
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
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
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	{
		// Put after removed should return tombstone error.
		resp, err := putStore(c, s.grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: newRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := s.grpcPDClient.StoreHeartbeat(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
}

// Make sure PD will not panic if it start and stop again and again.
func (s *testClusterSuite) TestRaftClusterRestart(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	defer cleanup()
	mustWaitLeader(c, []*Server{s.svr})
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()

	err = s.svr.createRaftCluster()
	c.Assert(err, IsNil)

	cluster = s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()
}

// Make sure PD will not deadlock if it start and stop again and again.
func (s *testClusterSuite) TestRaftClusterMultipleRestart(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	defer cleanup()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)
	// add an offline store
	store := s.newStore(c, s.allocID(c), "127.0.0.1:4")
	store.State = metapb.StoreState_Offline
	cluster := s.svr.GetRaftCluster()
	err = cluster.putStore(store)
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// let the job run at small interval
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/highFrequencyClusterJobs", `return(true)`), IsNil)
	for i := 0; i < 100; i++ {
		err = s.svr.createRaftCluster()
		c.Assert(err, IsNil)
		time.Sleep(time.Millisecond)
		cluster = s.svr.GetRaftCluster()
		c.Assert(cluster, NotNil)
		cluster.stop()
	}
}

func (s *testClusterSuite) TestGetPDMembers(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	defer cleanup()
	req := &pdpb.GetMembersRequest{
		Header: newRequestHeader(s.svr.ClusterID()),
	}

	resp, err := s.grpcPDClient.GetMembers(context.Background(), req)
	c.Assert(err, IsNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(len(resp.GetMembers()), Not(Equals), 0)
}

func (s *testClusterSuite) TestConcurrentHandleRegion(c *C) {
	var err error
	_, s.svr, _, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0"))
	c.Assert(err, IsNil)
	s.svr.cluster.RLock()
	s.svr.cluster.cachedCluster.Lock()
	s.svr.cluster.cachedCluster.kv = core.NewKV(core.NewMemoryKV())
	s.svr.cluster.cachedCluster.Unlock()
	s.svr.cluster.RUnlock()
	var stores []*metapb.Store
	for _, addr := range storeAddrs {
		store := s.newStore(c, 0, addr)
		stores = append(stores, store)
		_, err := putStore(c, s.grpcPDClient, s.svr.clusterID, store)
		c.Assert(err, IsNil)
	}

	var wg sync.WaitGroup
	// register store and bind stream
	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{
			Header: newRequestHeader(s.svr.clusterID),
			Stats: &pdpb.StoreStats{
				StoreId:   store.GetId(),
				Capacity:  1000 * (1 << 20),
				Available: 1000 * (1 << 20),
			},
		}
		_, err := s.svr.StoreHeartbeat(context.TODO(), req)
		c.Assert(err, IsNil)
		stream, err := s.grpcPDClient.RegionHeartbeat(context.Background())
		c.Assert(err, IsNil)
		peer := &metapb.Peer{Id: s.allocID(c), StoreId: store.GetId()}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: newRequestHeader(s.svr.clusterID),
			Region: &metapb.Region{
				Id:    s.allocID(c),
				Peers: []*metapb.Peer{peer},
			},
			Leader: peer,
		}
		err = stream.Send(regionReq)
		c.Assert(err, IsNil)
		// make sure the first store can receive one response
		if i == 0 {
			wg.Add(1)
		}
		go func(isReciver bool) {
			if isReciver {
				_, err := stream.Recv()
				c.Assert(err, IsNil)
				wg.Done()
			}
			for {
				_, err := stream.Recv()
				c.Assert(err, IsNil)
			}
		}(i == 0)
	}
	concurrent := 2000
	for i := 0; i < concurrent; i++ {
		region := &metapb.Region{
			Id:       s.allocID(c),
			StartKey: []byte(fmt.Sprintf("%5d", i)),
			EndKey:   []byte(fmt.Sprintf("%5d", i+1)),
			Peers:    []*metapb.Peer{{Id: s.allocID(c), StoreId: stores[0].GetId()}},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: initEpochConfVer,
				Version: initEpochVersion,
			},
		}
		if i == 0 {
			region.StartKey = []byte("")
		} else if i == concurrent-1 {
			region.EndKey = []byte("")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.svr.cluster.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
			c.Assert(err, IsNil)
		}()
	}
	wg.Wait()
}

var _ = Suite(&testGetStoresSuite{})

type testGetStoresSuite struct {
	cluster *clusterInfo
}

func (s *testGetStoresSuite) SetUpSuite(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	s.cluster = newClusterInfo(mockid.NewIDAllocator(), opt, core.NewKV(core.NewMemoryKV()))

	stores := newTestStores(200)

	for _, store := range stores {
		c.Assert(s.cluster.putStore(store), IsNil)
	}
}

func (s *testGetStoresSuite) BenchmarkGetStores(c *C) {
	for i := 0; i < c.N; i++ {
		// Logic to benchmark
		s.cluster.core.Stores.GetStores()
	}
}

func (s *testClusterSuite) TestSetScheduleOpt(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	defer cleanup()
	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	_, err = s.svr.bootstrapCluster(s.newBootstrapRequest(c, clusterID, storeAddr))
	c.Assert(err, IsNil)

	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)

	scheduleCfg := opt.load()
	replicateCfg := s.svr.GetReplicationConfig()
	pdServerCfg := s.svr.scheduleOpt.loadPDServerConfig()

	//PUT GET DELETE successed
	replicateCfg.MaxReplicas = 5
	scheduleCfg.MaxSnapshotCount = 10
	pdServerCfg.UseRegionStorage = true
	typ, labelKey, labelValue := "testTyp", "testKey", "testValue"
	nsConfig := NamespaceConfig{LeaderScheduleLimit: uint64(200)}

	c.Assert(s.svr.SetScheduleConfig(*scheduleCfg), IsNil)
	c.Assert(s.svr.SetPDServerConfig(*pdServerCfg), IsNil)
	c.Assert(s.svr.SetLabelProperty(typ, labelKey, labelValue), IsNil)
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), IsNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), IsNil)

	c.Assert(s.svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(s.svr.scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(s.svr.scheduleOpt.loadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))

	c.Assert(s.svr.DeleteNamespaceConfig("testNS"), IsNil)
	c.Assert(s.svr.DeleteLabelProperty(typ, labelKey, labelValue), IsNil)

	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(0))
	c.Assert(len(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ]), Equals, 0)

	//PUT GET failed
	oldKV := s.svr.kv
	s.svr.kv = core.NewKV(&testErrorKV{})
	replicateCfg.MaxReplicas = 7
	scheduleCfg.MaxSnapshotCount = 20
	pdServerCfg.UseRegionStorage = false

	c.Assert(s.svr.SetScheduleConfig(*scheduleCfg), NotNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), NotNil)
	c.Assert(s.svr.SetPDServerConfig(*pdServerCfg), NotNil)
	c.Assert(s.svr.SetLabelProperty(typ, labelKey, labelValue), NotNil)
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), NotNil)

	c.Assert(s.svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(s.svr.scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(s.svr.scheduleOpt.loadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(0))
	c.Assert(len(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ]), Equals, 0)

	//DELETE failed
	s.svr.kv = oldKV
	c.Assert(s.svr.SetNamespaceConfig("testNS", nsConfig), IsNil)
	c.Assert(s.svr.SetReplicationConfig(*replicateCfg), IsNil)

	s.svr.kv = core.NewKV(&testErrorKV{})
	c.Assert(s.svr.DeleteLabelProperty(typ, labelKey, labelValue), NotNil)
	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))
	c.Assert(s.svr.DeleteNamespaceConfig("testNS"), NotNil)

	c.Assert(s.svr.GetNamespaceConfig("testNS").LeaderScheduleLimit, Equals, uint64(200))
	c.Assert(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(s.svr.scheduleOpt.loadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
}
