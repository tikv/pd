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

package cluster_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/pkg/mock/mockid"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
	syncer "github.com/pingcap/pd/v4/server/region_syncer"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
	"github.com/pkg/errors"
)

func Test(t *testing.T) {
	TestingT(t)
}

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&clusterTestSuite{})

type clusterTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *clusterTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
	server.ConfigCheckInterval = 1 * time.Second
}

func (s *clusterTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

type testErrorKV struct {
	kv.Base
}

func (kv *testErrorKV) Save(key, value string) error {
	return errors.New("save failed")
}

func (s *clusterTestSuite) TestBootstrap(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	// IsBootstrapped returns false.
	req := newIsBootstrapRequest(clusterID)
	resp, err := grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	bootstrapCluster(c, clusterID, grpcPDClient, storeAddr)

	// IsBootstrapped returns true.
	req = newIsBootstrapRequest(clusterID)
	resp, err = grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	reqBoot := newBootstrapRequest(c, clusterID, storeAddr)
	respBoot, err := grpcPDClient.Bootstrap(context.Background(), reqBoot)
	c.Assert(err, IsNil)
	c.Assert(respBoot.GetHeader().GetError(), NotNil)
	c.Assert(respBoot.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_ALREADY_BOOTSTRAPPED)
}

func (s *clusterTestSuite) TestGetPutConfig(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	// Get region.
	region := getRegion(c, clusterID, grpcPDClient, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := getRegionByID(c, clusterID, grpcPDClient, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	// Get store.
	storeID := peer.GetStoreId()
	store := getStore(c, clusterID, grpcPDClient, storeID)

	// Update store.
	store.Address = "127.0.0.1:1"
	testPutStore(c, clusterID, rc, grpcPDClient, store)

	// Remove store.
	testRemoveStore(c, clusterID, rc, grpcPDClient, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := grpcPDClient.PutClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	meta := getClusterConfig(c, clusterID, grpcPDClient)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func (s *clusterTestSuite) TestReloadConfig(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config) { conf.PDServerCfg.UseRegionStorage = true })
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)

	// wait for creating config client
	time.Sleep(2 * time.Second)
	cmd := pdctl.InitCommand()
	pdAddr := leaderServer.GetAddr()
	args := []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// transfer leader
	tc.ResignLeader()
	tc.WaitLeader()
	leaderServer = tc.GetServer(tc.GetLeader())
	c.Assert(leaderServer.GetServer().GetScheduleOption().GetReplication().IsPlacementRulesEnabled(), IsFalse)
	rc = leaderServer.GetServer().GetRaftCluster()
	r := &metapb.Region{
		Id: 3,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte{byte(1)},
		EndKey:   []byte{byte(2)},
		Peers:    []*metapb.Peer{{Id: 4, StoreId: uint64(1), IsLearner: true}},
	}
	region := core.NewRegionInfo(r, r.Peers[0])
	c.Assert(rc.HandleRegionHeartbeat(region), IsNil)

	// wait for checking region
	time.Sleep(300 * time.Millisecond)
	c.Assert(leaderServer.GetServer().GetScheduleOption().GetReplication().IsPlacementRulesEnabled(), IsFalse)
	c.Assert(rc.GetOperatorController().GetOperator(3), IsNil)

	// wait for configuration valid
	time.Sleep(1 * time.Second)
	c.Assert(leaderServer.GetServer().GetScheduleOption().GetReplication().IsPlacementRulesEnabled(), IsTrue)
	c.Assert(rc.GetOperatorController().GetOperator(3), IsNil)
}

func testPutStore(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	// Update store.
	_, err := putStore(c, grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	updatedStore := getStore(c, clusterID, grpcPDClient, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	_, err = putStore(c, grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)

	rc.AllocID()
	id, err := rc.AllocID()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is up will fail.
	_, err = putStore(c, grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up))
	c.Assert(err, NotNil)

	id, err = rc.AllocID()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is offline will fail.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Offline)
	_, err = putStore(c, grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up))
	c.Assert(err, NotNil)

	id, err = rc.AllocID()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is tombstone is OK.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Tombstone)
	rc.GetStore(store.GetId())
	_, err = putStore(c, grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up))
	c.Assert(err, IsNil)

	id, err = rc.AllocID()
	c.Assert(err, IsNil)
	// Put a new store.
	_, err = putStore(c, grpcPDClient, clusterID, newMetaStore(id, "127.0.0.1:12345", "2.1.0", metapb.StoreState_Up))
	c.Assert(err, IsNil)

	// Put an existed store with duplicated address with other old stores.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Up)
	_, err = putStore(c, grpcPDClient, clusterID, newMetaStore(store.GetId(), "127.0.0.1:12345", "2.1.0", metapb.StoreState_Up))
	c.Assert(err, NotNil)
}

func resetStoreState(c *C, rc *cluster.RaftCluster, storeID uint64, state metapb.StoreState) {
	store := rc.GetStore(storeID)
	c.Assert(store, NotNil)
	newStore := store.Clone(core.SetStoreState(state))
	rc.GetCacheCluster().PutStore(newStore)
}

func testStateAndLimit(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store, beforeState metapb.StoreState, run func(*cluster.RaftCluster) error, expectStates ...metapb.StoreState) {
	// prepare
	storeID := store.GetId()
	oc := rc.GetOperatorController()
	oc.SetAllStoresLimit(1.0, schedule.StoreLimitManual)
	resetStoreState(c, rc, store.GetId(), beforeState)
	_, isOKBefore := oc.GetAllStoresLimit()[storeID]
	// run
	err := run(rc)
	// judge
	_, isOKAfter := oc.GetAllStoresLimit()[storeID]
	if len(expectStates) != 0 {
		c.Assert(err, IsNil)
		expectState := expectStates[0]
		c.Assert(getStore(c, clusterID, grpcPDClient, storeID).GetState(), Equals, expectState)
		if expectState == metapb.StoreState_Offline {
			c.Assert(isOKAfter, IsTrue)
		} else if expectState == metapb.StoreState_Tombstone {
			c.Assert(isOKAfter, IsFalse)
		}
	} else {
		c.Assert(err, NotNil)
		c.Assert(isOKAfter, Equals, isOKBefore)
	}
}

func testRemoveStore(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	{
		beforeState := metapb.StoreState_Up // When store is up
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId())
		}, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ force should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.BuryStore(store.GetId(), true)
		}, metapb.StoreState_Tombstone)
		// Case 3: BuryStore w/o force should fail.
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.BuryStore(store.GetId(), false)
		})
	}
	{
		beforeState := metapb.StoreState_Offline // When store is offline
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId())
		}, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ or w/o force should be OK.
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.BuryStore(store.GetId(), false)
		}, metapb.StoreState_Tombstone)
	}
	{
		beforeState := metapb.StoreState_Tombstone // When store is tombstone
		// Case 1: RemoveStore should should fail;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId())
		})
		// Case 2: BuryStore w/ or w/o force should be OK.
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.BuryStore(store.GetId(), false)
		}, metapb.StoreState_Tombstone)
	}
	{
		// Put after removed should return tombstone error.
		resp, err := putStore(c, grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := grpcPDClient.StoreHeartbeat(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
}

// Make sure PD will not panic if it start and stop again and again.
func (s *clusterTestSuite) TestRaftClusterRestart(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")

	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.Stop()

	err = rc.Start(leaderServer.GetServer())
	c.Assert(err, IsNil)

	rc = leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.Stop()
}

// Make sure PD will not deadlock if it start and stop again and again.
func (s *clusterTestSuite) TestRaftClusterMultipleRestart(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	// add an offline store
	storeID, err := leaderServer.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Offline)
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	err = rc.PutStore(store, false)
	c.Assert(err, IsNil)
	c.Assert(tc, NotNil)

	// let the job run at small interval
	c.Assert(failpoint.Enable("github.com/pingcap/pd/v4/server/highFrequencyClusterJobs", `return(true)`), IsNil)
	for i := 0; i < 100; i++ {
		err = rc.Start(leaderServer.GetServer())
		c.Assert(err, IsNil)
		time.Sleep(time.Millisecond)
		rc = leaderServer.GetRaftCluster()
		c.Assert(rc, NotNil)
		rc.Stop()
	}
}

func newMetaStore(storeID uint64, addr, version string, state metapb.StoreState) *metapb.Store {
	return &metapb.Store{Id: storeID, Address: addr, Version: version, State: state}
}

func (s *clusterTestSuite) TestGetPDMembers(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.GetMembersRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	resp, err := grpcPDClient.GetMembers(context.Background(), req)
	c.Assert(err, IsNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(len(resp.GetMembers()), Not(Equals), 0)
}

func (s *clusterTestSuite) TestStoreVersionChange(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	svr := leaderServer.GetServer()
	svr.SetClusterVersion("2.0.0")
	storeID, err := leaderServer.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Up)
	var wg sync.WaitGroup
	c.Assert(failpoint.Enable("github.com/pingcap/pd/v4/server/versionChangeConcurrency", `return(true)`), IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = putStore(c, grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
	}()
	time.Sleep(100 * time.Millisecond)
	svr.SetClusterVersion("1.0.0")
	wg.Wait()
	v, err := semver.NewVersion("1.0.0")
	c.Assert(err, IsNil)
	c.Assert(svr.GetClusterVersion(), Equals, *v)
	c.Assert(failpoint.Disable("github.com/pingcap/pd/v4/server/versionChangeConcurrency"), IsNil)
}

func (s *clusterTestSuite) TestConcurrentHandleRegion(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.SetStorage(core.NewStorage(kv.NewMemoryKV()))
	var stores []*metapb.Store
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, err := id.Alloc()
		c.Assert(err, IsNil)
		store := newMetaStore(storeID, addr, "2.1.0", metapb.StoreState_Up)
		stores = append(stores, store)
		_, err = putStore(c, grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	// register store and bind stream
	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats: &pdpb.StoreStats{
				StoreId:   store.GetId(),
				Capacity:  1000 * (1 << 20),
				Available: 1000 * (1 << 20),
			},
		}
		_, err := leaderServer.GetServer().StoreHeartbeat(context.TODO(), req)
		c.Assert(err, IsNil)
		stream, err := grpcPDClient.RegionHeartbeat(ctx)
		c.Assert(err, IsNil)
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		regionID, err := id.Alloc()
		c.Assert(err, IsNil)
		peer := &metapb.Peer{Id: peerID, StoreId: store.GetId()}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Region: &metapb.Region{
				Id:    regionID,
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
				select {
				case <-ctx.Done():
					return
				default:
					stream.Recv()
				}
			}
		}(i == 0)
	}

	concurrent := 1000
	for i := 0; i < concurrent; i++ {
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		regionID, err := id.Alloc()
		c.Assert(err, IsNil)
		region := &metapb.Region{
			Id:       regionID,
			StartKey: []byte(fmt.Sprintf("%5d", i)),
			EndKey:   []byte(fmt.Sprintf("%5d", i+1)),
			Peers:    []*metapb.Peer{{Id: peerID, StoreId: stores[0].GetId()}},
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
			err := rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
			c.Assert(err, IsNil)
		}()
	}
	wg.Wait()
}

func (s *clusterTestSuite) TestSetScheduleOpt(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1, func(cfg *config.Config) { cfg.EnableDynamicConfig = true })
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")

	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	cfg.Schedule.StoreBalanceRate = 60
	err = cfg.Adjust(nil)
	c.Assert(err, IsNil)
	opt := config.NewScheduleOption(cfg)
	c.Assert(err, IsNil)

	svr := leaderServer.GetServer()
	scheduleCfg := opt.Load()
	replicateCfg := svr.GetReplicationConfig()
	scheduleOpt := svr.GetScheduleOption()
	pdServerCfg := scheduleOpt.LoadPDServerConfig()

	// PUT GET DELETE succeed
	replicateCfg.MaxReplicas = 5
	scheduleCfg.MaxSnapshotCount = 10
	pdServerCfg.UseRegionStorage = true
	typ, labelKey, labelValue := "testTyp", "testKey", "testValue"

	c.Assert(svr.SetScheduleConfig(*scheduleCfg), IsNil)
	c.Assert(svr.SetPDServerConfig(*pdServerCfg), IsNil)
	c.Assert(svr.SetLabelProperty(typ, labelKey, labelValue), IsNil)
	c.Assert(svr.SetReplicationConfig(*replicateCfg), IsNil)

	c.Assert(svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(scheduleOpt.LoadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(scheduleOpt.LoadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(scheduleOpt.LoadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")

	c.Assert(svr.DeleteLabelProperty(typ, labelKey, labelValue), IsNil)

	c.Assert(len(scheduleOpt.LoadLabelPropertyConfig()[typ]), Equals, 0)

	// PUT GET failed
	oldStorage := svr.GetStorage()
	svr.SetStorage(core.NewStorage(&testErrorKV{}))
	replicateCfg.MaxReplicas = 7
	scheduleCfg.MaxSnapshotCount = 20
	pdServerCfg.UseRegionStorage = false

	c.Assert(svr.SetScheduleConfig(*scheduleCfg), NotNil)
	c.Assert(svr.SetReplicationConfig(*replicateCfg), NotNil)
	c.Assert(svr.SetPDServerConfig(*pdServerCfg), NotNil)
	c.Assert(svr.SetLabelProperty(typ, labelKey, labelValue), NotNil)

	c.Assert(svr.GetReplicationConfig().MaxReplicas, Equals, uint64(5))
	c.Assert(scheduleOpt.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(scheduleOpt.LoadPDServerConfig().UseRegionStorage, Equals, true)
	c.Assert(len(scheduleOpt.LoadLabelPropertyConfig()[typ]), Equals, 0)

	// DELETE failed
	svr.SetStorage(oldStorage)
	c.Assert(svr.SetReplicationConfig(*replicateCfg), IsNil)

	svr.SetStorage(core.NewStorage(&testErrorKV{}))
	c.Assert(svr.DeleteLabelProperty(typ, labelKey, labelValue), NotNil)

	c.Assert(scheduleOpt.LoadLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(scheduleOpt.LoadLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
	svr.SetStorage(oldStorage)
}

func (s *clusterTestSuite) TestLoadClusterInfo(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	svr := leaderServer.GetServer()
	rc := cluster.NewRaftCluster(s.ctx, svr.GetClusterRootPath(), svr.ClusterID(), syncer.NewRegionSyncer(svr), svr.GetClient())

	// Cluster is not bootstrapped.
	rc.InitCluster(svr.GetAllocator(), svr.GetScheduleOption(), svr.GetStorage(), svr.GetBasicCluster(), func() {})
	raftCluster, err := rc.LoadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(raftCluster, IsNil)

	storage := rc.GetStorage()
	basicCluster := rc.GetCacheCluster()
	opt := rc.GetOpt()
	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(storage.SaveMeta(meta), IsNil)
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(storage.SaveStore(store), IsNil)
	}

	regions := make([]*metapb.Region, 0, n)
	for i := uint64(0); i < uint64(n); i++ {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(storage.SaveRegion(region), IsNil)
	}
	c.Assert(storage.Flush(), IsNil)

	raftCluster = &cluster.RaftCluster{}
	raftCluster.InitCluster(mockid.NewIDAllocator(), opt, storage, basicCluster, func() {})
	raftCluster, err = raftCluster.LoadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(raftCluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(raftCluster.GetConfig(), DeepEquals, meta)
	c.Assert(raftCluster.GetStoreCount(), Equals, n)
	for _, store := range raftCluster.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(raftCluster.GetRegionCount(), Equals, n)
	for _, region := range raftCluster.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}

	m := 20
	regions = make([]*metapb.Region, 0, n)
	for i := uint64(0); i < uint64(m); i++ {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(storage.SaveRegion(region), IsNil)
	}
	raftCluster.GetStorage().LoadRegionsOnce(raftCluster.GetCacheCluster().PutRegion)
	c.Assert(raftCluster.GetRegionCount(), Equals, n)
}

func (s *clusterTestSuite) TestTiFlashWithPlacementRules(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")

	tiflashStore := &metapb.Store{
		Id:      11,
		Address: "127.0.0.1:1",
		Labels:  []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
		Version: "v4.1.0",
	}

	// cannot put TiFlash node without placement rules
	_, err = putStore(c, grpcPDClient, clusterID, tiflashStore)
	c.Assert(err, NotNil)
	rep := leaderServer.GetConfig().Replication
	rep.EnablePlacementRules = true
	err = leaderServer.GetServer().SetReplicationConfig(rep)
	c.Assert(err, IsNil)
	_, err = putStore(c, grpcPDClient, clusterID, tiflashStore)
	c.Assert(err, IsNil)

	// cannot disable placement rules with TiFlash nodes
	rep.EnablePlacementRules = false
	err = leaderServer.GetServer().SetReplicationConfig(rep)
	c.Assert(err, NotNil)
	err = leaderServer.GetServer().GetRaftCluster().BuryStore(11, true)
	c.Assert(err, IsNil)
	err = leaderServer.GetServer().SetReplicationConfig(rep)
	c.Assert(err, IsNil)
}

func newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	req := &pdpb.IsBootstrappedRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	return req
}

func newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.BootstrapRequest {
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: storeAddr},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, IsLearner: false}}},
	}

	return req
}

// helper function to check and bootstrap.
func bootstrapCluster(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, storeAddr string) {
	req := newBootstrapRequest(c, clusterID, storeAddr)
	_, err := grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func putStore(c *C, grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	req := &pdpb.PutStoreRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
	}
	resp, err := grpcPDClient.PutStore(context.Background(), req)
	return resp, err
}

func getStore(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, storeID uint64) *metapb.Store {
	req := &pdpb.GetStoreRequest{
		Header:  testutil.NewRequestHeader(clusterID),
		StoreId: storeID,
	}
	resp, err := grpcPDClient.GetStore(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetStore().GetId(), Equals, storeID)

	return resp.GetStore()
}

func getRegion(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, regionKey []byte) *metapb.Region {
	req := &pdpb.GetRegionRequest{
		Header:    testutil.NewRequestHeader(clusterID),
		RegionKey: regionKey,
	}

	resp, err := grpcPDClient.GetRegion(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func getRegionByID(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, regionID uint64) *metapb.Region {
	req := &pdpb.GetRegionByIDRequest{
		Header:   testutil.NewRequestHeader(clusterID),
		RegionId: regionID,
	}

	resp, err := grpcPDClient.GetRegionByID(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func getClusterConfig(c *C, clusterID uint64, grpcPDClient pdpb.PDClient) *metapb.Cluster {
	req := &pdpb.GetClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	resp, err := grpcPDClient.GetClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCluster(), NotNil)

	return resp.GetCluster()
}
