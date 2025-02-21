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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/mock/mockserver"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/server/api"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1

	testMetaStoreAddr = "127.0.0.1:12345"
	testStoreAddr     = "127.0.0.1:0"
)

func TestBootstrap(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	// IsBootstrapped returns false.
	req := newIsBootstrapRequest(clusterID)
	resp, err := grpcPDClient.IsBootstrapped(context.Background(), req)
	re.NoError(err)
	re.NotNil(resp)
	re.False(resp.GetBootstrapped())

	// Bootstrap the cluster.
	bootstrapCluster(re, clusterID, grpcPDClient)

	// IsBootstrapped returns true.
	req = newIsBootstrapRequest(clusterID)
	resp, err = grpcPDClient.IsBootstrapped(context.Background(), req)
	re.NoError(err)
	re.True(resp.GetBootstrapped())

	// check bootstrapped error.
	reqBoot := newBootstrapRequest(clusterID)
	respBoot, err := grpcPDClient.Bootstrap(context.Background(), reqBoot)
	re.NoError(err)
	re.NotNil(respBoot.GetHeader().GetError())
	re.Equal(pdpb.ErrorType_ALREADY_BOOTSTRAPPED, respBoot.GetHeader().GetError().GetType())
}

func TestDamagedRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()

	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1},
			{Id: 102, StoreId: 2},
			{Id: 103, StoreId: 3},
		},
	}

	// To put region.
	regionInfo := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)

	stores := []*pdpb.PutStoreRequest{
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      1,
				Address: "mock-1",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      2,
				Address: "mock-4",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      3,
				Address: "mock-6",
				Version: "2.0.1",
			},
		},
	}

	// To put stores.
	svr := &server.GrpcServer{Server: leaderServer.GetServer()}
	for _, store := range stores {
		resp, err := svr.PutStore(context.Background(), store)
		re.NoError(err)
		re.Nil(resp.GetHeader().GetError())
	}

	// To validate remove peer op be added.
	req1 := &pdpb.StoreHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Stats:  &pdpb.StoreStats{StoreId: 2, DamagedRegionsId: []uint64{10}},
	}
	re.Equal(uint64(0), rc.GetOperatorController().OperatorCount(operator.OpAdmin))
	_, err1 := grpcPDClient.StoreHeartbeat(context.Background(), req1)
	re.NoError(err1)
	re.Equal(uint64(1), rc.GetOperatorController().OperatorCount(operator.OpAdmin))
}

func TestRegionStatistics(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 3)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	leaderName := tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()

	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1},
			{Id: 102, StoreId: 2},
			{Id: 103, StoreId: 3},
			{Id: 104, StoreId: 4, Role: metapb.PeerRole_Learner},
		},
	}

	// To put region.
	regionInfo := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(0))
	err = tc.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)
	regions := rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Len(regions, 1)

	// wait for sync region
	time.Sleep(1000 * time.Millisecond)

	leaderServer.ResignLeader()
	re.NotEqual(tc.WaitLeader(), leaderName)
	leaderServer = tc.GetLeaderServer()
	leaderName = leaderServer.GetServer().Name()
	rc = leaderServer.GetRaftCluster()
	r := rc.GetRegion(region.Id)
	re.NotNil(r)
	re.True(r.LoadedFromSync())
	regions = rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Empty(regions)
	err = tc.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)
	regions = rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Len(regions, 1)

	leaderServer.ResignLeader()
	re.NotEqual(leaderName, tc.WaitLeader())
	leaderServer = tc.GetLeaderServer()
	leaderName = leaderServer.GetServer().Name()
	rc = leaderServer.GetRaftCluster()
	re.NotNil(r)
	re.True(r.LoadedFromStorage() || r.LoadedFromSync())
	regions = rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Empty(regions)
	regionInfo = regionInfo.Clone(core.SetSource(core.Heartbeat), core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)
	rc = leaderServer.GetRaftCluster()
	r = rc.GetRegion(region.Id)
	re.NotNil(r)
	re.False(r.LoadedFromStorage() && r.LoadedFromSync())

	leaderServer.ResignLeader()
	re.NotEqual(leaderName, tc.WaitLeader())
	leaderServer = tc.GetLeaderServer()
	leaderName = leaderServer.GetServer().Name()
	leaderServer.ResignLeader()
	re.NotEqual(leaderName, tc.WaitLeader())
	rc = tc.GetLeaderServer().GetRaftCluster()
	r = rc.GetRegion(region.Id)
	re.NotNil(r)
	re.False(r.LoadedFromStorage() && r.LoadedFromSync())
	regions = rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Empty(regions)

	regionInfo = regionInfo.Clone(core.SetSource(core.Heartbeat), core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(regionInfo)
	re.NoError(err)
	regions = rc.GetRegionStatsByType(statistics.LearnerPeer)
	re.Len(regions, 1)
}

func TestStaleRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)

	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1},
			{Id: 102, StoreId: 2},
			{Id: 103, StoreId: 3},
		},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 10,
			Version: 10,
		},
	}

	// To put region.
	regionInfoA := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(regionInfoA)
	re.NoError(err)
	regionInfoA = regionInfoA.Clone(core.WithIncConfVer(), core.WithIncVersion())
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/decEpoch", `return(true)`))
	tc.HandleRegionHeartbeat(regionInfoA)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/decEpoch"))
	regionInfoA = regionInfoA.Clone(core.WithIncConfVer(), core.WithIncVersion())
	err = tc.HandleRegionHeartbeat(regionInfoA)
	re.NoError(err)
}

func TestGetPutConfig(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	// Get region.
	region := getRegion(re, clusterID, grpcPDClient, []byte("abc"))
	re.Len(region.GetPeers(), 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := getRegionByID(re, clusterID, grpcPDClient, region.GetId())
	re.Equal(regionByID, region)

	r := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(r)
	re.NoError(err)

	// Get store.
	storeID := peer.GetStoreId()
	store := getStore(re, clusterID, grpcPDClient, storeID)

	// Update store.
	store.Address = "127.0.0.1:1"
	testPutStore(re, clusterID, rc, grpcPDClient, store)

	// Remove store.
	testRemoveStore(re, clusterID, rc, grpcPDClient, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := grpcPDClient.PutClusterConfig(context.Background(), req)
	re.NoError(err)
	re.NotNil(resp)
	meta := getClusterConfig(re, clusterID, grpcPDClient)
	re.Equal(uint32(5), meta.GetMaxPeerCount())
}

func testPutStore(re *require.Assertions, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	// Update store.
	resp, err := putStore(grpcPDClient, clusterID, store)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())

	updatedStore := getStore(re, clusterID, grpcPDClient, store.GetId())
	re.Equal(store, updatedStore)

	// Update store again.
	resp, err = putStore(grpcPDClient, clusterID, store)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())

	rc.AllocID(1)
	id, _, err := rc.AllocID(1)
	re.NoError(err)
	// Put new store with a duplicated address when old store is up will fail.
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_UNKNOWN, resp.GetHeader().GetError().GetType())

	id, _, err = rc.AllocID(1)
	re.NoError(err)
	// Put new store with a duplicated address when old store is offline will fail.
	resetStoreState(re, rc, store.GetId(), metapb.StoreState_Offline)
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_UNKNOWN, resp.GetHeader().GetError().GetType())

	id, _, err = rc.AllocID(1)
	re.NoError(err)
	// Put new store with a duplicated address when old store is tombstone is OK.
	resetStoreState(re, rc, store.GetId(), metapb.StoreState_Tombstone)
	rc.GetStore(store.GetId())
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())

	id, _, err = rc.AllocID(1)
	re.NoError(err)
	deployPath := getTestDeployPath(id)
	// Put a new store.
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(id, testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, deployPath))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	s := rc.GetStore(id).GetMeta()
	re.Equal(deployPath, s.DeployPath)

	deployPath = fmt.Sprintf("move/test/store%d", id)
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(id, testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, deployPath))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	s = rc.GetStore(id).GetMeta()
	re.Equal(deployPath, s.DeployPath)

	// Put an existed store with duplicated address with other old stores.
	resetStoreState(re, rc, store.GetId(), metapb.StoreState_Up)
	resp, err = putStore(grpcPDClient, clusterID, newMetaStore(store.GetId(), testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, getTestDeployPath(store.GetId())))
	re.NoError(err)
	re.Equal(pdpb.ErrorType_UNKNOWN, resp.GetHeader().GetError().GetType())
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func resetStoreState(re *require.Assertions, rc *cluster.RaftCluster, storeID uint64, state metapb.StoreState) {
	store := rc.GetStore(storeID)
	re.NotNil(store)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	if state == metapb.StoreState_Up {
		newStore = newStore.Clone(core.SetStoreState(metapb.StoreState_Up))
	} else if state == metapb.StoreState_Tombstone {
		newStore = newStore.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	}

	rc.GetBasicCluster().PutStore(newStore)
	if state == metapb.StoreState_Offline {
		rc.SetStoreLimit(storeID, storelimit.RemovePeer, storelimit.Unlimited)
	} else if state == metapb.StoreState_Tombstone {
		rc.RemoveStoreLimit(storeID)
	}
}

func testStateAndLimit(re *require.Assertions, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store, beforeState metapb.StoreState, run func(*cluster.RaftCluster) error, expectStates ...metapb.StoreState) {
	// prepare
	storeID := store.GetId()
	oc := rc.GetOperatorController()
	rc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	rc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: storeID, PeerID: 3})
	oc.AddOperator(op)
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: storeID})
	oc.AddOperator(op)

	resetStoreState(re, rc, store.GetId(), beforeState)
	_, isOKBefore := rc.GetAllStoresLimit()[storeID]
	// run
	err := run(rc)
	// judge
	_, isOKAfter := rc.GetAllStoresLimit()[storeID]
	if len(expectStates) != 0 {
		re.NoError(err)
		expectState := expectStates[0]
		re.Equal(expectState, getStore(re, clusterID, grpcPDClient, storeID).GetState())
		if expectState == metapb.StoreState_Offline {
			re.True(isOKAfter)
		} else if expectState == metapb.StoreState_Tombstone {
			re.False(isOKAfter)
		}
	} else {
		re.Error(err)
		re.Equal(isOKAfter, isOKBefore)
	}
}

func testRemoveStore(re *require.Assertions, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	rc.GetOpts().SetMaxReplicas(2)
	defer rc.GetOpts().SetMaxReplicas(3)
	{
		beforeState := metapb.StoreState_Up // When store is up
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		}, metapb.StoreState_Offline)
		// Case 2: RemoveStore with physically destroyed should be OK;
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		}, metapb.StoreState_Offline)
	}
	{
		beforeState := metapb.StoreState_Offline // When store is offline
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		}, metapb.StoreState_Offline)
		// Case 2: remove store with physically destroyed should be success
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		}, metapb.StoreState_Offline)
	}
	{
		beforeState := metapb.StoreState_Tombstone // When store is tombstone
		// Case 1: RemoveStore should should fail;
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		})
		// Case 2: RemoveStore with physically destroyed should fail;
		testStateAndLimit(re, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		})
	}
	{
		// Put after removed should return tombstone error.
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_STORE_TOMBSTONE, resp.GetHeader().GetError().GetType())
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := grpcPDClient.StoreHeartbeat(context.Background(), req)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_STORE_TOMBSTONE, resp.GetHeader().GetError().GetType())
	}
}

// Make sure PD will not panic if it start and stop again and again.
func TestRaftClusterRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)

	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	rc.Stop()

	err = rc.Start(leaderServer.GetServer(), false)
	re.NoError(err)

	rc = leaderServer.GetRaftCluster()
	re.NotNil(rc)
	rc.Stop()
}

// Make sure PD will not deadlock if it start and stop again and again.
func TestRaftClusterMultipleRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.LeaderLease = 300
	})
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	// add an offline store
	storeID, _, err := leaderServer.GetAllocator().Alloc(1)
	re.NoError(err)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Offline, getTestDeployPath(storeID))
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	err = rc.PutMetaStore(store)
	re.NoError(err)
	re.NotNil(tc)
	rc.Stop()

	// let the job run at small interval
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	for range 100 {
		// See https://github.com/tikv/pd/issues/8543
		rc.Wait()
		err = rc.Start(leaderServer.GetServer(), false)
		re.NoError(err)
		time.Sleep(time.Millisecond)
		rc.Stop()
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

// TestRaftClusterStartTSOJob is used to test whether tso job service is normally closed
// when raft cluster is stopped ahead of time.
// Ref: https://github.com/tikv/pd/issues/8836
func TestRaftClusterStartTSOJob(t *testing.T) {
	re := require.New(t)
	name := "pd1"
	// case 1: normal start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.LeaderLease = 300
	})
	defer tc.Destroy()
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	re.NotEmpty(tc.WaitLeader())
	leaderServer := tc.GetLeaderServer()
	re.NotNil(leaderServer)
	leaderServer.BootstrapCluster()
	testutil.Eventually(re, func() bool {
		allocator := tc.GetServer(name).GetServer().GetGlobalTSOAllocator()
		return allocator.IsInitialize()
	})
	tc.Destroy()
	cancel()
	// case 2: return ahead of time but no error when start raft cluster
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/raftClusterReturn", `return(false)`))
	ctx, cancel = context.WithCancel(context.Background())
	tc, err = tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.LeaderLease = 300
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	testutil.Eventually(re, func() bool {
		allocator := tc.GetServer(name).GetServer().GetGlobalTSOAllocator()
		return allocator.IsInitialize()
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/raftClusterReturn"))
	tc.Destroy()
	cancel()
	// case 3: meet error when start raft cluster
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/raftClusterReturn", `return(true)`))
	ctx, cancel = context.WithCancel(context.Background())
	tc, err = tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.LeaderLease = 300
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	testutil.Eventually(re, func() bool {
		allocator := tc.GetServer(name).GetServer().GetGlobalTSOAllocator()
		return !allocator.IsInitialize()
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/raftClusterReturn"))
	tc.Destroy()
	cancel()
	// case 4: multiple bootstrap in 3 pd cluster
	ctx, cancel = context.WithCancel(context.Background())
	tc, err = tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.LeaderLease = 300
	})
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	re.NotEmpty(tc.WaitLeader())
	leaderServer = tc.GetLeaderServer()
	re.NotNil(leaderServer)
	name = leaderServer.GetLeader().GetName()
	wg := sync.WaitGroup{}
	for range 3 {
		wg.Add(1)
		go func() {
			leaderServer.BootstrapCluster()
			wg.Done()
		}()
	}
	wg.Wait()
	testutil.Eventually(re, func() bool {
		allocator := leaderServer.GetServer().GetGlobalTSOAllocator()
		return allocator.IsInitialize()
	})
	re.NoError(tc.ResignLeader())
	re.NotEmpty(tc.WaitLeader())
	testutil.Eventually(re, func() bool {
		allocator := tc.GetServer(name).GetServer().GetGlobalTSOAllocator()
		return !allocator.IsInitialize()
	})
	tc.Destroy()
	cancel()
}

func newMetaStore(storeID uint64, addr, version string, state metapb.StoreState, deployPath string) *metapb.Store {
	return &metapb.Store{Id: storeID, Address: addr, Version: version, State: state, DeployPath: deployPath}
}

func TestGetPDMembers(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.GetMembersRequest{Header: testutil.NewRequestHeader(clusterID)}
	resp, err := grpcPDClient.GetMembers(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	// A more strict test can be found at api/member_test.go
	re.NotEmpty(resp.GetMembers())
}

func TestNotLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 2)
	defer tc.Destroy()
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	followerServer := tc.GetServer(tc.GetFollower())
	grpcPDClient := testutil.MustNewGrpcClient(re, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(clusterID)}
	resp, err := grpcPDClient.AllocID(context.Background(), req)
	re.Nil(resp)
	grpcStatus, ok := status.FromError(err)
	re.True(ok)
	re.Equal(codes.Unavailable, grpcStatus.Code())
	re.ErrorContains(errs.ErrNotLeader, grpcStatus.Message())
}

func TestStoreVersionChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	svr := leaderServer.GetServer()
	svr.SetClusterVersion("2.0.0")
	storeID, _, err := leaderServer.GetAllocator().Alloc(1)
	re.NoError(err)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Up, getTestDeployPath(storeID))
	var wg sync.WaitGroup
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/versionChangeConcurrency", `return(true)`))
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	}()
	time.Sleep(100 * time.Millisecond)
	svr.SetClusterVersion("1.0.0")
	wg.Wait()
	v, err := semver.NewVersion("1.0.0")
	re.NoError(err)
	re.Equal(*v, svr.GetClusterVersion())
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/versionChangeConcurrency"))
}

func TestConcurrentHandleRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dashboard.SetCheckInterval(30 * time.Minute)
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	stores := make([]*metapb.Store, 0, len(storeAddrs))
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, _, err := id.Alloc(1)
		re.NoError(err)
		store := newMetaStore(storeID, addr, "2.1.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		stores = append(stores, store)
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	}

	var wg sync.WaitGroup
	// register store and bind stream
	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats: &pdpb.StoreStats{
				StoreId:   store.GetId(),
				Capacity:  1000 * units.MiB,
				Available: 1000 * units.MiB,
			},
		}
		grpcServer := &server.GrpcServer{Server: leaderServer.GetServer()}
		resp, err := grpcServer.StoreHeartbeat(context.TODO(), req)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
		stream, err := grpcPDClient.RegionHeartbeat(ctx)
		re.NoError(err)
		peerID, _, err := id.Alloc(1)
		re.NoError(err)
		peer := &metapb.Peer{Id: peerID, StoreId: store.GetId()}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Region: &metapb.Region{
				// mock error msg to trigger stream.Recv()
				Id:    0,
				Peers: []*metapb.Peer{peer},
			},
			Leader: peer,
		}
		err = stream.Send(regionReq)
		re.NoError(err)
		// make sure the first store can receive one response(error msg)
		if i == 0 {
			wg.Add(1)
		}
		go func(isReceiver bool) {
			if isReceiver {
				_, err := stream.Recv()
				re.NoError(err)
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
	for i := range concurrent {
		peerID, _, err := id.Alloc(1)
		re.NoError(err)
		regionID, _, err := id.Alloc(1)
		re.NoError(err)
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
			re.NoError(err)
		}()
	}
	wg.Wait()
}

func TestSetScheduleOpt(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// TODO: enable placementRules
	tc, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) { cfg.Replication.EnablePlacementRules = false })
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)

	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	err = cfg.Adjust(nil, false)
	re.NoError(err)
	opt := config.NewPersistOptions(cfg)
	re.NoError(err)

	svr := leaderServer.GetServer()
	scheduleCfg := opt.GetScheduleConfig()
	replicationCfg := svr.GetReplicationConfig()
	persistOptions := svr.GetPersistOptions()
	pdServerCfg := persistOptions.GetPDServerConfig()

	// PUT GET DELETE succeed
	replicationCfg.MaxReplicas = 5
	scheduleCfg.MaxSnapshotCount = 10
	pdServerCfg.UseRegionStorage = true
	typ, labelKey, labelValue := "testTyp", "testKey", "testValue"
	re.NoError(svr.SetScheduleConfig(*scheduleCfg))
	re.NoError(svr.SetPDServerConfig(*pdServerCfg))
	re.NoError(svr.SetLabelProperty(typ, labelKey, labelValue))
	re.NoError(svr.SetReplicationConfig(*replicationCfg))
	re.Equal(5, persistOptions.GetMaxReplicas())
	re.Equal(uint64(10), persistOptions.GetMaxSnapshotCount())
	re.True(persistOptions.IsUseRegionStorage())
	re.Equal("testKey", persistOptions.GetLabelPropertyConfig()[typ][0].Key)
	re.Equal("testValue", persistOptions.GetLabelPropertyConfig()[typ][0].Value)
	re.NoError(svr.DeleteLabelProperty(typ, labelKey, labelValue))
	re.Empty(persistOptions.GetLabelPropertyConfig()[typ])

	// PUT GET failed
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/kv/etcdSaveFailed", `return(true)`))
	replicationCfg.MaxReplicas = 7
	scheduleCfg.MaxSnapshotCount = 20
	pdServerCfg.UseRegionStorage = false
	re.Error(svr.SetScheduleConfig(*scheduleCfg))
	re.Error(svr.SetReplicationConfig(*replicationCfg))
	re.Error(svr.SetPDServerConfig(*pdServerCfg))
	re.Error(svr.SetLabelProperty(typ, labelKey, labelValue))
	re.Equal(5, persistOptions.GetMaxReplicas())
	re.Equal(uint64(10), persistOptions.GetMaxSnapshotCount())
	re.True(persistOptions.GetPDServerConfig().UseRegionStorage)
	re.Empty(persistOptions.GetLabelPropertyConfig()[typ])

	// DELETE failed
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/kv/etcdSaveFailed"))
	re.NoError(svr.SetReplicationConfig(*replicationCfg))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/kv/etcdSaveFailed", `return(true)`))
	re.Error(svr.DeleteLabelProperty(typ, labelKey, labelValue))
	re.Equal("testKey", persistOptions.GetLabelPropertyConfig()[typ][0].Key)
	re.Equal("testValue", persistOptions.GetLabelPropertyConfig()[typ][0].Value)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/kv/etcdSaveFailed"))
}

func TestLoadClusterInfo(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)

	err = tc.RunInitialServers()
	re.NoError(err)

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	svr := leaderServer.GetServer()
	rc := cluster.NewRaftCluster(ctx, svr.GetMember(), svr.GetBasicCluster(), svr.GetStorage(), syncer.NewRegionSyncer(svr), svr.GetClient(), svr.GetHTTPClient(), svr.GetTSOAllocatorManager())

	// Cluster is not bootstrapped.
	rc.InitCluster(svr.GetAllocator(), svr.GetPersistOptions(), svr.GetHBStreams(), svr.GetKeyspaceGroupManager())
	raftCluster, err := rc.LoadClusterInfo()
	re.NoError(err)
	re.Nil(raftCluster)

	testStorage := rc.GetStorage()
	basicCluster := rc.GetBasicCluster()
	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	re.NoError(testStorage.SaveMeta(meta))
	stores := make([]*metapb.Store, 0, n)
	for i := range n {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		re.NoError(testStorage.SaveStoreMeta(store))
	}

	regions := make([]*metapb.Region, 0, n)
	for i := range uint64(n) {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		re.NoError(testStorage.SaveRegion(region))
	}
	re.NoError(testStorage.Flush())

	raftCluster = cluster.NewRaftCluster(ctx, svr.GetMember(), basicCluster,
		testStorage, syncer.NewRegionSyncer(svr), svr.GetClient(), svr.GetHTTPClient(), svr.GetTSOAllocatorManager())
	raftCluster.InitCluster(mockid.NewIDAllocator(), svr.GetPersistOptions(), svr.GetHBStreams(), svr.GetKeyspaceGroupManager())
	raftCluster, err = raftCluster.LoadClusterInfo()
	re.NoError(err)
	re.NotNil(raftCluster)

	// Check meta, stores, and regions.
	re.Equal(meta, raftCluster.GetMetaCluster())
	re.Equal(n, raftCluster.GetStoreCount())
	for _, store := range raftCluster.GetMetaStores() {
		re.Equal(stores[store.GetId()], store)
	}
	re.Equal(n, raftCluster.GetTotalRegionCount())
	for _, region := range raftCluster.GetMetaRegions() {
		re.Equal(regions[region.GetId()], region)
	}

	m := 20
	regions = make([]*metapb.Region, 0, n)
	for i := range uint64(m) {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		re.NoError(testStorage.SaveRegion(region))
	}
	re.NoError(storage.TryLoadRegionsOnce(ctx, testStorage, raftCluster.GetBasicCluster().PutRegion))
	re.Equal(n, raftCluster.GetTotalRegionCount())
}

func TestTiFlashWithPlacementRules(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) { cfg.Replication.EnablePlacementRules = false })
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)

	tiflashStore := &metapb.Store{
		Id:      11,
		Address: "127.0.0.1:1",
		Labels:  []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
		Version: "v4.1.0",
	}

	// cannot put TiFlash node without placement rules
	resp, err := putStore(grpcPDClient, clusterID, tiflashStore)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_UNKNOWN, resp.GetHeader().GetError().GetType())

	rep := leaderServer.GetConfig().Replication
	rep.EnablePlacementRules = true
	svr := leaderServer.GetServer()
	err = svr.SetReplicationConfig(rep)
	re.NoError(err)
	resp, err = putStore(grpcPDClient, clusterID, tiflashStore)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	// test TiFlash store limit
	expect := map[uint64]sc.StoreLimitConfig{11: {AddPeer: 30, RemovePeer: 30}}
	re.Equal(expect, svr.GetScheduleConfig().StoreLimit)

	// cannot disable placement rules with TiFlash nodes
	rep.EnablePlacementRules = false
	err = svr.SetReplicationConfig(rep)
	re.Error(err)
	err = svr.GetRaftCluster().BuryStore(11, true)
	re.NoError(err)
	err = svr.SetReplicationConfig(rep)
	re.NoError(err)
	re.Empty(svr.GetScheduleConfig().StoreLimit)
}

func TestReplicationModeStatus(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.ReplicationMode.ReplicationMode = "dr-auto-sync"
	})

	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := newBootstrapRequest(clusterID)
	res, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(replication_modepb.ReplicationMode_DR_AUTO_SYNC, res.GetReplicationStatus().GetMode()) // check status in bootstrap response
	store := &metapb.Store{Id: 11, Address: "127.0.0.1:1", Version: "v4.1.0"}
	putRes, err := putStore(grpcPDClient, clusterID, store)
	re.NoError(err)
	re.Equal(replication_modepb.ReplicationMode_DR_AUTO_SYNC, putRes.GetReplicationStatus().GetMode()) // check status in putStore response
	hbReq := &pdpb.StoreHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
	}
	hbRes, err := grpcPDClient.StoreHeartbeat(context.Background(), hbReq)
	re.NoError(err)
	re.Equal(replication_modepb.ReplicationMode_DR_AUTO_SYNC, hbRes.GetReplicationStatus().GetMode()) // check status in store heartbeat response
}

func newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	return &pdpb.IsBootstrappedRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}
}

func newBootstrapRequest(clusterID uint64) *pdpb.BootstrapRequest {
	return &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: testStoreAddr},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
}

// helper function to check and bootstrap.
func bootstrapCluster(re *require.Assertions, clusterID uint64, grpcPDClient pdpb.PDClient) {
	req := newBootstrapRequest(clusterID)
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
}

func putStore(grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	return grpcPDClient.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
	})
}

func getStore(re *require.Assertions, clusterID uint64, grpcPDClient pdpb.PDClient, storeID uint64) *metapb.Store {
	resp, err := grpcPDClient.GetStore(context.Background(), &pdpb.GetStoreRequest{
		Header:  testutil.NewRequestHeader(clusterID),
		StoreId: storeID,
	})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	re.Equal(storeID, resp.GetStore().GetId())
	return resp.GetStore()
}

func getRegion(re *require.Assertions, clusterID uint64, grpcPDClient pdpb.PDClient, regionKey []byte) *metapb.Region {
	resp, err := grpcPDClient.GetRegion(context.Background(), &pdpb.GetRegionRequest{
		Header:    testutil.NewRequestHeader(clusterID),
		RegionKey: regionKey,
	})
	re.NoError(err)
	re.NotNil(resp.GetRegion())
	return resp.GetRegion()
}

func getRegionByID(re *require.Assertions, clusterID uint64, grpcPDClient pdpb.PDClient, regionID uint64) *metapb.Region {
	resp, err := grpcPDClient.GetRegionByID(context.Background(), &pdpb.GetRegionByIDRequest{
		Header:   testutil.NewRequestHeader(clusterID),
		RegionId: regionID,
	})
	re.NoError(err)
	re.NotNil(resp.GetRegion())
	return resp.GetRegion()
}

func getClusterConfig(re *require.Assertions, clusterID uint64, grpcPDClient pdpb.PDClient) *metapb.Cluster {
	resp, err := grpcPDClient.GetClusterConfig(context.Background(), &pdpb.GetClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
	})
	re.NoError(err)
	re.NotNil(resp.GetCluster())
	return resp.GetCluster()
}

func TestOfflineStoreLimit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dashboard.SetCheckInterval(30 * time.Minute)
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1"}
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, _, err := id.Alloc(1)
		re.NoError(err)
		store := newMetaStore(storeID, addr, "4.0.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	}
	for i := uint64(1); i <= 2; i++ {
		r := &metapb.Region{
			Id: i,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i + 1)},
			EndKey:   []byte{byte(i + 2)},
			Peers:    []*metapb.Peer{{Id: i + 10, StoreId: i}},
		}
		region := core.NewRegionInfo(r, r.Peers[0], core.SetApproximateSize(10))

		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	oc := rc.GetOperatorController()
	opt := rc.GetOpts()
	opt.SetAllStoresLimit(storelimit.RemovePeer, 1)
	// only can add 5 remove peer operators on store 1
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	// only can add 5 remove peer operators on store 2
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	// reset all store limit
	opt.SetAllStoresLimit(storelimit.RemovePeer, 2)

	// only can add 5 remove peer operators on store 2
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	// offline store 1
	rc.SetStoreLimit(1, storelimit.RemovePeer, storelimit.Unlimited)
	rc.RemoveStore(1, false)

	// can add unlimited remove peer operators on store 1
	for i := uint64(1); i <= 30; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
}

func TestUpgradeStoreLimit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dashboard.SetCheckInterval(30 * time.Minute)
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	store := newMetaStore(1, "127.0.1.1:0", "4.0.0", metapb.StoreState_Up, "test/store1")
	resp, err := putStore(grpcPDClient, clusterID, store)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	r := &metapb.Region{
		Id: 1,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte{byte(2)},
		EndKey:   []byte{byte(3)},
		Peers:    []*metapb.Peer{{Id: 11, StoreId: uint64(1)}},
	}
	region := core.NewRegionInfo(r, r.Peers[0], core.SetApproximateSize(10))

	err = rc.HandleRegionHeartbeat(region)
	re.NoError(err)

	// restart PD
	// Here we use an empty storelimit to simulate the upgrade progress.
	scheduleCfg := rc.GetScheduleConfig().Clone()
	scheduleCfg.StoreLimit = map[uint64]sc.StoreLimitConfig{}
	re.NoError(leaderServer.GetServer().SetScheduleConfig(*scheduleCfg))
	err = leaderServer.Stop()
	re.NoError(err)
	err = leaderServer.Run()
	re.NoError(err)

	oc := rc.GetOperatorController()
	// only can add 5 remove peer operators on store 1
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))
}

func TestStaleTermHeartbeat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dashboard.SetCheckInterval(30 * time.Minute)
	tc, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	peers := make([]*metapb.Peer, 0, len(storeAddrs))
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, _, err := id.Alloc(1)
		re.NoError(err)
		peerID, _, err := id.Alloc(1)
		re.NoError(err)
		store := newMetaStore(storeID, addr, "3.0.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
		peers = append(peers, &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		})
	}

	regionReq := &pdpb.RegionHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Region: &metapb.Region{
			Id:       1,
			Peers:    peers,
			StartKey: []byte{byte(2)},
			EndKey:   []byte{byte(3)},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 2,
				Version: 1,
			},
		},
		Leader:          peers[0],
		Term:            5,
		ApproximateSize: 10,
	}
	flowRoundDivisor := leaderServer.GetConfig().PDServerCfg.FlowRoundByDigit
	region := core.RegionFromHeartbeat(regionReq, flowRoundDivisor)
	err = rc.HandleRegionHeartbeat(region)
	re.NoError(err)

	// Transfer leader
	regionReq.Term = 6
	regionReq.Leader = peers[1]
	region = core.RegionFromHeartbeat(regionReq, flowRoundDivisor)
	err = rc.HandleRegionHeartbeat(region)
	re.NoError(err)

	// issue #3379
	regionReq.KeysWritten = uint64(18446744073709551615)  // -1
	regionReq.BytesWritten = uint64(18446744073709550602) // -1024
	region = core.RegionFromHeartbeat(regionReq, flowRoundDivisor)
	re.Equal(uint64(0), region.GetKeysWritten())
	re.Equal(uint64(0), region.GetBytesWritten())
	err = rc.HandleRegionHeartbeat(region)
	re.NoError(err)

	// Stale heartbeat, update check should fail
	regionReq.Term = 5
	regionReq.Leader = peers[0]
	region = core.RegionFromHeartbeat(regionReq, flowRoundDivisor)
	err = rc.HandleRegionHeartbeat(region)
	re.Error(err)

	// Allow regions that are created by unsafe recover to send a heartbeat, even though they
	// are considered "stale" because their conf ver and version are both equal to 1.
	regionReq.Region.RegionEpoch.ConfVer = 1
	region = core.RegionFromHeartbeat(regionReq, flowRoundDivisor)
	err = rc.HandleRegionHeartbeat(region)
	re.NoError(err)
}

func TestTransferLeaderForScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	tc, err := tests.NewTestCluster(ctx, 2)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	// start
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)

	storesNum := 2
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	for i := 1; i <= storesNum; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: "127.0.0.1:" + strconv.Itoa(i),
		}
		resp, err := putStore(grpcPDClient, leaderServer.GetClusterID(), store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	}
	// region heartbeat
	id := leaderServer.GetAllocator()
	putRegionWithLeader(re, rc, id, 1)

	time.Sleep(time.Second)
	re.True(leaderServer.GetRaftCluster().IsPrepared())
	schedsNum := len(rc.GetCoordinator().GetSchedulersController().GetSchedulerNames())
	// Add evict leader scheduler
	api.MustAddScheduler(re, leaderServer.GetAddr(), types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	api.MustAddScheduler(re, leaderServer.GetAddr(), types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 2,
	})
	// Check scheduler updated.
	schedsNum += 1
	schedulersController := rc.GetCoordinator().GetSchedulersController()
	re.Len(schedulersController.GetSchedulerNames(), schedsNum)
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})

	// transfer PD leader to another PD
	tc.ResignLeader()
	rc.Stop()
	tc.WaitLeader()
	leaderServer = tc.GetLeaderServer()
	rc1 := leaderServer.GetServer().GetRaftCluster()
	rc1.Start(leaderServer.GetServer(), false)
	re.NoError(err)
	re.NotNil(rc1)
	// region heartbeat
	id = leaderServer.GetAllocator()
	putRegionWithLeader(re, rc1, id, 1)
	time.Sleep(time.Second)
	re.True(leaderServer.GetRaftCluster().IsPrepared())
	// Check scheduler updated.
	schedulersController = rc1.GetCoordinator().GetSchedulersController()
	re.Len(schedulersController.GetSchedulerNames(), schedsNum)
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})

	// transfer PD leader back to the previous PD
	tc.ResignLeader()
	rc1.Stop()
	tc.WaitLeader()
	leaderServer = tc.GetLeaderServer()
	rc = leaderServer.GetServer().GetRaftCluster()
	rc.Start(leaderServer.GetServer(), false)
	re.NotNil(rc)
	// region heartbeat
	id = leaderServer.GetAllocator()
	putRegionWithLeader(re, rc, id, 1)
	time.Sleep(time.Second)
	re.True(leaderServer.GetRaftCluster().IsPrepared())
	// Check scheduler updated
	schedulersController = rc.GetCoordinator().GetSchedulersController()
	re.Len(schedulersController.GetSchedulerNames(), schedsNum)
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
}

func checkEvictLeaderSchedulerExist(re *require.Assertions, sc *schedulers.Controller, exist bool) {
	testutil.Eventually(re, func() bool {
		if !exist {
			return sc.GetScheduler(types.EvictLeaderScheduler.String()) == nil
		}
		return sc.GetScheduler(types.EvictLeaderScheduler.String()) != nil
	})
}

func checkEvictLeaderStoreIDs(re *require.Assertions, sc *schedulers.Controller, expected []uint64) {
	handler, ok := sc.GetSchedulerHandlers()[types.EvictLeaderScheduler.String()]
	re.True(ok)
	h, ok := handler.(interface {
		EvictStoreIDs() []uint64
	})
	re.True(ok)
	var evictStoreIDs []uint64
	testutil.Eventually(re, func() bool {
		evictStoreIDs = h.EvictStoreIDs()
		return len(evictStoreIDs) == len(expected)
	})
	re.ElementsMatch(evictStoreIDs, expected)
}

func putRegionWithLeader(re *require.Assertions, rc *cluster.RaftCluster, id id.Allocator, storeID uint64) {
	for i := range 3 {
		regionID, _, err := id.Alloc(1)
		re.NoError(err)
		peerID, _, err := id.Alloc(1)
		re.NoError(err)
		region := &metapb.Region{
			Id:       regionID,
			Peers:    []*metapb.Peer{{Id: peerID, StoreId: storeID}},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0], core.SetSource(core.Heartbeat)))
	}

	time.Sleep(50 * time.Millisecond)
	re.Equal(3, rc.GetStore(storeID).GetLeaderCount())
}

func checkMinResolvedTS(re *require.Assertions, rc *cluster.RaftCluster, expect uint64) {
	re.Eventually(func() bool {
		ts := rc.GetMinResolvedTS()
		return expect == ts
	}, time.Second*10, time.Millisecond*50)
}

func checkStoreMinResolvedTS(re *require.Assertions, rc *cluster.RaftCluster, expectTS, storeID uint64) {
	re.Eventually(func() bool {
		ts := rc.GetStoreMinResolvedTS(storeID)
		return expectTS == ts
	}, time.Second*10, time.Millisecond*50)
}

func checkMinResolvedTSFromStorage(re *require.Assertions, rc *cluster.RaftCluster, expect uint64) {
	re.Eventually(func() bool {
		ts2, err := rc.GetStorage().LoadMinResolvedTS()
		re.NoError(err)
		return expect == ts2
	}, time.Second*10, time.Millisecond*50)
}

func setMinResolvedTSPersistenceInterval(re *require.Assertions, rc *cluster.RaftCluster, svr *server.Server, interval time.Duration) {
	cfg := rc.GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = typeutil.NewDuration(interval)
	err := svr.SetPDServerConfig(*cfg)
	re.NoError(err)
}

func TestMinResolvedTS(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Millisecond
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	id := leaderServer.GetAllocator()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	svr := leaderServer.GetServer()
	addStoreAndCheckMinResolvedTS := func(re *require.Assertions, isTiflash bool, minResolvedTS, expect uint64) uint64 {
		storeID, _, err := id.Alloc(1)
		re.NoError(err)
		store := &metapb.Store{
			Id:      storeID,
			Version: "v6.0.0",
			Address: "127.0.0.1:" + strconv.Itoa(int(storeID)),
		}
		if isTiflash {
			store.Labels = []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}
		}
		resp, err := putStore(grpcPDClient, clusterID, store)
		re.NoError(err)
		re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
		req := &pdpb.ReportMinResolvedTsRequest{
			Header:        testutil.NewRequestHeader(clusterID),
			StoreId:       storeID,
			MinResolvedTs: minResolvedTS,
		}
		_, err = grpcPDClient.ReportMinResolvedTS(context.Background(), req)
		re.NoError(err)
		ts := rc.GetMinResolvedTS()
		re.Equal(expect, ts)
		return storeID
	}

	// default run job
	re.NotZero(rc.GetPDServerConfig().MinResolvedTSPersistenceInterval.Duration)
	setMinResolvedTSPersistenceInterval(re, rc, svr, 0)
	re.Equal(time.Duration(0), rc.GetPDServerConfig().MinResolvedTSPersistenceInterval.Duration)

	// case1: cluster is no initialized
	// min resolved ts should be not available
	status, err := rc.LoadClusterStatus()
	re.NoError(err)
	re.False(status.IsInitialized)
	store1TS := uint64(233)
	store1 := addStoreAndCheckMinResolvedTS(re, false /* not tiflash */, store1TS, math.MaxUint64)

	// case2: add leader peer to store1 but no run job
	// min resolved ts should be zero
	putRegionWithLeader(re, rc, id, store1)
	checkMinResolvedTS(re, rc, 0)

	// case3: add leader peer to store1 and run job
	// min resolved ts should be store1TS
	setMinResolvedTSPersistenceInterval(re, rc, svr, time.Millisecond)
	checkMinResolvedTS(re, rc, store1TS)
	checkMinResolvedTSFromStorage(re, rc, store1TS)

	// case4: add tiflash store
	// min resolved ts should no change
	addStoreAndCheckMinResolvedTS(re, true /* is tiflash */, 0, store1TS)

	// case5: add new store with lager min resolved ts
	// min resolved ts should no change
	store3TS := store1TS + 10
	store3 := addStoreAndCheckMinResolvedTS(re, false /* not tiflash */, store3TS, store1TS)
	putRegionWithLeader(re, rc, id, store3)

	// case6: set store1 to tombstone
	// min resolved ts should change to store 3
	resetStoreState(re, rc, store1, metapb.StoreState_Tombstone)
	checkMinResolvedTS(re, rc, store3TS)
	checkMinResolvedTSFromStorage(re, rc, store3TS)
	checkStoreMinResolvedTS(re, rc, store3TS, store3)
	// check no-exist store
	checkStoreMinResolvedTS(re, rc, math.MaxUint64, 100)

	// case7: add a store with leader peer but no report min resolved ts
	// min resolved ts should be no change
	store4 := addStoreAndCheckMinResolvedTS(re, false /* not tiflash */, 0, store3TS)
	putRegionWithLeader(re, rc, id, store4)
	checkMinResolvedTS(re, rc, store3TS)
	checkMinResolvedTSFromStorage(re, rc, store3TS)
	resetStoreState(re, rc, store4, metapb.StoreState_Tombstone)

	// case8: set min resolved ts persist interval to zero
	// although min resolved ts increase, it should be not persisted until job running.
	store5TS := store3TS + 10
	setMinResolvedTSPersistenceInterval(re, rc, svr, 0)
	store5 := addStoreAndCheckMinResolvedTS(re, false /* not tiflash */, store5TS, store3TS)
	resetStoreState(re, rc, store3, metapb.StoreState_Tombstone)
	putRegionWithLeader(re, rc, id, store5)
	checkMinResolvedTS(re, rc, store3TS)
	setMinResolvedTSPersistenceInterval(re, rc, svr, time.Millisecond)
	checkMinResolvedTS(re, rc, store5TS)
	checkStoreMinResolvedTS(re, rc, store5TS, store5)
}

// See https://github.com/tikv/pd/issues/4941
func TestTransferLeaderBack(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 2)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	svr := leaderServer.GetServer()
	rc := cluster.NewRaftCluster(ctx, svr.GetMember(), svr.GetBasicCluster(),
		svr.GetStorage(), syncer.NewRegionSyncer(svr), svr.GetClient(),
		svr.GetHTTPClient(), svr.GetTSOAllocatorManager())
	rc.InitCluster(svr.GetAllocator(), svr.GetPersistOptions(), svr.GetHBStreams(), svr.GetKeyspaceGroupManager())
	storage := rc.GetStorage()
	meta := &metapb.Cluster{Id: 123}
	re.NoError(storage.SaveMeta(meta))
	n := 4
	stores := make([]*metapb.Store, 0, n)
	for i := 1; i <= n; i++ {
		store := &metapb.Store{Id: uint64(i), State: metapb.StoreState_Up}
		stores = append(stores, store)
	}

	for _, store := range stores {
		re.NoError(storage.SaveStoreMeta(store))
	}
	rc, err = rc.LoadClusterInfo()
	re.NoError(err)
	re.NotNil(rc)
	// offline a store
	re.NoError(rc.RemoveStore(1, false))
	re.Equal(metapb.StoreState_Offline, rc.GetStore(1).GetState())

	// transfer PD leader to another PD
	tc.ResignLeader()
	tc.WaitLeader()
	leaderServer = tc.GetLeaderServer()
	svr1 := leaderServer.GetServer()
	rc1 := svr1.GetRaftCluster()
	re.NoError(err)
	re.NotNil(rc1)

	// tombstone a store, and remove its record
	re.NoError(rc1.BuryStore(1, false))
	re.NoError(rc1.RemoveTombStoneRecords())

	// transfer PD leader back to the previous PD
	tc.ResignLeader()
	tc.WaitLeader()
	leaderServer = tc.GetLeaderServer()
	svr = leaderServer.GetServer()
	rc = svr.GetRaftCluster()
	re.NotNil(rc)

	// check store count
	re.Equal(meta, rc.GetMetaCluster())
	re.Equal(3, rc.GetStoreCount())
}

func TestExternalTimestamp(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(re, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	store := &metapb.Store{
		Id:      1,
		Version: "v6.0.0",
		Address: "127.0.0.1:" + strconv.Itoa(int(1)),
	}
	resp, err := putStore(grpcPDClient, clusterID, store)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	id := leaderServer.GetAllocator()
	putRegionWithLeader(re, rc, id, 1)
	time.Sleep(100 * time.Millisecond)

	ts := uint64(233)
	{ // case1: set external timestamp
		req := &pdpb.SetExternalTimestampRequest{
			Header:    testutil.NewRequestHeader(clusterID),
			Timestamp: ts,
		}
		_, err = grpcPDClient.SetExternalTimestamp(context.Background(), req)
		re.NoError(err)

		req2 := &pdpb.GetExternalTimestampRequest{
			Header: testutil.NewRequestHeader(clusterID),
		}
		resp2, err := grpcPDClient.GetExternalTimestamp(context.Background(), req2)
		re.NoError(err)
		re.Equal(ts, resp2.GetTimestamp())
	}

	{ // case2: set external timestamp less than now
		req := &pdpb.SetExternalTimestampRequest{
			Header:    testutil.NewRequestHeader(clusterID),
			Timestamp: ts - 1,
		}
		_, err = grpcPDClient.SetExternalTimestamp(context.Background(), req)
		re.NoError(err)

		req2 := &pdpb.GetExternalTimestampRequest{
			Header: testutil.NewRequestHeader(clusterID),
		}
		resp2, err := grpcPDClient.GetExternalTimestamp(context.Background(), req2)
		re.NoError(err)
		re.Equal(ts, resp2.GetTimestamp())
	}

	{ // case3: set external timestamp larger than global ts
		tsoClient, err := grpcPDClient.Tso(ctx)
		re.NoError(err)
		defer tsoClient.CloseSend()
		// get external ts
		req := &pdpb.GetExternalTimestampRequest{
			Header: testutil.NewRequestHeader(clusterID),
		}
		resp, err := grpcPDClient.GetExternalTimestamp(context.Background(), req)
		re.NoError(err)
		ts = resp.GetTimestamp()
		// get global ts
		req2 := &pdpb.TsoRequest{
			Header:     testutil.NewRequestHeader(clusterID),
			Count:      1,
			DcLocation: tso.GlobalDCLocation,
		}
		re.NoError(tsoClient.Send(req2))
		resp2, err := tsoClient.Recv()
		re.NoError(err)
		globalTS := resp2.GetTimestamp()
		// set external ts larger than global ts
		unexpectedTS := tsoutil.ComposeTS(globalTS.Physical+1000, 0) // add 1000ms to avoid test running too slow
		req3 := &pdpb.SetExternalTimestampRequest{
			Header:    testutil.NewRequestHeader(clusterID),
			Timestamp: unexpectedTS,
		}
		_, err = grpcPDClient.SetExternalTimestamp(context.Background(), req3)
		re.NoError(err)
		// get external ts again
		req4 := &pdpb.GetExternalTimestampRequest{
			Header: testutil.NewRequestHeader(clusterID),
		}
		resp4, err := grpcPDClient.GetExternalTimestamp(context.Background(), req4)
		re.NoError(err)
		// get global ts again
		req5 := &pdpb.TsoRequest{
			Header:     testutil.NewRequestHeader(clusterID),
			Count:      1,
			DcLocation: tso.GlobalDCLocation,
		}
		re.NoError(tsoClient.Send(req5))
		resp5, err := tsoClient.Recv()
		re.NoError(err)
		currentGlobalTS := tsoutil.GenerateTS(resp5.GetTimestamp())
		// check external ts should not be larger than global ts
		re.Equal(1, tsoutil.CompareTimestampUint64(unexpectedTS, currentGlobalTS))
		re.Equal(ts, resp4.GetTimestamp())
	}
}

func TestPatrolRegionConfigChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		tests.MustPutStore(re, tc, store)
	}
	for i := 1; i <= 200; i++ {
		startKey := []byte(fmt.Sprintf("%d", i*2-1))
		endKey := []byte(fmt.Sprintf("%d", i*2))
		tests.MustPutRegion(re, tc, uint64(i), uint64(i%3+1), startKey, endKey)
	}
	fname := testutil.InitTempFileLogger("debug")
	defer os.RemoveAll(fname)
	checkLog(re, fname, "coordinator starts patrol regions")

	// test change patrol region interval
	schedule := leaderServer.GetConfig().Schedule
	schedule.PatrolRegionInterval = typeutil.NewDuration(99 * time.Millisecond)
	leaderServer.GetServer().SetScheduleConfig(schedule)
	checkLog(re, fname, "starts patrol regions with new interval")

	// test change patrol region worker count
	schedule = leaderServer.GetConfig().Schedule
	schedule.PatrolRegionWorkerCount = 8
	leaderServer.GetServer().SetScheduleConfig(schedule)
	checkLog(re, fname, "starts patrol regions with new workers count")

	// test change schedule halt
	schedule = leaderServer.GetConfig().Schedule
	schedule.HaltScheduling = true
	leaderServer.GetServer().SetScheduleConfig(schedule)
	checkLog(re, fname, "skip patrol regions due to scheduling is halted")
}

func checkLog(re *require.Assertions, fname, expect string) {
	testutil.Eventually(re, func() bool {
		b, _ := os.ReadFile(fname)
		l := string(b)
		return strings.Contains(l, expect)
	})
	os.Truncate(fname, 0)
}

func TestFollowerExitSyncTime(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)

	server := mockserver.NewMockServer(
		context.Background(),
		&pdpb.Member{MemberId: 1, Name: "test", ClientUrls: []string{tempurl.Alloc()}},
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		core.NewBasicCluster(),
	)
	s := syncer.NewRegionSyncer(server)
	s.StartSyncWithLeader(leaderServer.GetAddr())
	time.Sleep(time.Second)

	// Record the time when exiting sync
	startTime := time.Now()

	// Simulate leader change scenario
	// Directly call StopSyncWithLeader to simulate exit
	s.StopSyncWithLeader()

	// Calculate time difference
	elapsedTime := time.Since(startTime)

	// Assert that the sync exit time is within expected range
	re.Less(elapsedTime, time.Second)
}
