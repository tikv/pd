// Copyright 2023 TiKV Project Authors.
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

package scheduling

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/server/api"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type serverTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(serverTestSuite))
}

func (suite *serverTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/changeRunCollectWaitTime", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
}

func (suite *serverTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.cluster.Destroy()
	suite.cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/changeRunCollectWaitTime"))
}

func (suite *serverTestSuite) TestAllocID() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	time.Sleep(200 * time.Millisecond)
	id, _, err := tc.GetPrimaryServer().GetCluster().AllocID(1)
	re.NoError(err)
	re.NotEqual(uint64(0), id)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
}

func (suite *serverTestSuite) TestAllocIDAfterLeaderChange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	pd2, err := suite.cluster.Join(suite.ctx)
	re.NoError(err, "error: %v", err)
	err = pd2.Run()
	re.NotEmpty(suite.cluster.WaitLeader())
	re.NoError(err)
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	time.Sleep(200 * time.Millisecond)
	cluster := tc.GetPrimaryServer().GetCluster()
	id, _, err := cluster.AllocID(1)
	re.NoError(err)
	re.NotEqual(uint64(0), id)
	suite.cluster.ResignLeader()
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	time.Sleep(time.Second)
	id1, _, err := cluster.AllocID(1)
	re.NoError(err)
	re.Greater(id1, id)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
	// Update the pdLeader in test suite.
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *serverTestSuite) TestPrimaryChange() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	primary := tc.GetPrimaryServer()
	oldPrimaryAddr := primary.GetAddr()
	testutil.Eventually(re, func() bool {
		watchedAddr, ok := suite.pdLeader.GetServicePrimaryAddr(suite.ctx, constant.SchedulingServiceName)
		return ok && oldPrimaryAddr == watchedAddr &&
			len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames()) == 4
	})
	// change primary
	primary.Close()
	tc.WaitForPrimaryServing(re)
	primary = tc.GetPrimaryServer()
	newPrimaryAddr := primary.GetAddr()
	re.NotEqual(oldPrimaryAddr, newPrimaryAddr)
	testutil.Eventually(re, func() bool {
		watchedAddr, ok := suite.pdLeader.GetServicePrimaryAddr(suite.ctx, constant.SchedulingServiceName)
		return ok && newPrimaryAddr == watchedAddr &&
			len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames()) == 4
	})
}

func (suite *serverTestSuite) TestForwardStoreHeartbeat() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}
	resp, err := s.PutStore(
		context.Background(), &pdpb.PutStoreRequest{
			Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
			Store: &metapb.Store{
				Id:      1,
				Address: "tikv1",
				State:   metapb.StoreState_Up,
				Version: "7.0.0",
			},
		},
	)
	re.NoError(err)
	re.Empty(resp.GetHeader().GetError())

	testutil.Eventually(re, func() bool {
		resp1, err := s.StoreHeartbeat(
			context.Background(), &pdpb.StoreHeartbeatRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Stats: &pdpb.StoreStats{
					StoreId:      1,
					Capacity:     1798985089024,
					Available:    1709868695552,
					UsedSize:     85150956358,
					KeysWritten:  20000,
					BytesWritten: 199,
					KeysRead:     10000,
					BytesRead:    99,
				},
			},
		)
		re.NoError(err)
		re.Empty(resp1.GetHeader().GetError())
		store := tc.GetPrimaryServer().GetCluster().GetStore(1)
		return store.GetStoreStats().GetCapacity() == uint64(1798985089024) &&
			store.GetStoreStats().GetAvailable() == uint64(1709868695552) &&
			store.GetStoreStats().GetUsedSize() == uint64(85150956358) &&
			store.GetStoreStats().GetKeysWritten() == uint64(20000) &&
			store.GetStoreStats().GetBytesWritten() == uint64(199) &&
			store.GetStoreStats().GetKeysRead() == uint64(10000) &&
			store.GetStoreStats().GetBytesRead() == uint64(99)
	})
}

func (suite *serverTestSuite) TestSchedulingServiceFallback() {
	re := suite.Require()
	leaderServer := suite.pdLeader.GetServer()
	conf := leaderServer.GetMicroserviceConfig().Clone()
	// Change back to the default value.
	conf.EnableSchedulingFallback = true
	leaderServer.SetMicroserviceConfig(*conf)
	// PD will execute scheduling jobs since there is no scheduling server.
	testutil.Eventually(re, func() bool {
		return suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	// After scheduling server is started, PD will not execute scheduling jobs.
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	// Scheduling server is responsible for executing scheduling jobs.
	testutil.Eventually(re, func() bool {
		return tc.GetPrimaryServer().GetCluster().IsBackgroundJobsRunning()
	})
	tc.GetPrimaryServer().Close()
	// Stop scheduling server. PD will execute scheduling jobs again.
	testutil.Eventually(re, func() bool {
		return suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	tc1, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc1.Destroy()
	tc1.WaitForPrimaryServing(re)
	// After scheduling server is started, PD will not execute scheduling jobs.
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	// Scheduling server is responsible for executing scheduling jobs again.
	testutil.Eventually(re, func() bool {
		return tc1.GetPrimaryServer().GetCluster().IsBackgroundJobsRunning()
	})
}

func (suite *serverTestSuite) TestDisableSchedulingServiceFallback() {
	re := suite.Require()

	// PD will execute scheduling jobs since there is no scheduling server.
	testutil.Eventually(re, func() bool {
		re.NotNil(suite.pdLeader.GetServer())
		re.NotNil(suite.pdLeader.GetServer().GetRaftCluster())
		return suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	leaderServer := suite.pdLeader.GetServer()
	// After Disabling scheduling service fallback, the PD will stop scheduling.
	conf := leaderServer.GetMicroserviceConfig().Clone()
	conf.EnableSchedulingFallback = false
	leaderServer.SetMicroserviceConfig(*conf)
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	// Enable scheduling service fallback again, the PD will restart scheduling.
	conf.EnableSchedulingFallback = true
	leaderServer.SetMicroserviceConfig(*conf)
	testutil.Eventually(re, func() bool {
		return suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	// After scheduling server is started, PD will not execute scheduling jobs.
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	// Scheduling server is responsible for executing scheduling jobs.
	testutil.Eventually(re, func() bool {
		return tc.GetPrimaryServer().GetCluster().IsBackgroundJobsRunning()
	})
	// Disable scheduling service fallback and stop scheduling server. PD won't execute scheduling jobs again.
	conf.EnableSchedulingFallback = false
	leaderServer.SetMicroserviceConfig(*conf)
	tc.GetPrimaryServer().Close()
	time.Sleep(time.Second)
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
}

func (suite *serverTestSuite) TestSchedulerSync() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	schedulersController := tc.GetPrimaryServer().GetCluster().GetCoordinator().GetSchedulersController()
	checkEvictLeaderSchedulerExist(re, schedulersController, false)
	// Add a new evict-leader-scheduler through the PD.
	api.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	// Check if the evict-leader-scheduler is added.
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	// Add a store_id to the evict-leader-scheduler through the PD.
	err = suite.pdLeader.GetServer().GetRaftCluster().PutMetaStore(
		&metapb.Store{
			Id:            2,
			Address:       "mock://2",
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Version:       "7.0.0",
		},
	)
	re.NoError(err)
	api.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 2,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})
	// Delete a store_id from the evict-leader-scheduler through the PD.
	api.MustDeleteScheduler(re, suite.backendEndpoints, fmt.Sprintf("%s-%d", types.EvictLeaderScheduler.String(), 1))
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{2})
	// Add a store_id to the evict-leader-scheduler through the PD by the scheduler handler.
	api.MustCallSchedulerConfigAPI(re, http.MethodPost, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"config"}, map[string]any{
		"name":     types.EvictLeaderScheduler.String(),
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})
	// Delete a store_id from the evict-leader-scheduler through the PD by the scheduler handler.
	api.MustCallSchedulerConfigAPI(re, http.MethodDelete, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"delete", "2"}, nil)
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	// If the last store is deleted, the scheduler should be removed.
	api.MustCallSchedulerConfigAPI(re, http.MethodDelete, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"delete", "1"}, nil)
	// Check if the scheduler is removed.
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	// Delete the evict-leader-scheduler through the PD by removing the last store_id.
	api.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	api.MustDeleteScheduler(re, suite.backendEndpoints, fmt.Sprintf("%s-%d", types.EvictLeaderScheduler.String(), 1))
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	// Delete the evict-leader-scheduler through the PD.
	api.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	api.MustDeleteScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String())
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	// The default scheduler could not be deleted, it could only be disabled.
	defaultSchedulerNames := []string{
		types.BalanceLeaderScheduler.String(),
		types.BalanceRegionScheduler.String(),
		types.BalanceHotRegionScheduler.String(),
	}
	checkDisabled := func(name string, shouldDisabled bool) {
		re.NotNil(schedulersController.GetScheduler(name), name)
		testutil.Eventually(re, func() bool {
			disabled, err := schedulersController.IsSchedulerDisabled(name)
			re.NoError(err, name)
			return disabled == shouldDisabled
		})
	}
	for _, name := range defaultSchedulerNames {
		checkDisabled(name, false)
		api.MustDeleteScheduler(re, suite.backendEndpoints, name)
		checkDisabled(name, true)
	}
	for _, name := range defaultSchedulerNames {
		checkDisabled(name, true)
		api.MustAddScheduler(re, suite.backendEndpoints, name, nil)
		checkDisabled(name, false)
	}
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

func (suite *serverTestSuite) TestForwardRegionHeartbeat() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}
	for i := uint64(1); i <= 3; i++ {
		resp, err := s.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://%d", i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	stream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}
	queryStats := &pdpb.QueryStats{
		Get:                    5,
		Coprocessor:            6,
		Scan:                   7,
		Put:                    8,
		Delete:                 9,
		DeleteRange:            10,
		AcquirePessimisticLock: 11,
		Rollback:               12,
		Prewrite:               13,
		Commit:                 14,
	}
	interval := &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}
	downPeers := []*pdpb.PeerStats{{Peer: peers[2], DownSeconds: 100}}
	pendingPeers := []*metapb.Peer{peers[2]}
	regionReq := &pdpb.RegionHeartbeatRequest{
		Header:          testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
		Region:          &metapb.Region{Id: 10, Peers: peers, StartKey: []byte("a"), EndKey: []byte("b")},
		Leader:          peers[0],
		DownPeers:       downPeers,
		PendingPeers:    pendingPeers,
		BytesWritten:    10,
		BytesRead:       20,
		KeysWritten:     100,
		KeysRead:        200,
		ApproximateSize: 30 * units.MiB,
		ApproximateKeys: 300,
		Interval:        interval,
		QueryStats:      queryStats,
		Term:            1,
		CpuUsage:        100,
	}
	err = stream.Send(regionReq)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		region := tc.GetPrimaryServer().GetCluster().GetRegion(10)
		return region != nil && region.GetBytesRead() == 20 && region.GetBytesWritten() == 10 &&
			region.GetKeysRead() == 200 && region.GetKeysWritten() == 100 && region.GetTerm() == 1 &&
			region.GetApproximateKeys() == 300 && region.GetApproximateSize() == 30 &&
			reflect.DeepEqual(region.GetLeader(), peers[0]) &&
			reflect.DeepEqual(region.GetInterval(), interval) && region.GetReadQueryNum() == 18 && region.GetWriteQueryNum() == 77 &&
			reflect.DeepEqual(region.GetDownPeers(), downPeers) && reflect.DeepEqual(region.GetPendingPeers(), pendingPeers)
	})
}

func (suite *serverTestSuite) TestStoreLimit() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	oc := tc.GetPrimaryServer().GetCluster().GetCoordinator().GetOperatorController()
	leaderServer := suite.pdLeader.GetServer()
	conf := leaderServer.GetReplicationConfig().Clone()
	conf.MaxReplicas = 1
	leaderServer.SetReplicationConfig(*conf)
	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	for i := uint64(1); i <= 2; i++ {
		resp, err := grpcPDClient.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://%d", i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	stream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	for i := uint64(2); i <= 10; i++ {
		peers := []*metapb.Peer{{Id: i, StoreId: 1}}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: []byte(fmt.Sprintf("t%d", i)),
				EndKey:   []byte(fmt.Sprintf("t%d", i+1)),
			},
			Leader:          peers[0],
			ApproximateSize: 10 * units.MiB,
		}
		err = stream.Send(regionReq)
		re.NoError(err)
	}

	leaderServer.GetRaftCluster().SetStoreLimit(1, storelimit.AddPeer, 60)
	leaderServer.GetRaftCluster().SetStoreLimit(1, storelimit.RemovePeer, 60)
	leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.AddPeer, 60)
	leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 60)
	// There is a time window between setting store limit in PD side and capturing the change in scheduling service.
	waitSyncFinish(re, tc, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
	checkOperatorFail(re, oc, op)

	leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.AddPeer, 120)
	waitSyncFinish(re, tc, storelimit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	leaderServer.GetRaftCluster().SetAllStoresLimit(storelimit.AddPeer, 60)
	waitSyncFinish(re, tc, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
	checkOperatorFail(re, oc, op)

	leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 60)
	waitSyncFinish(re, tc, storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		checkOperatorSuccess(re, oc, op)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	checkOperatorFail(re, oc, op)

	leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 120)
	waitSyncFinish(re, tc, storelimit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		checkOperatorSuccess(re, oc, op)
	}
	leaderServer.GetRaftCluster().SetAllStoresLimit(storelimit.RemovePeer, 60)
	waitSyncFinish(re, tc, storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		checkOperatorSuccess(re, oc, op)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	checkOperatorFail(re, oc, op)
}

func checkOperatorSuccess(re *require.Assertions, oc *operator.Controller, op *operator.Operator) {
	re.True(oc.AddOperator(op))
	re.True(oc.RemoveOperator(op))
	re.True(op.IsEnd())
	re.Equal(op, oc.GetOperatorStatus(op.RegionID()).Operator)
}

func checkOperatorFail(re *require.Assertions, oc *operator.Controller, op *operator.Operator) {
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))
}

func waitSyncFinish(re *require.Assertions, tc *tests.TestSchedulingCluster, typ storelimit.Type, expectedLimit float64) {
	testutil.Eventually(re, func() bool {
		return tc.GetPrimaryServer().GetCluster().GetSharedConfig().GetStoreLimitByType(2, typ) == expectedLimit
	})
}

type multipleServerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestMultipleServerTestSuite(t *testing.T) {
	suite.Run(t, new(multipleServerTestSuite))
}

func (suite *multipleServerTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 2)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
}

func (suite *multipleServerTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.cluster.Destroy()
	suite.cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func (suite *multipleServerTestSuite) TestReElectLeader() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	rc := suite.pdLeader.GetServer().GetRaftCluster()
	re.NotNil(rc)
	regionLen := 100
	regions := tests.InitRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	originLeaderName := suite.pdLeader.GetLeader().GetName()
	suite.pdLeader.ResignLeader()
	newLeaderName := suite.cluster.WaitLeader()
	re.NotEqual(originLeaderName, newLeaderName)

	suite.pdLeader = suite.cluster.GetServer(newLeaderName)
	suite.pdLeader.ResignLeader()
	newLeaderName = suite.cluster.WaitLeader()
	re.Equal(originLeaderName, newLeaderName)

	suite.pdLeader = suite.cluster.GetServer(newLeaderName)
	rc = suite.pdLeader.GetServer().GetRaftCluster()
	re.NotNil(rc)
	rc.IsPrepared()
}

func (suite *serverTestSuite) TestOnlineProgress() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	rc := suite.pdLeader.GetServer().GetRaftCluster()
	re.NotNil(rc)
	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}
	for i := uint64(1); i <= 3; i++ {
		resp, err := s.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://%d", i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}
	regionLen := 1000
	regions := tests.InitRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	time.Sleep(2 * time.Second)

	// add a new store
	resp, err := s.PutStore(
		context.Background(), &pdpb.PutStoreRequest{
			Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
			Store: &metapb.Store{
				Id:      4,
				Address: fmt.Sprintf("mock://%d", 4),
				State:   metapb.StoreState_Up,
				Version: "7.0.0",
			},
		},
	)
	re.NoError(err)
	re.Empty(resp.GetHeader().GetError())

	time.Sleep(2 * time.Second)
	for i, r := range regions {
		if i < 50 {
			r.GetMeta().Peers[2].StoreId = 4
			r.GetMeta().RegionEpoch.ConfVer = 2
			r.GetMeta().RegionEpoch.Version = 2
			err = rc.HandleRegionHeartbeat(r)
			re.NoError(err)
		}
	}
	time.Sleep(2 * time.Second)
	action, progress, ls, cs, err := rc.GetProgressByID("4")
	re.Equal("preparing", action)
	re.NotEmpty(progress)
	re.NotEmpty(cs)
	re.NotEmpty(ls)
	re.NoError(err)
	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *serverTestSuite) TestBatchSplit() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	rc := suite.pdLeader.GetServer().GetRaftCluster()
	re.NotNil(rc)
	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}
	for i := uint64(1); i <= 3; i++ {
		resp, err := s.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://%d", i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}
	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	stream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}

	interval := &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}
	regionReq := &pdpb.RegionHeartbeatRequest{
		Header:          testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
		Region:          &metapb.Region{Id: 10, Peers: peers, StartKey: []byte("a"), EndKey: []byte("b")},
		Leader:          peers[0],
		ApproximateSize: 30 * units.MiB,
		ApproximateKeys: 300,
		Interval:        interval,
		Term:            1,
		CpuUsage:        100,
	}
	err = stream.Send(regionReq)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		region := tc.GetPrimaryServer().GetCluster().GetRegion(10)
		return region != nil && region.GetTerm() == 1 &&
			region.GetApproximateKeys() == 300 && region.GetApproximateSize() == 30 &&
			reflect.DeepEqual(region.GetLeader(), peers[0]) &&
			reflect.DeepEqual(region.GetInterval(), interval)
	})

	req := &pdpb.AskBatchSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: suite.pdLeader.GetClusterID(),
		},
		Region:     regionReq.GetRegion(),
		SplitCount: 10,
	}

	resp, err := grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	re.Empty(resp.GetHeader().GetError())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))

	suite.TearDownSuite()
	suite.SetupSuite()
}
