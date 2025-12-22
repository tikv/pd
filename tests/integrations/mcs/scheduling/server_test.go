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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
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
}

func (suite *serverTestSuite) TestAllocID() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
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
	cluster := tc.GetPrimaryServer().GetCluster()
	id, _, err := cluster.AllocID(1)
	re.NoError(err)
	re.NotEqual(uint64(0), id)
	err = suite.cluster.ResignLeader()
	re.NoError(err)
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
	expectedSchedulerCount := len(types.DefaultSchedulers)
	testutil.Eventually(re, func() bool {
		watchedAddr, ok := suite.pdLeader.GetServicePrimaryAddr(suite.ctx, constant.SchedulingServiceName)
		return ok && oldPrimaryAddr == watchedAddr &&
			len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames()) == expectedSchedulerCount
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
			len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames()) == expectedSchedulerCount
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
				Address: "mock://tikv-1:1",
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
	err := leaderServer.SetMicroserviceConfig(*conf)
	re.NoError(err)
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
	err := leaderServer.SetMicroserviceConfig(*conf)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !suite.pdLeader.GetServer().GetRaftCluster().IsSchedulingControllerRunning()
	})
	// Enable scheduling service fallback again, the PD will restart scheduling.
	conf.EnableSchedulingFallback = true
	err = leaderServer.SetMicroserviceConfig(*conf)
	re.NoError(err)
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
	err = leaderServer.SetMicroserviceConfig(*conf)
	re.NoError(err)
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
	// Skip the prepare checker to avoid waiting for region collection.
	tc.GetPrimaryServer().GetCluster().SetPrepared()
	schedulersController := tc.GetPrimaryServer().GetCluster().GetCoordinator().GetSchedulersController()
	checkEvictLeaderSchedulerExist(re, schedulersController, false)
	// Add a new evict-leader-scheduler through the PD.
	tests.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	// Check if the evict-leader-scheduler is added.
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	// Add a store_id to the evict-leader-scheduler through the PD.
	err = suite.pdLeader.GetServer().GetRaftCluster().PutMetaStore(
		&metapb.Store{
			Id:            2,
			Address:       "mock://tikv-2:2",
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Version:       "7.0.0",
		},
	)
	re.NoError(err)
	tests.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 2,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})
	// Delete a store_id from the evict-leader-scheduler through the PD.
	tests.MustDeleteScheduler(re, suite.backendEndpoints, fmt.Sprintf("%s-%d", types.EvictLeaderScheduler.String(), 1))
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{2})
	// Add a store_id to the evict-leader-scheduler through the PD by the scheduler handler.
	tests.MustCallSchedulerConfigAPI(re, http.MethodPost, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"config"}, map[string]any{
		"name":     types.EvictLeaderScheduler.String(),
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1, 2})
	// Delete a store_id from the evict-leader-scheduler through the PD by the scheduler handler.
	tests.MustCallSchedulerConfigAPI(re, http.MethodDelete, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"delete", "2"}, nil)
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	// If the last store is deleted, the scheduler should be removed.
	tests.MustCallSchedulerConfigAPI(re, http.MethodDelete, suite.backendEndpoints, types.EvictLeaderScheduler.String(), []string{"delete", "1"}, nil)
	// Check if the scheduler is removed.
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	// Delete the evict-leader-scheduler through the PD by removing the last store_id.
	tests.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	tests.MustDeleteScheduler(re, suite.backendEndpoints, fmt.Sprintf("%s-%d", types.EvictLeaderScheduler.String(), 1))
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	// Delete the evict-leader-scheduler through the PD.
	tests.MustAddScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	checkEvictLeaderSchedulerExist(re, schedulersController, true)
	checkEvictLeaderStoreIDs(re, schedulersController, []uint64{1})
	tests.MustDeleteScheduler(re, suite.backendEndpoints, types.EvictLeaderScheduler.String())
	checkEvictLeaderSchedulerExist(re, schedulersController, false)

	defaultSchedulerNames := []string{
		types.BalanceLeaderScheduler.String(),
		types.BalanceRegionScheduler.String(),
		types.BalanceHotRegionScheduler.String(),
	}
	checkDisabled := func(name string, shouldDisabled bool) {
		testutil.Eventually(re, func() bool {
			if schedulersController.GetScheduler(name) == nil {
				return false
			}
			disabled, err := schedulersController.IsSchedulerDisabled(name)
			re.NoError(err, name)
			return disabled == shouldDisabled
		})
	}
	for _, name := range defaultSchedulerNames {
		checkDisabled(name, false)
		tests.MustDeleteScheduler(re, suite.backendEndpoints, name)
	}

	for _, name := range defaultSchedulerNames {
		testutil.Eventually(re, func() bool {
			return schedulersController.GetScheduler(name) == nil
		})
	}

	for _, name := range defaultSchedulerNames {
		tests.MustAddScheduler(re, suite.backendEndpoints, name, nil)
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

func (suite *serverTestSuite) TestRemoveScheduler() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	// Skip the prepare checker to avoid waiting for region collection.
	tc.GetPrimaryServer().GetCluster().SetPrepared()

	schedulersController := tc.GetPrimaryServer().GetCluster().GetCoordinator().GetSchedulersController()

	// Check the default schedulers exist.
	defaultSchedulerNames := []string{
		types.BalanceLeaderScheduler.String(),
		types.BalanceRegionScheduler.String(),
		types.BalanceHotRegionScheduler.String(),
		types.EvictSlowStoreScheduler.String(),
	}
	checkSchedulerExist := func(name string, shouldExist bool) {
		testutil.Eventually(re, func() bool {
			exist := schedulersController.GetScheduler(name) != nil
			return exist == shouldExist
		})
	}

	for _, name := range defaultSchedulerNames {
		checkSchedulerExist(name, true)
	}

	// Disable evict-slow-store-scheduler by calling DELETE API.
	// For the scheduling cluster, when a default scheduler is marked as disabled in config,
	// the updateScheduler goroutine will detect this and remove the scheduler from the controller.
	tests.MustDeleteScheduler(re, suite.backendEndpoints, types.EvictSlowStoreScheduler.String())

	// Wait and verify the scheduler is removed from the controller.
	checkSchedulerExist(types.EvictSlowStoreScheduler.String(), false)

	// Re-enable the scheduler by calling ADD API.
	tests.MustAddScheduler(re, suite.backendEndpoints, types.EvictSlowStoreScheduler.String(), nil)

	// Verify the scheduler exists again in the controller.
	checkSchedulerExist(types.EvictSlowStoreScheduler.String(), true)
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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

func (suite *serverTestSuite) TestForwardReportBuckets() {
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())

	// First create a region via region heartbeat
	heartbeatStream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}

	regionReq := &pdpb.RegionHeartbeatRequest{
		Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
		Region: &metapb.Region{
			Id:       10,
			Peers:    peers,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
		Leader:          peers[0],
		ApproximateSize: 30 * units.MiB,
		ApproximateKeys: 300,
	}
	err = heartbeatStream.Send(regionReq)
	re.NoError(err)

	// Wait for the region to be created
	testutil.Eventually(re, func() bool {
		region := tc.GetPrimaryServer().GetCluster().GetRegion(10)
		return region != nil && region.GetApproximateKeys() == 300
	})

	// Now test ReportBuckets forwarding with multiple requests
	bucketStream, err := grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)

	// Send multiple bucket reports to test streaming
	for version := uint64(1); version <= 3; version++ {
		// Create bucket stats with increasing read/write metrics for each version
		bucketStats := &metapb.BucketStats{
			ReadBytes:  []uint64{100 * version, 200 * version, 150 * version},
			ReadKeys:   []uint64{10 * version, 20 * version, 15 * version},
			ReadQps:    []uint64{5 * version, 10 * version, 8 * version},
			WriteBytes: []uint64{50 * version, 100 * version, 75 * version},
			WriteKeys:  []uint64{5 * version, 10 * version, 7 * version},
			WriteQps:   []uint64{2 * version, 5 * version, 3 * version},
		}

		buckets := &metapb.Buckets{
			RegionId:   10,
			Version:    version,
			Keys:       [][]byte{[]byte("a"), []byte("m"), []byte("r"), []byte("z")},
			Stats:      bucketStats,
			PeriodInMs: 10000, // 10 seconds
		}

		bucketReq := &pdpb.ReportBucketsRequest{
			Header:      testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Buckets:     buckets,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: version},
		}

		err = bucketStream.Send(bucketReq)
		re.NoError(err)

		// Wait a bit between requests to simulate real streaming
		time.Sleep(100 * time.Millisecond)
	}

	// Close the stream (ReportBuckets is fire-and-forget)
	err = bucketStream.CloseSend()
	re.NoError(err)

	// Verify the buckets are processed by the scheduling server
	// The scheduling server should receive and process the bucket keys and version
	// Should have the latest version (3) after all requests
	testutil.Eventually(re, func() bool {
		// Check if the scheduling server has the region and bucket information
		region := tc.GetPrimaryServer().GetCluster().GetRegion(10)
		return region != nil && region.GetBuckets() != nil &&
			region.GetBuckets().GetVersion() == 3 &&
			len(region.GetBuckets().GetKeys()) == 4
	})

	// Verify bucket keys are properly stored (stats are not stored in scheduling server)
	// Should contain the final version (3) with correct keys
	testutil.Eventually(re, func() bool {
		region := tc.GetPrimaryServer().GetCluster().GetRegion(10)
		if region == nil || region.GetBuckets() == nil {
			return false
		}
		buckets := region.GetBuckets()
		return buckets.GetRegionId() == 10 &&
			buckets.GetVersion() == 3 &&
			len(buckets.GetKeys()) == 4 &&
			string(buckets.GetKeys()[0]) == "a" &&
			string(buckets.GetKeys()[3]) == "z"
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
	err = leaderServer.SetReplicationConfig(*conf)
	re.NoError(err)
	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	for i := uint64(1); i <= 2; i++ {
		resp, err := grpcPDClient.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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

	err = leaderServer.GetRaftCluster().SetStoreLimit(1, storelimit.AddPeer, 60)
	re.NoError(err)
	err = leaderServer.GetRaftCluster().SetStoreLimit(1, storelimit.RemovePeer, 60)
	re.NoError(err)
	err = leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.AddPeer, 60)
	re.NoError(err)
	err = leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 60)
	re.NoError(err)
	// There is a time window between setting store limit in PD side and capturing the change in scheduling service.
	waitSyncFinish(re, tc, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
	checkOperatorFail(re, oc, op)

	err = leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.AddPeer, 120)
	re.NoError(err)
	waitSyncFinish(re, tc, storelimit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	err = leaderServer.GetRaftCluster().SetAllStoresLimit(storelimit.AddPeer, 60)
	re.NoError(err)
	waitSyncFinish(re, tc, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
		checkOperatorSuccess(re, oc, op)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 100})
	checkOperatorFail(re, oc, op)

	err = leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 60)
	re.NoError(err)
	waitSyncFinish(re, tc, storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		checkOperatorSuccess(re, oc, op)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	checkOperatorFail(re, oc, op)

	err = leaderServer.GetRaftCluster().SetStoreLimit(2, storelimit.RemovePeer, 120)
	re.NoError(err)
	waitSyncFinish(re, tc, storelimit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		checkOperatorSuccess(re, oc, op)
	}
	err = leaderServer.GetRaftCluster().SetAllStoresLimit(storelimit.RemovePeer, 60)
	re.NoError(err)
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
	err = suite.pdLeader.ResignLeader()
	re.NoError(err)
	newLeaderName := suite.cluster.WaitLeader()
	re.NotEqual(originLeaderName, newLeaderName)

	suite.pdLeader = suite.cluster.GetServer(newLeaderName)
	err = suite.pdLeader.ResignLeader()
	re.NoError(err)
	newLeaderName = suite.cluster.WaitLeader()
	re.Equal(originLeaderName, newLeaderName)

	suite.pdLeader = suite.cluster.GetServer(newLeaderName)
	rc = suite.pdLeader.GetServer().GetRaftCluster()
	re.NotNil(rc)
	rc.IsPrepared()
}

func (suite *serverTestSuite) TestPrepareChecker() {
	re := suite.Require()

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	// Create stores
	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}
	for i := uint64(1); i <= 3; i++ {
		resp, err := s.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	// Send enough regions to satisfy the prepare checker in the initial cluster
	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	stream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)

	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}

	// Send many regions to ensure the initial scheduling cluster's prepare checker is satisfied
	for i := uint64(1); i <= 100; i++ {
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: []byte(fmt.Sprintf("k%d", i)),
				EndKey:   []byte(fmt.Sprintf("k%d", i+1)),
			},
			Leader:          peers[0],
			ApproximateSize: 10 * units.MiB,
			ApproximateKeys: 100,
		}
		err = stream.Send(regionReq)
		re.NoError(err)
	}

	// Wait for the initial cluster's prepare checker to be satisfied
	primary1 := tc.GetPrimaryServer()
	cluster1 := primary1.GetCluster()
	coordinator1 := cluster1.GetCoordinator()
	prepareChecker1 := coordinator1.GetPrepareChecker()

	testutil.Eventually(re, func() bool {
		return prepareChecker1.IsPrepared()
	})

	re.True(prepareChecker1.IsPrepared(), "initial cluster's prepare checker should be prepared")
	re.True(coordinator1.ShouldRun(), "initial cluster's coordinator should run when prepare checker is ready")

	primary1.Close()

	// Wait for a new primary to be elected from the remaining servers
	testutil.Eventually(re, func() bool {
		primary2 := tc.GetPrimaryServer()
		return primary2 != nil && primary2 != primary1
	})

	// Get the new primary server
	primary2 := tc.GetPrimaryServer()
	re.NotNil(primary2)

	// Get the current cluster components
	cluster2 := primary2.GetCluster()
	coordinator2 := cluster2.GetCoordinator()
	prepareChecker2 := coordinator2.GetPrepareChecker()

	// After closing, the new scheduling server should NOT be prepared
	// because it hasn't received enough fresh region heartbeats
	re.False(prepareChecker2.IsPrepared())
	re.False(coordinator2.ShouldRun())

	// Send only a small number of region heartbeats (insufficient for prepare checker)
	for i := uint64(1); i <= 50; i++ {
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: []byte(fmt.Sprintf("k%d", i)),
				EndKey:   []byte(fmt.Sprintf("k%d", i+1)),
			},
			Leader:          peers[0],
			ApproximateSize: 10 * units.MiB,
			ApproximateKeys: 100,
		}
		err = stream.Send(regionReq)
		re.NoError(err)
	}

	// Wait a bit to ensure the prepare checker has time to process the incoming heartbeats
	time.Sleep(2 * time.Second)

	// The prepare checker should still be checking and potentially not satisfied
	shouldRun := coordinator2.ShouldRun()
	isPrepared := prepareChecker2.IsPrepared()

	// The coordinator's readiness should be controlled by the prepare checker
	re.False(isPrepared)
	re.False(shouldRun)

	// Send enough regions to eventually satisfy the prepare checker
	for i := uint64(51); i <= 100; i++ {
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: []byte(fmt.Sprintf("k%d", i)),
				EndKey:   []byte(fmt.Sprintf("k%d", i+1)),
			},
			Leader:          peers[0],
			ApproximateSize: 10 * units.MiB,
			ApproximateKeys: 100,
		}
		err = stream.Send(regionReq)
		re.NoError(err)
	}

	// Finally verify that once prepared, the coordinator should be ready to run
	testutil.Eventually(re, func() bool {
		return prepareChecker2.IsPrepared()
	})

	re.True(coordinator2.ShouldRun())
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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
				Address: "mock://tikv-4:4",
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
	p, err := rc.GetProgressByID(4)
	re.Equal("preparing", string(p.Action))
	re.NotEmpty(p.ProgressPercent)
	re.NotEmpty(p.CurrentSpeed)
	re.NotEmpty(p.LeftSecond)
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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

func (suite *serverTestSuite) TestBatchSplitCompatibility() {
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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

	// case 1: The scheduling server is upgraded first, and then the PD server is upgraded.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/handleAllocIDNonBatch", `return(true)`))
	resp, err := grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	allocatedIDs := map[uint64]struct{}{}
	var maxID uint64
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 40)
	// Use the batch AllocID, which means the PD has finished the upgrade.
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/handleAllocIDNonBatch"))
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 80)

	// case 2: The PD server is downgraded first.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/handleAllocIDNonBatch", `return(true)`))
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 120)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/handleAllocIDNonBatch"))
	// Use the batch AllocID, which means the scheduling server has finished the upgrade.
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 160)

	// case 3: The PD server is upgraded first, and then the scheduling server is upgraded.
	re.NoError(failpoint.Enable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch", `return(true)`))
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 200)
	re.NoError(failpoint.Disable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch"))
	// Use the batch AllocID, which means the scheduling server has finished the upgrade.
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 240)

	// case 4: The scheduling server is downgraded first.
	re.NoError(failpoint.Enable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch", `return(true)`))
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	maxID = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 280)
	re.NoError(failpoint.Disable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch"))
	// Use the batch AllocID, which means the scheduling server has finished the upgrade.
	resp, err = grpcPDClient.AskBatchSplit(suite.ctx, req)
	re.NoError(err)
	_ = checkAllocatedID(re, resp, allocatedIDs, maxID)
	re.Len(allocatedIDs, 320)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))

	suite.TearDownSuite()
	suite.SetupSuite()
}

func checkAllocatedID(re *require.Assertions, resp *pdpb.AskBatchSplitResponse, allocatedIDs map[uint64]struct{}, maxID uint64) uint64 {
	re.Empty(resp.GetHeader().GetError())
	for _, id := range resp.GetIds() {
		_, ok := allocatedIDs[id.NewRegionId]
		re.False(ok)
		re.Greater(id.NewRegionId, maxID)
		maxID = id.NewRegionId
		allocatedIDs[id.NewRegionId] = struct{}{}
		for _, peer := range id.NewPeerIds {
			_, ok := allocatedIDs[peer]
			re.False(ok)
			re.Greater(peer, maxID)
			maxID = peer
			allocatedIDs[peer] = struct{}{}
		}
	}
	return maxID
}

func (suite *serverTestSuite) TestConcurrentBatchSplit() {
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
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
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

	// case 1: The scheduling server is upgraded first, PD server has not been upgraded.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/handleAllocIDNonBatch", `return(true)`))
	suite.checkConcurrentAllocatedID(re, req, grpcPDClient)

	// case 2: The scheduling server is upgraded first, PD server has been upgraded.
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/handleAllocIDNonBatch"))
	suite.checkConcurrentAllocatedID(re, req, grpcPDClient)

	// case 3: The PD server is upgraded first, scheduling server has not been upgraded.
	re.NoError(failpoint.Enable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch", `return(true)`))
	suite.checkConcurrentAllocatedID(re, req, grpcPDClient)

	// case 4: The PD server is upgraded first, scheduling server has been upgraded.
	re.NoError(failpoint.Disable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch"))
	suite.checkConcurrentAllocatedID(re, req, grpcPDClient)

	// case 5: Both the PD server and scheduling server has not been upgraded.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/handleAllocIDNonBatch", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch", `return(true)`))
	suite.checkConcurrentAllocatedID(re, req, grpcPDClient)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/handleAllocIDNonBatch"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/mcs/scheduling/server/allocIDNonBatch"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))

	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *serverTestSuite) checkConcurrentAllocatedID(re *require.Assertions, req *pdpb.AskBatchSplitRequest, grpcPDClient pdpb.PDClient) {
	var wg sync.WaitGroup
	var allocatedIDs sync.Map
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := grpcPDClient.AskBatchSplit(suite.ctx, req)
			re.NoError(err)
			re.Empty(resp.GetHeader().GetError())
			for _, id := range resp.GetIds() {
				_, ok := allocatedIDs.Load(id.NewRegionId)
				re.False(ok)
				allocatedIDs.Store(id.NewRegionId, struct{}{})
				for _, peer := range id.NewPeerIds {
					_, ok := allocatedIDs.Load(peer)
					re.False(ok)
					allocatedIDs.Store(peer, struct{}{})
				}
			}
		}()
	}
	wg.Wait()
	var len int
	allocatedIDs.Range(func(_, _ any) bool {
		len++
		return true
	})
	re.Equal(4000, len)
}

func (suite *serverTestSuite) TestForwardSplitRegion() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	s := &server.GrpcServer{Server: suite.pdLeader.GetServer()}

	// Create stores
	for i := uint64(1); i <= 3; i++ {
		resp, err := s.PutStore(
			context.Background(), &pdpb.PutStoreRequest{
				Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
				Store: &metapb.Store{
					Id:      i,
					Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
					State:   metapb.StoreState_Up,
					Version: "7.0.0",
				},
			},
		)
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	// Create a region via region heartbeat
	grpcPDClient := testutil.MustNewGrpcClient(re, suite.pdLeader.GetServer().GetAddr())
	stream, err := grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)

	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}

	regionReq := &pdpb.RegionHeartbeatRequest{
		Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
		Region: &metapb.Region{
			Id:       100,
			Peers:    peers,
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
		Leader:          peers[0],
		ApproximateSize: 100 * units.MiB,
		ApproximateKeys: 1000,
	}
	err = stream.Send(regionReq)
	re.NoError(err)

	// Wait for the region to be created in scheduling cluster
	testutil.Eventually(re, func() bool {
		region := tc.GetPrimaryServer().GetCluster().GetRegion(100)
		return region != nil && region.GetApproximateSize() == 100
	})

	// Test SplitRegions request
	splitReq := &pdpb.SplitRegionsRequest{
		Header: &pdpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
		SplitKeys: [][]byte{
			[]byte("m"), // Split key in the middle
		},
		RetryLimit: 3,
	}

	go func() {
		// make sure the region heartbeat is sent after the SplitRegions request
		time.Sleep(time.Second)

		regionReq = &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:    100,
				Peers: peers,
				RegionEpoch: &metapb.RegionEpoch{
					Version: 1,
				},
				StartKey: []byte(""),
				EndKey:   []byte("m"),
			},
			Leader:          peers[0],
			ApproximateSize: 100 * units.MiB,
			ApproximateKeys: 1000,
		}
		re.NoError(stream.Send(regionReq))
		regionReq = &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(suite.pdLeader.GetClusterID()),
			Region: &metapb.Region{
				Id:    101,
				Peers: peers,
				RegionEpoch: &metapb.RegionEpoch{
					Version: 1,
				},
				StartKey: []byte("m"),
				EndKey:   []byte(""),
			},
			Leader:          peers[0],
			ApproximateSize: 100 * units.MiB,
			ApproximateKeys: 1000,
		}
		re.NoError(stream.Send(regionReq))
	}()

	// Forward SplitRegions request through PD to scheduling service
	splitResp, err := grpcPDClient.SplitRegions(suite.ctx, splitReq)
	re.NoError(err)
	re.Empty(splitResp.GetHeader().GetError())

	// The response should contain the finished percentage
	re.Equal(uint64(100), splitResp.GetFinishedPercentage())
	// Should have regions IDs for the split operation
	re.Equal([]uint64{101}, splitResp.GetRegionsId())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
}

func (suite *serverTestSuite) TestRegionBucketsStoreNotFound() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	addr := strings.TrimPrefix(tc.GetPrimaryServer().GetAddr(), "http://")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	defer conn.Close()

	schedulingClient := schedulingpb.NewSchedulingClient(conn)
	stream, err := schedulingClient.RegionBuckets(suite.ctx)
	re.NoError(err)

	// Create buckets for a region that doesn't exist in the cluster
	// This will trigger the store == nil condition because GetLeaderStoreByRegionID returns nil
	buckets := &metapb.Buckets{
		RegionId:   999, // Non-existent region ID
		Version:    1,
		Keys:       [][]byte{[]byte("key1"), []byte("key2")},
		PeriodInMs: 1000,
	}

	bucketsReq := &schedulingpb.RegionBucketsRequest{
		Header:  &schedulingpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
		Buckets: buckets,
	}

	// This should not return an error - the server will log a warning and continue
	err = stream.Send(bucketsReq)
	re.NoError(err)

	// Send another valid request to ensure the stream is still working after the store == nil case
	validBuckets := &metapb.Buckets{
		RegionId:   1000, // Another non-existent region
		Version:    1,
		Keys:       [][]byte{[]byte("key3"), []byte("key4")},
		PeriodInMs: 1000,
	}

	validReq := &schedulingpb.RegionBucketsRequest{
		Header:  &schedulingpb.RequestHeader{ClusterId: suite.pdLeader.GetClusterID()},
		Buckets: validBuckets,
	}

	err = stream.Send(validReq)
	re.NoError(err)
}
