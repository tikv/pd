// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	mcs "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/server/api"
	"go.uber.org/goleak"
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

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 3)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	suite.NoError(suite.pdLeader.BootstrapCluster())
}

func (suite *serverTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *serverTestSuite) TestAllocID() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	time.Sleep(200 * time.Millisecond)
	id, err := tc.GetPrimaryServer().GetCluster().AllocID()
	re.NoError(err)
	re.NotEqual(uint64(0), id)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
}

func (suite *serverTestSuite) TestAllocIDAfterLeaderChange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember", `return(true)`))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	time.Sleep(200 * time.Millisecond)
	cluster := tc.GetPrimaryServer().GetCluster()
	id, err := cluster.AllocID()
	re.NoError(err)
	re.NotEqual(uint64(0), id)
	suite.cluster.ResignLeader()
	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	time.Sleep(time.Second)
	id1, err := cluster.AllocID()
	re.NoError(err)
	re.Greater(id1, id)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/fastUpdateMember"))
	// Update the pdLeader in test suite.
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *serverTestSuite) TestPrimaryChange() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	primary := tc.GetPrimaryServer()
	oldPrimaryAddr := primary.GetAddr()
	re.Len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames(), 5)
	testutil.Eventually(re, func() bool {
		watchedAddr, ok := suite.pdLeader.GetServicePrimaryAddr(suite.ctx, mcs.SchedulingServiceName)
		return ok && oldPrimaryAddr == watchedAddr
	})
	// transfer leader
	primary.Close()
	tc.WaitForPrimaryServing(re)
	primary = tc.GetPrimaryServer()
	newPrimaryAddr := primary.GetAddr()
	re.NotEqual(oldPrimaryAddr, newPrimaryAddr)
	re.Len(primary.GetCluster().GetCoordinator().GetSchedulersController().GetSchedulerNames(), 5)
	testutil.Eventually(re, func() bool {
		watchedAddr, ok := suite.pdLeader.GetServicePrimaryAddr(suite.ctx, mcs.SchedulingServiceName)
		return ok && newPrimaryAddr == watchedAddr
	})
}

func (suite *serverTestSuite) TestForwardStoreHeartbeat() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.backendEndpoints)
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
	testutil.Eventually(re, func() bool {
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

func (suite *serverTestSuite) TestSchedulerSync() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)
	schedulersController := tc.GetPrimaryServer().GetCluster().GetCoordinator().GetSchedulersController()
	re.Len(schedulersController.GetSchedulerNames(), 5)
	re.Nil(schedulersController.GetScheduler(schedulers.EvictLeaderName))
	// Add a new evict-leader-scheduler through the API server.
	api.MustAddScheduler(re, suite.backendEndpoints, schedulers.EvictLeaderName, map[string]interface{}{
		"store_id": 1,
	})
	// Check if the evict-leader-scheduler is added.
	testutil.Eventually(re, func() bool {
		return len(schedulersController.GetSchedulerNames()) == 6 &&
			schedulersController.GetScheduler(schedulers.EvictLeaderName) != nil
	})
	handler, ok := schedulersController.GetSchedulerHandlers()[schedulers.EvictLeaderName]
	re.True(ok)
	h, ok := handler.(interface {
		EvictStoreIDs() []uint64
	})
	re.True(ok)
	re.ElementsMatch(h.EvictStoreIDs(), []uint64{1})
	// Update the evict-leader-scheduler through the API server.
	err = suite.pdLeader.GetServer().GetRaftCluster().PutStore(
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
	api.MustAddScheduler(re, suite.backendEndpoints, schedulers.EvictLeaderName, map[string]interface{}{
		"store_id": 2,
	})
	var evictStoreIDs []uint64
	testutil.Eventually(re, func() bool {
		evictStoreIDs = h.EvictStoreIDs()
		return len(evictStoreIDs) == 2
	})
	re.ElementsMatch(evictStoreIDs, []uint64{1, 2})
	api.MustDeleteScheduler(re, suite.backendEndpoints, fmt.Sprintf("%s-%d", schedulers.EvictLeaderName, 1))
	testutil.Eventually(re, func() bool {
		evictStoreIDs = h.EvictStoreIDs()
		return len(evictStoreIDs) == 1
	})
	re.ElementsMatch(evictStoreIDs, []uint64{2})
	// Remove the evict-leader-scheduler through the API server.
	api.MustDeleteScheduler(re, suite.backendEndpoints, schedulers.EvictLeaderName)
	// Check if the scheduler is removed.
	testutil.Eventually(re, func() bool {
		return len(schedulersController.GetSchedulerNames()) == 5 &&
			schedulersController.GetScheduler(schedulers.EvictLeaderName) == nil
	})

	// TODO: test more schedulers.
}
