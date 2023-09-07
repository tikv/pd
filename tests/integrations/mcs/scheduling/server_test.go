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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	mcs "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
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
