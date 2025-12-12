// Copyright 2025 TiKV Project Authors.
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

package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/core"
	rs "github.com/tikv/pd/pkg/mcs/router/server"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
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

	tsoCleanup    context.CancelFunc
	routerCleanup context.CancelFunc
	routerServer  *rs.Server
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(serverTestSuite))
}

func (suite *serverTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 3)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
	// make pd client can work
	_, suite.tsoCleanup = tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())

	// init some regions
	regions := tests.InitRegions(10)
	for _, region := range regions {
		re.NoError(suite.cluster.HandleRegionHeartbeat(region))
	}

	for i := range 2 {
		store := &metapb.Store{
			Id:            uint64(i + 1),
			Address:       "mock://tikv-100:100",
			State:         metapb.StoreState_Up,
			Version:       versioninfo.MinSupportedVersion(versioninfo.Version2_0).String(),
			LastHeartbeat: time.Now().UnixNano(),
		}

		tests.MustPutStore(re, suite.cluster, store)
	}
}

func (suite *serverTestSuite) TearDownSuite() {
	suite.routerCleanup()
	suite.tsoCleanup()
	suite.cluster.Destroy()
	suite.routerServer.Close()
	suite.cancel()
}

func (suite *serverTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop", `return(true)`))
	var err error
	suite.routerServer, suite.routerCleanup, err = tests.StartSingleRouterServerWithoutCheck(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	re.NoError(err)
	// check sync work well
	testutil.Eventually(re, func() bool {
		return suite.routerServer.IsReady()
	})
}

func (suite *serverTestSuite) TearDownTest() {
	suite.routerCleanup()
	suite.Require().NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop"))
	testutil.Eventually(suite.Require(), func() bool {
		return !suite.routerServer.IsReady()
	})
}

func (suite *serverTestSuite) TestStoreAPI() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/customTimeout"))
	}()

	store1 := uint64(1)
	// make sure pd server can't support region grpc request
	cli, err := pd.NewClientWithContext(suite.ctx, caller.TestComponent,
		[]string{suite.backendEndpoints}, pd.SecurityOption{})
	re.NoError(err)
	_, err = cli.GetStore(suite.ctx, store1)
	re.Error(err)
	defer cli.Close()

	// test router store apis
	re.NoError(cli.UpdateOption(opt.EnableRouterServiceHandler, true))

	// wait the router service watch the store info
	testutil.Eventually(re, func() bool {
		store, err := cli.GetStore(suite.ctx, store1)
		if err != nil {
			return false
		}
		re.Equal(store1, store.GetId())
		return true
	})

	stores, err := cli.GetAllStores(suite.ctx)
	re.NoError(err)
	re.Len(stores, 2)

	re.NoError(cli.UpdateOption(opt.EnableRouterServiceHandler, false))
	_, err = cli.GetStore(suite.ctx, store1)
	re.Error(err)
}

func (suite *serverTestSuite) TestRegionAPI() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/customTimeout"))
	}()
	// make sure pd server can't support region grpc request
	pdcli, err := pd.NewClientWithContext(suite.ctx, caller.TestComponent,
		[]string{suite.backendEndpoints}, pd.SecurityOption{})
	re.NoError(err)
	_, err = pdcli.GetRegionByID(suite.ctx, 1)
	re.Error(err)
	pdcli.Close()

	// test region apis
	cli, err := pd.NewClientWithContext(suite.ctx, caller.TestComponent,
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, opt.WithEnableRouterServiceHandler(true))
	re.NoError(err)
	suite.checkRegionAPI(cli)
	cli.Close()

	// test region apis with router client enabled
	cli2, err := pd.NewClientWithContext(suite.ctx, caller.TestComponent,
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, opt.WithEnableRouterServiceHandler(true), opt.WithEnableRouterClient(true))
	re.NoError(err)
	suite.checkRegionAPI(cli2)
	cli2.Close()
}

func (suite *serverTestSuite) checkRegionAPI(cli pd.Client) {
	re := suite.Require()

	// get region by id
	allowEnableRouterOpt := opt.WithAllowRouterServiceHandle()
	regionID := uint64(1)
	r1, err := cli.GetRegionByID(suite.ctx, regionID, allowEnableRouterOpt)
	re.NoError(err)
	re.Equal(regionID, r1.Meta.Id)

	// get region by key
	r2, err := cli.GetRegion(suite.ctx, r1.Meta.GetStartKey(), allowEnableRouterOpt)
	re.NoError(err)
	re.Equal(regionID, r2.Meta.Id)

	// get prev region by key
	r3, err := cli.GetPrevRegion(suite.ctx, r1.Meta.GetEndKey(), allowEnableRouterOpt)
	re.NoError(err)
	re.Equal(regionID, r3.Meta.Id)
	// batch scan regions
	regionsResp, err := cli.BatchScanRegions(suite.ctx, []router.KeyRange{{StartKey: []byte(""), EndKey: []byte("")}}, 0, allowEnableRouterOpt)
	re.NoError(err)
	re.GreaterOrEqual(len(regionsResp), 10)
}

func (suite *serverTestSuite) TestBasicSync() {
	re := suite.Require()
	tc := suite.routerServer.GetCluster()
	newRegion := tc.GetBasicCluster().GetRegion(1)
	epoch := newRegion.GetRegionEpoch()
	re.Equal(uint64(1), epoch.GetVersion())

	// change region1 and ensure this changes can sync to the router server
	newRegion = newRegion.Clone(core.WithIncVersion())
	re.NoError(suite.cluster.HandleRegionHeartbeat(newRegion))
	testutil.Eventually(re, func() bool {
		newEpoch := tc.GetBasicCluster().GetRegion(1).GetRegionEpoch()
		return newEpoch.GetVersion() == newRegion.GetRegionEpoch().GetVersion()
	})

	// resign pd leader to ensure router can reconnect to new leader
	testutil.Eventually(re, func() bool {
		return suite.cluster.ResignLeader() == nil
	})
	suite.cluster.WaitLeader()
	newRegion = newRegion.Clone(core.WithIncVersion())
	re.NoError(suite.cluster.HandleRegionHeartbeat(newRegion))
	testutil.Eventually(re, func() bool {
		newEpoch := tc.GetBasicCluster().GetRegion(1).GetRegionEpoch()
		return newEpoch.GetVersion() == newRegion.GetRegionEpoch().GetVersion()
	})

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/router/server/syncMetError", `1*return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/router/server/syncMetError"))
	}()
	newRegion = newRegion.Clone(core.WithIncVersion())
	re.NoError(suite.cluster.HandleRegionHeartbeat(newRegion))
	testutil.Eventually(re, func() bool {
		newEpoch := tc.GetBasicCluster().GetRegion(1).GetRegionEpoch()
		return newEpoch.GetVersion() == newRegion.GetRegionEpoch().GetVersion()
	})
}
