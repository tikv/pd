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
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
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
}

func (suite *serverTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *serverTestSuite) TestBasic() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop"))
	}()
	tc, cleanup, err := tests.StartSingleRouterServerWithoutCheck(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	re.NoError(err)
	defer cleanup()
	// check sync work well
	testutil.Eventually(re, func() bool {
		return tc.IsReady()
	})
	url := tc.GetAdvertiseListenAddr() + "/status"
	resp, err := http.DefaultClient.Get(url)
	defer re.NoError(resp.Body.Close())
	re.Equal(http.StatusOK, resp.StatusCode)
	re.NoError(err)

	url = tc.GetAdvertiseListenAddr() + "/metrics"
	resp, err = http.DefaultClient.Get(url)
	re.Equal(http.StatusOK, resp.StatusCode)
	re.NoError(err)
	re.NotNil(resp)
	body, err := io.ReadAll(resp.Body)
	re.NoError(err)
	lines := strings.Split(string(body), "\n")
	var grpcMetrics []string
	for _, line := range lines {
		if strings.Contains(line, "grpc_server_handling_seconds_count") {
			grpcMetrics = append(grpcMetrics, line)
		}
	}
	re.NotEmpty(grpcMetrics)
	re.NoError(resp.Body.Close())
	// stop router server and ensure it can not serve requests
	tc.Close()
	testutil.Eventually(re, func() bool {
		return !tc.IsReady()
	})
}

func (suite *serverTestSuite) TestBasicSync() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop"))
	}()
	regions := tests.InitRegions(10)
	for _, region := range regions {
		re.NoError(suite.cluster.HandleRegionHeartbeat(region))
	}
	tc, cleanup, err := tests.StartSingleRouterServerWithoutCheck(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	re.NoError(err)
	defer cleanup()
	// check sync work well
	testutil.Eventually(re, func() bool {
		return tc.IsReady()
	})
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

	// stop router server and ensure it can not serve requests
	tc.Close()
	testutil.Eventually(re, func() bool {
		return !tc.IsReady()
	})
}
