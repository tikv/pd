// Copyright 2020 TiKV Project Authors.
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

package dashboard_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type dashboardTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	httpClient *http.Client
}

func TestDashboardTestSuite(t *testing.T) {
	suite.Run(t, new(dashboardTestSuite))
}

func (suite *dashboardTestSuite) SetupSuite() {
	dashboard.SetCheckInterval(10 * time.Millisecond)
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.httpClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// ErrUseLastResponse can be returned by Client.CheckRedirect hooks to
			// control how redirects are processed. If returned, the next request
			// is not sent and the most recent response is returned with its body
			// unclosed.
			return http.ErrUseLastResponse
		},
	}
}

func (suite *dashboardTestSuite) TearDownSuite() {
	suite.cancel()
	suite.httpClient.CloseIdleConnections()
	dashboard.SetCheckInterval(time.Second)
}

func (suite *dashboardTestSuite) TestDashboardRedirect() {
	suite.testDashboard(false)
}

func (suite *dashboardTestSuite) TestDashboardProxy() {
	suite.testDashboard(true)
}

func (suite *dashboardTestSuite) checkRespCode(url string, code int) {
	resp, err := suite.httpClient.Get(url)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(code, resp.StatusCode)
}

func waitForConfigSync() {
	time.Sleep(time.Second)
}

func (suite *dashboardTestSuite) checkServiceIsStarted(internalProxy bool, servers map[string]*tests.TestServer, leader *tests.TestServer) string {
	waitForConfigSync()
	dashboardAddress := leader.GetServer().GetPersistOptions().GetDashboardAddress()
	hasServiceNode := false
	for _, srv := range servers {
		suite.Equal(dashboardAddress, srv.GetPersistOptions().GetDashboardAddress())
		addr := srv.GetAddr()
		if addr == dashboardAddress || internalProxy {
			suite.checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusOK)
			suite.checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusUnauthorized)
			if addr == dashboardAddress {
				hasServiceNode = true
			}
		} else {
			suite.checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusTemporaryRedirect)
			suite.checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusTemporaryRedirect)
		}
	}
	suite.True(hasServiceNode)
	return dashboardAddress
}

func (suite *dashboardTestSuite) checkServiceIsStopped(servers map[string]*tests.TestServer) {
	waitForConfigSync()
	for _, srv := range servers {
		suite.Equal("none", srv.GetPersistOptions().GetDashboardAddress())
		addr := srv.GetAddr()
		suite.checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusNotFound)
		suite.checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusNotFound)
	}
}

func (suite *dashboardTestSuite) testDashboard(internalProxy bool) {
	cluster, err := tests.NewTestCluster(suite.ctx, 3, func(conf *config.Config, serverName string) {
		conf.Dashboard.InternalProxy = internalProxy
	})
	suite.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	suite.NoError(err)

	cmd := pdctlCmd.GetRootCmd()

	cluster.WaitLeader()
	servers := cluster.GetServers()
	leader := cluster.GetLeaderServer()
	leaderAddr := leader.GetAddr()

	// auto select node
	dashboardAddress1 := suite.checkServiceIsStarted(internalProxy, servers, leader)

	// pd-ctl set another addr
	var dashboardAddress2 string
	for _, srv := range servers {
		if srv.GetAddr() != dashboardAddress1 {
			dashboardAddress2 = srv.GetAddr()
			break
		}
	}
	args := []string{"-u", leaderAddr, "config", "set", "dashboard-address", dashboardAddress2}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	suite.NoError(err)
	suite.checkServiceIsStarted(internalProxy, servers, leader)
	suite.Equal(dashboardAddress2, leader.GetServer().GetPersistOptions().GetDashboardAddress())

	// pd-ctl set stop
	args = []string{"-u", leaderAddr, "config", "set", "dashboard-address", "none"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	suite.NoError(err)
	suite.checkServiceIsStopped(servers)
}
