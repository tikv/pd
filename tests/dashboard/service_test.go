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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
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
		CheckRedirect: func(*http.Request, []*http.Request) error {
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
	suite.testDashboard(suite.Require(), false)
}

func (suite *dashboardTestSuite) TestDashboardProxy() {
	suite.testDashboard(suite.Require(), true)
}

func (suite *dashboardTestSuite) checkRespCode(re *require.Assertions, url string, code int) {
	resp, err := suite.httpClient.Get(url)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	resp.Body.Close()
	re.Equal(code, resp.StatusCode)
}

func waitForConfigSync() {
	// Need to wait dashboard service start.
	time.Sleep(3 * time.Second)
}

func (suite *dashboardTestSuite) checkServiceIsStarted(re *require.Assertions, internalProxy bool, servers map[string]*tests.TestServer, leader *tests.TestServer) string {
	waitForConfigSync()
	dashboardAddress := leader.GetServer().GetPersistOptions().GetDashboardAddress()
	hasServiceNode := false
	for _, srv := range servers {
		re.Equal(dashboardAddress, srv.GetPersistOptions().GetDashboardAddress())
		addr := srv.GetAddr()
		if addr == dashboardAddress || internalProxy {
			suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/", addr), http.StatusOK)
			suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusUnauthorized)
			if addr == dashboardAddress {
				hasServiceNode = true
			}
		} else {
			suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/", addr), http.StatusTemporaryRedirect)
			suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusTemporaryRedirect)
		}
	}
	re.True(hasServiceNode)
	return dashboardAddress
}

func (suite *dashboardTestSuite) checkServiceIsStopped(re *require.Assertions, servers map[string]*tests.TestServer) {
	waitForConfigSync()
	for _, srv := range servers {
		re.Equal("none", srv.GetPersistOptions().GetDashboardAddress())
		addr := srv.GetAddr()
		suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/", addr), http.StatusNotFound)
		suite.checkRespCode(re, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusNotFound)
	}
}

func (suite *dashboardTestSuite) testDashboard(re *require.Assertions, internalProxy bool) {
	cluster, err := tests.NewTestCluster(suite.ctx, 3, func(conf *config.Config, _ string) {
		conf.Dashboard.InternalProxy = internalProxy
	})
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)

	re.NotEmpty(cluster.WaitLeader())
	servers := cluster.GetServers()
	leader := cluster.GetLeaderServer()
	leaderAddr := leader.GetAddr()

	// auto select node
	dashboardAddress1 := suite.checkServiceIsStarted(re, internalProxy, servers, leader)

	// pd-ctl set another addr
	var dashboardAddress2 string
	for _, srv := range servers {
		if srv.GetAddr() != dashboardAddress1 {
			dashboardAddress2 = srv.GetAddr()
			break
		}
	}

	input := map[string]any{
		"dashboard-address": dashboardAddress2,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leaderAddr+"/pd/api/v1/config", bytes.NewBuffer(data))
	resp, err := suite.httpClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	suite.checkServiceIsStarted(re, internalProxy, servers, leader)
	re.Equal(dashboardAddress2, leader.GetServer().GetPersistOptions().GetDashboardAddress())

	// pd-ctl set stop
	input = map[string]any{
		"dashboard-address": "none",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leaderAddr+"/pd/api/v1/config", bytes.NewBuffer(data))
	resp, err = suite.httpClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	suite.checkServiceIsStopped(re, servers)
}
