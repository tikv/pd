// Copyright 2020 PingCAP, Inc.
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

package dashboard_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.uber.org/goleak"

	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
	// Register schedulers.
	_ "github.com/pingcap/pd/v4/server/schedulers"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct {
	ctx        context.Context
	cancel     context.CancelFunc
	httpClient *http.Client
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.httpClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// ErrUseLastResponse can be returned by Client.CheckRedirect hooks to
			// control how redirects are processed. If returned, the next request
			// is not sent and the most recent response is returned with its body
			// unclosed.
			return http.ErrUseLastResponse
		},
	}
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	s.cancel()
	s.httpClient.CloseIdleConnections()
}

func (s *serverTestSuite) checkRespCode(c *C, url string, code int) {
	resp, err := s.httpClient.Get(url) //nolint:gosec
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, code)
}

func (s *serverTestSuite) waitForConfigSync() {
	time.Sleep(time.Second * 5)
}

func (s *serverTestSuite) checkServiceIsStarted(c *C, servers map[string]*tests.TestServer, leader *tests.TestServer) string {
	s.waitForConfigSync()
	dashboardAddress := leader.GetServer().GetScheduleOption().GetDashboardAddress()
	hasServiceNode := false
	for _, srv := range servers {
		c.Assert(srv.GetScheduleOption().GetDashboardAddress(), Equals, dashboardAddress)
		addr := srv.GetAddr()
		if addr == dashboardAddress {
			s.checkRespCode(c, fmt.Sprintf("%s/dashboard/", addr), http.StatusOK)
			s.checkRespCode(c, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusUnauthorized)
			hasServiceNode = true
		} else {
			s.checkRespCode(c, fmt.Sprintf("%s/dashboard/", addr), http.StatusTemporaryRedirect)
			s.checkRespCode(c, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusUnauthorized)
		}
	}
	c.Assert(hasServiceNode, IsTrue)
	return dashboardAddress
}

func (s *serverTestSuite) checkServiceIsStopped(c *C, servers map[string]*tests.TestServer) {
	s.waitForConfigSync()
	for _, srv := range servers {
		c.Assert(srv.GetScheduleOption().GetDashboardAddress(), Equals, "none")
		addr := srv.GetAddr()
		s.checkRespCode(c, fmt.Sprintf("%s/dashboard/", addr), http.StatusNotFound)
		s.checkRespCode(c, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusNotFound)
	}
}

func (s *serverTestSuite) checkServiceIsChanging(c *C, servers map[string]*tests.TestServer) {
	s.waitForConfigSync()
	for _, srv := range servers {
		addr := srv.GetAddr()
		s.checkRespCode(c, fmt.Sprintf("%s/dashboard/", addr), http.StatusTemporaryRedirect)
		s.checkRespCode(c, fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusLoopDetected)
	}
}

func (s *serverTestSuite) TestDashboard(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config) {
		conf.EnableDynamicConfig = true
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cmd := pdctl.InitCommand()

	cluster.WaitLeader()
	servers := cluster.GetServers()
	leader := cluster.GetServer(cluster.GetLeader())
	leaderAddr := leader.GetAddr()

	// auto select node
	dashboardAddress1 := s.checkServiceIsStarted(c, servers, leader)

	// pd-ctl set another addr
	var dashboardAddress2 string
	for _, srv := range servers {
		if srv.GetAddr() != dashboardAddress1 {
			dashboardAddress2 = srv.GetAddr()
			break
		}
	}
	args := []string{"-u", leaderAddr, "config", "set", "dashboard-address", dashboardAddress2}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	s.checkServiceIsStarted(c, servers, leader)
	c.Assert(leader.GetServer().GetScheduleOption().GetDashboardAddress(), Equals, dashboardAddress2)

	// Changing dashboard address
	for _, srv := range servers {
		addr := srv.GetAddr()
		var changingAddr string
		if addr == dashboardAddress1 {
			changingAddr = dashboardAddress2
		} else {
			changingAddr = dashboardAddress1
		}
		args = []string{"-u", leaderAddr, "component", "set", addr[7:], "dashboard-address", changingAddr}
		_, _, err = pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
	}
	s.checkServiceIsChanging(c, servers)

	// pd-ctl set stop
	args = []string{"-u", leaderAddr, "config", "set", "dashboard-address", "none"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	s.checkServiceIsStopped(c, servers)
}
