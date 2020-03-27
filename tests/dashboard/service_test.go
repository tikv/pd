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
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
	"go.uber.org/goleak"

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

func (s *serverTestSuite) CheckRespCode(c *C, cluster *tests.TestCluster, hasServiceNode bool) (dashboardAddress string) {
	time.Sleep(time.Second * 5)
	leaderName := cluster.GetLeader()
	servers := cluster.GetServers()
	leader := servers[leaderName]

	dashboardAddress = leader.GetServer().GetScheduleOption().GetDashboardAddress()

	checkRespCode := func(url string, target int) {
		resp, err := s.httpClient.Get(url) //nolint:gosec
		c.Assert(err, IsNil)
		c.Assert(len(resp.Header.Get("PD-Follower-handle")), Equals, 0)
		_, err = ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		resp.Body.Close()
		c.Assert(resp.StatusCode, Equals, target)
	}

	countServiceNode := 0
	for _, srv := range servers {
		c.Assert(srv.GetScheduleOption().GetDashboardAddress(), Equals, dashboardAddress)
		addr := srv.GetAddr()
		if addr == dashboardAddress {
			checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusOK)
			checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusUnauthorized)
			countServiceNode++
		} else {
			if hasServiceNode {
				checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusTemporaryRedirect)
				checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusTemporaryRedirect)
			} else {
				checkRespCode(fmt.Sprintf("%s/dashboard/", addr), http.StatusNotFound)
				checkRespCode(fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", addr), http.StatusNotFound)
			}
		}
	}

	if hasServiceNode {
		c.Assert(countServiceNode, Equals, 1)
	} else {
		c.Assert(countServiceNode, Equals, 0)
	}

	return dashboardAddress
}

func (s *serverTestSuite) TestDashboard(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	leaderAddr := leaderServer.GetAddr()

	cmd := pdctl.InitCommand()

	// auto select node
	dashboardAddress := s.CheckRespCode(c, cluster, true)

	// pd-ctl set another addr
	for _, srv := range cluster.GetServers() {
		if srv.GetAddr() != dashboardAddress {
			dashboardAddress = srv.GetAddr()
			break
		}
	}
	args := []string{"-u", leaderAddr, "config", "set", "dashboard-address", dashboardAddress}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(s.CheckRespCode(c, cluster, true), Equals, dashboardAddress)

	// pd-ctl set stop
	args = []string{"-u", leaderAddr, "config", "set", "dashboard-address", "none"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	s.CheckRespCode(c, cluster, false)
}
