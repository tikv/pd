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

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
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
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *serverTestSuite) TestEnable(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config) {
		conf.EnableDashboard = true
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	leaderName := cluster.WaitLeader()
	leader := cluster.GetServer(leaderName)
	c.Assert(leaderName, Not(Equals), "")
	var follower *tests.TestServer
	for name, svr := range cluster.GetServers() {
		if name != leaderName {
			follower = svr
			break
		}
	}
	c.Assert(follower, NotNil)

	checkReqCode := func(url string, target int) {
		resp, err := http.Get(url) //nolint:gosec
		c.Assert(err, IsNil)
		c.Assert(len(resp.Header.Get("PD-Follower-handle")), Equals, 0)
		_, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, target)
	}

	url := fmt.Sprintf("%s/dashboard/", leader.GetAddr())
	checkReqCode(url, 200)
	url = fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", leader.GetAddr())
	checkReqCode(url, 401)
	url = fmt.Sprintf("%s/dashboard/", follower.GetAddr())
	checkReqCode(url, 200)
	url = fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", follower.GetAddr())
	checkReqCode(url, 401)
}

func (s *serverTestSuite) TestDisable(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config) {
		conf.EnableDashboard = false
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	leaderName := cluster.WaitLeader()
	leader := cluster.GetServer(leaderName)
	c.Assert(leaderName, Not(Equals), "")
	var follower *tests.TestServer
	for name, svr := range cluster.GetServers() {
		if name != leaderName {
			follower = svr
			break
		}
	}
	c.Assert(follower, NotNil)

	checkReqCode := func(url string, target int) {
		resp, err := http.Get(url) //nolint:gosec
		c.Assert(err, IsNil)
		c.Assert(len(resp.Header.Get("PD-Follower-handle")), Equals, 0)
		_, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, target)
	}

	url := fmt.Sprintf("%s/dashboard/", leader.GetAddr())
	checkReqCode(url, 404)
	url = fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", leader.GetAddr())
	checkReqCode(url, 404)
	url = fmt.Sprintf("%s/dashboard/", follower.GetAddr())
	checkReqCode(url, 404)
	url = fmt.Sprintf("%s/dashboard/api/keyvisual/heatmaps", follower.GetAddr())
	checkReqCode(url, 404)
}
