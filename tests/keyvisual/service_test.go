// Copyright 2018 PingCAP, Inc.
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

package keyvisual_test

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

func (s *serverTestSuite) TestLeader(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	leaderName := cluster.WaitLeader()
	c.Assert(leaderName, Not(Equals), "")
	var followerSvr *tests.TestServer
	for name, svr := range cluster.GetServers() {
		if name != leaderName {
			followerSvr = svr
			break
		}
	}
	c.Assert(followerSvr, NotNil)

	checkReq := func(url string, target string) {
		resp, err := http.Get(url)
		c.Assert(err, IsNil)
		c.Assert(len(resp.Header.Get("PD-Follower-handle")), Equals, 0)
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(string(bodyBytes), Equals, target)
	}
	leader := cluster.GetServer(leaderName)
	leaderURL := fmt.Sprintf("%s/pd/apis/keyvisual/v1/heatmaps", leader.GetAddr())
	followerURL := fmt.Sprintf("%s/pd/apis/keyvisual/v1/heatmaps", followerSvr.GetAddr())
	checkReq(leaderURL, "no service\n")
	checkReq(followerURL, "no service\n")

	cfg := leader.GetServer().GetServerOption().LoadPDServerConfig()
	cfg.RuntimeServices = []string{"keyvisual"}
	leader.GetServer().GetServerOption().SetPDServerConfig(cfg)
	time.Sleep(time.Second)
	checkReq(leaderURL, "\"not implemented\"\n")
}
