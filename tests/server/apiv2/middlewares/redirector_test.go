// Copyright 2018 TiKV Project Authors.
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

package middlewares_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/apiutil/serverapi"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
	cleanup context.CancelFunc
	cluster *tests.TestCluster
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	s.cluster = cluster
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	c.Assert(leader, NotNil)
	request, err := http.NewRequest(http.MethodGet, leader.GetServer().GetAddr()+"/pd/api/v2/members", nil)
	c.Assert(err, IsNil)
	header := mustRequestSuccess(c, request)
	header.Del("Date")
	for _, svr := range s.cluster.GetServers() {
		if svr != leader {
			h := mustRequestSuccess(c, request)
			h.Del("Date")
			c.Assert(header, DeepEquals, h)
		}
	}
}

func (s *testRedirectorSuite) TestRedirectAfterStop(c *C) {
	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := s.cluster.GetLeader()
	for name, s := range s.cluster.GetServers() {
		if name != leader {
			request, err := http.NewRequest(http.MethodGet, s.GetConfig().AdvertiseClientUrls+"/pd/api/v2/members", nil)
			c.Assert(err, IsNil)
			_ = mustRequestSuccess(c, request)
		}
	}

	// Close the leader and wait for a new one.
	err := s.cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	defer s.cluster.GetServer(leader).Run()
	newLeader := s.cluster.WaitLeader()
	c.Assert(newLeader, Not(HasLen), 0)

	// Make sure they proxy requests to the new leader.
	for name, s := range s.cluster.GetServers() {
		if name != leader {
			testutil.WaitUntil(c, func() bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v2/members")
				c.Assert(err, IsNil)
				defer res.Body.Close()
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	err = s.cluster.GetServer(newLeader).Stop()
	c.Assert(err, IsNil)
	defer s.cluster.GetServer(newLeader).Run()

	// Request will fail with no leader.
	for name, s := range s.cluster.GetServers() {
		if name != leader && name != newLeader {
			testutil.WaitUntil(c, func() bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v2/")
				c.Assert(err, IsNil)
				defer res.Body.Close()
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}

func (s *testRedirectorSuite) TestAllowFollowerHandle(c *C) {
	// Find a follower.
	var follower *server.Server
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	for _, svr := range s.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v2/members"
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	c.Assert(err, IsNil)
	request.Header.Add(serverapi.AllowFollowerHandle, "true")
	header := mustRequestSuccess(c, request)
	c.Assert(header.Get(serverapi.RedirectorHeader), Equals, "")
}

func (s *testRedirectorSuite) TestNotLeader(c *C) {
	// Find a follower.
	var follower *server.Server
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	for _, svr := range s.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v2/members"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	c.Assert(err, IsNil)
	_ = mustRequestSuccess(c, request)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(serverapi.RedirectorHeader, "pd")
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Not(Equals), http.StatusOK)
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

func mustRequestSuccess(c *C, req *http.Request) http.Header {
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	return resp.Header
}
