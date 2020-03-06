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

package api_test

import (
	"context"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/apiutil/serverapi"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
	"go.uber.org/goleak"
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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct{}

func (s *serverTestSuite) TestReconnect(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := cluster.WaitLeader()
	for name, s := range cluster.GetServers() {
		if name != leader {
			res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
			c.Assert(e, IsNil)
			c.Assert(res.StatusCode, Equals, http.StatusOK)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	newLeader := cluster.WaitLeader()
	c.Assert(len(newLeader), Not(Equals), 0)

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.GetServers() {
		if name != leader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(e, IsNil)
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	err = cluster.GetServer(newLeader).Stop()
	c.Assert(err, IsNil)

	// Request will fail with no leader.
	for name, s := range cluster.GetServers() {
		if name != leader && name != newLeader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(err, IsNil)
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
	cleanup func()
	cluster *tests.TestCluster
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(len(cluster.WaitLeader()), Not(Equals), 0)
	s.cluster = cluster
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	c.Assert(leader, NotNil)
	header := mustRequestSuccess(c, leader.GetServer())
	header.Del("Date")
	for _, svr := range s.cluster.GetServers() {
		if svr != leader {
			h := mustRequestSuccess(c, svr.GetServer())
			h.Del("Date")
			c.Assert(header, DeepEquals, h)
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

	addr := follower.GetAddr() + "/pd/api/v1/version"
	request, err := http.NewRequest("GET", addr, nil)
	c.Assert(err, IsNil)
	request.Header.Add(serverapi.AllowFollowerHandle, "true")
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get(serverapi.FollowerHandle), Equals, "true")
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
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

	addr := follower.GetAddr() + "/pd/api/v1/version"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest("GET", addr, nil)
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(serverapi.RedirectorHeader, "pd")
	resp1, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(resp1.StatusCode, Not(Equals), http.StatusOK)
	_, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
}

func mustRequestSuccess(c *C, s *server.Server) http.Header {
	resp, err := dialClient.Get(s.GetAddr() + "/pd/api/v1/version")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	return resp.Header
}
