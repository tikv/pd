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

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/apiutil/serverapi"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
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
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
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
			res.Body.Close()
			c.Assert(res.StatusCode, Equals, http.StatusOK)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	newLeader := cluster.WaitLeader()
	c.Assert(newLeader, Not(HasLen), 0)

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.GetServers() {
		if name != leader {
			testutil.WaitUntil(c, func() bool {
				res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(e, IsNil)
				defer res.Body.Close()
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
			testutil.WaitUntil(c, func() bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(err, IsNil)
				defer res.Body.Close()
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}

var _ = Suite(&testMiddlewareSuite{})

type testMiddlewareSuite struct {
	cleanup func()
	cluster *tests.TestCluster
}

func (s *testMiddlewareSuite) SetUpSuite(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/enableFailpointAPI", "return(true)"), IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.cluster = cluster
}

func (s *testMiddlewareSuite) TearDownSuite(c *C) {
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/api/enableFailpointAPI"), IsNil)
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testMiddlewareSuite) TestRequestInfoMiddleware(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/addRequestInfoMiddleware", "return(true)"), IsNil)
	leader := s.cluster.GetServer(s.cluster.GetLeader())

	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=true", nil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsAuditMiddlewareEnabled(), Equals, true)

	labels := make(map[string]interface{})
	labels["testkey"] = "testvalue"
	data, _ := json.Marshal(labels)
	resp, err = dialClient.Post(leader.GetAddr()+"/pd/api/v1/debug/pprof/profile?force=true", "application/json", bytes.NewBuffer(data))
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	c.Assert(resp.Header.Get("service-label"), Equals, "DebugPProfProfile")
	c.Assert(resp.Header.Get("url-param"), Equals, "{\"force\":[\"true\"]}")
	c.Assert(resp.Header.Get("body-param"), Equals, "{\"testkey\":\"testvalue\"}")
	c.Assert(resp.Header.Get("method"), Equals, "HTTP/1.1/POST:/pd/api/v1/debug/pprof/profile")
	c.Assert(resp.Header.Get("component"), Equals, "anonymous")
	c.Assert(resp.Header.Get("ip"), Equals, "127.0.0.1")

	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=false", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsAuditMiddlewareEnabled(), Equals, false)

	header := mustRequestSuccess(c, leader.GetServer())
	c.Assert(header.Get("service-label"), Equals, "")

	c.Assert(failpoint.Disable("github.com/tikv/pd/server/api/addRequestInfoMiddleware"), IsNil)
}

func BenchmarkDoRequestWithServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=true", nil)
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequest(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithoutServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=false", nil)
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequest(leader)
	}
	cancel()
	cluster.Destroy()
}

func doTestRequest(srv *tests.TestServer) {
	timeUnix := time.Now().Unix() - 20
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/pd/api/v1/trend?from=%d", srv.GetAddr(), timeUnix), nil)
	req.Header.Set("component", "test")
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
}

func (s *testMiddlewareSuite) TestAuditMiddleware(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/addAuditMiddleware", "return(true)"), IsNil)
	leader := s.cluster.GetServer(s.cluster.GetLeader())

	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=true", nil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsAuditMiddlewareEnabled(), Equals, true)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/pd/api/v1/fail/", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.Header.Get("audit-label"), Equals, "test")

	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/audit-middleware?enable=false", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsAuditMiddlewareEnabled(), Equals, false)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/pd/api/v1/fail/", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.Header.Get("audit-label"), Equals, "")

	c.Assert(failpoint.Disable("github.com/tikv/pd/server/api/addAuditMiddleware"), IsNil)
}

func (s *testMiddlewareSuite) TestRateLimitMiddleware(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/addRateLimitMiddleware", "return(true)"), IsNil)
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/ratelimit-middleware?enable=true", nil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsRateLimitMiddlewareEnabled(), Equals, true)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/pd/api/v1/fail/", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.Header.Get("rate-limit"), Equals, "rate-limit")

	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/ratelimit-middleware?enable=false", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().IsRateLimitMiddlewareEnabled(), Equals, false)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/pd/api/v1/fail/", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.Header.Get("rate-limit"), Equals, "")
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/api/addRateLimitMiddleware"), IsNil)
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
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
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
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	c.Assert(err, IsNil)
	request.Header.Add(serverapi.AllowFollowerHandle, "true")
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get(serverapi.RedirectorHeader), Equals, "")
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = io.ReadAll(resp.Body)
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
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(serverapi.RedirectorHeader, "pd")
	resp1, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(resp1.StatusCode, Not(Equals), http.StatusOK)
	_, err = io.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
}

func mustRequestSuccess(c *C, s *server.Server) http.Header {
	resp, err := dialClient.Get(s.GetAddr() + "/pd/api/v1/version")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	return resp.Header
}
