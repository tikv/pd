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

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestReconnect(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	for name, s := range cluster.GetServers() {
		if name != leader {
			res, err := tests.TestDialClient.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
			re.NoError(err)
			res.Body.Close()
			re.Equal(http.StatusOK, res.StatusCode)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	re.NoError(err)
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.GetServers() {
		if name != leader {
			testutil.Eventually(re, func() bool {
				res, err := tests.TestDialClient.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				re.NoError(err)
				defer res.Body.Close()
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	re.NoError(cluster.GetServer(newLeader).Stop())

	// Request will fail with no leader.
	for name, s := range cluster.GetServers() {
		if name != leader && name != newLeader {
			testutil.Eventually(re, func() bool {
				res, err := tests.TestDialClient.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				re.NoError(err)
				defer res.Body.Close()
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}

type middlewareTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
}

func TestMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(middlewareTestSuite))
}

func (suite *middlewareTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/api/enableFailpointAPI", "return(true)"))
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.cluster = cluster
}

func (suite *middlewareTestSuite) SetupTest() {
	re := suite.Require()
	re.NotEmpty(suite.cluster.WaitLeader())
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
}

func (suite *middlewareTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/api/enableFailpointAPI"))
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *middlewareTestSuite) TestRequestInfoMiddleware() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/api/addRequestInfoMiddleware", "return(true)"))
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)

	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	labels := make(map[string]any)
	labels["testkey"] = "testvalue"
	data, _ = json.Marshal(labels)
	resp, err = tests.TestDialClient.Post(leader.GetAddr()+"/pd/api/v1/debug/pprof/profile?seconds=1", "application/json", bytes.NewBuffer(data))
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)

	re.Equal("Profile", resp.Header.Get("service-label"))
	re.Equal("{\"seconds\":[\"1\"]}", resp.Header.Get("url-param"))
	re.Equal("{\"testkey\":\"testvalue\"}", resp.Header.Get("body-param"))
	re.Equal("HTTP/1.1/POST:/pd/api/v1/debug/pprof/profile", resp.Header.Get("method"))
	re.Equal("anonymous", resp.Header.Get("caller-id"))
	re.Equal("127.0.0.1", resp.Header.Get("ip"))

	input = map[string]any{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	header := mustRequestSuccess(re, leader.GetServer())
	re.Equal("GetVersion", header.Get("service-label"))

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/api/addRequestInfoMiddleware"))
}

func BenchmarkDoRequestWithServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func (suite *middlewareTestSuite) TestRateLimitMiddleware() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	input := map[string]any{
		"enable-rate-limit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())

	// returns StatusOK when no rate-limit config
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	input = make(map[string]any)
	input["type"] = "label"
	input["label"] = "SetLogLevel"
	input["qps"] = 0.5
	input["concurrency"] = 1
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config/rate-limit", bytes.NewBuffer(jsonBody))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)

	for i := range 3 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		if i > 0 {
			re.Equal(http.StatusTooManyRequests, resp.StatusCode)
			re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			re.Equal(http.StatusOK, resp.StatusCode)
		}
	}

	// qps = 0.5, so sleep 2s
	time.Sleep(time.Second * 2)
	for i := range 2 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		if i > 0 {
			re.Equal(http.StatusTooManyRequests, resp.StatusCode)
			re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			re.Equal(http.StatusOK, resp.StatusCode)
		}
	}

	// test only sleep 1s
	time.Sleep(time.Second)
	for range 2 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		re.Equal(http.StatusTooManyRequests, resp.StatusCode)
		re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
	}

	// resign leader
	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(re, servers)
	leader = suite.cluster.GetLeaderServer()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	cfg, ok := leader.GetServer().GetRateLimitConfig().LimiterConfig["SetLogLevel"]
	re.True(ok)
	re.Equal(uint64(1), cfg.ConcurrencyLimit)
	re.Equal(0.5, cfg.QPS)
	re.Equal(1, cfg.QPSBurst)

	for i := range 3 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		if i > 0 {
			re.Equal(http.StatusTooManyRequests, resp.StatusCode)
			re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			re.Equal(http.StatusOK, resp.StatusCode)
		}
	}

	// qps = 0.5, so sleep 2s
	time.Sleep(time.Second * 2)
	for i := range 2 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		if i > 0 {
			re.Equal(http.StatusTooManyRequests, resp.StatusCode)
			re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			re.Equal(http.StatusOK, resp.StatusCode)
		}
	}

	// test only sleep 1s
	time.Sleep(time.Second)
	for range 2 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		re.Equal(http.StatusTooManyRequests, resp.StatusCode)
		re.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
	}

	input = map[string]any{
		"enable-rate-limit": "false",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())

	for range 3 {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = tests.TestDialClient.Do(req)
		re.NoError(err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
	}

	// reset rate limit
	input = map[string]any{
		"enable-rate-limit": "true",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
}

func (suite *middlewareTestSuite) TestSwaggerUrl() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	req, _ := http.NewRequest(http.MethodGet, leader.GetAddr()+"/swagger/ui/index", http.NoBody)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusNotFound, resp.StatusCode)
	resp.Body.Close()
}

func (suite *middlewareTestSuite) TestAuditPrometheusBackend() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())
	timeUnix := time.Now().Unix() - 20
	req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), http.NoBody)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)

	req, _ = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", http.NoBody)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	output := string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_count{caller_id=\"anonymous\",ip=\"127.0.0.1\",method=\"HTTP\",service=\"GetTrend\"} 1")

	// resign to test persist config
	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(re, servers)
	leader = suite.cluster.GetLeaderServer()

	timeUnix = time.Now().Unix() - 20
	req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), http.NoBody)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)

	req, _ = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", http.NoBody)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, _ = io.ReadAll(resp.Body)
	output = string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_count{caller_id=\"anonymous\",ip=\"127.0.0.1\",method=\"HTTP\",service=\"GetTrend\"} 2")

	input = map[string]any{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())
}

func (suite *middlewareTestSuite) TestAuditLocalLogBackend() {
	re := suite.Require()
	fname := testutil.InitTempFileLogger("info")
	defer os.RemoveAll(fname)
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	b, _ := os.ReadFile(fname)
	re.Contains(string(b), "audit log")
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
}

func BenchmarkDoRequestWithLocalLogAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithPrometheusAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithPrometheus(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithoutServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "false",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func doTestRequestWithLogAudit(srv *tests.TestServer) {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/pd/api/v1/admin/cache/regions", srv.GetAddr()), http.NoBody)
	req.Header.Set(apiutil.XCallerIDHeader, "test")
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
}

func doTestRequestWithPrometheus(srv *tests.TestServer) {
	timeUnix := time.Now().Unix() - 20
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", srv.GetAddr(), timeUnix), http.NoBody)
	req.Header.Set(apiutil.XCallerIDHeader, "test")
	resp, _ := tests.TestDialClient.Do(req)
	resp.Body.Close()
}

type redirectorTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
}

func TestRedirectorTestSuite(t *testing.T) {
	suite.Run(t, new(redirectorTestSuite))
}

func (suite *redirectorTestSuite) SetupSuite() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.cluster = cluster
}

func (suite *redirectorTestSuite) TearDownSuite() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *redirectorTestSuite) TestRedirect() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	header := mustRequestSuccess(re, leader.GetServer())
	header.Del("Date")
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			h := mustRequestSuccess(re, svr.GetServer())
			h.Del("Date")
			re.Equal(h, header)
		}
	}
	// Test redirect during leader election.
	leader = suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	err := leader.ResignLeader()
	re.NoError(err)
	for _, svr := range suite.cluster.GetServers() {
		url := fmt.Sprintf("%s/pd/api/v1/version", svr.GetServer().GetAddr())
		testutil.Eventually(re, func() bool {
			resp, err := tests.TestDialClient.Get(url)
			re.NoError(err)
			defer resp.Body.Close()
			_, err = io.ReadAll(resp.Body)
			re.NoError(err)
			// Should not meet 503 since the retry logic ensure the request is sent to the new leader eventually.
			re.NotEqual(http.StatusServiceUnavailable, resp.StatusCode)
			return resp.StatusCode == http.StatusOK
		})
	}
}

func (suite *redirectorTestSuite) TestAllowFollowerHandle() {
	re := suite.Require()
	// Find a follower.
	var follower *server.Server
	leader := suite.cluster.GetLeaderServer()
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v1/version"
	request, err := http.NewRequest(http.MethodGet, addr, http.NoBody)
	re.NoError(err)
	request.Header.Add(apiutil.PDAllowFollowerHandleHeader, "true")
	resp, err := tests.TestDialClient.Do(request)
	re.NoError(err)
	re.Equal("", resp.Header.Get(apiutil.PDRedirectorHeader))
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
}

func (suite *redirectorTestSuite) TestNotLeader() {
	re := suite.Require()
	// Find a follower.
	var follower *server.Server
	leader := suite.cluster.GetLeaderServer()
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v1/version"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest(http.MethodGet, addr, http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(request)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(apiutil.PDRedirectorHeader, "pd")
	resp1, err := tests.TestDialClient.Do(request)
	re.NoError(err)
	defer resp1.Body.Close()
	re.NotEqual(http.StatusOK, resp1.StatusCode)
	_, err = io.ReadAll(resp1.Body)
	re.NoError(err)
}

func (suite *redirectorTestSuite) TestXForwardedFor() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	fname := testutil.InitTempFileLogger("info")
	defer os.RemoveAll(fname)

	follower := suite.cluster.GetServer(suite.cluster.GetFollower())
	addr := follower.GetAddr() + "/pd/api/v1/regions"
	request, err := http.NewRequest(http.MethodGet, addr, http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(request)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	time.Sleep(1 * time.Second)
	b, _ := os.ReadFile(fname)
	l := string(b)
	re.Contains(l, "/pd/api/v1/regions")
	re.NotContains(l, suite.cluster.GetConfig().GetClientURLs())
}

func mustRequestSuccess(re *require.Assertions, s *server.Server) http.Header {
	resp, err := tests.TestDialClient.Get(s.GetAddr() + "/pd/api/v1/version")
	re.NoError(err)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	return resp.Header
}

func TestRemovingProgress(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Replication.MaxReplicas = 1
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	tests.MustPutRegion(re, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	tests.MustPutRegion(re, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(30))
	tests.MustPutRegion(re, cluster, 1002, 1, []byte("e"), []byte("f"), core.SetApproximateSize(50))
	tests.MustPutRegion(re, cluster, 1003, 2, []byte("g"), []byte("h"), core.SetApproximateSize(40))

	// no store removing
	output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusNotFound)
	re.Contains(string(output), "no progress found for the action")
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusNotFound)
	re.Contains(string(output), "no progress found for the given store ID")

	// remove store 1 and store 2
	_ = sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/1", http.MethodDelete, http.StatusOK)
	_ = sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/2", http.MethodDelete, http.StatusOK)

	// size is not changed.
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	var p api.Progress
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	re.Equal(0.0, p.Progress)
	re.Equal(0.0, p.CurrentSpeed)
	re.Equal(math.MaxFloat64, p.LeftSeconds)

	// update size
	tests.MustPutRegion(re, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(20))
	tests.MustPutRegion(re, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(10))

	if !leader.GetRaftCluster().IsPrepared() {
		testutil.Eventually(re, func() bool {
			if leader.GetRaftCluster().IsPrepared() {
				return true
			}
			url := leader.GetAddr() + "/pd/api/v1/stores/progress?action=removing"
			req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
			resp, err := tests.TestDialClient.Do(req)
			re.NoError(err)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			// is not prepared
			re.NoError(json.Unmarshal(output, &p))
			re.Equal("removing", p.Action)
			re.Equal(0.0, p.Progress)
			re.Equal(0.0, p.CurrentSpeed)
			re.Equal(math.MaxFloat64, p.LeftSeconds)
			return true
		})
	}

	testutil.Eventually(re, func() bool {
		// wait for cluster prepare
		if !leader.GetRaftCluster().IsPrepared() {
			leader.GetRaftCluster().SetPrepared()
			return false
		}
		url := leader.GetAddr() + "/pd/api/v1/stores/progress?action=removing"
		req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
		resp, err := tests.TestDialClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		output, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &p))
		if p.Action != "removing" {
			return false
		}
		// store 1: (60-20)/(60+50) ~= 0.36
		// store 2: (30-10)/(30+40) ~= 0.28
		// average progress ~= (0.36+0.28)/2 = 0.32
		if fmt.Sprintf("%.2f", p.Progress) != "0.32" {
			return false
		}
		// store 1: 40/10s = 4
		// store 2: 20/10s = 2
		// average speed = (2+4)/2 = 33
		if p.CurrentSpeed != 3.0 {
			return false
		}
		// store 1: (20+50)/4 = 17.5s
		// store 2: (10+40)/2 = 25s
		// average time = (17.5+25)/2 = 21.25s
		if p.LeftSeconds != 21.25 {
			return false
		}
		return true
	})

	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	// store 2: (30-10)/(30+40) ~= 0.285
	re.Equal("0.29", fmt.Sprintf("%.2f", p.Progress))
	// store 2: 20/10s = 2
	re.Equal(2.0, p.CurrentSpeed)
	// store 2: (10+40)/2 = 25s
	re.Equal(25.0, p.LeftSeconds)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func TestSendApiWhenRestartRaftCluster(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.Replication.MaxReplicas = 1
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()

	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())

	// Mock restart raft cluster
	rc := leader.GetRaftCluster()
	re.NotNil(rc)
	rc.Stop()

	// Mock client-go will still send request
	output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/min-resolved-ts", http.MethodGet, http.StatusInternalServerError)
	re.Contains(string(output), "TiKV cluster not bootstrapped, please start TiKV first")

	err = rc.Start(leader.GetServer(), false)
	re.NoError(err)
	rc = leader.GetRaftCluster()
	re.NotNil(rc)
}

func TestPreparingProgress(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Replication.MaxReplicas = 1
		// prevent scheduling
		conf.Schedule.RegionScheduleLimit = 0
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	stores := []*metapb.Store{
		{
			Id:             1,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             2,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             3,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             4,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             5,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
	}
	// store 4 and store 5 are preparing state while store 1, store 2 and store 3 are state serving state
	for _, store := range stores[:2] {
		tests.MustPutStore(re, cluster, store)
	}
	for i := range core.InitClusterRegionThreshold {
		tests.MustPutRegion(re, cluster, uint64(i+1), uint64(i)%3+1, []byte(fmt.Sprintf("%20d", i)), []byte(fmt.Sprintf("%20d", i+1)), core.SetApproximateSize(10))
	}
	testutil.Eventually(re, func() bool {
		return leader.GetRaftCluster().GetTotalRegionCount() == core.InitClusterRegionThreshold
	})
	// to avoid forcing the store to the `serving` state with too few regions
	for _, store := range stores[2:] {
		tests.MustPutStore(re, cluster, store)
	}

	if !leader.GetRaftCluster().IsPrepared() {
		testutil.Eventually(re, func() bool {
			if leader.GetRaftCluster().IsPrepared() {
				return true
			}

			// no store preparing
			output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusNotFound)
			re.Contains(string(output), "no progress found for the action")
			output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusNotFound)
			re.Contains(string(output), "no progress found for the given store ID")
			return true
		})
	}

	var p api.Progress
	testutil.Eventually(re, func() bool {
		// wait for cluster prepare
		if !leader.GetRaftCluster().IsPrepared() {
			leader.GetRaftCluster().SetPrepared()
			return false
		}
		url := leader.GetAddr() + "/pd/api/v1/stores/progress?action=preparing"
		req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
		resp, err := tests.TestDialClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		output, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		re.NoError(json.Unmarshal(output, &p))
		re.Equal("preparing", p.Action)
		re.Equal(0.0, p.Progress)
		re.Equal(0.0, p.CurrentSpeed)
		re.Equal(math.MaxFloat64, p.LeftSeconds)
		return true
	})

	// update size
	tests.MustPutRegion(re, cluster, 1000, 4, []byte(fmt.Sprintf("%20d", 1000)), []byte(fmt.Sprintf("%20d", 1001)), core.SetApproximateSize(10))
	tests.MustPutRegion(re, cluster, 1001, 5, []byte(fmt.Sprintf("%20d", 1001)), []byte(fmt.Sprintf("%20d", 1002)), core.SetApproximateSize(40))
	testutil.Eventually(re, func() bool {
		output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
		re.NoError(json.Unmarshal(output, &p))
		if p.Action != "preparing" {
			return false
		}
		// store 4: 10/(210*0.9) ~= 0.05
		// store 5: 40/(210*0.9) ~= 0.21
		// average progress ~= (0.05+0.21)/2 = 0.13
		if fmt.Sprintf("%.2f", p.Progress) != "0.13" {
			return false
		}
		// store 4: 10/10s = 1
		// store 5: 40/10s = 4
		// average speed = (1+4)/2 = 2.5
		if p.CurrentSpeed != 2.5 {
			return false
		}
		// store 4: 179/1 ~= 179
		// store 5: 149/4 ~= 37.25
		// average time ~= (179+37.25)/2 = 108.125
		if p.LeftSeconds != 108.125 {
			return false
		}
		return true
	})

	output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("preparing", p.Action)
	re.Equal("0.05", fmt.Sprintf("%.2f", p.Progress))
	re.Equal(1.0, p.CurrentSpeed)
	re.Equal(179.0, p.LeftSeconds)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func sendRequest(re *require.Assertions, url string, method string, statusCode int) []byte {
	req, _ := http.NewRequest(method, url, http.NoBody)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	re.Equal(statusCode, resp.StatusCode)
	output, err := io.ReadAll(resp.Body)
	re.NoError(err)
	resp.Body.Close()
	return output
}
