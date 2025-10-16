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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
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
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	labels := make(map[string]any)
	labels["testkey"] = "testvalue"
	data, err = json.Marshal(labels)
	re.NoError(err)
	resp, err = tests.TestDialClient.Post(leader.GetAddr()+"/pd/api/v1/debug/pprof/profile?seconds=1", "application/json", bytes.NewBuffer(data))
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)

	re.Equal("Profile", resp.Header.Get("service-label"))
	re.JSONEq("{\"seconds\":[\"1\"]}", resp.Header.Get("url-param"))
	re.JSONEq("{\"testkey\":\"testvalue\"}", resp.Header.Get("body-param"))
	re.Equal("HTTP/1.1/POST:/pd/api/v1/debug/pprof/profile", resp.Header.Get("method"))
	re.Equal("anonymous", resp.Header.Get("caller-id"))
	re.Equal("127.0.0.1", resp.Header.Get("ip"))

	input = map[string]any{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	header := mustRequestSuccess(re, leader.GetServer())
	re.Equal("GetVersion", header.Get("service-label"))

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/api/addRequestInfoMiddleware"))
}

func BenchmarkDoRequestWithServiceMiddleware(b *testing.B) {
	re := require.New(b)
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	b.StartTimer()
	for range b.N {
		err = doTestRequestWithLogAudit(leader)
		re.NoError(err)
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
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())

	// returns StatusOK when no rate-limit config
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	re.NoError(err)
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
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config/rate-limit", bytes.NewBuffer(jsonBody))
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)

	for i := range 3 {
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
	err = leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	re.NoError(err)
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	tests.MustWaitLeader(re, servers)
	leader = suite.cluster.GetLeaderServer()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	cfg, ok := leader.GetServer().GetRateLimitConfig().LimiterConfig["SetLogLevel"]
	re.True(ok)
	re.Equal(uint64(1), cfg.ConcurrencyLimit)
	re.Equal(0.5, cfg.QPS)
	re.Equal(1, cfg.QPSBurst)

	for i := range 3 {
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())

	for range 3 {
		req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		re.NoError(err)
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
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
}

func (suite *middlewareTestSuite) TestSwaggerUrl() {
	re := suite.Require()
	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	req, err := http.NewRequest(http.MethodGet, leader.GetAddr()+"/swagger/ui/index", http.NoBody)
	re.NoError(err)
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
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())
	timeUnix := time.Now().Unix() - 20
	req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), http.NoBody)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)

	req, err = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", http.NoBody)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	re.NoError(err)
	output := string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_count{method=\"HTTP\",service=\"GetTrend\"} 1")

	// resign to test persist config
	oldLeaderName := leader.GetServer().Name()
	err = leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	re.NoError(err)
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	tests.MustWaitLeader(re, servers)
	leader = suite.cluster.GetLeaderServer()

	timeUnix = time.Now().Unix() - 20
	req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), http.NoBody)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)

	req, err = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", http.NoBody)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, err = io.ReadAll(resp.Body)
	re.NoError(err)
	output = string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_count{method=\"HTTP\",service=\"GetTrend\"} 2")

	input = map[string]any{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	re.NoError(err)
	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
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
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	req, err = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	resp.Body.Close()
	b, err := os.ReadFile(fname)
	re.NoError(err)
	re.Contains(string(b), "audit log")
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
}

func BenchmarkDoRequestWithLocalLogAudit(b *testing.B) {
	re := require.New(b)
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	b.StartTimer()
	for range b.N {
		err = doTestRequestWithLogAudit(leader)
		re.NoError(err)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithPrometheusAudit(b *testing.B) {
	re := require.New(b)
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	b.StartTimer()
	for range b.N {
		err = doTestRequestWithPrometheus(leader)
		re.NoError(err)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithoutServiceMiddleware(b *testing.B) {
	re := require.New(b)
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leader := cluster.GetLeaderServer()
	input := map[string]any{
		"enable-audit": "false",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	b.StartTimer()
	for range b.N {
		err = doTestRequestWithLogAudit(leader)
		re.NoError(err)
	}
	cancel()
	cluster.Destroy()
}

func doTestRequestWithLogAudit(srv *tests.TestServer) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/pd/api/v1/admin/cache/regions", srv.GetAddr()), http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set(apiutil.XCallerIDHeader, "test")
	resp, err := tests.TestDialClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func doTestRequestWithPrometheus(srv *tests.TestServer) error {
	timeUnix := time.Now().Unix() - 20
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", srv.GetAddr(), timeUnix), http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set(apiutil.XCallerIDHeader, "test")
	resp, err := tests.TestDialClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
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
	err := leader.ResignLeaderWithRetry()
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
	re.Empty(resp.Header.Get(apiutil.PDRedirectorHeader))
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
}

func (suite *redirectorTestSuite) TestPing() {
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

	for _, svr := range suite.cluster.GetServers() {
		if svr.GetServer() != follower {
			err := svr.Stop()
			re.NoError(err)
		}
	}
	addr := follower.GetAddr() + "/pd/api/v1/ping"
	request, err := http.NewRequest(http.MethodGet, addr, http.NoBody)
	// ping request should not be redirected.
	request.Header.Add(apiutil.PDAllowFollowerHandleHeader, "true")
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(request)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	for _, svr := range suite.cluster.GetServers() {
		if svr.GetServer() != follower {
			re.NoError(svr.Run())
		}
	}
	re.NotEmpty(suite.cluster.WaitLeader())
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
	b, err := os.ReadFile(fname)
	re.NoError(err)
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
		Store:  &metapb.Store{Id: 1, Address: "mock://tikv-1:1"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	stores := []*metapb.Store{
		{
			Id:        1,
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		},
		{
			Id:        2,
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		},
		{
			Id:        3,
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
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

	// wait that stores are up
	testutil.Eventually(re, func() bool {
		output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores", http.MethodGet, http.StatusOK)
		var storesInfo response.StoresInfo
		if err := json.Unmarshal(output, &storesInfo); err != nil {
			return false
		}
		if len(storesInfo.Stores) != 3 {
			return false
		}
		for _, store := range storesInfo.Stores {
			if store.Store.GetNodeState() != metapb.NodeState_Serving {
				return false
			}
		}
		return true
	})

	// remove store 1 and store 2
	sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/1", http.MethodDelete, http.StatusOK)
	sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/2", http.MethodDelete, http.StatusOK)

	time.Sleep(100 * time.Millisecond) // wait for the removing progress to be created
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
			req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
			re.NoError(err)
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
		if leader.GetRaftCluster() == nil {
			return false
		}
		if !leader.GetRaftCluster().IsPrepared() {
			leader.GetRaftCluster().SetPrepared()
			return false
		}
		url := leader.GetAddr() + "/pd/api/v1/stores/progress?action=removing"
		req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		re.NoError(err)
		resp, err := tests.TestDialClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		output, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &p))
		t.Logf("progress: %v", p)
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
		// average speed = (2+4)/2 = 3.0
		// If checkStore is executed multiple times, the time windows will increase
		// which is 10s, 20s, 30s ..., the corresponding speed will be 3.0, 1.5, 1 ...
		if p.CurrentSpeed > 3.0 {
			return false
		}
		// store 1: (20+50)/4 = 17.5s
		// store 2: (10+40)/2 = 25s
		// average time = (17.5+25)/2 = 21.25s
		if p.LeftSeconds < 21.25 {
			return false
		}

		output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusOK)
		re.NoError(json.Unmarshal(output, &p))
		re.Equal("removing", p.Action)
		t.Logf("progress for store 2: %v", p)
		// store 2: (30-10)/(30+40) ~= 0.285
		// average progress ~= (0.36+0.28)/2 = 0.32
		if p.Progress > 0.3 {
			return false
		}
		// store 2: 20/10s = 2
		if p.CurrentSpeed > 2.0 {
			return false
		}
		// store 2: (10+40)/2 = 25s
		if p.LeftSeconds < 25.0 {
			return false
		}
		return true
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

type forwardTestSuite struct {
	suite.Suite
	env      *tests.SchedulingTestEnvironment
	leader   *server.Server
	follower *server.Server
}

func TestSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(forwardTestSuite))
}

func (suite *forwardTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/skipStoreConfigSync", `return(true)`))
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
	suite.env.PDCount = 3
	suite.env.RunFunc(func(cluster *tests.TestCluster) {
		suite.leader = cluster.GetLeaderServer().GetServer()
		for _, svr := range cluster.GetServers() {
			if svr.GetAddr() != suite.leader.GetAddr() {
				suite.follower = svr.GetServer()
				break
			}
		}
		re.NotNil(suite.follower)
	})
}

func (suite *forwardTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.env.Cleanup()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/skipStoreConfigSync"))
}

func (suite *forwardTestSuite) TestFollowerLocalAPIs() {
	suite.env.RunTest(suite.checkAdminLog)
	suite.env.RunTest(suite.checkGetRequest)
	suite.env.RunTest(suite.checkConfig)
}

func (suite *forwardTestSuite) checkAdminLog(_ *tests.TestCluster) {
	re := suite.Require()

	addr := suite.follower.GetAddr() + "/pd/api/v1/admin/log"
	level := "debug"
	data, err := json.Marshal(level)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, addr, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()

	re.Equal(http.StatusOK, resp.StatusCode)
	re.Equal(suite.follower.GetConfig().Name, resp.Header.Get(apiutil.XPDHandleHeader), "request should be handled locally")
}

func (suite *forwardTestSuite) checkGetRequest(_ *tests.TestCluster) {
	re := suite.Require()
	followerName := suite.follower.GetConfig().Name

	testPaths := map[string]bool{
		"/pd/api/v1/ping":                  true,
		"/pd/api/v1/config":                true, // GET config is handled by local server
		"/pd/api/v1/version":               true,
		"/pd/api/v1/status":                true,
		"/pd/api/v1/health":                true,
		"/pd/api/v1/debug/pprof/goroutine": true,
		"/pd/api/v1/debug/pprof/zip":       true,
		"/pd/api/v1/schedulers":            false,
		"/pd/api/v1/operators":             false,
	}

	for path, isLocal := range testPaths {
		addr := suite.follower.GetAddr() + path
		req, err := http.NewRequest(http.MethodGet, addr, http.NoBody)
		re.NoError(err)
		resp, err := tests.TestDialClient.Do(req)
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		if isLocal {
			re.Equal(followerName, resp.Header.Get(apiutil.XPDHandleHeader), path+" request should not be redirected")
		} else {
			re.NotEqual(followerName, resp.Header.Get(apiutil.XPDHandleHeader), path+" request should not be redirected")
		}
		resp.Body.Close()
	}
}

func (suite *forwardTestSuite) checkConfig(_ *tests.TestCluster) {
	re := suite.Require()
	followerURL := suite.follower.GetAddr() + "/pd/api/v1/config"
	followerName := suite.follower.GetConfig().Name
	// Test local config
	var followerCfg config.Config
	err := testutil.ReadGetJSON(re, tests.TestDialClient, followerURL, &followerCfg,
		testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(suite.follower.GetConfig().ClientUrls, followerCfg.ClientUrls)
	re.NotEqual(suite.leader.GetConfig().ClientUrls, followerCfg.ClientUrls)
	re.Equal(suite.follower.GetConfig().AdvertiseClientUrls, followerCfg.AdvertiseClientUrls)
	re.NotEqual(suite.leader.GetConfig().AdvertiseClientUrls, followerCfg.AdvertiseClientUrls)
	re.Equal(suite.follower.GetConfig().Name, followerCfg.Name)
	re.NotEqual(suite.leader.GetConfig().Name, followerCfg.Name)
	re.Equal(suite.follower.GetConfig().PeerUrls, followerCfg.PeerUrls)
	re.NotEqual(suite.leader.GetConfig().PeerUrls, followerCfg.PeerUrls)
	// Test sync config
	leaderURL := suite.leader.GetAddr() + "/pd/api/v1/config"
	reqData, err := json.Marshal(map[string]any{
		"max-replicas": 4,
	})
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leaderURL, bytes.NewBuffer(reqData))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	re.NoError(err)
	re.NotEqual(followerName, resp.Header.Get(apiutil.XPDHandleHeader), "POST /config request should not be redirected")
	testutil.Eventually(re, func() bool {
		var followerCfg config.Config
		err = testutil.ReadGetJSON(re, tests.TestDialClient, followerURL, &followerCfg)
		re.NoError(err)
		return followerCfg.Replication.MaxReplicas == 4.
	})
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
		Store:  &metapb.Store{Id: 1, Address: "mock://tikv-1:1"},
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
		Store:  &metapb.Store{Id: 1, Address: "mock://tikv-1:1"},
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
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             2,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             3,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             4,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             5,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
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

	ch := make(chan struct{})
	defer close(ch)
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/server/cluster/blockCheckStores", func() {
		<-ch
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/blockCheckStores"))
	}()
	triggerCheckStores := func() { ch <- struct{}{} }

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
		defer triggerCheckStores()
		// wait for cluster prepare
		if !leader.GetRaftCluster().IsPrepared() {
			leader.GetRaftCluster().SetPrepared()
			return false
		}
		url := leader.GetAddr() + "/pd/api/v1/stores/progress?action=preparing"
		req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		re.NoError(err)
		resp, err := tests.TestDialClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		output, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		re.NoError(json.Unmarshal(output, &p))
		t.Logf("progress: %v", p)

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
		defer triggerCheckStores()
		output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
		re.NoError(json.Unmarshal(output, &p))
		t.Logf("progress: %v", p)
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
		// If checkStore is executed multiple times, the time windows will increase
		// which is 10s, 20s, 30s ..., the corresponding speed will be 2.5, 1.5, 1 ...
		if p.CurrentSpeed > 2.5 {
			return false
		}
		// store 4: 179/1 ~= 179
		// store 5: 149/4 ~= 37.25
		// average time ~= (179+37.25)/2 = 108.125
		if p.LeftSeconds < 108.125 {
			return false
		}

		output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusOK)
		re.NoError(json.Unmarshal(output, &p))
		t.Logf("progress for store 4: %v", p)
		if p.Action != "preparing" {
			return false
		}
		if fmt.Sprintf("%.2f", p.Progress) != "0.05" {
			return false
		}
		if p.CurrentSpeed > 1.0 {
			return false
		}
		if p.LeftSeconds < 179.0 {
			return false
		}

		return true
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func sendRequest(re *require.Assertions, url string, method string, statusCode int) (output []byte) {
	req, err := http.NewRequest(method, url, http.NoBody)
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		resp, err := tests.TestDialClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		// Due to service unavailability caused by environmental issues,
		// we will retry it.
		if resp.StatusCode == http.StatusServiceUnavailable {
			return false
		}
		if resp.StatusCode != statusCode {
			return false
		}
		output, err = io.ReadAll(resp.Body)
		re.NoError(err)
		return true
	})

	return output
}
