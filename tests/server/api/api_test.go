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
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil/serverapi"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
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
	cluster, err := tests.NewTestCluster(ctx, 3)
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

	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled(), Equals, true)

	labels := make(map[string]interface{})
	labels["testkey"] = "testvalue"
	data, _ = json.Marshal(labels)
	resp, err = dialClient.Post(leader.GetAddr()+"/pd/api/v1/debug/pprof/profile?force=true", "application/json", bytes.NewBuffer(data))
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	c.Assert(resp.Header.Get("service-label"), Equals, "Profile")
	c.Assert(resp.Header.Get("url-param"), Equals, "{\"force\":[\"true\"]}")
	c.Assert(resp.Header.Get("body-param"), Equals, "{\"testkey\":\"testvalue\"}")
	c.Assert(resp.Header.Get("method"), Equals, "HTTP/1.1/POST:/pd/api/v1/debug/pprof/profile")
	c.Assert(resp.Header.Get("component"), Equals, "anonymous")
	c.Assert(resp.Header.Get("ip"), Equals, "127.0.0.1")

	input = map[string]interface{}{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled(), Equals, false)

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
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
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
	input := map[string]interface{}{
		"enable-audit": "false",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
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

func (s *testMiddlewareSuite) TestAuditPrometheusBackend(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled(), Equals, true)
	timeUnix := time.Now().Unix() - 20
	req, _ = http.NewRequest("GET", fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/metrics", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	output := string(content)
	c.Assert(strings.Contains(output, "pd_service_audit_handling_seconds_count{component=\"anonymous\",method=\"HTTP\",service=\"GetTrend\"} 1"), Equals, true)

	// resign to test persist config
	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	mustWaitLeader(c, s.cluster.GetServers())
	leader = s.cluster.GetServer(s.cluster.GetLeader())

	timeUnix = time.Now().Unix() - 20
	req, _ = http.NewRequest("GET", fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)

	req, _ = http.NewRequest("GET", leader.GetAddr()+"/metrics", nil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	content, _ = io.ReadAll(resp.Body)
	output = string(content)
	c.Assert(strings.Contains(output, "pd_service_audit_handling_seconds_count{component=\"anonymous\",method=\"HTTP\",service=\"GetTrend\"} 2"), Equals, true)

	input = map[string]interface{}{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled(), Equals, false)
}

func (s *testMiddlewareSuite) TestAuditLocalLogBackend(c *C) {
	tempStdoutFile, _ := os.CreateTemp("/tmp", "pd_tests")
	cfg := &log.Config{}
	cfg.File.Filename = tempStdoutFile.Name()
	cfg.Level = "info"
	lg, p, _ := log.InitLogger(cfg)
	log.ReplaceGlobals(lg, p)
	leader := s.cluster.GetServer(s.cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled(), Equals, true)

	req, _ = http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	b, _ := os.ReadFile(tempStdoutFile.Name())
	c.Assert(strings.Contains(string(b), "Audit Log"), Equals, true)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	os.Remove(tempStdoutFile.Name())
}

func BenchmarkDoRequestWithLocalLogAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequest(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithoutLocalLogAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "false",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequest(leader)
	}
	cancel()
	cluster.Destroy()
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

var _ = Suite(&testProgressSuite{})

type testProgressSuite struct{}

func (s *testProgressSuite) TestRemovingProgress(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`), IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Replication.MaxReplicas = 1
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	_, err = grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
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
		pdctl.MustPutStore(c, leader.GetServer(), store)
	}
	pdctl.MustPutRegion(c, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	pdctl.MustPutRegion(c, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(30))
	pdctl.MustPutRegion(c, cluster, 1002, 1, []byte("e"), []byte("f"), core.SetApproximateSize(50))
	pdctl.MustPutRegion(c, cluster, 1003, 2, []byte("g"), []byte("h"), core.SetApproximateSize(40))

	// no store removing
	output := sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the action"), IsTrue)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the given store ID"), IsTrue)

	// remove store 1 and store 2
	_ = sendRequest(c, leader.GetAddr()+"/pd/api/v1/store/1", http.MethodDelete, http.StatusOK)
	_ = sendRequest(c, leader.GetAddr()+"/pd/api/v1/store/2", http.MethodDelete, http.StatusOK)

	// size is not changed.
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	var p api.Progress
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "removing")
	c.Assert(p.Progress, Equals, 0.0)
	c.Assert(p.CurrentSpeed, Equals, 0.0)
	c.Assert(p.LeftSeconds, Equals, math.MaxFloat64)

	// update size
	pdctl.MustPutRegion(c, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(20))
	pdctl.MustPutRegion(c, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(10))

	// is not prepared
	time.Sleep(2 * time.Second)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "removing")
	c.Assert(p.Progress, Equals, 0.0)
	c.Assert(p.CurrentSpeed, Equals, 0.0)
	c.Assert(p.LeftSeconds, Equals, math.MaxFloat64)

	leader.GetRaftCluster().SetPrepared()
	time.Sleep(2 * time.Second)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "removing")
	// store 1: (60-20)/(60+50) ~= 0.36
	// store 2: (30-10)/(30+40) ~= 0.28
	// average progress ~= (0.36+0.28)/2 = 0.32
	c.Assert(fmt.Sprintf("%.2f", p.Progress), Equals, "0.32")
	// store 1: 40/10s = 4
	// store 2: 20/10s = 2
	// average speed = (2+4)/2 = 33
	c.Assert(p.CurrentSpeed, Equals, 3.0)
	// store 1: (20+50)/4 = 17.5s
	// store 2: (10+40)/2 = 25s
	// average time = (17.5+25)/2 = 21.25s
	c.Assert(p.LeftSeconds, Equals, 21.25)

	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusOK)
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "removing")
	// store 2: (30-10)/(30+40) ~= 0.285
	c.Assert(fmt.Sprintf("%.2f", p.Progress), Equals, "0.29")
	// store 2: 20/10s = 2
	c.Assert(p.CurrentSpeed, Equals, 2.0)
	// store 2: (10+40)/2 = 25s
	c.Assert(p.LeftSeconds, Equals, 25.0)

	c.Assert(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"), IsNil)
}

func (s *testProgressSuite) TestPreparingProgress(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`), IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Replication.MaxReplicas = 1
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	_, err = grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
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

	for _, store := range stores {
		pdctl.MustPutStore(c, leader.GetServer(), store)
	}
	for i := 0; i < 100; i++ {
		pdctl.MustPutRegion(c, cluster, uint64(i+1), uint64(i)%3+1, []byte(fmt.Sprintf("p%d", i)), []byte(fmt.Sprintf("%d", i+1)), core.SetApproximateSize(10))
	}
	// no store preparing
	output := sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the action"), IsTrue)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the given store ID"), IsTrue)

	// is not prepared
	time.Sleep(2 * time.Second)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the action"), IsTrue)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusNotFound)
	c.Assert(strings.Contains((string(output)), "no progress found for the given store ID"), IsTrue)

	// size is not changed.
	leader.GetRaftCluster().SetPrepared()
	time.Sleep(2 * time.Second)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
	var p api.Progress
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "preparing")
	c.Assert(p.Progress, Equals, 0.0)
	c.Assert(p.CurrentSpeed, Equals, 0.0)
	c.Assert(p.LeftSeconds, Equals, math.MaxFloat64)

	// update size
	pdctl.MustPutRegion(c, cluster, 1000, 4, []byte(fmt.Sprintf("%d", 1000)), []byte(fmt.Sprintf("%d", 1001)), core.SetApproximateSize(10))
	pdctl.MustPutRegion(c, cluster, 1001, 5, []byte(fmt.Sprintf("%d", 1001)), []byte(fmt.Sprintf("%d", 1002)), core.SetApproximateSize(40))
	time.Sleep(2 * time.Second)
	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "preparing")
	// store 4: 10/(210*0.9) ~= 0.05
	// store 5: 40/(210*0.9) ~= 0.21
	// average progress ~= (0.05+0.21)/2 = 0.13
	c.Assert(fmt.Sprintf("%.2f", p.Progress), Equals, "0.13")
	// store 4: 10/10s = 1
	// store 5: 40/10s = 4
	// average speed = (1+4)/2 = 2.5
	c.Assert(p.CurrentSpeed, Equals, 2.5)
	// store 4: 179/1 ~= 179
	// store 5: 149/4 ~= 37.25
	// average time ~= (179+37.25)/2 = 108.125
	c.Assert(p.LeftSeconds, Equals, 108.125)

	output = sendRequest(c, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusOK)
	c.Assert(json.Unmarshal(output, &p), IsNil)
	c.Assert(p.Action, Equals, "preparing")
	c.Assert(fmt.Sprintf("%.2f", p.Progress), Equals, "0.05")
	c.Assert(p.CurrentSpeed, Equals, 1.0)
	c.Assert(p.LeftSeconds, Equals, 179.0)

	c.Assert(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"), IsNil)
}

func sendRequest(c *C, url string, method string, statusCode int) []byte {
	req, _ := http.NewRequest(method, url, nil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, statusCode)
	output, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	resp.Body.Close()
	return output
}

func mustWaitLeader(c *C, svrs map[string]*tests.TestServer) *server.Server {
	var leader *server.Server
	testutil.WaitUntil(c, func() bool {
		for _, s := range svrs {
			if !s.GetServer().IsClosed() && s.GetServer().GetMember().IsLeader() {
				leader = s.GetServer()
				return true
			}
		}
		return false
	})
	return leader
}
