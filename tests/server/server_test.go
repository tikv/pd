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

package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"

	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestUpdateAdvertiseUrls(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	// AdvertisePeerUrls should equals to PeerUrls.
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		re.Equal(conf.PeerURLs, serverConf.AdvertisePeerUrls)
		re.Equal(conf.ClientURLs, serverConf.AdvertiseClientUrls)
	}

	err = cluster.StopAll()
	re.NoError(err)

	// Change config will not affect peer urls.
	// Recreate servers with new peer URLs.
	for _, conf := range cluster.GetConfig().InitialServers {
		conf.AdvertisePeerURLs = conf.PeerURLs + "," + tempurl.Alloc()
	}
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf, err := conf.Generate()
		re.NoError(err)
		s, err := tests.NewTestServer(ctx, serverConf, nil)
		re.NoError(err)
		cluster.GetServers()[conf.Name] = s
	}
	err = cluster.RunInitialServers()
	re.NoError(err)
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		re.Equal(conf.PeerURLs, serverConf.AdvertisePeerUrls)
	}
}

func TestClusterID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	clusterID := keypath.ClusterID()
	keypath.ResetClusterID()

	// Restart all PDs.
	re.NoError(cluster.StopAll())
	re.NoError(cluster.RunInitialServers())

	// PD should have the same cluster ID as before.
	re.Equal(clusterID, keypath.ClusterID())
	keypath.ResetClusterID()

	cluster2, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) { conf.InitialClusterToken = "foobar" })
	defer cluster2.Destroy()
	re.NoError(err)
	err = cluster2.RunInitialServers()
	re.NoError(err)
	re.NotEqual(clusterID, keypath.ClusterID())
}

func TestLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	re.NotEmpty(leader1)

	err = cluster.GetServer(leader1).Stop()
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return cluster.GetLeader() != leader1
	})
}

func TestGRPCRateLimit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetServer(leader)
	clusterID := leaderServer.GetClusterID()
	addr := leaderServer.GetAddr()
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, addr)
	defer conn.Close()
	err = leaderServer.BootstrapCluster()
	re.NoError(err)
	for range 100 {
		resp, err := grpcPDClient.GetRegion(context.Background(), &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: clusterID},
			RegionKey: []byte(""),
		})
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	// test rate limit
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/service-middleware/config/grpc-rate-limit", addr)
	input := make(map[string]any)
	input["label"] = "GetRegion"
	input["qps"] = 1
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix, jsonBody,
		testutil.StatusOK(re), testutil.StringContain(re, "gRPC limiter is updated"))
	re.NoError(err)
	for i := range 2 {
		resp, err := grpcPDClient.GetRegion(context.Background(), &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			RegionKey: []byte(""),
		})
		re.Empty(resp.GetHeader().GetError())
		if i == 0 {
			re.NoError(err)
		} else {
			re.Error(err)
			re.Contains(err.Error(), "rate limit exceeded")
		}
	}

	input["label"] = "GetRegion"
	input["qps"] = 0
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix, jsonBody,
		testutil.StatusOK(re), testutil.StringContain(re, "gRPC limiter is deleted"))
	re.NoError(err)
	for range 100 {
		resp, err := grpcPDClient.GetRegion(context.Background(), &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			RegionKey: []byte(""),
		})
		re.NoError(err)
		re.Empty(resp.GetHeader().GetError())
	}

	// test concurrency limit
	input["concurrency"] = 1
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	var (
		okCh  = make(chan struct{})
		errCh = make(chan string)
	)
	err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix, jsonBody,
		testutil.StatusOK(re), testutil.StringContain(re, "gRPC limiter is updated"))
	re.NoError(err)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayProcess", `pause`))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := grpcPDClient.GetRegion(context.Background(), &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			RegionKey: []byte(""),
		})
		re.Empty(resp.GetHeader().GetError())
		if err != nil {
			errCh <- err.Error()
		} else {
			okCh <- struct{}{}
		}
	}()

	grpcPDClient1, conn1 := testutil.MustNewGrpcClient(re, addr)
	defer conn1.Close()
	go func() {
		defer wg.Done()
		resp, err := grpcPDClient1.GetRegion(context.Background(), &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			RegionKey: []byte(""),
		})
		re.Empty(resp.GetHeader().GetError())
		if err != nil {
			errCh <- err.Error()
		} else {
			okCh <- struct{}{}
		}
	}()
	errStr := <-errCh
	re.Contains(errStr, "rate limit exceeded")
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayProcess"))
	<-okCh
	wg.Wait()
}

type leaderServerTestSuite struct {
	suite.Suite

	ctx        context.Context
	cancel     context.CancelFunc
	cluster    *tests.TestCluster
	svrs       map[string]*server.Server
	leaderPath string
}

func TestLeaderServerTestSuite(t *testing.T) {
	suite.Run(t, new(leaderServerTestSuite))
}

func (suite *leaderServerTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.svrs = make(map[string]*server.Server)

	cluster, err := tests.NewTestCluster(suite.ctx, 3)
	re.NoError(err)
	suite.cluster = cluster

	err = cluster.RunInitialServers()
	re.NoError(err)

	cluster.WaitLeader()

	for _, s := range cluster.GetServers() {
		suite.svrs[s.GetAddr()] = s.GetServer()
		suite.leaderPath = s.GetServer().GetMember().GetElectionPath()
	}
}

func (suite *leaderServerTestSuite) TearDownSuite() {
	suite.cancel()
	if suite.cluster != nil {
		suite.cluster.Destroy()
	}
}

func (suite *leaderServerTestSuite) TestRegisterServerHandler() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockHandler := createMockHandler(re, "127.0.0.1")
	cluster, err := tests.NewTestClusterWithHandlers(ctx, 1, []server.HandlerBuilder{mockHandler})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer()

	resp, err := http.Get(fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()))
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderForwarded() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockHandler := createMockHandler(re, "127.0.0.2")
	cluster, err := tests.NewTestClusterWithHandlers(ctx, 1, []server.HandlerBuilder{mockHandler})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XForwardedForHeader, "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderXReal() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockHandler := createMockHandler(re, "127.0.0.2")
	cluster, err := tests.NewTestClusterWithHandlers(ctx, 1, []server.HandlerBuilder{mockHandler})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XRealIPHeader, "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderBoth() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockHandler := createMockHandler(re, "127.0.0.2")
	cluster, err := tests.NewTestClusterWithHandlers(ctx, 1, []server.HandlerBuilder{mockHandler})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), http.NoBody)
	re.NoError(err)
	req.Header.Add(apiutil.XForwardedForHeader, "127.0.0.2")
	req.Header.Add(apiutil.XRealIPHeader, "127.0.0.3")
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	bodyString := string(bodyBytes)
	re.Equal("Hello World\n", bodyString)
}

func TestAPIService(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer().GetServer()

	re.True(svr.IsKeyspaceGroupEnabled())
}

func TestIsPathInDirectory(t *testing.T) {
	re := require.New(t)
	fileName := "test"
	directory := "/root/project"
	path := filepath.Join(directory, fileName)
	re.True(apiutil.IsPathInDirectory(path, directory))

	fileName = filepath.Join("..", "..", "test")
	path = filepath.Join(directory, fileName)
	re.False(apiutil.IsPathInDirectory(path, directory))
}

func TestCheckClusterID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a standalone cluster A
	clusterA, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer clusterA.Destroy()

	err = clusterA.RunInitialServers()
	re.NoError(err)

	leaderA := clusterA.WaitLeader()
	re.NotEmpty(leaderA)

	// Close cluster A
	err = clusterA.StopAll()
	re.NoError(err)

	// Start another standalone cluster B
	clusterB, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer clusterB.Destroy()

	err = clusterB.RunInitialServers()
	re.NoError(err)

	leaderB := clusterB.WaitLeader()
	re.NotEmpty(leaderB)

	// Now try to check if cluster A's cluster ID matches cluster B
	// This should fail because they have different cluster IDs
	cfgA := clusterA.GetServer(leaderA).GetConfig()
	cfgB := clusterB.GetServer(leaderB).GetConfig()

	mockHandler := createMockHandler(re, "127.0.0.1")
	svr, err := server.CreateServer(ctx, cfgA, nil, mockHandler)
	re.NoError(err)

	etcdCfg, err := svr.GetConfig().GenEmbedEtcdConfig()
	re.NoError(err)
	etcd, err := embed.StartEtcd(etcdCfg)
	re.NoError(err)
	defer etcd.Close()

	// Try to check cluster A's ID against cluster B's URLs
	// This should fail because the cluster IDs don't match
	urlsMap, err := etcdtypes.NewURLsMap(cfgB.InitialCluster)
	re.NoError(err)
	tlsConfig, err := svr.GetConfig().Security.ToClientTLSConfig()
	re.NoError(err)
	err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlsMap, tlsConfig)
	re.Error(err)
}

func TestMeteringWriter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		// Ensure the metering writer can be started.
		conf.Metering = mconfig.MeteringConfig{
			Type:   storage.ProviderTypeS3,
			Region: "us-west-2",
			Bucket: "test-bucket",
		}
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetLeaderServer().GetServer()

	collectors := svr.GetMeteringWriter().GetCollectors()
	re.Len(collectors, 1)
	ruCollector, ok := collectors[rmserver.ResourceManagerCategory]
	re.True(ok)
	re.Equal(rmserver.ResourceManagerCategory, ruCollector.Category())
}

func TestSetPDServerConfigWithDashboard(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	svr := cluster.GetServer(leader).GetServer()

	// Test updating config without changing dashboard address
	cfg := svr.GetPDServerConfig()
	originalDashboard := cfg.DashboardAddress
	originalUseRegionStorage := cfg.UseRegionStorage

	// Change some other field but keep dashboard the same
	cfg.UseRegionStorage = !cfg.UseRegionStorage
	err = svr.SetPDServerConfig(*cfg)
	re.NoError(err)

	newCfg := svr.GetPDServerConfig()
	re.Equal(originalDashboard, newCfg.DashboardAddress)
	re.NotEqual(originalUseRegionStorage, newCfg.UseRegionStorage)

	// Change both other field and dashboard
	cfg.UseRegionStorage = !cfg.UseRegionStorage
	cfg.DashboardAddress = "https://new-dashboard-address:1234"
	err = svr.SetPDServerConfig(*cfg)
	re.ErrorContains(err, "is not the client url of any member")
}

// createMockHandler creates a mock handler for test.
func createMockHandler(re *require.Assertions, ip string) server.HandlerBuilder {
	return func(context.Context, *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/apis/mock/v1/hello", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello World")
			// test getting ip
			clientIP, _ := apiutil.GetIPPortFromHTTPRequest(r)
			re.Equal(ip, clientIP)
		})
		info := apiutil.APIServiceGroup{
			Name:    "mock",
			Version: "v1",
		}
		return mux, info, nil
	}
}
