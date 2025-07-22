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

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
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
	grpcPDClient := testutil.MustNewGrpcClient(re, addr)
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

	grpcPDClient1 := testutil.MustNewGrpcClient(re, addr)
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

	cfgs := tests.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), 3)

	ch := make(chan *server.Server, 3)
	for i := range 3 {
		cfg := cfgs[i]

		go func() {
			mockHandler := tests.CreateMockHandler(re, "127.0.0.1")
			svr, err := server.CreateServer(suite.ctx, cfg, nil, mockHandler)
			re.NoError(err)
			err = svr.Run()
			re.NoError(err)
			ch <- svr
		}()
	}

	for range 3 {
		svr := <-ch
		suite.svrs[svr.GetAddr()] = svr
		suite.leaderPath = svr.GetMember().GetLeaderPath()
	}
}

func (suite *leaderServerTestSuite) TearDownSuite() {
	suite.cancel()
	for _, svr := range suite.svrs {
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}
}

func newTestServersWithCfgs(
	ctx context.Context,
	cfgs []*config.Config,
	re *require.Assertions,
) ([]*server.Server, testutil.CleanupFunc) {
	svrs := make([]*server.Server, 0, len(cfgs))

	ch := make(chan *server.Server)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			mockHandler := tests.CreateMockHandler(re, "127.0.0.1")
			svr, err := server.CreateServer(ctx, cfg, nil, mockHandler)
			// prevent blocking if Asserts fails
			failed := true
			defer func() {
				if failed {
					ch <- nil
				} else {
					ch <- svr
				}
			}()
			re.NoError(err)
			err = svr.Run()
			re.NoError(err)
			failed = false
		}(cfg)
	}

	for range cfgs {
		svr := <-ch
		re.NotNil(svr)
		svrs = append(svrs, svr)
	}
	tests.MustWaitLeader(re, svrs)

	cleanup := func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return svrs, cleanup
}

func (suite *leaderServerTestSuite) TestRegisterServerHandler() {
	re := suite.Require()
	cfg := tests.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	mockHandler := tests.CreateMockHandler(re, "127.0.0.1")
	svr, err := server.CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = server.CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()
	err = svr.Run()
	re.NoError(err)
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
	mockHandler := tests.CreateMockHandler(re, "127.0.0.2")
	cfg := tests.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = server.CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

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
	mockHandler := tests.CreateMockHandler(re, "127.0.0.2")
	cfg := tests.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = server.CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

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
	mockHandler := tests.CreateMockHandler(re, "127.0.0.2")
	cfg := tests.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, nil, mockHandler)
	re.NoError(err)
	_, err = server.CreateServer(ctx, cfg, nil, mockHandler, mockHandler)
	// Repeat register.
	re.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()
	err = svr.Run()
	re.NoError(err)

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

	cfg := tests.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	defer testutil.CleanServer(cfg.DataDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockHandler := tests.CreateMockHandler(re, "127.0.0.1")
	svr, err := server.CreateServer(ctx, cfg, []string{constant.PDServiceName}, mockHandler)
	re.NoError(err)
	defer svr.Close()
	err = svr.Run()
	re.NoError(err)
	tests.MustWaitLeader(re, []*server.Server{svr})
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
	cfgs := tests.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), 2)
	for _, cfg := range cfgs {
		cfg.DataDir = t.TempDir()
		// Clean up before testing.
		testutil.CleanServer(cfg.DataDir)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster.
	svrsA, cleanA := newTestServersWithCfgs(ctx, []*config.Config{cfgA}, re)
	defer cleanA()
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := newTestServersWithCfgs(ctx, []*config.Config{cfgB}, re)
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	mockHandler := tests.CreateMockHandler(re, "127.0.0.1")
	svr, err := server.CreateServer(ctx, cfgA, nil, mockHandler)
	re.NoError(err)

	etcdCfg, err := svr.GetConfig().GenEmbedEtcdConfig()
	re.NoError(err)
	etcd, err := embed.StartEtcd(etcdCfg)
	re.NoError(err)
	urlsMap, err := etcdtypes.NewURLsMap(svr.GetConfig().InitialCluster)
	re.NoError(err)
	tlsConfig, err := svr.GetConfig().Security.ToClientTLSConfig()
	re.NoError(err)
	err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlsMap, tlsConfig)
	re.Error(err)
	etcd.Close()
	testutil.CleanServer(cfgA.DataDir)
}
