// Copyright 2016 TiKV Project Authors.
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

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type leaderServerTestSuite struct {
	suite.Suite

	ctx        context.Context
	cancel     context.CancelFunc
	svrs       map[string]*Server
	leaderPath string
}

func TestLeaderServerTestSuite(t *testing.T) {
	suite.Run(t, new(leaderServerTestSuite))
}

func (suite *leaderServerTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.svrs = make(map[string]*Server)

	cfgs := NewTestMultiConfig(assertutil.CheckerWithNilAssert(suite.Require()), 3)

	ch := make(chan *Server, 3)
	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			MockHandler := CreateMockHandler(suite.Require(), "127.0.0.1")
			svr, err := CreateServer(suite.ctx, cfg, MockHandler)
			suite.NoError(err)
			err = svr.Run()
			suite.NoError(err)
			ch <- svr
		}()
	}

	for i := 0; i < 3; i++ {
		svr := <-ch
		suite.svrs[svr.GetAddr()] = svr
		suite.leaderPath = svr.GetMember().GetLeaderPath()
	}
}

func (suite *leaderServerTestSuite) TearDownSuite() {
	suite.cancel()
	for _, svr := range suite.svrs {
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}
}

func (suite *leaderServerTestSuite) newTestServersWithCfgs(ctx context.Context, cfgs []*config.Config) ([]*Server, CleanupFunc) {
	svrs := make([]*Server, 0, len(cfgs))

	ch := make(chan *Server)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			MockHandler := CreateMockHandler(suite.Require(), "127.0.0.1")
			svr, err := CreateServer(ctx, cfg, MockHandler)
			// prevent blocking if Asserts fails
			failed := true
			defer func() {
				if failed {
					ch <- nil
				} else {
					ch <- svr
				}
			}()
			suite.NoError(err)
			err = svr.Run()
			suite.NoError(err)
			failed = false
		}(cfg)
	}

	for i := 0; i < len(cfgs); i++ {
		svr := <-ch
		suite.NotNil(svr)
		svrs = append(svrs, svr)
	}
	MustWaitLeader(suite.Require(), svrs)

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

func (suite *leaderServerTestSuite) TestCheckClusterID() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgs := NewTestMultiConfig(assertutil.CheckerWithNilAssert(suite.Require()), 2)
	for i, cfg := range cfgs {
		cfg.DataDir = fmt.Sprintf("/tmp/test_pd_check_clusterID_%d", i)
		// Clean up before testing.
		testutil.CleanServer(cfg.DataDir)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster.
	svrsA, cleanA := suite.newTestServersWithCfgs(ctx, []*config.Config{cfgA})
	defer cleanA()
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := suite.newTestServersWithCfgs(ctx, []*config.Config{cfgB})
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	MockHandler := CreateMockHandler(suite.Require(), "127.0.0.1")
	svr, err := CreateServer(ctx, cfgA, MockHandler)
	suite.NoError(err)

	etcd, err := embed.StartEtcd(svr.etcdCfg)
	suite.NoError(err)
	urlsMap, err := types.NewURLsMap(svr.cfg.InitialCluster)
	suite.NoError(err)
	tlsConfig, err := svr.cfg.Security.ToTLSConfig()
	suite.NoError(err)
	err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlsMap, tlsConfig)
	suite.Error(err)
	etcd.Close()
	testutil.CleanServer(cfgA.DataDir)
}

func (suite *leaderServerTestSuite) TestRegisterServerHandler() {
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(suite.Require()))
	ctx, cancel := context.WithCancel(context.Background())
	MockHandler := CreateMockHandler(suite.Require(), "127.0.0.1")
	svr, err := CreateServer(ctx, cfg, MockHandler)
	suite.NoError(err)
	_, err = CreateServer(ctx, cfg, MockHandler, MockHandler)
	// Repeat register.
	suite.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	suite.NoError(err)
	resp, err := http.Get(fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()))
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	suite.NoError(err)
	bodyString := string(bodyBytes)
	suite.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderForwarded() {
	MockHandler := CreateMockHandler(suite.Require(), "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(suite.Require()))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, MockHandler)
	suite.NoError(err)
	_, err = CreateServer(ctx, cfg, MockHandler, MockHandler)
	// Repeat register.
	suite.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	suite.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), nil)
	suite.NoError(err)
	req.Header.Add("X-Forwarded-For", "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	suite.NoError(err)
	bodyString := string(bodyBytes)
	suite.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderXReal() {
	MockHandler := CreateMockHandler(suite.Require(), "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(suite.Require()))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, MockHandler)
	suite.NoError(err)
	_, err = CreateServer(ctx, cfg, MockHandler, MockHandler)
	// Repeat register.
	suite.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	suite.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), nil)
	suite.NoError(err)
	req.Header.Add("X-Real-Ip", "127.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	suite.NoError(err)
	bodyString := string(bodyBytes)
	suite.Equal("Hello World\n", bodyString)
}

func (suite *leaderServerTestSuite) TestSourceIpForHeaderBoth() {
	MockHandler := CreateMockHandler(suite.Require(), "127.0.0.2")
	cfg := NewTestSingleConfig(assertutil.CheckerWithNilAssert(suite.Require()))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, MockHandler)
	suite.NoError(err)
	_, err = CreateServer(ctx, cfg, MockHandler, MockHandler)
	// Repeat register.
	suite.Error(err)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	suite.NoError(err)

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/apis/mock/v1/hello", svr.GetAddr()), nil)
	suite.NoError(err)
	req.Header.Add("X-Forwarded-For", "127.0.0.2")
	req.Header.Add("X-Real-Ip", "127.0.0.3")
	resp, err := http.DefaultClient.Do(req)
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	suite.NoError(err)
	bodyString := string(bodyBytes)
	suite.Equal("Hello World\n", bodyString)
}
