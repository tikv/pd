// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/mcs/discovery"
	tsosvr "github.com/tikv/pd/pkg/mcs/tso/server"
	tsoapi "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoServerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	tsosvrs          map[string]*tsosvr.Server
}

func TestTSOServerTestSuite(t *testing.T) {
	suite.Run(t, new(tsoServerTestSuite))
}

func (suite *tsoServerTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	for _, s := range suite.tsosvrs {
		s.Close()
		testutil.CleanServer(s.GetConfig().DataDir)
	}
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *tsoServerTestSuite) TestTSOServerStartAndStopNormally() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from an unexpected panic", r)
			suite.T().Errorf("Expected no panic, but something bad occurred with")
		}
	}()

	re := suite.Require()
	s, cleanup, err := startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	re.NoError(err)

	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Test registered GRPC Service
	cc, err := grpc.DialContext(suite.ctx, s.GetListenURL().Host, grpc.WithInsecure())
	re.NoError(err)
	cc.Close()
	url := s.GetConfig().ListenAddr + tsoapi.APIPathPrefix
	{
		resetJSON := `{"tso":"121312", "force-use-larger":true}`
		re.NoError(err)
		resp, err := http.Post(url+"/admin/reset-ts", "application/json", strings.NewReader(resetJSON))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
	{
		resetJSON := `{}`
		re.NoError(err)
		resp, err := http.Post(url+"/admin/reset-ts", "application/json", strings.NewReader(resetJSON))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
	}
}

func (suite *tsoServerTestSuite) TestTSOServerRegister() {
	re := suite.Require()
	s, cleanup, err := startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	re.NoError(err)

	serviceName := "tso"
	client := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Equal(s.GetConfig().ListenAddr, endpoints[0])

	// test API server discovery
	exist, addr, err := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.NoError(err)
	re.True(exist)
	re.Equal(s.GetConfig().ListenAddr, addr)

	cleanup()
	endpoints, err = discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Empty(endpoints)
}

func TestForwardTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestAPICluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	pdLeader := cluster.GetServer(leaderName)
	backendEndpoints := pdLeader.GetAddr()

	{ // cannot get ts from api server addr
		pdClient, err := pd.NewClientWithContext(ctx, []string{backendEndpoints}, pd.SecurityOption{})
		re.NoError(err)
		_, _, err = pdClient.GetTS(ctx)
		re.Error(err)
		re.Contains(err.Error(), "transport: Error while dialing dial tcp: missing address")
	}

	// add tso server
	s, cleanup, err := startSingleTSOTestServer(ctx, re, backendEndpoints)
	re.NoError(err)
	etcdClient := pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(etcdClient, "tso")
	re.NoError(err)
	re.Equal(s.GetConfig().ListenAddr, endpoints[0])

	{ // can get ts from api addr with pd client
		pdClient, err := pd.NewClientWithContext(ctx, []string{backendEndpoints}, pd.SecurityOption{})
		re.NoError(err)
		_, _, err = pdClient.GetTS(ctx)
		re.NoError(err)
	}

	cleanup()
	endpoints, err = discovery.Discover(etcdClient, "tso")
	re.NoError(err)
	re.Empty(endpoints)
}
