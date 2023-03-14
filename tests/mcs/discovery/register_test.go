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

package register_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type serverRegisterTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestServerRegisterTestSuite(t *testing.T) {
	suite.Run(t, new(serverRegisterTestSuite))
}

func (suite *serverRegisterTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *serverRegisterTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *serverRegisterTestSuite) TestServerRegister() {
	// test register, primary and unregister when start tso and resource-manager with only one server
	for i := 0; i < 3; i++ {
		suite.checkServerRegister("tso")
	}
	for i := 0; i < 3; i++ {
		suite.checkServerRegister("resource_manager")
	}
}

func (suite *serverRegisterTestSuite) checkServerRegister(serviceName string) {
	var (
		s       bs.Server
		cleanup func()
	)
	re := suite.Require()
	switch serviceName {
	case "tso":
		s, cleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	case "resource_manager":
		s, cleanup = mcs.StartSingleResourceManagerTestServer(suite.ctx, re, suite.backendEndpoints)
	default:
	}

	addr := s.GetAddr()
	client := suite.pdLeader.GetEtcdClient()

	// test API server discovery
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Equal(addr, endpoints[0])

	// test primary when only one server
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(primary, addr)
	re.Equal(primary, s.GetPrimary().GetName())

	// test API server discovery after unregister
	cleanup()
	endpoints, err = discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Empty(endpoints)
}
