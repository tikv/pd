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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
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
	clusterID        string
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
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.clusterID = strconv.FormatUint(suite.pdLeader.GetClusterID(), 10)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *serverRegisterTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *serverRegisterTestSuite) TestServerRegister() {
	for range 3 {
		suite.checkServerRegister(constant.TSOServiceName)
	}
}

func (suite *serverRegisterTestSuite) checkServerRegister(serviceName string) {
	re := suite.Require()
	s, cleanup := suite.addServer(serviceName)

	addr := s.GetAddr()
	client := suite.pdLeader.GetEtcdClient()
	// test API server discovery

	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	returnedEntry := &discovery.ServiceRegistryEntry{}
	returnedEntry.Deserialize([]byte(endpoints[0]))
	re.Equal(addr, returnedEntry.ServiceAddr)

	// test primary when only one server
	expectedPrimary := tests.WaitForPrimaryServing(re, map[string]bs.Server{addr: s})
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(expectedPrimary, primary)

	// test API server discovery after unregister
	cleanup()
	endpoints, err = discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Empty(endpoints)
	testutil.Eventually(re, func() bool {
		return !s.IsServing()
	}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(50*time.Millisecond))
}

func (suite *serverRegisterTestSuite) TestServerPrimaryChange() {
	suite.checkServerPrimaryChange(constant.TSOServiceName, 3)
}

func (suite *serverRegisterTestSuite) checkServerPrimaryChange(serviceName string, serverNum int) {
	re := suite.Require()
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.False(exist)
	re.Empty(primary)

	serverMap := make(map[string]bs.Server)
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range serverNum {
		s, cleanup := suite.addServer(serviceName)
		cleanups = append(cleanups, cleanup)
		serverMap[s.GetAddr()] = s
	}

	expectedPrimary := tests.WaitForPrimaryServing(re, serverMap)
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(expectedPrimary, primary)
	// close old primary
	serverMap[primary].Close()
	delete(serverMap, primary)

	expectedPrimary = tests.WaitForPrimaryServing(re, serverMap)
	// test API server discovery
	client := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Len(endpoints, serverNum-1)

	// test primary changed
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(expectedPrimary, primary)
}

func (suite *serverRegisterTestSuite) addServer(serviceName string) (bs.Server, func()) {
	re := suite.Require()
	switch serviceName {
	case constant.TSOServiceName:
		return tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	case constant.ResourceManagerServiceName:
		return tests.StartSingleResourceManagerTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	default:
		return nil, nil
	}
}
