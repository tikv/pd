// Copyright 2023 TiKV Project Authors.
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

package members_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	pdClient "github.com/tikv/pd/client/http"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type memberTestSuite struct {
	suite.Suite
	ctx              context.Context
	cleanupFunc      []testutil.CleanupFunc
	cluster          *tests.TestCluster
	server           *tests.TestServer
	backendEndpoints string
	dialClient       pdClient.Client
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestAPICluster(suite.ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.dialClient = pdClient.NewClient("mcs-member-test", []string{suite.server.GetAddr()})

	// TSO
	nodes := make(map[string]bs.Server)
	for i := 0; i < utils.DefaultKeyspaceGroupReplicaCount; i++ {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		nodes[s.GetAddr()] = s
		suite.cleanupFunc = append(suite.cleanupFunc, func() {
			cleanup()
		})
	}
	tests.WaitForPrimaryServing(re, nodes)

	// Scheduling
	nodes = make(map[string]bs.Server)
	for i := 0; i < 3; i++ {
		s, cleanup := tests.StartSingleSchedulingTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		nodes[s.GetAddr()] = s
		suite.cleanupFunc = append(suite.cleanupFunc, func() {
			cleanup()
		})
	}
	tests.WaitForPrimaryServing(re, nodes)

	suite.cleanupFunc = append(suite.cleanupFunc, func() {
		cancel()
	})
}

func (suite *memberTestSuite) TearDownTest() {
	for _, cleanup := range suite.cleanupFunc {
		cleanup()
	}
	suite.dialClient.Close()
	suite.cluster.Destroy()
}

func (suite *memberTestSuite) TestMembers() {
	re := suite.Require()
	members, err := suite.dialClient.GetMicroServiceMembers(suite.ctx, "tso")
	re.NoError(err)
	re.Len(members, utils.DefaultKeyspaceGroupReplicaCount)

	members, err = suite.dialClient.GetMicroServiceMembers(suite.ctx, "scheduling")
	re.NoError(err)
	re.Len(members, 3)
}

func (suite *memberTestSuite) TestPrimary() {
	re := suite.Require()
	primary, err := suite.dialClient.GetMicroServicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	primary, err = suite.dialClient.GetMicroServicePrimary(suite.ctx, "scheduling")
	re.NoError(err)
	re.NotEmpty(primary)
}
