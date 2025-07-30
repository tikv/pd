// Copyright 2025 TiKV Project Authors.
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

package microservice_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

// microServiceSuite is a test suite for microservice related tests.
type microServiceSuite struct {
	suite.Suite
	cluster *pdTests.TestCluster
	cancels []testutil.CleanupFunc
}

func TestMicroServiceSuite(t *testing.T) {
	suite.Run(t, new(microServiceSuite))
}

func (suite *microServiceSuite) SetupSuite() {
	suite.startCluster()
}

func (suite *microServiceSuite) TearDownSuite() {
	for _, fn := range suite.cancels {
		fn()
	}
	suite.cluster.Destroy()
}

func (suite *microServiceSuite) TestMicroService() {
	cluster := suite.cluster
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	pdAddr := cluster.GetConfig().GetClientURL()
	if primaryServer := cluster.GetSchedulingPrimaryServer(); primaryServer != nil {
		address := primaryServer.GetAddr()
		res := tests.MustExec(re, cmd, []string{"-u", pdAddr, "ms", "scheduling", "primary"}, nil)
		primaryAddress := strings.Trim(res, "\"\n")
		suite.Equal(address, primaryAddress)

		v := make([]any, 0)
		tests.MustExec(re, cmd, []string{"-u", pdAddr, "ms", "scheduling", "members"}, &v)
		re.Len(v, 2)
	}
	if primaryServer := cluster.GetDefaultTSOPrimaryServer(); primaryServer != nil {
		address := primaryServer.GetAddr()
		res := tests.MustExec(re, cmd, []string{"-u", pdAddr, "ms", "tso", "primary"}, nil)
		primaryAddress := strings.Trim(res, "\"\n")
		suite.Equal(address, primaryAddress)

		v := make([]any, 0)
		tests.MustExec(re, cmd, []string{"-u", pdAddr, "ms", "tso", "members"}, &v)
		re.Len(v, 2)
	}
}

func (suite *microServiceSuite) startCluster() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cancels = append(suite.cancels, func() {
		cancel()
	})

	cluster, err := pdTests.NewTestClusterWithKeyspaceGroup(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	leaderServer.GetRaftCluster().SetPrepared()
	// start scheduling cluster
	tc, err := pdTests.NewTestSchedulingCluster(ctx, 2, cluster)
	re.NoError(err)
	tc.WaitForPrimaryServing(re)
	tc.GetPrimaryServer().GetCluster().SetPrepared()
	cluster.SetSchedulingCluster(tc)
	testutil.Eventually(re, func() bool {
		return cluster.GetLeaderServer().GetServer().IsServiceIndependent(mcs.SchedulingServiceName)
	})
	// start tso cluster
	ts, err := pdTests.NewTestTSOCluster(ctx, 2, leaderServer.GetAddr())
	ts.WaitForDefaultPrimaryServing(re)
	re.NoError(err)
	cluster.SetTSOCluster(ts)
	suite.cluster = cluster
}
