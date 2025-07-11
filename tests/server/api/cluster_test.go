// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
)

type clusterTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestClusterTestSuite(t *testing.T) {
	suite.Run(t, new(clusterTestSuite))
}

func (suite *clusterTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
	suite.env.SkipBootstrap = true
}

func (suite *clusterTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *clusterTestSuite) TestCluster() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkCluster)
}

func (suite *clusterTestSuite) checkCluster(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	svr := leader.GetServer()

	// Test get cluster status, and bootstrap cluster
	suite.testGetClusterStatus(leader, urlPrefix)
	svr.GetPersistOptions().SetPlacementRuleEnabled(true)
	svr.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	rm := svr.GetRaftCluster().GetRuleManager()
	rule := rm.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"host"}
	rule.Count = 1
	err := rm.SetRule(rule)
	re.NoError(err)

	// Test set the config
	url := fmt.Sprintf("%s/cluster", urlPrefix)
	c1 := &metapb.Cluster{}
	err = testutil.ReadGetJSON(re, tests.TestDialClient, url, c1)
	re.NoError(err)

	c2 := &metapb.Cluster{}
	r := sc.ReplicationConfig{
		MaxReplicas:          6,
		EnablePlacementRules: true,
	}
	re.NoError(svr.SetReplicationConfig(r))

	err = testutil.ReadGetJSON(re, tests.TestDialClient, url, c2)
	re.NoError(err)

	c1.MaxPeerCount = 6
	re.Equal(c2, c1)
	re.Equal(int(r.MaxReplicas), svr.GetRaftCluster().GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID).Count)
}

func (suite *clusterTestSuite) testGetClusterStatus(leader *tests.TestServer, urlPrefix string) {
	re := suite.Require()
	url := fmt.Sprintf("%s/cluster/status", urlPrefix)
	status := cluster.Status{}
	err := testutil.ReadGetJSON(re, tests.TestDialClient, url, &status)
	re.NoError(err)
	re.True(status.RaftBootstrapTime.IsZero())
	re.False(status.IsInitialized)
	now := time.Now()
	re.NoError(leader.BootstrapCluster())
	err = testutil.ReadGetJSON(re, tests.TestDialClient, url, &status)
	re.NoError(err)
	re.True(status.RaftBootstrapTime.After(now))
	re.False(status.IsInitialized)
	err = leader.GetServer().SetReplicationConfig(sc.ReplicationConfig{MaxReplicas: 1})
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, url, &status)
	re.NoError(err)
	re.True(status.RaftBootstrapTime.After(now))
	re.True(status.IsInitialized)
}
