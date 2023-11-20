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

package client_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/tests"
)

type httpClientTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancelFunc context.CancelFunc
	cluster    *tests.TestCluster
	client     pd.Client
}

func TestHTTPClientTestSuite(t *testing.T) {
	suite.Run(t, new(httpClientTestSuite))
}

func (suite *httpClientTestSuite) SetupSuite() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancelFunc = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leader := suite.cluster.WaitLeader()
	re.NotEmpty(leader)
	err = suite.cluster.GetLeaderServer().BootstrapCluster()
	re.NoError(err)
	var (
		testServers = suite.cluster.GetServers()
		endpoints   = make([]string, 0, len(testServers))
	)
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	suite.client = pd.NewClient(endpoints)
}

func (suite *httpClientTestSuite) TearDownSuite() {
	suite.cancelFunc()
	suite.client.Close()
	suite.cluster.Destroy()
}

func (suite *httpClientTestSuite) TestGetMinResolvedTSByStoresIDs() {
	re := suite.Require()
	// Get the cluster-level min resolved TS.
	minResolvedTS, storeMinResolvedTSMap, err := suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, nil)
	re.NoError(err)
	re.Greater(minResolvedTS, uint64(0))
	re.Empty(storeMinResolvedTSMap)
	// Get the store-level min resolved TS.
	minResolvedTS, storeMinResolvedTSMap, err = suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, []uint64{1})
	re.NoError(err)
	re.Greater(minResolvedTS, uint64(0))
	re.Len(storeMinResolvedTSMap, 1)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	// Get the store-level min resolved TS with an invalid store ID.
	minResolvedTS, storeMinResolvedTSMap, err = suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, []uint64{1, 2})
	re.NoError(err)
	re.Greater(minResolvedTS, uint64(0))
	re.Len(storeMinResolvedTSMap, 2)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	re.Equal(uint64(math.MaxUint64), storeMinResolvedTSMap[2])
}

func (suite *httpClientTestSuite) TestRule() {
	re := suite.Require()
	rules, err := suite.client.GetPlacementRulesByGroup(suite.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Len(rules, 1)
	re.Equal(placement.DefaultGroupID, rules[0].GroupID)
	re.Equal(placement.DefaultRuleID, rules[0].ID)
	re.Equal(pd.Voter, rules[0].Role)
	re.Equal(3, rules[0].Count)
	err = suite.client.SetPlacementRule(suite.ctx, &pd.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test",
		Role:    pd.Learner,
		Count:   3,
	})
	re.NoError(err)
	rules, err = suite.client.GetPlacementRulesByGroup(suite.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Len(rules, 2)
	re.Equal(placement.DefaultGroupID, rules[1].GroupID)
	re.Equal("test", rules[1].ID)
	re.Equal(pd.Learner, rules[1].Role)
	re.Equal(3, rules[1].Count)
	err = suite.client.DeletePlacementRule(suite.ctx, placement.DefaultGroupID, "test")
	re.NoError(err)
	rules, err = suite.client.GetPlacementRulesByGroup(suite.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Len(rules, 1)
	re.Equal(placement.DefaultGroupID, rules[0].GroupID)
	re.Equal(placement.DefaultRuleID, rules[0].ID)
}

func (suite *httpClientTestSuite) TestAccelerateSchedule() {
	re := suite.Require()
	suspectRegions := suite.cluster.GetLeaderServer().GetRaftCluster().GetSuspectRegions()
	re.Len(suspectRegions, 0)
	err := suite.client.AccelerateSchedule(suite.ctx, []byte("a1"), []byte("a2"))
	re.NoError(err)
	suspectRegions = suite.cluster.GetLeaderServer().GetRaftCluster().GetSuspectRegions()
	re.Len(suspectRegions, 1)
}
