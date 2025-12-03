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

package resourcemanager_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// resourceManagerForwardingTestSuite is a test suite for testing the forwarding behavior of Resource Manager APIs.
type resourceManagerForwardingTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	pdCluster        *tests.TestCluster
	rmCluster        *tests.TestResourceManagerCluster
	backendEndpoints string
	primary          *server.Server
	follower         *server.Server
}

func TestResourceManagerForwarding(t *testing.T) {
	suite.Run(t, new(resourceManagerForwardingTestSuite))
}

func (suite *resourceManagerForwardingTestSuite) SetupTest() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())

	suite.pdCluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.pdCluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeaderServer := suite.pdCluster.GetServer(leaderName)
	re.NoError(pdLeaderServer.BootstrapCluster())
	suite.backendEndpoints = pdLeaderServer.GetAddr()

	suite.rmCluster, err = tests.NewTestResourceManagerCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)

	suite.primary = suite.rmCluster.WaitForPrimaryServing(re)
	re.NotNil(suite.primary)
	for _, srv := range suite.rmCluster.GetServers() {
		if srv.GetAddr() != suite.primary.GetAddr() {
			suite.follower = srv
			break
		}
	}
	re.NotNil(suite.follower, "follower should not be nil")
	re.False(suite.follower.IsServing(), "follower should not be serving")
}

func (suite *resourceManagerForwardingTestSuite) TearDownTest() {
	suite.cancel()
	suite.rmCluster.Destroy()
	suite.pdCluster.Destroy()
}

// TestResourceManagerForwardingBehavior checks that requests are correctly forwarded or handled locally.
func (suite *resourceManagerForwardingTestSuite) TestResourceManagerForwardingBehavior() {
	re := suite.Require()
	followerAddr := suite.follower.GetAddr()
	followerURL := func(path string) string {
		return fmt.Sprintf("%s%s%s", followerAddr, apis.APIPathPrefix, path)
	}

	// Case 1: PUT /admin/log should be handled by the follower locally.
	logURL := followerURL("admin/log")
	level := "debug"
	logPayload, err := json.Marshal(level)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPut, logURL, bytes.NewBuffer(logPayload))
	re.NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)

	// Case 2: GET /config should be handled by the follower locally.
	configURL := followerURL("config")
	var followerCfg server.Config
	err = testutil.ReadGetJSON(re, tests.TestDialClient, configURL, &followerCfg, testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(suite.follower.GetConfig().GetListenAddr(), followerCfg.GetListenAddr())
	re.NotEqual(suite.primary.GetConfig().GetListenAddr(), followerCfg.GetListenAddr())
	re.Equal(level, followerCfg.Log.Level)

	// Case 3: GET /config/groups should be handled by the follower forwarded to the primary.
	controllerURL := followerURL("config/groups")
	groups := make([]*server.ResourceGroup, 0)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, controllerURL, &groups, testutil.StatusOK(re))
	re.NoError(err)
}
