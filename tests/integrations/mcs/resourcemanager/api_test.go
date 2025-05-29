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
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/tests"
)

type resourceManagerAPITestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestResourceManagerAPITestSuite(t *testing.T) {
	suite.Run(t, new(resourceManagerAPITestSuite))
}

func (suite *resourceManagerAPITestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
}

func (suite *resourceManagerAPITestSuite) TearDownTest() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *resourceManagerAPITestSuite) getEndpoint(re *require.Assertions, elems ...string) string {
	endpoint, err := url.JoinPath(
		suite.cluster.GetLeaderServer().GetAddr(),
		append([]string{apis.APIPathPrefix}, elems...)...,
	)
	re.NoError(err)
	return endpoint
}

// sendRequest is a helper function to send HTTP requests and handle common response processing
func (suite *resourceManagerAPITestSuite) sendRequest(
	re *require.Assertions,
	method, path string,
	body any,
) ([]byte, int) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		re.NoError(err)
		bodyReader = bytes.NewBuffer(data)
	}

	httpReq, err := http.NewRequest(method, suite.getEndpoint(re, path), bodyReader)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	return bodyBytes, resp.StatusCode
}

// mustSendRequest is a helper function that expects a successful response
func (suite *resourceManagerAPITestSuite) mustSendRequest(
	re *require.Assertions,
	method, path string,
	body any,
) []byte {
	bodyBytes, statusCode := suite.sendRequest(re, method, path, body)
	re.Equal(http.StatusOK, statusCode)
	return bodyBytes
}

func (suite *resourceManagerAPITestSuite) TestResourceGroupAPI() {
	re := suite.Require()

	// Add a resource group.
	groupToAdd := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	suite.mustAddResourceGroup(re, groupToAdd)
	// Get the resource group.
	group := suite.mustGetResourceGroup(re, groupToAdd.Name)
	re.Equal(groupToAdd.Name, group.Name)
	re.Equal(groupToAdd.Mode, group.Mode)
	re.Equal(groupToAdd.Priority, group.Priority)
	re.Equal(groupToAdd.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
	re.Equal(groupToAdd.RUSettings.RU.Settings.BurstLimit, group.RUSettings.RU.Settings.BurstLimit)
	// Update the resource group.
	groupToUpdate := group.Clone(false)
	groupToUpdate.Priority = 10
	groupToUpdate.RUSettings.RU.Settings.FillRate = 200
	suite.mustUpdateResourceGroup(re, groupToUpdate.IntoProtoResourceGroup())
	group = suite.mustGetResourceGroup(re, groupToAdd.Name)
	re.Equal(groupToUpdate.Name, group.Name)
	re.Equal(groupToUpdate.Mode, group.Mode)
	re.Equal(groupToUpdate.Priority, group.Priority)
	re.Equal(groupToUpdate.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
	re.Equal(groupToUpdate.RUSettings.RU.Settings.BurstLimit, group.RUSettings.RU.Settings.BurstLimit)
	// Get the resource group list.
	groups := suite.mustGetResourceGroupList(re)
	re.Len(groups, 2) // Include the default resource group.
	for _, group := range groups {
		// Skip the default resource group.
		if group.Name == server.DefaultResourceGroupName {
			continue
		}
		re.Equal(groupToUpdate.Name, group.Name)
		re.Equal(groupToUpdate.Mode, group.Mode)
		re.Equal(groupToUpdate.Priority, group.Priority)
		re.Equal(groupToUpdate.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
	}
	// Delete the resource group.
	suite.mustDeleteResourceGroup(re, groupToAdd.Name)
	group = suite.mustGetResourceGroup(re, groupToAdd.Name)
	re.Nil(group)
	groups = suite.mustGetResourceGroupList(re)
	re.Len(groups, 1)
	re.Equal(server.DefaultResourceGroupName, groups[0].Name)
}

func (suite *resourceManagerAPITestSuite) mustAddResourceGroup(re *require.Assertions, group *rmpb.ResourceGroup) {
	bodyBytes := suite.mustSendRequest(re, http.MethodPost, "/config/group", group)
	re.Equal("Success!", string(bodyBytes))
}

func (suite *resourceManagerAPITestSuite) mustUpdateResourceGroup(re *require.Assertions, group *rmpb.ResourceGroup) {
	bodyBytes := suite.mustSendRequest(re, http.MethodPut, "/config/group", group)
	re.Equal("Success!", string(bodyBytes))
}

func (suite *resourceManagerAPITestSuite) mustGetResourceGroup(re *require.Assertions, name string) *server.ResourceGroup {
	bodyBytes, statusCode := suite.sendRequest(re, http.MethodGet, "/config/group/"+name, nil)
	if statusCode != http.StatusOK {
		re.Equal(http.StatusNotFound, statusCode)
		return nil
	}
	group := &server.ResourceGroup{}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(group))
	return group
}

func (suite *resourceManagerAPITestSuite) mustGetResourceGroupList(re *require.Assertions) []*server.ResourceGroup {
	bodyBytes := suite.mustSendRequest(re, http.MethodGet, "/config/groups", nil)
	groups := []*server.ResourceGroup{}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&groups))
	return groups
}

func (suite *resourceManagerAPITestSuite) mustDeleteResourceGroup(re *require.Assertions, name string) {
	bodyBytes := suite.mustSendRequest(re, http.MethodDelete, "/config/group/"+name, nil)
	re.Equal("Success!", string(bodyBytes))
}

func (suite *resourceManagerAPITestSuite) TestControllerConfigAPI() {
	re := suite.Require()

	// Get the controller config.
	config := suite.mustGetControllerConfig(re)
	re.Equal(1.0, config.RequestUnit.WriteBaseCost)
	// Set the controller config.
	configToSet := map[string]any{"write-base-cost": 2.0}
	suite.mustSetControllerConfig(re, configToSet)
	config = suite.mustGetControllerConfig(re)
	re.Equal(2.0, config.RequestUnit.WriteBaseCost)
}

func (suite *resourceManagerAPITestSuite) mustGetControllerConfig(re *require.Assertions) *server.ControllerConfig {
	bodyBytes := suite.mustSendRequest(re, http.MethodGet, "/config/controller", nil)
	config := &server.ControllerConfig{}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(config))
	return config
}

func (suite *resourceManagerAPITestSuite) mustSetControllerConfig(re *require.Assertions, config map[string]any) {
	bodyBytes := suite.mustSendRequest(re, http.MethodPost, "/config/controller", config)
	re.Equal("Success!", string(bodyBytes))
}
