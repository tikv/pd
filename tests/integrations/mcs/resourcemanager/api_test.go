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
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/keyspace"
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
	queryParams url.Values,
	body any,
) ([]byte, int) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		re.NoError(err)
		bodyReader = bytes.NewBuffer(data)
	}
	path = suite.getEndpoint(re, path)
	if len(queryParams) > 0 {
		path += "?" + queryParams.Encode()
	}
	httpReq, err := http.NewRequest(method, path, bodyReader)
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
	bodyBytes, statusCode := suite.sendRequest(re, method, path, nil, body)
	re.Equal(http.StatusOK, statusCode, string(bodyBytes))
	return bodyBytes
}

func (suite *resourceManagerAPITestSuite) TestResourceGroupAPI() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()
	for i := range 3 {
		// Create keyspace
		keyspaceName := fmt.Sprint("test_keyspace_", i)
		leaderServer := suite.cluster.GetLeaderServer()
		meta, err := leaderServer.GetKeyspaceManager().CreateKeyspace(
			&keyspace.CreateKeyspaceRequest{
				Name: keyspaceName,
			},
		)
		re.NoError(err)
		keyspaceID := &rmpb.KeyspaceIDValue{
			Value: meta.GetId(),
		}
		// Add a resource group.
		groupToAdd := &rmpb.ResourceGroup{
			Name:     "test_group",
			Mode:     rmpb.GroupMode_RUMode,
			Priority: uint32(5 + i),
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   uint64(100 + i),
						BurstLimit: int64(200 + i),
					},
				},
			},
			KeyspaceId: keyspaceID,
		}
		suite.mustAddResourceGroup(re, groupToAdd)
		// Get the resource group.
		group := suite.mustGetResourceGroup(re, groupToAdd.Name, keyspaceName+"_err")
		re.Nil(group)
		group = suite.mustGetResourceGroup(re, groupToAdd.Name, keyspaceName)
		re.NotNil(group)
		re.Equal(groupToAdd.Name, group.Name)
		re.Equal(groupToAdd.Mode, group.Mode)
		re.Equal(groupToAdd.Priority, group.Priority)
		re.Equal(groupToAdd.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
		re.Equal(groupToAdd.RUSettings.RU.Settings.BurstLimit, group.RUSettings.RU.Settings.BurstLimit)
		// Update the resource group.
		groupToUpdate := group.Clone(false)
		groupToUpdate.Priority = 10
		groupToUpdate.RUSettings.RU.Settings.FillRate = 200
		groupToUpdateProto := groupToUpdate.IntoProtoResourceGroup()
		groupToUpdateProto.KeyspaceId = keyspaceID
		suite.mustUpdateResourceGroup(re, groupToUpdateProto)
		group = suite.mustGetResourceGroup(re, groupToUpdate.Name, keyspaceName)
		re.NotNil(group)
		re.Equal(groupToUpdate.Name, group.Name)
		re.Equal(groupToUpdate.Mode, group.Mode)
		re.Equal(groupToUpdate.Priority, group.Priority)
		re.Equal(groupToUpdate.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
		re.Equal(groupToUpdate.RUSettings.RU.Settings.BurstLimit, group.RUSettings.RU.Settings.BurstLimit)
		// Get the resource group list.
		groups := suite.mustGetResourceGroupList(re, keyspaceName+"_err")
		re.Nil(groups)
		groups = suite.mustGetResourceGroupList(re, keyspaceName)
		re.NotNil(groups)
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
		suite.mustDeleteResourceGroup(re, groupToAdd.Name, keyspaceName+"_err")
		group = suite.mustGetResourceGroup(re, groupToAdd.Name, keyspaceName)
		re.NotNil(group)
		suite.mustDeleteResourceGroup(re, groupToAdd.Name+"_err", keyspaceName)
		group = suite.mustGetResourceGroup(re, groupToAdd.Name, keyspaceName)
		re.NotNil(group)
		suite.mustDeleteResourceGroup(re, groupToAdd.Name, keyspaceName)
		group = suite.mustGetResourceGroup(re, groupToAdd.Name, keyspaceName)
		re.Nil(group)
		groups = suite.mustGetResourceGroupList(re, keyspaceName)
		re.Len(groups, 1)
		re.Equal(server.DefaultResourceGroupName, groups[0].Name)
	}
}

func (suite *resourceManagerAPITestSuite) TestResourceGroupAPIInit() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()
	testFuncs := []func(keyspaceID uint32, keyspaceName string){
		func(_ uint32, keyspaceName string) {
			group := suite.mustGetResourceGroup(re, "group", keyspaceName)
			re.Nil(group)
			group = suite.mustGetResourceGroup(re, "default", keyspaceName)
			re.NotNil(group)
		},
		func(_ uint32, keyspaceName string) {
			groups := suite.mustGetResourceGroupList(re, keyspaceName)
			re.Len(groups, 1)
			re.Equal(server.DefaultResourceGroupName, groups[0].Name)
		},
		func(keyspaceID uint32, _ string) {
			groupToUpdate := &rmpb.ResourceGroup{
				Name:     server.DefaultResourceGroupName,
				Mode:     rmpb.GroupMode_RUMode,
				Priority: 5,
				RUSettings: &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   200,
							BurstLimit: 200,
						},
					},
				},
				KeyspaceId: &rmpb.KeyspaceIDValue{
					Value: keyspaceID,
				},
			}
			suite.mustUpdateResourceGroup(re, groupToUpdate)
		},
	}
	for i, testFunc := range testFuncs {
		// Create keyspace
		keyspaceName := fmt.Sprint("test_keyspace_", i)
		leaderServer := suite.cluster.GetLeaderServer()
		meta, err := leaderServer.GetKeyspaceManager().CreateKeyspace(
			&keyspace.CreateKeyspaceRequest{
				Name: keyspaceName,
			},
		)
		re.NoError(err)
		// Run test
		testFunc(meta.GetId(), keyspaceName)
	}
}

func (suite *resourceManagerAPITestSuite) mustAddResourceGroup(re *require.Assertions, group *rmpb.ResourceGroup) {
	bodyBytes := suite.mustSendRequest(re, http.MethodPost, "/config/group", group)
	re.Equal("Success!", string(bodyBytes))
}

func (suite *resourceManagerAPITestSuite) mustUpdateResourceGroup(re *require.Assertions, group *rmpb.ResourceGroup) {
	bodyBytes := suite.mustSendRequest(re, http.MethodPut, "/config/group", group)
	re.Equal("Success!", string(bodyBytes))
}

func (suite *resourceManagerAPITestSuite) mustGetResourceGroup(re *require.Assertions, name string, keyspaceName string) *server.ResourceGroup {
	queryParams := url.Values{}
	queryParams.Set("keyspace_name", keyspaceName)
	bodyBytes, statusCode := suite.sendRequest(re, http.MethodGet, "/config/group/"+name, queryParams, nil)
	if statusCode != http.StatusOK {
		re.Equal(http.StatusNotFound, statusCode)
		return nil
	}
	group := &server.ResourceGroup{}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(group))
	return group
}

func (suite *resourceManagerAPITestSuite) mustGetResourceGroupList(re *require.Assertions, keyspaceName string) []*server.ResourceGroup {
	queryParams := url.Values{}
	queryParams.Set("keyspace_name", keyspaceName)
	bodyBytes, statusCode := suite.sendRequest(re, http.MethodGet, "/config/groups", queryParams, nil)
	if statusCode != http.StatusOK {
		re.Equal(http.StatusNotFound, statusCode)
		return nil
	}
	groups := []*server.ResourceGroup{}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&groups))
	return groups
}

func (suite *resourceManagerAPITestSuite) mustDeleteResourceGroup(re *require.Assertions, name string, keyspaceName string) {
	queryParams := url.Values{}
	queryParams.Set("keyspace_name", keyspaceName)
	bodyBytes, statusCode := suite.sendRequest(re, http.MethodDelete, "/config/group/"+name, queryParams, nil)
	if statusCode != http.StatusOK {
		re.Equal(http.StatusNotFound, statusCode)
		return
	}
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

func (suite *resourceManagerAPITestSuite) TestKeyspaceServiceLimitAPI() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	// Prepare the keyspace for later test.
	leaderServer := suite.cluster.GetLeaderServer()
	leaderServer.GetKeyspaceManager().CreateKeyspace(
		&keyspace.CreateKeyspaceRequest{
			Name: "test_keyspace",
		},
	)
	for _, keyspaceName := range []string{"", "test_keyspace"} {
		// Get the keyspace service limit.
		limit, statusCode := suite.tryToGetKeyspaceServiceLimit(re, keyspaceName)
		if len(keyspaceName) == 0 {
			// The null keyspace is always available.
			re.Equal(http.StatusOK, statusCode)
			re.Equal(0.0, limit)
		} else {
			// The keyspace manager has not been created yet.
			re.Equal(http.StatusNotFound, statusCode)
			re.Equal(0.0, limit)
		}
		// Try to set the keyspace service limit to a negative value.
		resp, statusCode := suite.tryToSetKeyspaceServiceLimit(re, keyspaceName, -1.0)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Equal("service_limit must be non-negative", resp)
		// Set the keyspace service limit to a positive value.
		resp, statusCode = suite.tryToSetKeyspaceServiceLimit(re, keyspaceName, 1.0)
		re.Equal(http.StatusOK, statusCode)
		re.Equal("Success!", resp)
		limit, statusCode = suite.tryToGetKeyspaceServiceLimit(re, keyspaceName)
		re.Equal(http.StatusOK, statusCode)
		re.Equal(1.0, limit)
	}
	// Try to set a non-existing keyspace's service limit.
	resp, statusCode := suite.tryToSetKeyspaceServiceLimit(re, "non_existing_keyspace", 1.0)
	re.Equal(http.StatusBadRequest, statusCode)
	re.Equal("keyspace not found with name: non_existing_keyspace", resp)
	// Try to get a non-existing keyspace's service limit.
	limit, statusCode := suite.tryToGetKeyspaceServiceLimit(re, "non_existing_keyspace")
	re.Equal(http.StatusBadRequest, statusCode)
	re.Equal(0.0, limit)
}

func (suite *resourceManagerAPITestSuite) tryToGetKeyspaceServiceLimit(re *require.Assertions, keyspaceName string) (float64, int) {
	bodyBytes, statusCode := suite.sendRequest(re, http.MethodGet, "/config/keyspace/service-limit/"+keyspaceName, nil, nil)
	if statusCode != http.StatusOK {
		return 0.0, statusCode
	}
	var limiter struct {
		ServiceLimit float64 `json:"service_limit"`
	}
	re.NoError(json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&limiter))
	return limiter.ServiceLimit, statusCode
}

func (suite *resourceManagerAPITestSuite) tryToSetKeyspaceServiceLimit(re *require.Assertions, keyspaceName string, limit float64) (string, int) {
	bodyBytes, statusCode := suite.sendRequest(
		re,
		http.MethodPost,
		"/config/keyspace/service-limit/"+keyspaceName,
		nil,
		apis.KeyspaceServiceLimitRequest{
			ServiceLimit: limit,
		},
	)
	return string(bodyBytes), statusCode
}
