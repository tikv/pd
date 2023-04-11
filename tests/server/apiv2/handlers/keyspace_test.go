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

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

const keyspacesPrefix = "/pd/api/v2/keyspaces"

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type keyspaceTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceTestSuite) TearDownTest() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *keyspaceTestSuite) TestCreateLoadKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		loaded := mustLoadKeyspaces(re, suite.server, created.Name)
		re.Equal(created, loaded)
	}
	defaultKeyspace := mustLoadKeyspaces(re, suite.server, keyspace.DefaultKeyspaceName)
	re.Equal(keyspace.DefaultKeyspaceName, defaultKeyspace.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, defaultKeyspace.State)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		config1val := "300"
		updateRequest := &handlers.UpdateConfigParams{
			Config: map[string]*string{
				"config1": &config1val,
				"config2": nil,
			},
		}
		updated := mustUpdateKeyspaceConfig(re, suite.server, created.Name, updateRequest)
		checkUpdateRequest(re, updateRequest, created.Config, updated.Config)
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		// Should NOT allow archiving ENABLED keyspace.
		success, _ := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.False(success)
		// Disabling an ENABLED keyspace is allowed.
		success, disabled := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, disabled.State)
		// Disabling an already DISABLED keyspace should not result in any change.
		success, disabledAgain := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(disabled, disabledAgain)
		// Tombstoning a DISABLED keyspace should not be allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.False(success)
		// Archiving a DISABLED keyspace should be allowed.
		success, archived := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, archived.State)
		// Enabling an ARCHIVED keyspace is not allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "enabled"})
		re.False(success)
		// Tombstoning an ARCHIVED keyspace is allowed.
		success, tombstone := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_TOMBSTONE, tombstone.State)
	}
	// Changing default keyspace's state is NOT allowed.
	success, _ := sendUpdateStateRequest(re, suite.server, keyspace.DefaultKeyspaceName, &handlers.UpdateStateParam{State: "disabled"})
	re.False(success)
}

func (suite *keyspaceTestSuite) TestLoadRangeKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 50)
	loadResponse := sendLoadRangeRequest(re, suite.server, "", "")
	re.Empty(loadResponse.NextPageToken) // Load response should contain no more pages.
	// Load response should contain all created keyspace and a default.
	re.Equal(len(keyspaces)+1, len(loadResponse.Keyspaces))
	for i, created := range keyspaces {
		re.Equal(created, loadResponse.Keyspaces[i+1].KeyspaceMeta)
	}
	re.Equal(keyspace.DefaultKeyspaceName, loadResponse.Keyspaces[0].Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, loadResponse.Keyspaces[0].State)
}

func sendLoadRangeRequest(re *require.Assertions, server *tests.TestServer, token, limit string) *handlers.LoadAllKeyspacesResponse {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspacesPrefix, nil)
	re.NoError(err)
	query := httpReq.URL.Query()
	query.Add("page_token", token)
	query.Add("limit", limit)
	httpReq.URL.RawQuery = query.Encode()
	// Send request.
	httpResp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)
	// Receive & decode response.
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	resp := &handlers.LoadAllKeyspacesResponse{}
	re.NoError(json.Unmarshal(data, resp))
	return resp
}

func sendUpdateStateRequest(re *require.Assertions, server *tests.TestServer, name string, request *handlers.UpdateStateParam) (bool, *keyspacepb.KeyspaceMeta) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPut, server.GetAddr()+keyspacesPrefix+"/"+name+"/state", bytes.NewBuffer(data))
	re.NoError(err)
	httpResp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return false, nil
	}
	data, err = io.ReadAll(httpResp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return true, meta.KeyspaceMeta
}
func mustMakeTestKeyspaces(re *require.Assertions, server *tests.TestServer, count int) []*keyspacepb.KeyspaceMeta {
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	resultMeta := make([]*keyspacepb.KeyspaceMeta, count)
	for i := 0; i < count; i++ {
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   fmt.Sprintf("test_keyspace_%d", i),
			Config: testConfig,
		}
		resultMeta[i] = mustCreateKeyspace(re, server, createRequest)
	}
	return resultMeta
}

func mustCreateKeyspace(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceParams) *keyspacepb.KeyspaceMeta {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspacesPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	// When creating a keyspace, it will be assigned a keyspace group id.
	request.Config[keyspace.TSOKeyspaceGroupIDKey] = "0"
	request.Config[keyspace.UserKindKey] = endpoint.Basic.String()
	checkCreateRequest(re, request, meta.KeyspaceMeta)
	return meta.KeyspaceMeta
}

func mustUpdateKeyspaceConfig(re *require.Assertions, server *tests.TestServer, name string, request *handlers.UpdateConfigParams) *keyspacepb.KeyspaceMeta {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPatch, server.GetAddr()+keyspacesPrefix+"/"+name+"/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return meta.KeyspaceMeta
}

func mustLoadKeyspaces(re *require.Assertions, server *tests.TestServer, name string) *keyspacepb.KeyspaceMeta {
	resp, err := dialClient.Get(server.GetAddr() + keyspacesPrefix + "/" + name)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return meta.KeyspaceMeta
}

// checkCreateRequest verifies a keyspace meta matches a create request.
func checkCreateRequest(re *require.Assertions, request *handlers.CreateKeyspaceParams, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	re.Equal(request.Config, meta.Config)
}

// checkUpdateRequest verifies a keyspace meta matches a update request.
func checkUpdateRequest(re *require.Assertions, request *handlers.UpdateConfigParams, oldConfig, newConfig map[string]string) {
	expected := map[string]string{}
	for k, v := range oldConfig {
		expected[k] = v
	}
	for k, v := range request.Config {
		if v == nil {
			delete(expected, k)
		} else {
			expected[k] = *v
		}
	}
	re.Equal(expected, newConfig)
}
