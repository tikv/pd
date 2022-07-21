// Copyright 2022 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
	"io"
	"net/http"
	"testing"
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

func (suite *keyspaceTestSuite) SetupSuite() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceTestSuite) TearDownSuite() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *keyspaceTestSuite) TestCreateLoadKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 0, 10)
	for _, created := range keyspaces {
		loaded := mustLoadKeyspaces(re, suite.server, created.Name)
		re.Equal(created, loaded)
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 0, 10)
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

func mustMakeTestKeyspaces(re *require.Assertions, server *tests.TestServer, start, count int) []*keyspacepb.KeyspaceMeta {
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	resultMeta := make([]*keyspacepb.KeyspaceMeta, count)
	for i := 0; i < count; i++ {
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   fmt.Sprintf("test_keyspace%d", start+i),
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
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.NoError(resp.Body.Close())
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	checkCreateRequest(re, request, meta.KeyspaceMeta)
	return meta.KeyspaceMeta
}

func mustUpdateKeyspaceConfig(re *require.Assertions, server *tests.TestServer, name string, request *handlers.UpdateConfigParams) *keyspacepb.KeyspaceMeta {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPatch, server.GetAddr()+keyspacesPrefix+"/"+name+"/updateConfig", bytes.NewBuffer(data))
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.NoError(resp.Body.Close())
	meta := &handlers.KeyspaceMeta{}
	re.NoError(json.Unmarshal(data, meta))
	return meta.KeyspaceMeta
}

func mustLoadKeyspaces(re *require.Assertions, server *tests.TestServer, name string) *keyspacepb.KeyspaceMeta {
	resp, err := dialClient.Get(server.GetAddr() + keyspacesPrefix + "/" + name)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.NoError(resp.Body.Close())
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
