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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

const keyspaceGroupsPrefix = "/pd/api/v2/tso/keyspace-groups"

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestAPICluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceGroupTestSuite) TestCreateKeyspaceGroups() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}

	mustCreateKeyspaceGroup(re, suite.server, kgs)
}

func (suite *keyspaceGroupTestSuite) TestLoadKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}

	mustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := sendLoadKeyspaceGroupRequest(re, suite.server, "0", "0")
	re.Len(resp, 3)
}

func (suite *keyspaceGroupTestSuite) TestSplitKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
		},
	}}

	mustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := sendLoadKeyspaceGroupRequest(re, suite.server, "0", "0")
	re.Len(resp, 2)
	mustSplitKeyspaceGroup(re, suite.server, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     uint32(2),
		Keyspaces: []uint32{111, 222},
	})
	resp = sendLoadKeyspaceGroupRequest(re, suite.server, "0", "0")
	re.Len(resp, 3)
	// Check keyspace group 1.
	kg1 := mustLoadKeyspaceGroupByID(re, suite.server, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{333}, kg1.Keyspaces)
	re.True(kg1.InSplit)
	re.Empty(kg1.SplitFrom)
	// Check keyspace group 2.
	kg2 := mustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 222}, kg2.Keyspaces)
	re.True(kg2.InSplit)
	re.Equal(kg1.ID, kg2.SplitFrom)
	// They should have the same user kind and members.
	re.Equal(kg1.UserKind, kg2.UserKind)
	re.Equal(kg1.Members, kg2.Members)
	// Finish the split and check the split state.
	mustFinishSplitKeyspaceGroup(re, suite.server, 2)
	kg2 = mustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.False(kg2.InSplit)
	re.Equal(kg1.ID, kg2.SplitFrom)
}

func sendLoadKeyspaceGroupRequest(re *require.Assertions, server *tests.TestServer, token, limit string) []*endpoint.KeyspaceGroup {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspaceGroupsPrefix, nil)
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
	var resp []*endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &resp))
	return resp
}

func mustLoadKeyspaceGroupByID(re *require.Assertions, server *tests.TestServer, id uint32) *endpoint.KeyspaceGroup {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), nil)
	re.NoError(err)
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	var kg endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &kg))
	return &kg
}

func mustCreateKeyspaceGroup(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceGroupParams) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
}

func mustSplitKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32, request *handlers.SplitKeyspaceGroupByIDParams) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/split", id), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
}

func mustFinishSplitKeyspaceGroup(re *require.Assertions, server *tests.TestServer, id uint32) {
	httpReq, err := http.NewRequest(http.MethodDelete, server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/split", id), nil)
	re.NoError(err)
	// Send request.
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
}
