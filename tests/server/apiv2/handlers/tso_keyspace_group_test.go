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
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/utils"
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
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/disableAllocate", "return(true)"))
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/disableAllocate", "return(true)"))
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/disableAllocate"))
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/disableAllocate"))
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceGroupTestSuite) TestCreateKeyspaceGroups() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code := suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusOK, code)

	// miss user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:      uint32(3),
			Replica: 1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusBadRequest, code)

	// invalid user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(3),
			UserKind: "invalid",
			Replica:  1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusBadRequest, code)

	// miss ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusInternalServerError, code)

	// invalid ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       utils.MaxKeyspaceGroupCount + 1,
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusBadRequest, code)

	// repeated ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusInternalServerError, code)
}

func (suite *keyspaceGroupTestSuite) TestLoadKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}

	code := suite.tryCreateKeyspaceGroup(kgs)
	re.Equal(http.StatusOK, code)
	resp := suite.sendLoadKeyspaceGroupRequest("0", "0")
	re.Equal(3, len(resp))
}

func (suite *keyspaceGroupTestSuite) sendLoadKeyspaceGroupRequest(token, limit string) []*endpoint.KeyspaceGroup {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, suite.server.GetAddr()+keyspaceGroupsPrefix, nil)
	suite.NoError(err)
	query := httpReq.URL.Query()
	query.Add("page_token", token)
	query.Add("limit", limit)
	httpReq.URL.RawQuery = query.Encode()
	// Send request.
	httpResp, err := dialClient.Do(httpReq)
	suite.NoError(err)
	defer httpResp.Body.Close()
	suite.Equal(http.StatusOK, httpResp.StatusCode)
	// Receive & decode response.
	data, err := io.ReadAll(httpResp.Body)
	suite.NoError(err)
	var resp []*endpoint.KeyspaceGroup
	suite.NoError(json.Unmarshal(data, &resp))
	return resp
}

func (suite *keyspaceGroupTestSuite) tryCreateKeyspaceGroup(request *handlers.CreateKeyspaceGroupParams) int {
	data, err := json.Marshal(request)
	suite.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	suite.NoError(err)
	resp, err := dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	return resp.StatusCode
}
