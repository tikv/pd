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

package keyspace_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
)

const (
	keyspaceGroupsPrefix = "/pd/api/v2/tso/keyspace-groups"
)

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx              context.Context
	cleanupFunc      testutil.CleanupFunc
	cluster          *tests.TestCluster
	server           *tests.TestServer
	backendEndpoints string
	dialClient       *http.Client
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestCluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.dialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	suite.cleanupFunc = func() {
		cancel()
	}
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.cleanupFunc()
	suite.cluster.Destroy()
}

func (suite *keyspaceGroupTestSuite) TestAllocNodesUpdate() {
	// add three nodes.
	nodes := make(map[string]bs.Server)
	for i := 0; i < 3; i++ {
		s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		nodes[s.GetAddr()] = s
	}
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code := suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusOK, code)

	// alloc nodes for the keyspace group.
	id := 1
	params := &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 1,
	}
	code, got := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(1, len(got))
	suite.Contains(nodes, got[0].Address)
	oldNode := got[0].Address

	// alloc node update to 2.
	params.Replica = 2
	code, got = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(2, len(got))
	suite.Contains(nodes, got[0].Address)
	suite.Contains(nodes, got[1].Address)
	suite.True(oldNode == got[0].Address || oldNode == got[1].Address) // the old node is also in the new result.
	suite.NotEqual(got[0].Address, got[1].Address)                     // the two nodes are different.
}

func (suite *keyspaceGroupTestSuite) TestReplica() {
	nodes := make(map[string]bs.Server)
	s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	nodes[s.GetAddr()] = s
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// miss replica.
	id := 1
	params := &handlers.AllocNodeForKeyspaceGroupParams{}
	code, got := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
	suite.Empty(got)

	// replica is negative.
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: -1,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// there is no any keyspace group.
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 1,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(id),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusOK, code)
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 1,
	}
	code, got = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.True(checkNodes(got, nodes))

	// the keyspace group is exist, but the replica is more than the num of nodes.
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 2,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
	// the keyspace group is exist, the new replica is more than the old replica.
	s2, cleanup2 := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	nodes[s2.GetAddr()] = s2
	mcs.WaitForPrimaryServing(suite.Require(), nodes)
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 2,
	}
	code, got = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.True(checkNodes(got, nodes))

	// the keyspace group is exist, the new replica is equal to the old replica.
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 2,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is less than the old replica.
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 1,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.AllocNodeForKeyspaceGroupParams{
		Replica: 1,
	}
	code, _ = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) tryAllocNodesForKeyspaceGroup(id int, request *handlers.AllocNodeForKeyspaceGroupParams) (int, []endpoint.KeyspaceGroupMember) {
	data, err := json.Marshal(request)
	suite.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/alloc", id), bytes.NewBuffer(data))
	suite.NoError(err)
	resp, err := suite.dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	nodes := make([]endpoint.KeyspaceGroupMember, 0)
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		suite.NoError(err)
		suite.NoError(json.Unmarshal(bodyBytes, &nodes))
	}
	return resp.StatusCode, nodes
}

func (suite *keyspaceGroupTestSuite) tryCreateKeyspaceGroup(request *handlers.CreateKeyspaceGroupParams) int {
	data, err := json.Marshal(request)
	suite.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	suite.NoError(err)
	resp, err := suite.dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func checkNodes(nodes []endpoint.KeyspaceGroupMember, servers map[string]bs.Server) bool {
	if len(nodes) != len(servers) {
		return false
	}
	for _, node := range nodes {
		if _, ok := servers[node.Address]; !ok {
			return false
		}
	}
	return true
}
