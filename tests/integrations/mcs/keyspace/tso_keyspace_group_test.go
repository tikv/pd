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
	"time"

	"github.com/stretchr/testify/suite"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/utils"
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
	cluster, err := tests.NewTestAPICluster(suite.ctx, 1)
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
	params := &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 1,
	}
	got, code := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(1, len(got))
	suite.Contains(nodes, got[0].Address)
	oldNode := got[0].Address

	// alloc node update to 2.
	params.Replica = 2
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(2, len(got))
	suite.Contains(nodes, got[0].Address)
	suite.Contains(nodes, got[1].Address)
	suite.True(oldNode == got[0].Address || oldNode == got[1].Address) // the old node is also in the new result.
	suite.NotEqual(got[0].Address, got[1].Address)                     // the two nodes are different.
}

func (suite *keyspaceGroupTestSuite) TestAllocReplica() {
	nodes := make(map[string]bs.Server)
	s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	nodes[s.GetAddr()] = s
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// miss replica.
	id := 1
	params := &handlers.AllocNodesForKeyspaceGroupParams{}
	got, code := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
	suite.Empty(got)

	// replica is negative.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: -1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// there is no any keyspace group.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
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
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 1,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.True(checkNodes(got, nodes))

	// the keyspace group is exist, but the replica is more than the num of nodes.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 2,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
	// the keyspace group is exist, the new replica is more than the old replica.
	s2, cleanup2 := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	nodes[s2.GetAddr()] = s2
	mcs.WaitForPrimaryServing(suite.Require(), nodes)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 2,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.True(checkNodes(got, nodes))

	// the keyspace group is exist, the new replica is equal to the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 2,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is less than the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestSetNodes() {
	nodes := make(map[string]bs.Server)
	s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	nodes[s.GetAddr()] = s
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// the keyspace group is not exist.
	id := 1
	params := &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{s.GetAddr()},
	}
	_, code := suite.trySetNodesForKeyspaceGroup(id, params)
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
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{s.GetAddr()},
	}
	kg, code := suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Len(kg.Members, 1)
	suite.Equal(s.GetAddr(), kg.Members[0].Address)

	// the keyspace group is exist, but the nodes is not exist.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{"pingcap.com:2379"},
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, but the nodes is empty.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{},
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{s.GetAddr()},
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestDefaultKeyspaceGroup() {
	nodes := make(map[string]bs.Server)
	s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	nodes[s.GetAddr()] = s
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// the default keyspace group is exist.
	time.Sleep(2 * time.Second)
	kg, code := suite.tryGetKeyspaceGroup(utils.DefaultKeyspaceGroupID)
	suite.Equal(http.StatusOK, code)
	suite.Equal(utils.DefaultKeyspaceGroupID, kg.ID)
	suite.Len(kg.Members, 1)
	suite.Equal(s.GetAddr(), kg.Members[0].Address)
}

func (suite *keyspaceGroupTestSuite) tryAllocNodesForKeyspaceGroup(id int, request *handlers.AllocNodesForKeyspaceGroupParams) ([]endpoint.KeyspaceGroupMember, int) {
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
	return nodes, resp.StatusCode
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

func (suite *keyspaceGroupTestSuite) tryGetKeyspaceGroup(id uint32) (*endpoint.KeyspaceGroup, int) {
	httpReq, err := http.NewRequest(http.MethodGet, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), nil)
	suite.NoError(err)
	resp, err := suite.dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	kg := &endpoint.KeyspaceGroup{}
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		suite.NoError(err)
		suite.NoError(json.Unmarshal(bodyBytes, kg))
	}
	return kg, resp.StatusCode
}

func (suite *keyspaceGroupTestSuite) trySetNodesForKeyspaceGroup(id int, request *handlers.SetNodesForKeyspaceGroupParams) (*endpoint.KeyspaceGroup, int) {
	data, err := json.Marshal(request)
	suite.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/nodes", id), bytes.NewBuffer(data))
	suite.NoError(err)
	resp, err := suite.dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	return suite.tryGetKeyspaceGroup(uint32(id))
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
