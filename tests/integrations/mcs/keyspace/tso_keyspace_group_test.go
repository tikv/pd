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

	"github.com/pingcap/failpoint"
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
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
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
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func (suite *keyspaceGroupTestSuite) TestAllocNodesUpdate() {
	// add three nodes.
	nodes := make(map[string]bs.Server)
	for i := 0; i < utils.DefaultKeyspaceGroupReplicaCount+1; i++ {
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
		Replica: utils.DefaultKeyspaceGroupReplicaCount,
	}
	got, code := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(utils.DefaultKeyspaceGroupReplicaCount, len(got))
	oldMembers := make(map[string]struct{})
	for _, member := range got {
		suite.Contains(nodes, member.Address)
		oldMembers[member.Address] = struct{}{}
	}

	// alloc node update to 3.
	params.Replica = utils.DefaultKeyspaceGroupReplicaCount + 1
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Equal(params.Replica, len(got))
	newMembers := make(map[string]struct{})
	for _, member := range got {
		suite.Contains(nodes, member.Address)
		newMembers[member.Address] = struct{}{}
	}
	for member := range oldMembers {
		// old members should be in new members.
		suite.Contains(newMembers, member)
	}
}

func (suite *keyspaceGroupTestSuite) TestAllocReplica() {
	nodes := make(map[string]bs.Server)
	for i := 0; i < utils.DefaultKeyspaceGroupReplicaCount; i++ {
		s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		nodes[s.GetAddr()] = s
	}
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// miss replica.
	id := 1
	params := &handlers.AllocNodesForKeyspaceGroupParams{}
	got, code := suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
	suite.Empty(got)

	// replica is less than default replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount - 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// there is no any keyspace group.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(id),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusOK, code)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	for _, member := range got {
		suite.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, but the replica is more than the num of nodes.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is more than the old replica.
	s2, cleanup2 := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	nodes[s2.GetAddr()] = s2
	mcs.WaitForPrimaryServing(suite.Require(), nodes)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount + 1,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	for _, member := range got {
		suite.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, the new replica is equal to the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is less than the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: utils.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestSetNodes() {
	nodes := make(map[string]bs.Server)
	nodesList := []string{}
	for i := 0; i < utils.DefaultKeyspaceGroupReplicaCount; i++ {
		s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		nodes[s.GetAddr()] = s
		nodesList = append(nodesList, s.GetAddr())
	}
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// the keyspace group is not exist.
	id := 1
	params := &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: nodesList,
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
		Nodes: nodesList,
	}
	kg, code := suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusOK, code)
	suite.Len(kg.Members, 2)
	for _, member := range kg.Members {
		suite.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, but the nodes is not exist.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: append(nodesList, "pingcap.com:2379"),
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, but the count of nodes is less than the default replica.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{nodesList[0]},
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: nodesList,
	}
	_, code = suite.trySetNodesForKeyspaceGroup(id, params)
	suite.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestDefaultKeyspaceGroup() {
	nodes := make(map[string]bs.Server)
	for i := 0; i < utils.DefaultKeyspaceGroupReplicaCount; i++ {
		s, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		nodes[s.GetAddr()] = s
	}
	mcs.WaitForPrimaryServing(suite.Require(), nodes)

	// the default keyspace group is exist.
	var kg *endpoint.KeyspaceGroup
	var code int
	testutil.Eventually(suite.Require(), func() bool {
		kg, code = suite.tryGetKeyspaceGroup(utils.DefaultKeyspaceGroupID)
		return code == http.StatusOK && kg != nil
	}, testutil.WithWaitFor(time.Second*1))
	suite.Equal(utils.DefaultKeyspaceGroupID, kg.ID)
	suite.Len(kg.Members, utils.DefaultKeyspaceGroupReplicaCount)
	for _, member := range kg.Members {
		suite.Contains(nodes, member.Address)
	}
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
	httpReq, err := http.NewRequest(http.MethodPatch, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), bytes.NewBuffer(data))
	suite.NoError(err)
	resp, err := suite.dialClient.Do(httpReq)
	suite.NoError(err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	return suite.tryGetKeyspaceGroup(uint32(id))
}
