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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
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
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestPDServiceCluster(suite.ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.cleanupFunc = func() {
		cancel()
	}
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cleanupFunc()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func (suite *keyspaceGroupTestSuite) TestAllocNodesUpdate() {
	re := suite.Require()
	// add three nodes.
	nodes := make(map[string]bs.Server)
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range constant.DefaultKeyspaceGroupReplicaCount + 1 {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
	}
	tests.WaitForPrimaryServing(re, nodes)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code := suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)

	// alloc nodes for the keyspace group.
	id := 1
	params := &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount,
	}
	got, code := suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	re.Len(got, constant.DefaultKeyspaceGroupReplicaCount)
	oldMembers := make(map[string]struct{})
	for _, member := range got {
		re.Contains(nodes, member.Address)
		oldMembers[member.Address] = struct{}{}
	}

	// alloc node update to 3.
	params.Replica = constant.DefaultKeyspaceGroupReplicaCount + 1
	got, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	re.Len(got, params.Replica)
	newMembers := make(map[string]struct{})
	for _, member := range got {
		re.Contains(nodes, member.Address)
		newMembers[member.Address] = struct{}{}
	}
	for member := range oldMembers {
		// old members should be in new members.
		re.Contains(newMembers, member)
	}
}

func (suite *keyspaceGroupTestSuite) TestAllocReplica() {
	re := suite.Require()
	nodes := make(map[string]bs.Server)
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range constant.DefaultKeyspaceGroupReplicaCount {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
	}
	tests.WaitForPrimaryServing(re, nodes)

	// miss replica.
	id := 1
	params := &handlers.AllocNodesForKeyspaceGroupParams{}
	got, code := suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)
	re.Empty(got)

	// replica is less than default replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount - 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// there is no any keyspace group.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(id),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	for _, member := range got {
		re.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, but the replica is more than the num of nodes.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is more than the old replica.
	s2, cleanup2 := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	nodes[s2.GetAddr()] = s2
	tests.WaitForPrimaryServing(re, nodes)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount + 1,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	for _, member := range got {
		re.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, the new replica is equal to the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is less than the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: constant.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestSetNodes() {
	re := suite.Require()
	nodes := make(map[string]bs.Server)
	nodesList := []string{}
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range constant.DefaultKeyspaceGroupReplicaCount {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
		nodesList = append(nodesList, s.GetAddr())
	}
	tests.WaitForPrimaryServing(re, nodes)

	// the keyspace group is not exist.
	id := 1
	params := &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: nodesList,
	}
	_, code := suite.trySetNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(id),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: nodesList,
	}
	kg, code := suite.trySetNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	re.Len(kg.Members, 2)
	for _, member := range kg.Members {
		re.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, but the nodes is not exist.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: append(nodesList, "pingcap.com:2379"),
	}
	_, code = suite.trySetNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, but the count of nodes is less than the default replica.
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: []string{nodesList[0]},
	}
	_, code = suite.trySetNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.SetNodesForKeyspaceGroupParams{
		Nodes: nodesList,
	}
	_, code = suite.trySetNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestDefaultKeyspaceGroup() {
	re := suite.Require()
	nodes := make(map[string]bs.Server)
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range constant.DefaultKeyspaceGroupReplicaCount {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
	}
	tests.WaitForPrimaryServing(re, nodes)

	// the default keyspace group is exist.
	var kg *endpoint.KeyspaceGroup
	var code int
	testutil.Eventually(re, func() bool {
		kg, code = suite.tryGetKeyspaceGroup(re, constant.DefaultKeyspaceGroupID)
		return code == http.StatusOK && kg != nil
	}, testutil.WithWaitFor(time.Second*1))
	re.Equal(constant.DefaultKeyspaceGroupID, kg.ID)
	// the allocNodesToAllKeyspaceGroups loop will run every 100ms.
	testutil.Eventually(re, func() bool {
		return len(kg.Members) == constant.DefaultKeyspaceGroupReplicaCount
	})
	for _, member := range kg.Members {
		re.Contains(nodes, member.Address)
	}
}

func (suite *keyspaceGroupTestSuite) TestAllocNodes() {
	re := suite.Require()
	// add three nodes.
	nodes := make(map[string]bs.Server)
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()
	for range constant.DefaultKeyspaceGroupReplicaCount + 1 {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
	}
	tests.WaitForPrimaryServing(re, nodes)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code := suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)

	// alloc nodes for the keyspace group
	var kg *endpoint.KeyspaceGroup
	testutil.Eventually(re, func() bool {
		kg, code = suite.tryGetKeyspaceGroup(re, constant.DefaultKeyspaceGroupID)
		return code == http.StatusOK && kg != nil && len(kg.Members) == constant.DefaultKeyspaceGroupReplicaCount
	})
	stopNode := kg.Members[0].Address
	// close one of members
	nodes[stopNode].Close()

	// the member list will be updated
	testutil.Eventually(re, func() bool {
		kg, code = suite.tryGetKeyspaceGroup(re, constant.DefaultKeyspaceGroupID)
		for _, member := range kg.Members {
			if member.Address == stopNode {
				return false
			}
		}
		return code == http.StatusOK && kg != nil && len(kg.Members) == constant.DefaultKeyspaceGroupReplicaCount
	})
}

func (suite *keyspaceGroupTestSuite) TestAllocOneNode() {
	re := suite.Require()
	// add one tso server
	nodes := make(map[string]bs.Server)
	oldTSOServer, cleanupOldTSOserver := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanupOldTSOserver()
	nodes[oldTSOServer.GetAddr()] = oldTSOServer

	tests.WaitForPrimaryServing(re, nodes)

	// create a keyspace group.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code := suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)

	// alloc nodes for the keyspace group
	var kg *endpoint.KeyspaceGroup
	testutil.Eventually(re, func() bool {
		kg, code = suite.tryGetKeyspaceGroup(re, constant.DefaultKeyspaceGroupID)
		return code == http.StatusOK && kg != nil && len(kg.Members) == 1
	})
	stopNode := kg.Members[0].Address
	// close old tso server
	nodes[stopNode].Close()

	// create a new tso server
	newTSOServer, cleanupNewTSOServer := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanupNewTSOServer()
	nodes[newTSOServer.GetAddr()] = newTSOServer

	tests.WaitForPrimaryServing(re, nodes)

	// the member list will be updated
	testutil.Eventually(re, func() bool {
		kg, code = suite.tryGetKeyspaceGroup(re, constant.DefaultKeyspaceGroupID)
		if len(kg.Members) != 0 && kg.Members[0].Address == stopNode {
			return false
		}
		return code == http.StatusOK && kg != nil && len(kg.Members) == 1
	})
}

func (suite *keyspaceGroupTestSuite) tryAllocNodesForKeyspaceGroup(re *require.Assertions, id int, request *handlers.AllocNodesForKeyspaceGroupParams) ([]endpoint.KeyspaceGroupMember, int) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d/alloc", id), bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	nodes := make([]endpoint.KeyspaceGroupMember, 0)
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.NoError(json.Unmarshal(bodyBytes, &nodes))
	}
	return nodes, resp.StatusCode
}

func (suite *keyspaceGroupTestSuite) tryCreateKeyspaceGroup(re *require.Assertions, request *handlers.CreateKeyspaceGroupParams) int {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func (suite *keyspaceGroupTestSuite) tryGetKeyspaceGroup(re *require.Assertions, id uint32) (*endpoint.KeyspaceGroup, int) {
	httpReq, err := http.NewRequest(http.MethodGet, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	kg := &endpoint.KeyspaceGroup{}
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.NoError(json.Unmarshal(bodyBytes, kg))
	}
	return kg, resp.StatusCode
}

func (suite *keyspaceGroupTestSuite) trySetNodesForKeyspaceGroup(re *require.Assertions, id int, request *handlers.SetNodesForKeyspaceGroupParams) (*endpoint.KeyspaceGroup, int) {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPatch, suite.server.GetAddr()+keyspaceGroupsPrefix+fmt.Sprintf("/%d", id), bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	return suite.tryGetKeyspaceGroup(re, uint32(id))
}
