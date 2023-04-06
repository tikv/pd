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
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
)

const keyspaceGroupsPrefix = "/pd/api/v2/tso/keyspace-groups"

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

func (suite *keyspaceGroupTestSuite) TestAllocNodesForGroup() {
	manager := suite.server.GetServer().GetKeyspaceGroupManager()
	// successful to allocate one node, there is one node in the cluster.
	_, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	members := manager.AllocNodesForGroup(1)
	suite.Len(members, 1)
	// failed to allocate two nodes, there is only one node in the cluster.
	members = manager.AllocNodesForGroup(2)
	suite.Empty(members)
	// successful to allocate two nodes, there are two nodes in the cluster.
	_, cleanup2 := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	client := suite.server.GetEtcdClient()
	clusterID := strconv.FormatUint(suite.server.GetClusterID(), 10)
	endpoints, err := discovery.Discover(client, clusterID, "tso")
	suite.NoError(err)
	suite.Len(endpoints, 2)
	for i := 0; i < 10; i++ {
		members = manager.AllocNodesForGroup(2)
		suite.Len(members, 2)
		// the two nodes are different always.
		suite.NotEqual(members[0].Address, members[1].Address)
		// the two nodes are in the cluster.
		for _, member := range members {
			switch member.Address {
			case endpoints[0]:
			case endpoints[1]:
			default:
				suite.Fail("unexpected member", member.Address)
			}
		}
	}
}

func (suite *keyspaceGroupTestSuite) TestReplicaNum() {
	_, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	// replica is more than the num of nodes.
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
			Replica:  2,
		},
	}}
	code := suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusBadRequest, code)
	// miss replica.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusBadRequest, code)

	// replica is negative.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(3),
			UserKind: endpoint.Standard.String(),
			Replica:  -1,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusBadRequest, code)

	// replica is equal with the num of nodes.
	_, cleanup2 := mcs.StartSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(4),
			UserKind: endpoint.Standard.String(),
			Replica:  2,
		},
	}}
	code = suite.tryCreateKeyspaceGroup(kgs)
	suite.Equal(http.StatusOK, code)
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
