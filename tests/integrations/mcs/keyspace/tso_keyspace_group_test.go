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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs/utils"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

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
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
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
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

// getTSOServerURLs gets the TSO server URLs from PD, mimicking the client's getTSOServer logic
func (suite *keyspaceGroupTestSuite) getTSOServerURLs() ([]string, error) {
	// Connect to PD via gRPC
	// Remove "http://" or "https://" prefix if present, as gRPC Dial expects address without scheme
	addr := suite.backendEndpoints
	addr = strings.TrimPrefix(addr, "http://")

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Call GetClusterInfo to get TsoUrls (same as client's discoverMicroservice does)
	pdClient := pdpb.NewPDClient(conn)
	resp, err := pdClient.GetClusterInfo(suite.ctx, &pdpb.GetClusterInfoRequest{})
	if err != nil {
		return nil, err
	}

	return resp.TsoUrls, nil
}

// closeAllTSONodesAndWait closes all TSO nodes and waits for them to deregister from etcd
func (suite *keyspaceGroupTestSuite) closeAllTSONodesAndWait(re *require.Assertions, nodes map[string]bs.Server) {
	// Close all TSO nodes
	nodeCount := 0
	for _, node := range nodes {
		node.Close()
		nodeCount++
	}

	// Wait for nodes to deregister from etcd (TTL is 3 seconds)
	// Use the same logic as client's getTSOServer: check if TsoUrls is empty
	testutil.Eventually(re, func() bool {
		// Get TSO server URLs using the same method as client's getTSOServer
		tsoURLs, err := suite.getTSOServerURLs()
		if err != nil {
			return false
		}

		// Check if TsoUrls is empty (same condition as in client's getTSOServer)
		// When len(tsoURLs) == 0, client will enter the fallback logic
		noTSOServers := len(tsoURLs) == 0
		return noTSOServers
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(500*time.Millisecond))
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
	for range mcs.DefaultKeyspaceGroupReplicaCount + 1 {
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
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
	}
	got, code := suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	re.Len(got, mcs.DefaultKeyspaceGroupReplicaCount)
	oldMembers := make(map[string]struct{})
	for _, member := range got {
		re.Contains(nodes, member.Address)
		oldMembers[member.Address] = struct{}{}
	}

	// alloc node update to 3.
	params.Replica = mcs.DefaultKeyspaceGroupReplicaCount + 1
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
	for range mcs.DefaultKeyspaceGroupReplicaCount {
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
		Replica: mcs.DefaultKeyspaceGroupReplicaCount - 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// there is no any keyspace group.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
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
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	for _, member := range got {
		re.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, but the replica is more than the num of nodes.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is more than the old replica.
	s2, cleanup2 := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanup2()
	nodes[s2.GetAddr()] = s2
	tests.WaitForPrimaryServing(re, nodes)
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount + 1,
	}
	got, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusOK, code)
	for _, member := range got {
		re.Contains(nodes, member.Address)
	}

	// the keyspace group is exist, the new replica is equal to the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount + 1,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is exist, the new replica is less than the old replica.
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
	}
	_, code = suite.tryAllocNodesForKeyspaceGroup(re, id, params)
	re.Equal(http.StatusBadRequest, code)

	// the keyspace group is not exist.
	id = 2
	params = &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
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
	for range mcs.DefaultKeyspaceGroupReplicaCount {
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
	for range mcs.DefaultKeyspaceGroupReplicaCount {
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
		return len(kg.Members) == mcs.DefaultKeyspaceGroupReplicaCount
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
	for range mcs.DefaultKeyspaceGroupReplicaCount + 1 {
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
		return code == http.StatusOK && kg != nil && len(kg.Members) == mcs.DefaultKeyspaceGroupReplicaCount
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
		return code == http.StatusOK && kg != nil && len(kg.Members) == mcs.DefaultKeyspaceGroupReplicaCount
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

// tsoTestSetup holds the setup information for TSO tests
type tsoTestSetup struct {
	nodes         map[string]bs.Server
	cleanups      []func()
	client        pd.Client
	initialTS     uint64
	firstNodeAddr string
	keyspaceID    uint32
}

// setupTSONodesAndClient creates TSO nodes, keyspace group, and returns initialized client
func (suite *keyspaceGroupTestSuite) setupTSONodesAndClient(re *require.Assertions, nodeCount int, keyspaceGroupID uint32, opts ...opt.ClientOption) *tsoTestSetup {
	// Create TSO nodes
	nodes := make(map[string]bs.Server)
	var cleanups []func()

	// Start TSO servers
	for range nodeCount {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		cleanups = append(cleanups, cleanup)
		nodes[s.GetAddr()] = s
	}
	tests.WaitForPrimaryServing(re, nodes)

	// Enable failpoint to assign keyspace to the specified keyspace group instead of default group 0
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/assignToSpecificKeyspaceGroup", fmt.Sprintf("return(%d)", keyspaceGroupID)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/assignToSpecificKeyspaceGroup"))
	}()

	// Create keyspace group and assign the keyspace to it
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       keyspaceGroupID,
			UserKind: endpoint.Standard.String(),
		},
	}}
	code := suite.tryCreateKeyspaceGroup(re, kgs)
	re.Equal(http.StatusOK, code)

	// Alloc nodes for keyspace group
	params := &handlers.AllocNodesForKeyspaceGroupParams{
		Replica: nodeCount,
	}
	got, code := suite.tryAllocNodesForKeyspaceGroup(re, int(keyspaceGroupID), params)
	re.Equal(http.StatusOK, code)
	re.Len(got, nodeCount)

	// Wait for keyspace group to be ready
	testutil.Eventually(re, func() bool {
		kg, code := suite.tryGetKeyspaceGroup(re, keyspaceGroupID)
		return code == http.StatusOK && kg != nil && len(kg.Members) == nodeCount
	})

	// Create keyspace (will be assigned to keyspaceGroupID via failpoint)
	keyspaceID := keyspaceGroupID // Use same ID for simplicity
	keyspaceName := fmt.Sprintf("keyspace_%d", keyspaceID)
	handlersutil.MustCreateKeyspaceByID(re, suite.server, &handlers.CreateKeyspaceByIDParams{
		ID:   &keyspaceID,
		Name: keyspaceName,
		Config: map[string]string{
			keyspace.UserKindKey: endpoint.Standard.String(), // Keep UserKind consistent with keyspace group
		},
	})

	// Wait for keyspace to be fully created and available
	testutil.Eventually(re, func() bool {
		resp, err := tests.TestDialClient.Get(suite.server.GetAddr() + "/pd/api/v2/keyspaces/" + keyspaceName)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	// Create client using keyspace name (not ID) to ensure keyspace meta is loaded and passed to tsoServiceDiscovery
	apiCtx := pd.NewAPIContextV2(keyspaceName)
	client := utils.SetupClientWithAPIContext(suite.ctx, re, apiCtx, []string{suite.backendEndpoints}, opts...)
	utils.WaitForTSOServiceAvailable(suite.ctx, re, client)

	physical, logical, err := client.GetTS(suite.ctx)
	re.NoError(err)
	initialTS := tsoutil.ComposeTS(physical, logical)
	re.NotZero(initialTS)

	// Save the first node's address for later restart
	var firstNodeAddr string
	for addr := range nodes {
		firstNodeAddr = addr
		break
	}
	return &tsoTestSetup{
		nodes:         nodes,
		cleanups:      cleanups,
		client:        client,
		initialTS:     initialTS,
		firstNodeAddr: firstNodeAddr,
		keyspaceID:    keyspaceID,
	}
}

// TestUpdateMemberWhenRecovery verifies that in TSO microservice mode (API_SVC_MODE), when all TSO nodes
// become temporarily unavailable and then recover, the client should NOT fallback to
// the legacy path (group 0), but should wait and successfully get TSO after nodes restart.
//
// Test scenario:
// 1. Setup: Start 2 TSO nodes and create keyspace group 1, client gets initial TSO
// 2. Close all TSO nodes to simulate total TSO microservice failure
// 3. Wait until all TSO nodes are deregistered (getTSOServerURLs returns empty)
// 4. Enable failpoints: assertNotReachLegacyPath (panic if fallback) and extend timeout
// 5. Start async GetTS call (will wait for TSO service to recover)
// 6. Restart one TSO node while GetTS is waiting
// 7. Verify GetTS succeeds after node restart (assertNotReachLegacyPath ensures no fallback)
func (suite *keyspaceGroupTestSuite) TestUpdateMemberWhenRecovery() {
	re := suite.Require()

	// Test configuration constants
	const (
		// Time to wait for GetTS to start
		waitForGetTSStart = 3 * time.Second
	)

	// Step 1: Setup - Create 2 TSO nodes and client, get initial TSO
	setup := suite.setupTSONodesAndClient(re, 2, 1, opt.WithCustomTimeoutOption(30*time.Second))
	defer func() {
		for _, cleanup := range setup.cleanups {
			cleanup()
		}
	}()
	defer setup.client.Close()

	nodes := setup.nodes
	client := setup.client
	firstNodeAddr := setup.firstNodeAddr

	// Step 2: Close all TSO nodes to simulate total TSO microservice failure
	// Step 3: Wait until all TSO nodes are deregistered
	suite.closeAllTSONodesAndWait(re, nodes)

	// Step 4: Enable failpoints for verification and timeout extension
	// assertNotReachLegacyPath: ensures we don't fallback to legacy path (will panic if we do)
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/assertNotReachLegacyPath", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/assertNotReachLegacyPath"))
	}()

	// Step 5: Start async GetTS call - it will wait for TSO service to recover
	type getTSResult struct {
		physicalTS int64
		logicalTS  int64
		err        error
	}
	resultCh := make(chan getTSResult, 1)

	go func() {
		physicalTS, logicalTS, getErr := client.GetTS(suite.ctx)
		resultCh <- getTSResult{physicalTS: physicalTS, logicalTS: logicalTS, err: getErr}
	}()

	time.Sleep(waitForGetTSStart) // Give it time to begin execution

	// Step 6: Restart one TSO node while GetTS is waiting
	newNode, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, firstNodeAddr)
	setup.cleanups = append(setup.cleanups, cleanup)
	nodes[newNode.GetAddr()] = newNode
	tests.WaitForPrimaryServing(re, map[string]bs.Server{newNode.GetAddr(): newNode})

	// Step 7: Verify GetTS succeeds after node restart
	result := <-resultCh
	re.NoError(result.err, "GetTS should succeed after TSO node restart")

	// KEY VERIFICATION: If code incorrectly tried to fallback to legacy path,
	// assertNotReachLegacyPath failpoint would have panicked already
}
