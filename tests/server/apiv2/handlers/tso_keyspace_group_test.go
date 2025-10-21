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

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/keyspace"
	ksconstant "github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

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
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
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
	MustCreateKeyspaceGroup(re, suite.server, kgs)

	// miss user kind, use default value.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID: uint32(3),
		},
	}}
	MustCreateKeyspaceGroup(re, suite.server, kgs)

	// invalid user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(4),
			UserKind: "invalid",
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusBadRequest)

	// miss ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusInternalServerError)

	// invalid ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       constant.MaxKeyspaceGroupCount + 1,
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusBadRequest)

	// repeated ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
		},
	}}
	FailCreateKeyspaceGroupWithCode(re, suite.server, kgs, http.StatusInternalServerError)
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

	MustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 3)
}

func (suite *keyspaceGroupTestSuite) TestSplitKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:        uint32(1),
			UserKind:  endpoint.Standard.String(),
			Keyspaces: []uint32{111, 222, 333},
			Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
		},
	}}

	MustCreateKeyspaceGroup(re, suite.server, kgs)
	resp := MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 2)
	MustSplitKeyspaceGroup(re, suite.server, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     uint32(2),
		Keyspaces: []uint32{111, 222},
	})
	resp = MustLoadKeyspaceGroups(re, suite.server, "0", "0")
	re.Len(resp, 3)
	// Check keyspace group 1.
	kg1 := MustLoadKeyspaceGroupByID(re, suite.server, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{333}, kg1.Keyspaces)
	re.True(kg1.IsSplitSource())
	re.Equal(kg1.ID, kg1.SplitSource())
	// Check keyspace group 2.
	kg2 := MustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{111, 222}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	re.Equal(kg1.ID, kg2.SplitSource())
	// They should have the same user kind and members.
	re.Equal(kg1.UserKind, kg2.UserKind)
	re.Equal(kg1.Members, kg2.Members)
	// Finish the split and check the split state.
	MustFinishSplitKeyspaceGroup(re, suite.server, 2)
	kg1 = MustLoadKeyspaceGroupByID(re, suite.server, 1)
	re.False(kg1.IsSplitting())
	kg2 = MustLoadKeyspaceGroupByID(re, suite.server, 2)
	re.False(kg2.IsSplitting())
}

// TestKeyspaceGroupErrorMessage verifies that BindJSON errors return clear error messages.
func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupErrorMessage() {
	re := suite.Require()

	// Test SplitKeyspaceGroupByID with invalid JSON
	httpReq, err := http.NewRequest(
		http.MethodPost,
		suite.server.GetAddr()+keyspaceGroupsPrefix+"/1/split",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	var errorMsg string
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test MergeKeyspaceGroups with invalid JSON
	httpReq, err = http.NewRequest(
		http.MethodPost,
		suite.server.GetAddr()+keyspaceGroupsPrefix+"/1/merge",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test AllocNodesForKeyspaceGroup with invalid JSON
	httpReq, err = http.NewRequest(
		http.MethodPost,
		suite.server.GetAddr()+keyspaceGroupsPrefix+"/1/alloc",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test SetNodesForKeyspaceGroup with invalid JSON
	httpReq, err = http.NewRequest(
		http.MethodPatch,
		suite.server.GetAddr()+keyspaceGroupsPrefix+"/1",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test SetPriorityForKeyspaceGroup with invalid JSON
	httpReq, err = http.NewRequest(
		http.MethodPatch,
		suite.server.GetAddr()+keyspaceGroupsPrefix+"/1/test-node",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")
}

func (suite *keyspaceGroupTestSuite) TestRemoveKeyspacesFromGroup() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))

	keyspaceManager := suite.server.GetKeyspaceManager()
	re.NotNil(keyspaceManager)

	// Create test keyspaces (automatically added to default keyspace group 0)
	keyspaceMeta1, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "test_keyspace_1",
		CreateTime: 0,
	})
	re.NoError(err)
	keyspaceID1 := keyspaceMeta1.GetId()

	keyspaceMeta2, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "test_keyspace_2",
		CreateTime: 0,
	})
	re.NoError(err)
	keyspaceID2 := keyspaceMeta2.GetId()

	keyspaceMeta3, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "test_keyspace_3",
		CreateTime: 0,
	})
	re.NoError(err)
	keyspaceID3 := keyspaceMeta3.GetId()

	// Verify all keyspaces are in the default group
	kg := MustLoadKeyspaceGroupByID(re, suite.server, ksconstant.DefaultKeyspaceGroupID)
	re.Contains(kg.Keyspaces, keyspaceID1)
	re.Contains(kg.Keyspaces, keyspaceID2)
	re.Contains(kg.Keyspaces, keyspaceID3)

	// Test 1: Try to remove ENABLED keyspaces (should succeed but nothing removed)
	kg = MustRemoveKeyspacesFromGroup(re, suite.server, ksconstant.DefaultKeyspaceGroupID,
		[]uint32{keyspaceID1})
	// Verify nothing is removed (keyspace is still there because it's ENABLED)
	re.Contains(kg.Keyspaces, keyspaceID1)

	// Test 2: Update keyspaces to ARCHIVED/TOMBSTONE state and batch remove
	// Set keyspace1 to ARCHIVED
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID1, keyspacepb.KeyspaceState_DISABLED, 0)
	re.NoError(err)
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID1, keyspacepb.KeyspaceState_ARCHIVED, 0)
	re.NoError(err)

	// Set keyspace2 to TOMBSTONE
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID2, keyspacepb.KeyspaceState_DISABLED, 0)
	re.NoError(err)
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID2, keyspacepb.KeyspaceState_ARCHIVED, 0)
	re.NoError(err)
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID2, keyspacepb.KeyspaceState_TOMBSTONE, 0)
	re.NoError(err)

	// Batch remove keyspace1 and keyspace2
	MustRemoveKeyspacesFromGroup(re, suite.server, ksconstant.DefaultKeyspaceGroupID,
		[]uint32{keyspaceID1, keyspaceID2})

	// Verify both keyspaces are removed
	kg = MustLoadKeyspaceGroupByID(re, suite.server, ksconstant.DefaultKeyspaceGroupID)
	re.NotContains(kg.Keyspaces, keyspaceID1)
	re.NotContains(kg.Keyspaces, keyspaceID2)
	re.Contains(kg.Keyspaces, keyspaceID3) // keyspace3 should still be there

	// Test 3: Mix valid and invalid keyspaces
	// Set keyspace3 to ARCHIVED
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID3, keyspacepb.KeyspaceState_DISABLED, 0)
	re.NoError(err)
	_, err = keyspaceManager.UpdateKeyspaceStateByID(keyspaceID3, keyspacepb.KeyspaceState_ARCHIVED, 0)
	re.NoError(err)

	// Include: valid (keyspace3), already removed (keyspace1), non-existent (99999)
	// Should only remove keyspace3, others are skipped
	MustRemoveKeyspacesFromGroup(re, suite.server, ksconstant.DefaultKeyspaceGroupID,
		[]uint32{keyspaceID3, keyspaceID1, 99999})

	// Verify only keyspace3 is removed
	kg = MustLoadKeyspaceGroupByID(re, suite.server, ksconstant.DefaultKeyspaceGroupID)
	re.NotContains(kg.Keyspaces, keyspaceID3)

	// Test 4: Try to remove from non-existent group
	FailRemoveKeyspacesFromGroupWithCode(re, suite.server, 999,
		[]uint32{keyspaceID1}, http.StatusInternalServerError)

	// Test 5: Try to remove with empty keyspace list (should fail - empty list)
	FailRemoveKeyspacesFromGroupWithCode(re, suite.server, ksconstant.DefaultKeyspaceGroupID,
		[]uint32{}, http.StatusBadRequest)

	// Test 6: All keyspaces in wrong state (should succeed but nothing removed)
	keyspaceMeta4, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "test_keyspace_4",
		CreateTime: 0,
	})
	re.NoError(err)
	keyspaceID4 := keyspaceMeta4.GetId()

	kg = MustRemoveKeyspacesFromGroup(re, suite.server, ksconstant.DefaultKeyspaceGroupID,
		[]uint32{keyspaceID4}) // ENABLED state, will be skipped
	// Verify keyspace4 is still there
	re.Contains(kg.Keyspaces, keyspaceID4)
}
