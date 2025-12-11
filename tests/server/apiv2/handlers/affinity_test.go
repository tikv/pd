// Copyright 2025 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type affinityHandlerTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestAffinityHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(affinityHandlerTestSuite))
}

func (suite *affinityHandlerTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, _ string) {
		conf.Schedule.EnableAffinityScheduling = true
	})
}

func (suite *affinityHandlerTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *affinityHandlerTestSuite) TearDownTest() {
	// Clean up any remaining affinity groups after each test to avoid interference between tests.
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		manager, err := leader.GetServer().GetAffinityManager()
		if err != nil {
			// If affinity manager is not available, skip cleanup
			// This can happen during test teardown
			return
		}

		allGroups := manager.GetAllAffinityGroupStates()
		groupIDs := make([]string, 0, len(allGroups))
		for _, group := range allGroups {
			groupIDs = append(groupIDs, group.ID)
		}

		if len(groupIDs) > 0 {
			err = manager.DeleteAffinityGroups(groupIDs, true)
			// Use NoError to ensure cleanup succeeds and catch state pollution issues
			re.NoError(err, "failed to cleanup affinity groups in TearDownTest")
		}

		testutil.Eventually(re, func() bool {
			listResp := mustGetAllAffinityGroups(re, leader.GetAddr())
			return len(listResp.AffinityGroups) == 0
		})
	})
}

// Helper functions for affinity group API calls

func getAffinityGroupURL(serverAddr string, paths ...string) string {
	var builder strings.Builder
	builder.WriteString(serverAddr)
	builder.WriteString("/pd/api/v2/affinity-groups")
	for _, path := range paths {
		builder.WriteString("/")
		builder.WriteString(path)
	}
	return builder.String()
}

// mustCreateAffinityGroups creates affinity groups and expects success.
func mustCreateAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.CreateAffinityGroupsRequest) *handlers.AffinityGroupsResponse {
	data, err := json.Marshal(req)
	re.NoError(err)
	var result handlers.AffinityGroupsResponse
	err = testutil.CheckPostJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr), data,
		testutil.StatusOK(re), testutil.ExtractJSON(re, &result))
	re.NoError(err)
	return &result
}

// doCreateAffinityGroups creates affinity groups and returns status code and error message.
func doCreateAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.CreateAffinityGroupsRequest) (int, string) {
	data, err := json.Marshal(req)
	re.NoError(err)
	resp, err := tests.TestDialClient.Post(getAffinityGroupURL(serverAddr), "application/json", bytes.NewReader(data))
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		// Try to decode as JSON string, if fails, read as plain text
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

// mustGetAllAffinityGroups gets all affinity groups and expects success.
func mustGetAllAffinityGroups(re *require.Assertions, serverAddr string) *handlers.AffinityGroupsResponse {
	var result handlers.AffinityGroupsResponse
	err := testutil.ReadGetJSON(re, tests.TestDialClient, getAffinityGroupURL(serverAddr), &result)
	re.NoError(err)
	return &result
}

// mustGetAffinityGroup gets a specific affinity group and expects success.
func mustGetAffinityGroup(re *require.Assertions, serverAddr, groupID string) *affinity.GroupState {
	var result affinity.GroupState
	err := testutil.ReadGetJSON(re, tests.TestDialClient, getAffinityGroupURL(serverAddr, groupID), &result)
	re.NoError(err)
	return &result
}

// doGetAffinityGroup gets a specific affinity group and returns status code and error message.
func doGetAffinityGroup(re *require.Assertions, serverAddr, groupID string) (int, string) {
	resp, err := tests.TestDialClient.Get(getAffinityGroupURL(serverAddr, groupID))
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

// mustBatchModifyAffinityGroups batch modifies affinity groups and expects success.
func mustBatchModifyAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.BatchModifyAffinityGroupsRequest) *handlers.AffinityGroupsResponse {
	data, err := json.Marshal(req)
	re.NoError(err)
	var result handlers.AffinityGroupsResponse
	err = testutil.CheckPatchJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr), data,
		testutil.StatusOK(re), testutil.ExtractJSON(re, &result))
	re.NoError(err)
	return &result
}

// doBatchModifyAffinityGroups batch modifies affinity groups and returns status code and error message.
func doBatchModifyAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.BatchModifyAffinityGroupsRequest) (int, string) {
	data, err := json.Marshal(req)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPatch, getAffinityGroupURL(serverAddr), bytes.NewReader(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

// mustUpdateAffinityGroupPeers updates affinity group peers and expects success.
func mustUpdateAffinityGroupPeers(re *require.Assertions, serverAddr, groupID string, req *handlers.UpdateAffinityGroupPeersRequest) *affinity.GroupState {
	data, err := json.Marshal(req)
	re.NoError(err)
	var result affinity.GroupState
	err = testutil.CheckPutJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr, groupID), data,
		testutil.StatusOK(re), testutil.ExtractJSON(re, &result))
	re.NoError(err)
	return &result
}

// doUpdateAffinityGroupPeers updates affinity group peers and returns status code and error message.
func doUpdateAffinityGroupPeers(re *require.Assertions, serverAddr, groupID string, req *handlers.UpdateAffinityGroupPeersRequest) (int, string) {
	data, err := json.Marshal(req)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPut, getAffinityGroupURL(serverAddr, groupID), bytes.NewReader(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

// mustDeleteAffinityGroup deletes an affinity group and expects success.
func mustDeleteAffinityGroup(re *require.Assertions, serverAddr, groupID string, force bool) {
	url := getAffinityGroupURL(serverAddr, groupID)
	if force {
		url += "?force=true"
	}
	err := testutil.CheckDelete(tests.TestDialClient, url, testutil.StatusOK(re))
	re.NoError(err)
}

// doDeleteAffinityGroup deletes an affinity group and returns status code and error message.
func doDeleteAffinityGroup(re *require.Assertions, serverAddr, groupID string, force bool) (int, string) {
	url := getAffinityGroupURL(serverAddr, groupID)
	if force {
		url += "?force=true"
	}
	httpReq, err := http.NewRequest(http.MethodDelete, url, http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

// mustBatchDeleteAffinityGroups batch deletes affinity groups and expects success.
func mustBatchDeleteAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.BatchDeleteAffinityGroupsRequest) {
	data, err := json.Marshal(req)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr, "batch-delete"), data, testutil.StatusOK(re))
	re.NoError(err)
}

// doBatchDeleteAffinityGroups batch deletes affinity groups and returns status code and error message.
func doBatchDeleteAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.BatchDeleteAffinityGroupsRequest) (int, string) {
	data, err := json.Marshal(req)
	re.NoError(err)
	resp, err := tests.TestDialClient.Post(getAffinityGroupURL(serverAddr, "batch-delete"), "application/json", bytes.NewReader(data))
	re.NoError(err)
	defer resp.Body.Close()
	var errorMsg string
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if err := json.Unmarshal(body, &errorMsg); err != nil {
			errorMsg = string(body)
		}
	}
	return resp.StatusCode, errorMsg
}

func (suite *affinityHandlerTestSuite) TestAffinityGroupLifecycle() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create two non-overlapping groups.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		createResp := mustCreateAffinityGroups(re, serverAddr, &createReq)
		re.Len(createResp.AffinityGroups, 2)
		re.Equal(1, createResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, createResp.AffinityGroups["group-2"].RangeCount)

		// Creating an overlapping group should be rejected.
		overlapReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-overlap": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x05}, EndKey: []byte{0x0f}}}},
			},
		}
		statusCode, _ := doCreateAffinityGroups(re, serverAddr, &overlapReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Query all groups.
		testutil.Eventually(re, func() bool {
			listResp := mustGetAllAffinityGroups(re, serverAddr)
			return len(listResp.AffinityGroups) == 2
		})

		// Update peers for group-1.
		updatePeersReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		groupState := mustUpdateAffinityGroupPeers(re, serverAddr, "group-1", &updatePeersReq)
		re.Equal(affinity.PhasePreparing, groupState.Phase)
		re.Equal(updatePeersReq.LeaderStoreID, groupState.LeaderStoreID)
		re.ElementsMatch(updatePeersReq.VoterStoreIDs, groupState.VoterStoreIDs)

		// Batch modify ranges: add one to group-1 and remove the only one from group-2.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "group-1", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
			Remove: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		patchResp := mustBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Contains(patchResp.AffinityGroups, "group-1")
		re.Contains(patchResp.AffinityGroups, "group-2")

		// Delete with ranges should be blocked unless force=true.
		statusCode, _ = doDeleteAffinityGroup(re, serverAddr, "group-1", false)
		re.Equal(http.StatusBadRequest, statusCode)

		mustDeleteAffinityGroup(re, serverAddr, "group-1", true)

		// Batch delete the remaining empty group.
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{"group-2"}}
		mustBatchDeleteAffinityGroups(re, serverAddr, &batchDeleteReq)

		// Listing again should be empty.
		testutil.Eventually(re, func() bool {
			listResp := mustGetAllAffinityGroups(re, serverAddr)
			return len(listResp.AffinityGroups) == 0
		})
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityFirstRegionWins() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group without peer placement; range covers the default region.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"first-win": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		group := manager.GetAffinityGroupState("first-win")
		re.NotNil(group)
		re.Equal(affinity.PhasePending, group.Phase)
		re.Equal(uint64(0), group.LeaderStoreID)

		// Add a store to the cluster.
		tests.MustHandleStoreHeartbeat(re, cluster, &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Stats:  &pdpb.StoreStats{StoreId: 1, Capacity: 100 * units.GiB, Available: 100 * units.GiB},
		})

		// Fake a healthy region that matches store 1.
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: []byte(""),
				EndKey:   []byte("ffff"),
				Peers:    []*metapb.Peer{{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter}},
			},
			&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
		)

		// Fetch group via API to ensure effect and peers are set.
		// Retry until group schedule is observed.
		var state *affinity.GroupState
		testutil.Eventually(re, func() bool {
			group := manager.GetAffinityGroupState("first-win")
			if group == nil {
				return false
			}
			// we need to call ObserveAvailableRegion and GetRegionAffinityGroupState to update the group
			manager.ObserveAvailableRegion(region, group)
			state, _ = manager.GetRegionAffinityGroupState(region)
			return state != nil && state.Phase == affinity.PhaseStable
		})
		re.Equal(region.GetLeader().GetStoreId(), state.LeaderStoreID)
		re.ElementsMatch([]uint64{region.GetLeader().GetStoreId()}, state.VoterStoreIDs)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityRemoveOnlyPatch() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"remove-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Remove the only range.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "remove-only", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		mustBatchModifyAffinityGroups(re, serverAddr, &patchReq)

		// Verify range count cleared.
		testutil.Eventually(re, func() bool {
			state := mustGetAffinityGroup(re, serverAddr, "remove-only")
			return state.RangeCount == 0
		})
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityBatchModifySuccess() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"patch-success": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
			Add: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "patch-success")
		re.Contains(errorMsg, "cannot appear in both add and remove")
	})
}

func (suite *affinityHandlerTestSuite) TestUpdatePeersLeaderNotInVoters() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Prepare group.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"mismatch": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Leader is not in voters
		// Note: Using storeID 2 which doesn't exist in test environment.
		// AdjustGroup validates store existence before checking leader-in-voters,
		// so we expect "invalid affinity group content, leader must be in voter stores" error.
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{2},
		}
		statusCode, errorMsg := doUpdateAffinityGroupPeers(re, serverAddr, "mismatch", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)
		// Error comes from AdjustGroup - it checks store existence before leader-in-voters
		re.Contains(errorMsg, "invalid affinity group content, leader must be in voter stores")
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityHandlersErrors() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Empty payload should be rejected.
		statusCode, _ := doCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{})
		re.Equal(http.StatusBadRequest, statusCode)

		// Illegal group ID.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"bad id": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		statusCode, _ = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Create a valid group for follow-up checks.
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"ok": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Duplicate creation should fail.
		statusCode, _ = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Get non-existent group.
		statusCode, _ = doGetAffinityGroup(re, serverAddr, "nope")
		re.Equal(http.StatusNotFound, statusCode)

		// Make sure the default store is healthy so UpdateAffinityGroupPeers reaches the
		// "group not found" branch instead of failing on store availability.
		tests.MustHandleStoreHeartbeat(re, cluster, &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Stats:  &pdpb.StoreStats{StoreId: 1, Capacity: 100 * units.GiB, Available: 100 * units.GiB},
		})

		// Update peers for non-existent group.
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		statusCode, _ = doUpdateAffinityGroupPeers(re, serverAddr, "ghost", &updateReq)
		re.Equal(http.StatusNotFound, statusCode)

		// Update peers with non-existent store should fail.
		updateReq = handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 99,
			VoterStoreIDs: []uint64{99},
		}
		statusCode, _ = doUpdateAffinityGroupPeers(re, serverAddr, "ok", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Batch delete without IDs.
		emptyDelete := handlers.BatchDeleteAffinityGroupsRequest{}
		statusCode, _ = doBatchDeleteAffinityGroups(re, serverAddr, &emptyDelete)
		re.Equal(http.StatusBadRequest, statusCode)

		// Batch modify referencing non-existent group.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{{ID: "ghost", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}}},
		}
		statusCode, _ = doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusNotFound, statusCode)

		// Verify existing group "ok" is not affected (state not polluted)
		testutil.Eventually(re, func() bool {
			groupState := mustGetAffinityGroup(re, serverAddr, "ok")
			return groupState.RangeCount == 1
		})

		// Batch modify with empty operations.
		patchReq = handlers.BatchModifyAffinityGroupsRequest{}
		statusCode, _ = doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusBadRequest, statusCode)
	})
}

// TestAffinityGroupDuplicateErrorMessage verifies that duplicate group creation
// returns a clear error message.
func (suite *affinityHandlerTestSuite) TestAffinityGroupDuplicateErrorMessage() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group successfully.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to create the same group again, should get clear error message.
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Verify error message is not empty and contains useful information.
		re.NotEmpty(errorMsg, "Error message should not be empty")
		re.Contains(errorMsg, "test-group", "Error message should contain the group ID")
		re.Contains(errorMsg, "already exists", "Error message should indicate the group already exists")
	})
}

// TestAffinityInvalidKeyRanges tests various invalid key range scenarios.
func (suite *affinityHandlerTestSuite) TestAffinityInvalidKeyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Test StartKey > EndKey
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"invalid-range": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x01}}}},
			},
		}
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "start_key must be less than end_key")

		// Test StartKey == EndKey
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"equal-keys": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x01}}}},
			},
		}
		statusCode, errorMsg = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "start_key must be less than end_key")

		// Test only StartKey provided
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"only-start": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{}}}},
			},
		}
		statusCode, errorMsg = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "key range must have both start_key and end_key")

		// Test only EndKey provided
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"only-end": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{0x10}}}},
			},
		}
		statusCode, errorMsg = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "key range must have both start_key and end_key")
	})
}

// TestAffinityInvalidGroupIDs tests various invalid group ID scenarios.
func (suite *affinityHandlerTestSuite) TestAffinityInvalidGroupIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		testCases := []struct {
			name    string
			groupID string
		}{
			{"empty string", ""},
			{"space in id", "bad id"},
			{"special char @", "bad@id"},
			{"special char .", "bad.id"},
			{"65 characters", "a1234567890123456789012345678901234567890123456789012345678901234"},
		}

		for _, tc := range testCases {
			createReq := handlers.CreateAffinityGroupsRequest{
				AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
					tc.groupID: {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				},
			}
			statusCode, _ := doCreateAffinityGroups(re, serverAddr, &createReq)
			re.Equal(http.StatusBadRequest, statusCode, "Test case: %s", tc.name)
		}

		// Test 64 characters (boundary, should succeed)
		validLongID := "a12345678901234567890123456789012345678901234567890123456789012"
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				validLongID: {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)
	})
}

// TestAffinityUpdatePeersDuplicateStores tests duplicate store IDs in VoterStoreIDs.
func (suite *affinityHandlerTestSuite) TestAffinityUpdatePeersDuplicateStores() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group first
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-dup": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to update with duplicate store IDs
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 1, 2}, // duplicate 1
		}
		statusCode, errorMsg := doUpdateAffinityGroupPeers(re, serverAddr, "test-dup", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)
		// Error comes from AdjustGroup in manager layer
		re.Contains(errorMsg, "duplicate voter store ID")
	})
}

// TestAffinityForceParameterVariants tests different values for the force parameter.
func (suite *affinityHandlerTestSuite) TestAffinityForceParameterVariants() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group with ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"force-test": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Test force=false (should fail because group has ranges)
		statusCode, _ := doDeleteAffinityGroup(re, serverAddr, "force-test", false)
		re.Equal(http.StatusBadRequest, statusCode)

		// Test force=1 (should succeed with bool parsing)
		mustDeleteAffinityGroup(re, serverAddr, "force-test", true)
	})
}

// TestAffinityGetInvalidGroupID tests getting a group with invalid ID format.
func (suite *affinityHandlerTestSuite) TestAffinityGetInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Test getting group with invalid ID format
		statusCode, errorMsg := doGetAffinityGroup(re, serverAddr, "bad@id")
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestDeleteInvalidGroupID tests deleting a group with invalid ID format.
func (suite *affinityHandlerTestSuite) TestDeleteInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Test deleting group with invalid ID format
		statusCode, errorMsg := doDeleteAffinityGroup(re, serverAddr, "bad@id", false)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchDeleteWithInvalidIDs tests batch delete with invalid group IDs.
func (suite *affinityHandlerTestSuite) TestBatchDeleteWithInvalidIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a valid group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"valid-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to batch delete with one valid ID and one invalid ID
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs:   []string{"valid-group", "bad@id"},
			Force: true,
		}
		statusCode, errorMsg := doBatchDeleteAffinityGroups(re, serverAddr, &batchDeleteReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchModifyInvalidGroupID tests batch modify with invalid group ID.
func (suite *affinityHandlerTestSuite) TestBatchModifyInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Try to add ranges to a group with invalid ID
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "bad@id", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchModifyEmptyRanges tests batch modify with empty ranges array.
func (suite *affinityHandlerTestSuite) TestBatchModifyEmptyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a valid group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to add empty ranges array
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "test-group", Ranges: []handlers.AffinityKeyRange{}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "no key ranges provided")
	})
}

// TestBatchModifyAddOnly tests batch modify with only add operations.
func (suite *affinityHandlerTestSuite) TestBatchModifyAddOnly() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"add-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Add more ranges (no remove)
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "add-only", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		mustBatchModifyAffinityGroups(re, serverAddr, &patchReq)

		// Verify the group now has 2 ranges
		testutil.Eventually(re, func() bool {
			state := mustGetAffinityGroup(re, serverAddr, "add-only")
			return state.RangeCount == 2
		})
	})
}

// TestBatchDeleteWithForce tests batch delete with force parameter.
func (suite *affinityHandlerTestSuite) TestBatchDeleteWithForce() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group with ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"force-delete": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to batch delete without force (should fail)
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs:   []string{"force-delete"},
			Force: false,
		}
		statusCode, _ := doBatchDeleteAffinityGroups(re, serverAddr, &batchDeleteReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Try to batch delete with force (should succeed)
		batchDeleteReq.Force = true
		mustBatchDeleteAffinityGroups(re, serverAddr, &batchDeleteReq)

		// Verify the group is deleted
		statusCode, _ = doGetAffinityGroup(re, serverAddr, "force-delete")
		re.Equal(http.StatusNotFound, statusCode)
	})
}

// TestUpdatePeersInvalidGroupID tests updating peers with invalid group ID format in URL path.
func (suite *affinityHandlerTestSuite) TestUpdatePeersInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Try to update peers with invalid group ID format in URL path
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		statusCode, errorMsg := doUpdateAffinityGroupPeers(re, serverAddr, "bad@id", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchModifyRemoveNonExistentGroup tests removing ranges from a non-existent group.
func (suite *affinityHandlerTestSuite) TestBatchModifyRemoveNonExistentGroup() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a baseline group to verify it's not affected
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"baseline": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to remove ranges from a non-existent group
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "non-existent-group", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusNotFound, statusCode)
		re.Contains(errorMsg, "not found")

		// Verify baseline group is not affected (state not polluted)
		var listResp *handlers.AffinityGroupsResponse
		testutil.Eventually(re, func() bool {
			listResp = mustGetAllAffinityGroups(re, serverAddr)
			return len(listResp.AffinityGroups) == 1
		})
		re.Equal(1, listResp.AffinityGroups["baseline"].RangeCount)
	})
}

// TestAffinityCreateEmptyRanges tests creating a group with empty ranges array.
func (suite *affinityHandlerTestSuite) TestAffinityCreateEmptyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Try to create a group with empty ranges array
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"empty-ranges": {Ranges: []handlers.AffinityKeyRange{}},
			},
		}
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "no key ranges provided")
	})
}

// TestBatchModifyOverlappingRanges tests adding overlapping ranges via batch modify.
func (suite *affinityHandlerTestSuite) TestBatchModifyOverlappingRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create two groups with non-overlapping ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to add overlapping range to group-2 (overlaps with group-1's range)
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x05}, EndKey: []byte{0x15}}}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "overlap")

		// Verify system state not polluted: both groups still have only 1 range each
		var listResp *handlers.AffinityGroupsResponse
		testutil.Eventually(re, func() bool {
			listResp = mustGetAllAffinityGroups(re, serverAddr)
			g1, ok1 := listResp.AffinityGroups["group-1"]
			g2, ok2 := listResp.AffinityGroups["group-2"]
			return ok1 && ok2 && g1.RangeCount == 1 && g2.RangeCount == 1
		})
		re.Equal(1, listResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, listResp.AffinityGroups["group-2"].RangeCount)
	})
}

// TestUpdatePeersMissingRequiredFields tests updating peers with missing required fields.
func (suite *affinityHandlerTestSuite) TestUpdatePeersMissingRequiredFields() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group first
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-peers": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Test 1: LeaderStoreID = 0 (missing)
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 0,
			VoterStoreIDs: []uint64{1},
		}
		statusCode, errorMsg := doUpdateAffinityGroupPeers(re, serverAddr, "test-peers", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "required")

		// Test 2: VoterStoreIDs empty
		updateReq = handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{},
		}
		statusCode, errorMsg = doUpdateAffinityGroupPeers(re, serverAddr, "test-peers", &updateReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "required")
	})
}

// TestBatchDeleteNonExistentGroup tests batch deleting a non-existent group.
func (suite *affinityHandlerTestSuite) TestBatchDeleteNonExistentGroup() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a baseline group to verify it's not affected
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"existing": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to delete a non-existent group
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs: []string{"non-existent-group"},
		}
		statusCode, errorMsg := doBatchDeleteAffinityGroups(re, serverAddr, &batchDeleteReq)
		re.Equal(http.StatusNotFound, statusCode)
		re.Contains(errorMsg, "not found")

		// Verify baseline group is not affected (state not polluted)
		var listResp *handlers.AffinityGroupsResponse
		testutil.Eventually(re, func() bool {
			listResp = mustGetAllAffinityGroups(re, serverAddr)
			return len(listResp.AffinityGroups) == 1
		})
		re.Contains(listResp.AffinityGroups, "existing")
		re.Equal(1, listResp.AffinityGroups["existing"].RangeCount)
	})
}

// TestBatchModifyRemoveNonExistentRange tests removing a range that the group doesn't contain.
func (suite *affinityHandlerTestSuite) TestBatchModifyRemoveNonExistentRange() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		id := "test-group-batch-modify-remove-non-existent-range"
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group with a specific range
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				id: {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to remove a range that the group doesn't contain
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: id, Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		statusCode, errorMsg := doBatchModifyAffinityGroups(re, serverAddr, &patchReq)
		re.Equal(http.StatusNotFound, statusCode)
		re.Contains(errorMsg, "not found")

		// Verify the original range is still intact (state not polluted)
		testutil.Eventually(re, func() bool {
			state := mustGetAffinityGroup(re, serverAddr, id)
			return state.RangeCount == 1
		})
	})
}

// TestAffinityForwardedHeader verifies microservice forwarding sets the header.
func (suite *affinityHandlerTestSuite) TestAffinityForwardedHeader() {
	// TODO: remove this test when mcs support affinity
	suite.T().Skip("wait mcs support")
	suite.env.RunTestInMicroserviceEnv(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a group to ensure GET succeeds.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"header-check": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		resp, err := tests.TestDialClient.Get(getAffinityGroupURL(serverAddr))
		re.NoError(err)
		defer resp.Body.Close()

		re.Equal(http.StatusOK, resp.StatusCode)
		re.Equal("true", resp.Header.Get(apiutil.XForwardedToMicroserviceHeader))
	})
}

func TestAffinitySchedulingDisabled(t *testing.T) {
	re := require.New(t)
	env := tests.NewSchedulingTestEnvironment(t, func(conf *config.Config, _ string) {
		conf.Schedule.EnableAffinityScheduling = false
	})
	defer env.Cleanup()

	env.RunTest(func(cluster *tests.TestCluster) {
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"disabled": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusServiceUnavailable, statusCode)
		re.Contains(errorMsg, "affinity is disabled")
	})
}
