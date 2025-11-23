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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/server/apiv2/handlers"
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
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *affinityHandlerTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *affinityHandlerTestSuite) TestAffinityGroupLifecycle() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create two non-overlapping groups.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		var createResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(resp.Body).Decode(&createResp))
		re.Len(createResp.AffinityGroups, 2)
		re.Equal(1, createResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, createResp.AffinityGroups["group-2"].RangeCount)

		// Creating an overlapping group should be rejected.
		overlapReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-overlap": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x05}, EndKey: []byte{0x0f}}}},
			},
		}
		data, err = json.Marshal(overlapReq)
		re.NoError(err)
		res, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusBadRequest, res.StatusCode)

		// Query all groups.
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var listResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Len(listResp.AffinityGroups, 2)

		// Update peers for group-1.
		updatePeersReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		data, err = json.Marshal(updatePeersReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/group-1", bytes.NewReader(data))
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		groupState := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(groupState))
		re.True(groupState.Effect)
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
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var patchResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&patchResp))
		re.Contains(patchResp.AffinityGroups, "group-1")
		re.Contains(patchResp.AffinityGroups, "group-2")

		// Delete with ranges should be blocked unless force=true.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-1", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusBadRequest, res.StatusCode)

		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-1?force=true", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		// Batch delete the remaining empty group.
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{"group-2"}}
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		res, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		// Listing again should be empty.
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		listResp = handlers.AffinityGroupsResponse{}
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Empty(listResp.AffinityGroups)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityFirstRegionWins() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group without peer placement; range covers the default region.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"first-win": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		group := manager.GetAffinityGroupState("first-win")
		re.NotNil(group)
		re.False(group.Effect)
		re.Equal(uint64(0), group.LeaderStoreID)

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

		// Manually observe region; first available region should set the peer layout.
		manager.ObserveAvailableRegion(region, group)

		// Fetch group via API to ensure effect and peers are set.
		res, err := client.Get(baseURL + "/first-win")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		finalState := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(finalState))
		re.True(finalState.Effect)
		re.Equal(region.GetLeader().GetStoreId(), finalState.LeaderStoreID)
		re.ElementsMatch([]uint64{region.GetLeader().GetStoreId()}, finalState.VoterStoreIDs)

		// Cleanup to avoid overlaps for following cases.
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/first-win?force=true", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityRemoveOnlyPatch() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"remove-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Remove the only range.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "remove-only", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Verify range count cleared.
		res, err := client.Get(baseURL + "/remove-only")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		state := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(state))
		re.Equal(0, state.RangeCount)

		// Cleanup.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/remove-only?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityBatchModifySuccess() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"patch-success": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
			Add: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		var respBody handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(resp.Body).Decode(&respBody))
		state := respBody.AffinityGroups["patch-success"]
		re.NotNil(state)
		re.Equal(1, state.RangeCount)
		re.False(state.Effect) // peers未设置，保持未生效

		// Cleanup.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/patch-success?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestUpdatePeersLeaderNotInVoters() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Prepare group.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"mismatch": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Leader 不在 voters 中，预期 400。
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{2},
		}
		payload, err := json.Marshal(updateReq)
		re.NoError(err)
		req, err := http.NewRequest(http.MethodPut, baseURL+"/mismatch", bytes.NewReader(payload))
		re.NoError(err)
		resp, err = client.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Cleanup.
		req, err = http.NewRequest(http.MethodDelete, baseURL+"/mismatch?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityHandlersErrors() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Empty payload should be rejected.
		data, err := json.Marshal(handlers.CreateAffinityGroupsRequest{})
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Illegal group ID.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"bad id": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Create a valid group for follow-up checks.
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"ok": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Duplicate creation should fail.
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Get non-existent group.
		resp, err = client.Get(baseURL + "/nope")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)

		// Update peers for non-existent group.
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/ghost", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)

		// Update peers with non-existent store should fail.
		updateReq = handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 99,
			VoterStoreIDs: []uint64{99},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPut, baseURL+"/ok", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Batch delete without IDs.
		emptyDelete := handlers.BatchDeleteAffinityGroupsRequest{}
		data, err = json.Marshal(emptyDelete)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Batch modify referencing non-existent group.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{{ID: "ghost", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}}},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Batch modify with empty operations.
		patchReq = handlers.BatchModifyAffinityGroupsRequest{}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Force delete the created group.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/ok?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}
