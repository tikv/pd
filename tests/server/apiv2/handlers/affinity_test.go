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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
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
		conf.Schedule.AffinityScheduleLimit = 4
	})
	// Exercise the full affinity handler matrix through the local scheduling
	// path. TestAffinityForwardedHeader covers the microservice forwarding path.
	suite.env.Env = tests.NonMicroserviceEnv
}

func (suite *affinityHandlerTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *affinityHandlerTestSuite) TestAffinityAuditActions() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		re.Equal("?", logutil.RedactString("private-group"))
		logFile := testutil.InitTempFileLogger(suite.T(), "info")
		mustCreateAffinityGroups(re, leader.GetAddr(), &handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"private-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x05})}}},
			},
		})
		mustBatchDeleteAffinityGroups(re, leader.GetAddr(), &handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{"private-group"}, Force: true})
		data, err := os.ReadFile(logFile)
		re.NoError(err)
		re.Equal(1, strings.Count(string(data), "ServiceLabel:CreateAffinityGroups,"))
		re.Equal(1, strings.Count(string(data), "ServiceLabel:DeleteAffinityGroups,"))
		for line := range strings.SplitSeq(string(data), "\n") {
			if strings.Contains(line, "audit log") {
				re.NotContains(line, "private-group")
			}
		}
	})
}

func (suite *affinityHandlerTestSuite) TestNextGenTiDBGroupIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		manager, err := leader.GetServer().GetAffinityManager()
		re.NoError(err)
		const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
		re.NoError(failpoint.Enable(modeFlag, "return(false)"))
		legacy := []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_tg_100"}
		defer func() {
			re.NoError(manager.DeleteAffinityGroups(legacy, true))
			re.NoError(failpoint.Disable(modeFlag))
		}()
		for _, id := range legacy {
			re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: id}}))
		}
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		url := getAffinityGroupURL(leader.GetAddr())
		for _, id := range legacy {
			for _, req := range []struct{ method, path, body string }{
				{http.MethodPost, "", fmt.Sprintf(`{"affinity_groups":{"%s":{}}}`, id)},
				{http.MethodPost, "?skip_exist_check=true", fmt.Sprintf(`{"affinity_groups":{"%s":{}}}`, id)},
				{http.MethodPatch, "", fmt.Sprintf(`{"add":[{"id":"%s","ranges":[]}]}`, id)},
				{http.MethodPut, "/" + id, `{"leader_store_id":1,"voter_store_ids":[1]}`},
			} {
				httpReq, err := http.NewRequest(req.method, url+req.path, strings.NewReader(req.body))
				re.NoError(err)
				httpReq.Header.Set("Content-Type", "application/json")
				resp, err := tests.TestDialClient.Do(httpReq)
				re.NoError(err)
				body, err := io.ReadAll(resp.Body)
				re.NoError(err)
				re.NoError(resp.Body.Close())
				re.Equal(http.StatusBadRequest, resp.StatusCode, "%s %s: %s", req.method, req.path, body)
				re.Contains(string(body), "pd-ctl --keyspace-id")
				re.Len(manager.GetAllAffinityGroupStates(), len(legacy))
				re.True(manager.IsGroupExist(id))
			}
		}
		for i, id := range legacy {
			re.Equal(id, mustGetAffinityGroup(re, leader.GetAddr(), id).ID)
			filtered, _, ok := tryGetAffinityGroups(url + "?ids=" + id)
			re.True(ok)
			re.Contains(filtered.AffinityGroups, id)
			switch i {
			case 0:
				mustDeleteAffinityGroup(re, leader.GetAddr(), id, true)
			case 1:
				mustBatchDeleteAffinityGroups(re, leader.GetAddr(), &handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{id}, Force: true})
			default:
				// Empty legacy groups also support ordinary deletion in NextGen.
				mustDeleteAffinityGroup(re, leader.GetAddr(), id, false)
			}
			re.False(manager.IsGroupExist(id))
		}
		// A scoped table group uses the keyspace encoded in its ID.
		for _, id := range []string{"_tidb_ks42_t_1001", "_tidb_ks42_tg_100"} {
			lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 42)
			upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 43)
			ranges := []handlers.AffinityKeyRange{{StartKey: lower[:], EndKey: upper[:]}}
			mustCreateAffinityGroups(re, leader.GetAddr(), &handlers.CreateAffinityGroupsRequest{
				AffinityGroups: map[string]handlers.CreateAffinityGroupInput{id: {Ranges: ranges}},
			})
			outside := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 44)
			status, message := doBatchModifyAffinityGroups(re, leader.GetAddr(), &handlers.BatchModifyAffinityGroupsRequest{
				Add: []handlers.GroupRangesModification{{ID: id, Ranges: []handlers.AffinityKeyRange{{StartKey: upper[:], EndKey: outside[:]}}}},
			})
			re.Equal(http.StatusBadRequest, status)
			re.Contains(message, "transaction keyspace 42")
			re.Equal(1, manager.GetAffinityGroupState(id).RangeCount)
			mustDeleteAffinityGroup(re, leader.GetAddr(), id, true)
		}
	})
}

func (suite *affinityHandlerTestSuite) TestRemoveLegacyHalfUnboundedRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		manager, err := leader.GetServer().GetAffinityManager()
		re.NoError(err)
		key := func(table int64) []byte {
			return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), codec.GenerateTableKey(table)...))
		}
		const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
		defer func() { re.NoError(failpoint.Disable(modeFlag)) }()
		for i, legacy := range []keyutil.KeyRange{{EndKey: key(100)}, {StartKey: key(300)}} {
			id := fmt.Sprintf("legacy-half-%d", i)
			valid := keyutil.KeyRange{StartKey: key(200), EndKey: key(201)}
			re.NoError(failpoint.Enable(modeFlag, "return(false)"))
			re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{legacy, valid}}}))
			re.NoError(failpoint.Disable(modeFlag))
			before := manager.GetAffinityGroupState(id)
			ranges := []handlers.AffinityKeyRange{{StartKey: legacy.StartKey, EndKey: legacy.EndKey}}
			status, _ := doBatchModifyAffinityGroups(re, leader.GetAddr(), &handlers.BatchModifyAffinityGroupsRequest{
				Add: []handlers.GroupRangesModification{{ID: id, Ranges: ranges}},
			})
			re.Equal(http.StatusBadRequest, status)
			resp := mustBatchModifyAffinityGroups(re, leader.GetAddr(), &handlers.BatchModifyAffinityGroupsRequest{
				Remove: []handlers.GroupRangesModification{{ID: id, Ranges: ranges}},
			})
			re.Equal(1, resp.AffinityGroups[id].RangeCount)
			re.Equal(before.CreateTimestamp, resp.AffinityGroups[id].CreateTimestamp)
			re.NoError(manager.DeleteAffinityGroups([]string{id}, true))
		}
	})
}

func (suite *affinityHandlerTestSuite) TearDownTest() {
	// Clean up any remaining affinity groups after each test to avoid interference between tests.
	suite.env.RunFuncInStartedEnvs(func(cluster *tests.TestCluster) {
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

func affinityTestKey(raw []byte) []byte {
	if kerneltype.IsNextGen() && len(raw) > 0 {
		return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), raw...))
	}
	return raw
}

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

func tryGetAffinityGroups(url string) (handlers.AffinityGroupsResponse, http.Header, bool) {
	var result handlers.AffinityGroupsResponse
	resp, err := apiutil.GetJSON(tests.TestDialClient, url, nil)
	if err != nil {
		return result, nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, resp.Header, false
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, resp.Header, false
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, resp.Header, false
	}
	return result, resp.Header, true
}

func waitForAffinityGroups(re *require.Assertions, url string, groupIDs ...string) {
	var result handlers.AffinityGroupsResponse
	testutil.Eventually(re, func() bool {
		var ok bool
		result, _, ok = tryGetAffinityGroups(url)
		if !ok || len(result.AffinityGroups) != len(groupIDs) {
			return false
		}
		for _, groupID := range groupIDs {
			if _, ok := result.AffinityGroups[groupID]; !ok {
				return false
			}
		}
		return true
	})
}

func mustPutHealthyStore(re *require.Assertions, cluster *tests.TestCluster, store *metapb.Store) {
	tests.MustPutStore(re, cluster, store)

	leader := cluster.GetLeaderServer()
	re.NotNil(leader)
	raftCluster := leader.GetServer().GetRaftCluster()
	re.NotNil(raftCluster)
	storeInfo := raftCluster.GetStore(store.GetId())
	re.NotNil(storeInfo)

	stats := &pdpb.StoreStats{
		Capacity:  uint64(10 * units.GiB),
		UsedSize:  uint64(1 * units.GiB),
		Available: uint64(9 * units.GiB),
	}
	newStore := storeInfo.Clone(core.SetStoreStats(stats))
	raftCluster.GetBasicCluster().PutStore(newStore)
	raftCluster.UpdateAllStoreStatus()

	sche := cluster.GetSchedulingPrimaryServer()
	if sche != nil {
		sche.GetCluster().PutStore(newStore)
		sche.GetCluster().UpdateAllStoreStatus()
	}
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
	err = testutil.CheckPostJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr, "?delete"), data, testutil.StatusOK(re))
	re.NoError(err)
}

// doBatchDeleteAffinityGroups batch deletes affinity groups and returns status code and error message.
func doBatchDeleteAffinityGroups(re *require.Assertions, serverAddr string, req *handlers.BatchDeleteAffinityGroupsRequest) (int, string) {
	data, err := json.Marshal(req)
	re.NoError(err)
	resp, err := tests.TestDialClient.Post(getAffinityGroupURL(serverAddr, "?delete"), "application/json", bytes.NewReader(data))
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
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x20})}}},
			},
		}
		createResp := mustCreateAffinityGroups(re, serverAddr, &createReq)
		re.Len(createResp.AffinityGroups, 2)
		re.Equal(1, createResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, createResp.AffinityGroups["group-2"].RangeCount)

		// Creating an overlapping group should be rejected.
		overlapReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-overlap": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x05}), EndKey: affinityTestKey([]byte{0x0f})}}},
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
		mustPutHealthyStore(re, cluster, &metapb.Store{Id: 1, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})
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
				{ID: "group-1", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
			},
			Remove: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x20})}}},
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

		// Create a group without peer placement.
		kr := handlers.AffinityKeyRange{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"first-win": {Ranges: []handlers.AffinityKeyRange{kr}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		group := manager.GetAffinityGroupState("first-win")
		re.NotNil(group)
		re.Equal(affinity.PhasePending, group.Phase)
		re.Equal(uint64(0), group.LeaderStoreID)

		// Add a store to the cluster.
		mustPutHealthyStore(re, cluster, &metapb.Store{Id: 1, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})

		// Fake a healthy region that matches store 1.
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: kr.StartKey,
				EndKey:   kr.EndKey,
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
			// we need to call ObserveAvailableRegion and GetAndCacheRegionAffinityGroupState to update the group
			manager.ObserveAvailableRegion(region, group)
			state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
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
				"remove-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x02})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Remove the only range.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "remove-only", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x02})}}},
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

func (suite *affinityHandlerTestSuite) TestAffinityBatchModifySameGroupInAddAndRemove() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"patch-success": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x05})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x05})}}},
			},
			Add: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x20})}}},
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
				"mismatch": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x00}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				"bad id": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x00}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		statusCode, _ = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)

		// Create a valid group for follow-up checks.
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"ok": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x00}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Duplicate creation should fail.
		statusCode, _ = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusConflict, statusCode)

		// Get non-existent group.
		statusCode, _ = doGetAffinityGroup(re, serverAddr, "nope")
		re.Equal(http.StatusNotFound, statusCode)

		// Make sure the default store is healthy so UpdateAffinityGroupPeers reaches the
		// "group not found" branch instead of failing on store availability.
		mustPutHealthyStore(re, cluster, &metapb.Store{Id: 1, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})

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
			Add: []handlers.GroupRangesModification{{ID: "ghost", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x20})}}}},
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
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to create the same group again, should get clear error message.
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusConflict, statusCode)

		// Verify error message is not empty and contains useful information.
		re.NotEmpty(errorMsg, "Error message should not be empty")
		re.Contains(errorMsg, "test-group", "Error message should contain the group ID")
		re.Contains(errorMsg, "already exists", "Error message should indicate the group already exists")
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityGroupCreateDuplicateJSONMembers() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		url := getAffinityGroupURL(leader.GetAddr())
		input := func(start, end string) []byte {
			data, err := json.Marshal(handlers.CreateAffinityGroupInput{
				Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte(start)), EndKey: affinityTestKey([]byte(end))}},
			})
			re.NoError(err)
			return data
		}
		first, second := input("a", "b"), input("c", "d")
		start := base64.StdEncoding.EncodeToString(affinityTestKey([]byte("a")))
		end := base64.StdEncoding.EncodeToString(affinityTestKey([]byte("b")))
		duplicateBound := fmt.Appendf(nil, `{"affinity_groups":{"g1":{"ranges":[{"start_key":%q,"start_key":%q,"end_key":%q}]}}}`, start, start, end)
		for _, tc := range []struct {
			name string
			body []byte
		}{
			{"affinity_groups", fmt.Appendf(nil, `{"affinity_groups":{"g1":%s},"affinity_groups":{"g2":%s}}`, first, second)},
			{"g1", fmt.Appendf(nil, `{"affinity_groups":{"g1":%s,"g2":%s,"g1":%s}}`, first, second, first)},
			{"start_key", duplicateBound},
		} {
			for _, suffix := range []string{"", "?skip_exist_check=true"} {
				var message string
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, url+suffix, tc.body,
					testutil.Status(re, http.StatusBadRequest), testutil.ExtractJSON(re, &message)))
				re.Contains(message, "ErrAffinityGroupContent")
				re.Contains(message, tc.name)
			}
		}
		re.Empty(mustGetAllAffinityGroups(re, leader.GetAddr()).AffinityGroups)
		re.NoError(leader.GetServer().GetStorage().LoadAllAffinityGroups(func(id, _ string) {
			suite.T().Errorf("rejected request stored affinity group %s", id)
		}))

		var created handlers.AffinityGroupsResponse
		body := fmt.Appendf(nil, `{"affinity_groups":{"g1":%s,"G1":%s}}`, first, second)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, url, body,
			testutil.Status(re, http.StatusOK), testutil.ExtractJSON(re, &created)))
		re.Len(created.AffinityGroups, 2)
		re.Contains(created.AffinityGroups, "g1")
		re.Contains(created.AffinityGroups, "G1")
	})
}

// TestAffinityGroupCreateSkipExistCheck verifies skip_exist_check ignores existing groups.
func (suite *affinityHandlerTestSuite) TestAffinityGroupCreateSkipExistCheck() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		// Create a baseline group.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"existing": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Create another group while including the existing one.
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"existing": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				"new":      {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		var result handlers.AffinityGroupsResponse
		err = testutil.CheckPostJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr)+"?skip_exist_check=true", data,
			testutil.StatusOK(re), testutil.ExtractJSON(re, &result))
		re.NoError(err)
		re.Contains(result.AffinityGroups, "existing")
		re.Contains(result.AffinityGroups, "new")

		waitForAffinityGroups(re, getAffinityGroupURL(serverAddr), "existing", "new")
	})
}

// TestAffinityGroupCreateSkipExistCheckInvalidBool verifies invalid bool value is rejected.
func (suite *affinityHandlerTestSuite) TestAffinityGroupCreateSkipExistCheckInvalidBool() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		err = testutil.CheckPostJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr)+"?skip_exist_check=not-bool", data,
			testutil.Status(re, http.StatusBadRequest))
		re.NoError(err)
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
				"invalid-range": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x01})}}},
			},
		}
		statusCode, errorMsg := doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "start_key must be less than end_key")

		// Test StartKey == EndKey
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"equal-keys": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x01})}}},
			},
		}
		statusCode, errorMsg = doCreateAffinityGroups(re, serverAddr, &createReq)
		re.Equal(http.StatusBadRequest, statusCode)
		re.Contains(errorMsg, "start_key must be less than end_key")
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityOpenEndedKeyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		serverAddr := cluster.GetLeaderServer().GetAddr()
		key := affinityTestKey([]byte{0x61})

		for _, tc := range []struct {
			groupID  string
			keyRange handlers.AffinityKeyRange
		}{
			{"only-start", handlers.AffinityKeyRange{StartKey: key}},
			{"only-end", handlers.AffinityKeyRange{EndKey: key}},
			{"whole", handlers.AffinityKeyRange{}},
		} {
			status, message := doCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{
				AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
					tc.groupID: {Ranges: []handlers.AffinityKeyRange{tc.keyRange}},
				},
			})
			if kerneltype.IsNextGen() {
				re.Equal(http.StatusBadRequest, status)
				if tc.groupID == "whole" {
					re.Contains(message, "transaction keyspace")
				} else {
					re.Contains(message, "key range must have both start_key and end_key")
				}
				continue
			}
			re.Equal(http.StatusOK, status, message)
			mustDeleteAffinityGroup(re, serverAddr, tc.groupID, true)
		}
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
					tc.groupID: {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				},
			}
			statusCode, _ := doCreateAffinityGroups(re, serverAddr, &createReq)
			re.Equal(http.StatusBadRequest, statusCode, "Test case: %s", tc.name)
		}

		// Test 64 characters (boundary, should succeed)
		validLongID := "a12345678901234567890123456789012345678901234567890123456789012"
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				validLongID: {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				"test-dup": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				"force-test": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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

// TestAffinityListFilters verifies that a prefix selects groups and can be combined with IDs.
func (suite *affinityHandlerTestSuite) TestAffinityListFilters() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		serverAddr := cluster.GetLeaderServer().GetAddr()
		mustCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"prefix-1":  {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				"prefix-10": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
				"other-1":   {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x40}), EndKey: affinityTestKey([]byte{0x50})}}},
			},
		})
		url := getAffinityGroupURL(serverAddr)
		waitForAffinityGroups(re, url+"?ids=prefix-1&ids=other-1&ids=missing", "prefix-1", "other-1")
		waitForAffinityGroups(re, url+"?prefix=prefix-", "prefix-1", "prefix-10")
		waitForAffinityGroups(re, url+"?prefix=prefix-&ids=prefix-10&ids=other-1", "prefix-10")
		resp, err := apiutil.GetJSON(tests.TestDialClient, url+"?prefix=bad%20prefix", nil)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
	})
}

// TestAffinityForceDeleteLeftoverLabelRule verifies that a forced delete removes
// a label rule whose group no longer exists, through both delete APIs.
func (suite *affinityHandlerTestSuite) TestAffinityForceDeleteLeftoverLabelRule() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()
		regionLabeler := leader.GetRaftCluster().GetRegionLabeler()
		storedRuleIDs := func() []string {
			var ids []string
			re.NoError(leader.GetServer().GetStorage().LoadRegionRules(func(k, _ string) {
				ids = append(ids, k)
			}))
			return ids
		}
		// A rule left without its group, as a creation request that failed after
		// committing the transaction with the label rules leaves it.
		putLeftover := func(groupID string, start, end byte) {
			re.NoError(regionLabeler.SetLabelRule(affinity.MakeLabelRule(affinity.GroupKeyRanges{
				GroupID:   groupID,
				KeyRanges: []keyutil.KeyRange{{StartKey: affinityTestKey([]byte{start}), EndKey: affinityTestKey([]byte{end})}},
			})))
			re.Contains(storedRuleIDs(), affinity.GetLabelRuleID(groupID))
		}

		for _, id := range []string{"leftover-single", "_tidb_t_9999"} {
			putLeftover(id, 0x01, 0x02)
			status, message := doDeleteAffinityGroup(re, serverAddr, id, false)
			re.Equal(http.StatusNotFound, status, message)
			ruleID := affinity.GetLabelRuleID(id)
			re.NotNil(regionLabeler.GetLabelRule(ruleID))
			for range 2 {
				if id == "leftover-single" {
					status, message = doDeleteAffinityGroup(re, serverAddr, id, true)
				} else {
					status, message = doBatchDeleteAffinityGroups(re, serverAddr,
						&handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{id}, Force: true})
				}
				re.Equal(http.StatusOK, status, message)
				re.Nil(regionLabeler.GetLabelRule(ruleID))
				re.NotContains(storedRuleIDs(), ruleID)
			}
		}
	})
}

// TestAffinityListWithInvalidIDs verifies invalid ID values are rejected.
func (suite *affinityHandlerTestSuite) TestAffinityListWithInvalidIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		err := testutil.CheckGetJSON(tests.TestDialClient, getAffinityGroupURL(serverAddr)+"?ids=group-1&ids=bad$id", nil,
			testutil.Status(re, http.StatusBadRequest))
		re.NoError(err)
	})
}

// TestAffinityListWithEmptyID verifies empty ID entries are ignored.
func (suite *affinityHandlerTestSuite) TestAffinityListWithEmptyID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		var result handlers.AffinityGroupsResponse
		testutil.Eventually(re, func() bool {
			result = handlers.AffinityGroupsResponse{}
			err := testutil.ReadGetJSON(re, tests.TestDialClient, getAffinityGroupURL(serverAddr)+"?ids=&ids=group-2", &result)
			re.NoError(err)
			_, ok := result.AffinityGroups["group-2"]
			return len(result.AffinityGroups) == 1 && ok
		})
		re.Contains(result.AffinityGroups, "group-2")
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
				"valid-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				{ID: "bad@id", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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

func (suite *affinityHandlerTestSuite) TestBatchModifyRequestOrder() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		manager, err := leader.GetServer().GetAffinityManager()
		re.NoError(err)
		addr := leader.GetAddr()
		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "z"}, {GroupID: "a"}}))
		op := func(id, start, end string) handlers.GroupRangesModification {
			return handlers.GroupRangesModification{ID: id, Ranges: []handlers.AffinityKeyRange{{
				StartKey: affinityTestKey([]byte(start)), EndKey: affinityTestKey([]byte(end)),
			}}}
		}
		ranges := []handlers.GroupRangesModification{op("z", "a", "b"), op("a", "c", "d"), op("z", "e", "f")}
		added := mustBatchModifyAffinityGroups(re, addr, &handlers.BatchModifyAffinityGroupsRequest{Add: ranges})
		re.Equal(2, added.AffinityGroups["z"].RangeCount)
		re.Equal(1, added.AffinityGroups["a"].RangeCount)
		for _, ids := range [][2]string{{"z", "a"}, {"a", "z"}} {
			remove := []handlers.GroupRangesModification{op(ids[0], "g", "h"), op(ids[1], "i", "j")}
			for range 8 {
				status, message := doBatchModifyAffinityGroups(re, addr, &handlers.BatchModifyAffinityGroupsRequest{Remove: remove})
				re.Equal(http.StatusNotFound, status)
				re.Contains(message, "affinity group "+ids[0]+" not found")
			}
		}
		removed := mustBatchModifyAffinityGroups(re, addr, &handlers.BatchModifyAffinityGroupsRequest{Remove: ranges})
		re.Zero(removed.AffinityGroups["z"].RangeCount)
		re.Zero(removed.AffinityGroups["a"].RangeCount)
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
				"force-delete": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
		testutil.Eventually(re, func() bool {
			statusCode, _ = doGetAffinityGroup(re, serverAddr, "force-delete")
			return statusCode == http.StatusNotFound
		})
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
				"baseline": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to remove ranges from a non-existent group
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "non-existent-group", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x10}), EndKey: affinityTestKey([]byte{0x20})}}},
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
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to add overlapping range to group-2 (overlaps with group-1's range)
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x05}), EndKey: affinityTestKey([]byte{0x15})}}},
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
				"test-peers": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				"existing": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
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
				id: {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x01}), EndKey: affinityTestKey([]byte{0x10})}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Try to remove a range that the group doesn't contain
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: id, Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte{0x20}), EndKey: affinityTestKey([]byte{0x30})}}},
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

func (suite *affinityHandlerTestSuite) TestNextGenTiDBGroupIDQueriesMCS() {
	suite.env.RunTestInMicroserviceEnv(func(cluster *tests.TestCluster) {
		re := suite.Require()
		const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		defer func() { re.NoError(failpoint.Disable(modeFlag)) }()

		urls := []string{
			cluster.GetSchedulingPrimaryServer().GetAddr() + "/scheduling/api/v1/affinity-groups",
			getAffinityGroupURL(cluster.GetLeaderServer().GetAddr()),
		}
		for _, url := range urls {
			for _, id := range []string{"_tidb_t_123", "_tidb_pt_123_p456", "_tidb_ks42_t_123"} {
				re.NoError(testutil.CheckGetJSON(tests.TestDialClient, url+"/"+id, nil, testutil.Status(re, http.StatusNotFound)))
				var result handlers.AffinityGroupsResponse
				re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, url+"?ids="+id, &result))
				re.Empty(result.AffinityGroups)
			}
		}
	})
}

// TestAffinityForwardedHeader verifies scheduling responses and audit metrics on forwarded reads.
func (suite *affinityHandlerTestSuite) TestAffinityForwardedHeader() {
	suite.env.RunTestInMicroserviceEnv(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		initialAudit := leader.GetServer().GetAuditConfig()
		defer func() { re.NoError(leader.GetServer().SetAuditConfig(*initialAudit)) }()
		serverAddr := leader.GetAddr()
		mustCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"forwarded-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte("a")), EndKey: affinityTestKey([]byte("b"))}}},
				"other":       {Ranges: []handlers.AffinityKeyRange{{StartKey: affinityTestKey([]byte("c")), EndKey: affinityTestKey([]byte("d"))}}},
			},
		})
		for _, tc := range []struct {
			query string
			count int
		}{{"", 2}, {"?prefix=forwarded-", 1}} {
			testutil.Eventually(re, func() bool {
				result, header, ok := tryGetAffinityGroups(getAffinityGroupURL(serverAddr) + tc.query)
				return ok && header.Get(apiutil.XForwardedToMicroserviceHeader) == "true" &&
					len(result.AffinityGroups) == tc.count && result.AffinityGroups["forwarded-1"] != nil
			})
		}
		for _, enabled := range []bool{false, true, false} {
			re.NoError(leader.GetServer().SetAuditConfig(config.AuditConfig{EnableAudit: enabled}))
			for _, tc := range []struct{ path, service string }{
				{getAffinityGroupURL(serverAddr), "GetAllAffinityGroups"},
				{getAffinityGroupURL(serverAddr, "forwarded-1"), "GetAffinityGroup"},
			} {
				before := auditMetricCounts(re)
				response, err := tests.TestDialClient.Get(tc.path)
				re.NoError(err)
				re.NoError(response.Body.Close())
				re.Equal(http.StatusOK, response.StatusCode)
				re.Equal("true", response.Header.Get(apiutil.XForwardedToMicroserviceHeader))
				after := auditMetricCounts(re)
				increment := float64(0)
				if enabled {
					increment = 1
				}
				for _, family := range []string{"pd_service_audit_requests_total", "pd_service_audit_handling_seconds"} {
					key := family + ":" + tc.service
					re.Equal(increment, after[key]-before[key], key)
				}
			}
		}
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityGroupTargets() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		serverAddr := cluster.GetLeaderServer().GetAddr()
		keyRange := testutil.TxnKeyspaceRange([]byte{0x20}, []byte{0x30})
		created := mustCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"targets": {Ranges: []handlers.AffinityKeyRange{handlers.AffinityKeyRange(keyRange)}},
			},
		})
		for id := uint64(1); id <= 3; id++ {
			mustPutHealthyStore(re, cluster, &metapb.Store{Id: id, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})
		}
		data, err := json.Marshal(affinity.GroupTargetsRequest{Targets: []affinity.GroupTarget{
			{Expected: created.AffinityGroups["targets"].Group, LeaderStoreID: 2, VoterStoreIDs: []uint64{3, 2, 1}},
		}})
		re.NoError(err)
		var result affinity.GroupTargetsResponse
		endpoint := getAffinityGroupURL(serverAddr, "targets")
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, endpoint, data, testutil.StatusOK(re), testutil.ExtractJSON(re, &result)))
		re.Equal([]string{"targets"}, result.WrittenGroupIDs)
		want := created.AffinityGroups["targets"].Group
		want.LeaderStoreID, want.VoterStoreIDs = 2, []uint64{1, 2, 3}
		re.Equal(want, mustGetAffinityGroup(re, serverAddr, "targets").Group)

		const scopedID = "_tidb_ks42_t_1001"
		lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 42)
		upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 43)
		scoped := mustCreateAffinityGroups(re, serverAddr, &handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				scopedID: {Ranges: []handlers.AffinityKeyRange{{StartKey: lower[:], EndKey: upper[:]}}},
			},
		})
		expected := scoped.AffinityGroups[scopedID].Group
		re.Equal(scopedID, expected.ID)
		data, err = json.Marshal(affinity.GroupTargetsRequest{Targets: []affinity.GroupTarget{
			{Expected: expected, LeaderStoreID: 3, VoterStoreIDs: []uint64{3, 2, 1}},
		}})
		re.NoError(err)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, endpoint, data, testutil.StatusOK(re), testutil.ExtractJSON(re, &result)))
		re.Equal([]string{scopedID}, result.WrittenGroupIDs)
		expected.LeaderStoreID, expected.VoterStoreIDs = 3, []uint64{1, 2, 3}
		value, err := cluster.GetLeaderServer().GetServer().GetStorage().LoadAffinityGroup(scopedID)
		re.NoError(err)
		var persisted affinity.Group
		re.NoError(json.Unmarshal([]byte(value), &persisted))
		re.Equal(expected, persisted)
		re.Equal(expected, mustGetAffinityGroup(re, serverAddr, scopedID).Group)
		re.Equal(want, mustGetAffinityGroup(re, serverAddr, "targets").Group)

		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, endpoint, []byte(`{}`), testutil.Status(re, http.StatusBadRequest)))
	})
}
