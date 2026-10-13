// Copyright 2026 TiKV Project Authors.
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

package command

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/servicediscovery"
)

const (
	affinityGroupsPath = "/pd/api/v2/affinity-groups"
)

type affinityRebalanceTestServer struct {
	groups          map[string]*pdhttp.AffinityGroupState
	plans           []pdhttp.AffinityBalancePlan
	rebalanceIDs    []string
	rebalanceBodies []pdhttp.RebalanceAffinityGroupRequest
	responseStatus  int
}

func newAffinityRebalanceTestServer(t *testing.T, groups map[string]*pdhttp.AffinityGroupState, plans ...pdhttp.AffinityBalancePlan) (*affinityRebalanceTestServer, *httptest.Server) {
	t.Helper()

	testServer := &affinityRebalanceTestServer{groups: groups, plans: plans}
	server := httptest.NewServer(http.HandlerFunc(testServer.handle))
	t.Cleanup(server.Close)
	return testServer, server
}

func (s *affinityRebalanceTestServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == affinityGroupsPath:
		_ = json.NewEncoder(w).Encode(pdhttp.AffinityGroupsResponse{AffinityGroups: s.groups})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, affinityGroupsPath+"/") && strings.HasSuffix(r.URL.Path, "/rebalance"):
		groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, affinityGroupsPath+"/"), "/rebalance")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request pdhttp.RebalanceAffinityGroupRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.rebalanceIDs = append(s.rebalanceIDs, groupID)
		s.rebalanceBodies = append(s.rebalanceBodies, request)
		if s.responseStatus != 0 && s.responseStatus != http.StatusOK {
			w.WriteHeader(s.responseStatus)
			_, _ = io.WriteString(w, `{"error":"rebalance failed"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(s.plans)
	default:
		http.NotFound(w, r)
	}
}

func affinityGroup(id string, voters ...uint64) *pdhttp.AffinityGroupState {
	return &pdhttp.AffinityGroupState{
		AffinityGroup: pdhttp.AffinityGroup{
			ID:            id,
			LeaderStoreID: 1,
			VoterStoreIDs: voters,
		},
	}
}

func executeAffinityRebalance(t *testing.T, client pdhttp.Client, args ...string) string {
	t.Helper()
	oldClient := PDCli
	PDCli = client
	t.Cleanup(func() { PDCli = oldClient })

	cmd := NewAffinityCommand()
	cmd.PersistentPreRunE = nil
	cmd.SetArgs(append([]string{"rebalance"}, args...))
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	require.NoError(t, cmd.Execute())
	return output.String()
}

func newAffinityHTTPClient(t *testing.T, server *httptest.Server) pdhttp.Client {
	t.Helper()
	serviceDiscovery := servicediscovery.NewMockServiceDiscovery([]string{server.URL}, nil)
	require.NoError(t, serviceDiscovery.Init())
	t.Cleanup(serviceDiscovery.Close)
	client := pdhttp.NewClientWithServiceDiscovery("affinity-command-test", serviceDiscovery, pdhttp.WithHTTPClient(server.Client()))
	require.NotNil(t, client)
	t.Cleanup(client.Close)
	return client
}

func TestAffinityRebalancePreviewFiltersTableAndRequestsServerOnce(t *testing.T) {
	groups := map[string]*pdhttp.AffinityGroupState{
		"_tidb_t_42":      affinityGroup("_tidb_t_42", 1, 2, 3),
		"_tidb_pt_42_p1":  affinityGroup("_tidb_pt_42_p1", 1, 2, 3),
		"_tidb_t_99":      affinityGroup("_tidb_t_99", 1, 2, 3),
		"unrelated-group": affinityGroup("unrelated-group", 1, 2, 3),
	}
	plans := []pdhttp.AffinityBalancePlan{
		{GroupID: "_tidb_pt_42_p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true,
			OldLeaderStoreID: 3, OldVoterStoreIDs: []uint64{1, 2, 3},
			BeforeScore: &pdhttp.AffinityBalanceScore{Leader: 9, Voter: 27},
			AfterScore:  &pdhttp.AffinityBalanceScore{Leader: 5, Voter: 27}, MoveBytes: 123,
			Revision: "table-revision", Status: "planned"},
		{GroupID: "_tidb_t_42", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	testServer, server := newAffinityRebalanceTestServer(t, groups, plans...)
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42")
	var result []affinityRebalanceResult
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	require.Len(t, result, 2)
	require.Equal(t, []string{"_tidb_pt_42_p1", "_tidb_t_42"}, []string{result[0].GroupID, result[1].GroupID})
	require.Equal(t, []string{"_tidb_pt_42_p1"}, testServer.rebalanceIDs)
	require.Equal(t, []pdhttp.RebalanceAffinityGroupRequest{{Apply: false}}, testServer.rebalanceBodies)
	require.Equal(t, uint64(1), result[1].OldLeader)
	require.Equal(t, uint64(2), result[0].NewLeader)
	require.False(t, result[0].Applied)
	require.Equal(t, uint64(3), result[0].OldLeader, "the server snapshot supersedes the earlier CLI read")
	require.Equal(t, plans[0].BeforeScore, result[0].BeforeScore)
	require.Equal(t, plans[0].AfterScore, result[0].AfterScore)
	require.Equal(t, uint64(123), result[0].MoveBytes)
	require.Equal(t, "table-revision", result[0].Revision)
	require.Equal(t, "planned", result[0].Status)
}

func TestAffinityRebalanceApplyRequiresForce(t *testing.T) {
	testServer, server := newAffinityRebalanceTestServer(t, map[string]*pdhttp.AffinityGroupState{
		"_tidb_t_42": affinityGroup("_tidb_t_42", 1, 2, 3),
	})
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42", "--apply")
	require.Contains(t, output, "--force is required with --apply")
	require.Empty(t, testServer.rebalanceIDs)
	require.Empty(t, testServer.rebalanceBodies)
}

func TestAffinityRebalanceApplyUsesServerPlan(t *testing.T) {
	groups := map[string]*pdhttp.AffinityGroupState{
		"_tidb_pt_42_p1": affinityGroup("_tidb_pt_42_p1", 1, 2, 3),
		"_tidb_pt_42_p2": affinityGroup("_tidb_pt_42_p2", 1, 2, 3),
		"_tidb_pt_42_p3": affinityGroup("_tidb_pt_42_p3", 1, 2, 3),
	}
	plans := []pdhttp.AffinityBalancePlan{
		{GroupID: "_tidb_pt_42_p2", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}, Changed: true},
	}
	testServer, server := newAffinityRebalanceTestServer(t, groups, plans...)
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42", "--apply", "--force")
	var result []affinityRebalanceResult
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	require.Len(t, result, 1)
	require.Equal(t, "_tidb_pt_42_p2", result[0].GroupID)
	require.True(t, result[0].Applied)
	require.Equal(t, uint64(1), result[0].OldLeader)
	require.Equal(t, []uint64{1, 2, 3}, result[0].NewVoters)
	require.Equal(t, []string{"_tidb_pt_42_p1"}, testServer.rebalanceIDs)
	require.Equal(t, []pdhttp.RebalanceAffinityGroupRequest{{Apply: true}}, testServer.rebalanceBodies)
}

func TestAffinityRebalanceStaleApplyReturnsNoAppliedPlan(t *testing.T) {
	groups := map[string]*pdhttp.AffinityGroupState{
		"_tidb_t_42": affinityGroup("_tidb_t_42", 1, 2, 3),
	}
	testServer, server := newAffinityRebalanceTestServer(t, groups)
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42", "--apply", "--force")
	require.Equal(t, "[]\n", output)
	require.Equal(t, []string{"_tidb_t_42"}, testServer.rebalanceIDs)
	require.Equal(t, []pdhttp.RebalanceAffinityGroupRequest{{Apply: true}}, testServer.rebalanceBodies)
}
