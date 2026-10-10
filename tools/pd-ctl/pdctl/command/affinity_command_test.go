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
	storesPath         = "/pd/api/v1/stores"
)

type affinityRebalanceTestServer struct {
	groups       map[string]*pdhttp.AffinityGroupState
	stores       pdhttp.StoresInfo
	putIDs       []string
	putBodies    []pdhttp.UpdateAffinityGroupPeersRequest
	putResponses []int
}

func newAffinityRebalanceTestServer(t *testing.T, groups map[string]*pdhttp.AffinityGroupState, storeIDs ...uint64) (*affinityRebalanceTestServer, *httptest.Server) {
	t.Helper()

	stores := pdhttp.StoresInfo{Count: len(storeIDs), Stores: make([]pdhttp.StoreInfo, 0, len(storeIDs))}
	for _, id := range storeIDs {
		stores.Stores = append(stores.Stores, pdhttp.StoreInfo{
			Store: pdhttp.MetaStore{ID: int64(id), StateName: "Up"},
		})
	}
	testServer := &affinityRebalanceTestServer{groups: groups, stores: stores}
	server := httptest.NewServer(http.HandlerFunc(testServer.handle))
	t.Cleanup(server.Close)
	return testServer, server
}

func (s *affinityRebalanceTestServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == affinityGroupsPath:
		_ = json.NewEncoder(w).Encode(pdhttp.AffinityGroupsResponse{AffinityGroups: s.groups})
	case r.Method == http.MethodGet && r.URL.Path == storesPath:
		_ = json.NewEncoder(w).Encode(s.stores)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, affinityGroupsPath+"/"):
		groupID := strings.TrimPrefix(r.URL.Path, affinityGroupsPath+"/")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request pdhttp.UpdateAffinityGroupPeersRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.putIDs = append(s.putIDs, groupID)
		s.putBodies = append(s.putBodies, request)
		responseIndex := len(s.putIDs) - 1
		if responseIndex < len(s.putResponses) && s.putResponses[responseIndex] != http.StatusOK {
			w.WriteHeader(s.putResponses[responseIndex])
			_, _ = io.WriteString(w, `{"error":"update failed"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(s.groups[groupID])
	default:
		http.NotFound(w, r)
	}
}

func affinityGroup(id string, leader uint64, voters ...uint64) *pdhttp.AffinityGroupState {
	return &pdhttp.AffinityGroupState{
		AffinityGroup: pdhttp.AffinityGroup{
			ID:            id,
			LeaderStoreID: leader,
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

func TestAffinityRebalancePreviewFiltersTableAndDoesNotWrite(t *testing.T) {
	groups := map[string]*pdhttp.AffinityGroupState{
		"_tidb_t_42":      affinityGroup("_tidb_t_42", 1, 1, 2, 3),
		"_tidb_pt_42_p1":  affinityGroup("_tidb_pt_42_p1", 1, 1, 2, 3),
		"_tidb_t_99":      affinityGroup("_tidb_t_99", 1, 1, 2, 3),
		"unrelated-group": affinityGroup("unrelated-group", 1, 1, 2, 3),
	}
	testServer, server := newAffinityRebalanceTestServer(t, groups, 1, 2, 3)
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42")
	var result []affinityRebalanceResult
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	require.Len(t, result, 2)
	require.Equal(t, []string{"_tidb_pt_42_p1", "_tidb_t_42"}, []string{result[0].GroupID, result[1].GroupID})
	require.Empty(t, testServer.putIDs)
	require.Empty(t, testServer.putBodies)
}

func TestAffinityRebalanceApplyRequiresForce(t *testing.T) {
	testServer, server := newAffinityRebalanceTestServer(t, map[string]*pdhttp.AffinityGroupState{
		"_tidb_t_42": affinityGroup("_tidb_t_42", 1, 1, 2, 3),
	}, 1, 2, 3)
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42", "--apply")
	require.Contains(t, output, "--force is required with --apply")
	require.Empty(t, testServer.putIDs)
	require.Empty(t, testServer.putBodies)
}

func TestAffinityRebalanceApplyStopsAfterFailure(t *testing.T) {
	groups := map[string]*pdhttp.AffinityGroupState{
		"_tidb_pt_42_p1": affinityGroup("_tidb_pt_42_p1", 1, 1, 2, 3),
		"_tidb_pt_42_p2": affinityGroup("_tidb_pt_42_p2", 1, 1, 2, 3),
		"_tidb_pt_42_p3": affinityGroup("_tidb_pt_42_p3", 1, 1, 2, 3),
	}
	testServer, server := newAffinityRebalanceTestServer(t, groups, 1, 2, 3)
	testServer.putResponses = []int{http.StatusInternalServerError}
	client := newAffinityHTTPClient(t, server)

	output := executeAffinityRebalance(t, client, "--table-id", "42", "--apply", "--force")
	var result []affinityRebalanceResult
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	require.Len(t, result, 1)
	require.Equal(t, "_tidb_pt_42_p1", result[0].GroupID)
	require.NotEmpty(t, result[0].Error)
	require.False(t, result[0].Applied)
	require.Equal(t, []string{"_tidb_pt_42_p1"}, testServer.putIDs)
	require.Len(t, testServer.putBodies, 1)
	require.Equal(t, uint64(2), testServer.putBodies[0].LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, testServer.putBodies[0].VoterStoreIDs)
}

func TestAffinityRebalanceStoreFilterUsesCoreEngineSemantics(t *testing.T) {
	require.True(t, isTiKVStore(pdhttp.StoreInfo{Store: pdhttp.MetaStore{ID: 1}}))
	require.True(t, isTiKVStore(pdhttp.StoreInfo{Store: pdhttp.MetaStore{ID: 2, Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tikv"}}}}))
	require.False(t, isTiKVStore(pdhttp.StoreInfo{Store: pdhttp.MetaStore{ID: 3, Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tiflash"}}}}))
}
