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

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAffinityGroupsWithDiagnostics(t *testing.T) {
	re := require.New(t)
	as := assert.New(t)
	const responseBody = `{
		"status_version": 1,
		"affinity_groups": {
			"group/a": {
				"id": "group/a",
				"phase": "stable",
				"diagnostics": {
					"effective_policy": null,
					"observed_at": "",
					"actual_leader_store_ids": null,
					"actual_voter_store_ids": null,
					"approximate_size_mib": null,
					"pending_move_mib": null,
					"leader_spread": null,
					"voter_spread": null,
					"running_operators": null,
					"blocked_reasons": null
				}
			},
			"group b": {
				"id": "group b",
				"phase": "stable",
				"diagnostics": {
					"effective_policy": "",
					"observed_at": "2026-11-25T01:02:03Z",
					"actual_leader_store_ids": [],
					"actual_voter_store_ids": [[]],
					"approximate_size_mib": 0,
					"pending_move_mib": 0,
					"leader_spread": 0,
					"voter_spread": 0,
					"running_operators": [{"kind": "move", "count": 0}],
					"blocked_reasons": []
				}
			}
		}
	}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		as.Equal(http.MethodGet, r.Method)
		as.Equal(AffinityGroups, r.URL.Path)
		as.Equal([]string{"group/a", "group b"}, r.URL.Query()["ids"])
		as.Equal([]string{"true"}, r.URL.Query()["include_status"])
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(responseBody))
		as.NoError(err)
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics", []string{server.URL})
	defer c.Close()

	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"group/a", "group b", "group/a"})
	re.NoError(err)
	re.Equal(1, response.StatusVersion)
	groups := response.AffinityGroups
	re.Len(groups, 2)

	unknown := groups["group/a"].Diagnostics
	re.NotNil(unknown)
	re.Nil(unknown.EffectivePolicy)
	re.Nil(unknown.ActualLeaderStoreIDs)
	re.Nil(unknown.ActualVoterStoreIDs)
	re.Nil(unknown.ApproximateSizeMiB)
	re.Nil(unknown.PendingMoveMiB)
	re.Nil(unknown.LeaderSpread)
	re.Nil(unknown.VoterSpread)
	re.Nil(unknown.RunningOperators)
	re.Nil(unknown.BlockedReasons)

	zero := groups["group b"].Diagnostics
	re.NotNil(zero)
	re.NotNil(zero.EffectivePolicy)
	re.Empty(*zero.EffectivePolicy)
	re.Equal("2026-11-25T01:02:03Z", zero.ObservedAt)
	re.NotNil(zero.ActualLeaderStoreIDs)
	re.Empty(zero.ActualLeaderStoreIDs)
	re.Len(zero.ActualVoterStoreIDs, 1)
	re.NotNil(zero.ActualVoterStoreIDs[0])
	re.Empty(zero.ActualVoterStoreIDs[0])
	re.NotNil(zero.ApproximateSizeMiB)
	re.Zero(*zero.ApproximateSizeMiB)
	re.NotNil(zero.PendingMoveMiB)
	re.Zero(*zero.PendingMoveMiB)
	re.NotNil(zero.LeaderSpread)
	re.Zero(*zero.LeaderSpread)
	re.NotNil(zero.VoterSpread)
	re.Zero(*zero.VoterSpread)
	re.Equal([]AffinityGroupOperatorCount{{Kind: "move", Count: 0}}, zero.RunningOperators)
	re.NotNil(zero.BlockedReasons)
	re.Empty(zero.BlockedReasons)
}

func TestGetAffinityGroupsWithDiagnosticsValidatesIDs(t *testing.T) {
	re := require.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-empty", []string{server.URL})
	defer c.Close()

	for _, ids := range [][]string{nil, {}} {
		response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), ids)
		re.ErrorContains(err, "group IDs must not be empty")
		re.Nil(response)
	}
	tooManyIDs := make([]string, 101)
	for i := range tooManyIDs {
		tooManyIDs[i] = strconv.Itoa(i)
	}
	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), tooManyIDs)
	re.ErrorContains(err, "at most 100 unique affinity group IDs are allowed")
	re.Nil(response)
	re.Zero(requests.Load())
}

func TestGetAffinityGroupsWithDiagnosticsQueryLimits(t *testing.T) {
	re := require.New(t)
	as := assert.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			as.Len(r.URL.Query()["ids"], 100)
			as.Equal([]string{"true"}, r.URL.Query()["include_status"])
		case 2:
			as.Len(r.URL.RawQuery, 4096)
		}
		w.Header().Set("Content-Type", "application/json")
		as.NoError(json.NewEncoder(w).Encode(AffinityGroupsResponse{
			StatusVersion:  1,
			AffinityGroups: map[string]*AffinityGroupState{},
		}))
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-limits", []string{server.URL})
	defer c.Close()

	ids := make([]string, 0, 200)
	for i := range 100 {
		ids = append(ids, strconv.Itoa(i), strconv.Itoa(i))
	}
	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), ids)
	re.NoError(err)
	re.Equal(1, response.StatusVersion)

	// Each slash is escaped to %2F, so this produces exactly 4096 query bytes.
	escapedID := strings.Repeat("/", 1357) + "a"
	response, err = c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{escapedID})
	re.NoError(err)
	re.Equal(1, response.StatusVersion)
	re.Equal(int32(2), requests.Load())

	response, err = c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{escapedID + "/"})
	re.ErrorContains(err, "affinity group diagnostics query exceeds 4096 bytes")
	re.Nil(response)
	re.Equal(int32(2), requests.Load())
}

func TestGetAffinityGroupsWithDiagnosticsLegacyResponse(t *testing.T) {
	re := require.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, AffinityGroups, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"affinity_groups":{"legacy":{"id":"legacy","phase":"stable"}}}`))
		assert.NoError(t, err)
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-legacy", []string{server.URL})
	defer c.Close()

	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"legacy"})
	re.NoError(err)
	re.Equal(int32(1), requests.Load())
	re.Zero(response.StatusVersion)
	re.Contains(response.AffinityGroups, "legacy")
	re.Nil(response.AffinityGroups["legacy"].Diagnostics)
}

func TestGetAffinityGroupsWithDiagnosticsFallback(t *testing.T) {
	re := require.New(t)
	as := assert.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := requests.Add(1)
		as.Equal([]string{"legacy/a", "legacy b"}, r.URL.Query()["ids"])
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Has("include_status") {
			as.Equal(int32(1), requestNumber)
			as.Equal([]string{"true"}, r.URL.Query()["include_status"])
			w.WriteHeader(http.StatusBadRequest)
			as.NoError(json.NewEncoder(w).Encode("invalid include_status value"))
			return
		}
		as.Equal(int32(2), requestNumber)
		as.NoError(json.NewEncoder(w).Encode(AffinityGroupsResponse{
			StatusVersion: 1,
			AffinityGroups: map[string]*AffinityGroupState{
				"legacy/a": {AffinityGroup: AffinityGroup{ID: "legacy/a"}},
				"legacy b": {AffinityGroup: AffinityGroup{ID: "legacy b"}},
			},
		}))
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-fallback", []string{server.URL})
	defer c.Close()

	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"legacy/a", "legacy b", "legacy/a"})
	re.NoError(err)
	re.Equal(int32(2), requests.Load())
	re.Zero(response.StatusVersion)
	re.Len(response.AffinityGroups, 2)
}

func TestGetAffinityGroupsWithDiagnosticsFallbackOnlyOnce(t *testing.T) {
	re := require.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		assert.NoError(t, json.NewEncoder(w).Encode("invalid include_status value"))
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-fallback-once", []string{server.URL})
	defer c.Close()

	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"legacy"})
	re.ErrorContains(err, "400 Bad Request")
	re.Nil(response)
	re.Equal(int32(2), requests.Load())
}

func TestGetAffinityGroupsWithDiagnosticsDoesNotFallback(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "unrelated bad request", statusCode: http.StatusBadRequest, body: "invalid ids"},
		{name: "internal error", statusCode: http.StatusInternalServerError, body: "invalid include_status value"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(testCase.statusCode)
				assert.NoError(t, json.NewEncoder(w).Encode(testCase.body))
			}))
			defer server.Close()
			c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-no-fallback", []string{server.URL})
			defer c.Close()

			response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"group"})
			re.Error(err)
			re.Nil(response)
			re.Equal(int32(1), requests.Load())
		})
	}
}

func TestGetAffinityGroupsWithDiagnosticsDoesNotFallbackOnContextErrors(t *testing.T) {
	re := require.New(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-context", []string{server.URL})
	defer c.Close()

	cancelledContext, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := c.GetAffinityGroupsWithDiagnostics(cancelledContext, []string{"group"})
	re.ErrorIs(err, context.Canceled)
	re.Nil(response)
	re.Zero(requests.Load())

	timeoutContext, timeoutCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer timeoutCancel()
	response, err = c.GetAffinityGroupsWithDiagnostics(timeoutContext, []string{"group"})
	re.ErrorIs(err, context.DeadlineExceeded)
	re.Nil(response)
	re.Equal(int32(1), requests.Load())
}

func TestGetAffinityGroupsWithDiagnosticsFiltersResponseIDs(t *testing.T) {
	re := require.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(AffinityGroupsResponse{
			StatusVersion: 1,
			AffinityGroups: map[string]*AffinityGroupState{
				"requested": {AffinityGroup: AffinityGroup{ID: "requested"}},
				"other":     {AffinityGroup: AffinityGroup{ID: "other"}},
			},
		}))
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-diagnostics-filter", []string{server.URL})
	defer c.Close()

	response, err := c.GetAffinityGroupsWithDiagnostics(t.Context(), []string{"requested"})
	re.NoError(err)
	re.Len(response.AffinityGroups, 1)
	re.Contains(response.AffinityGroups, "requested")
	re.NotContains(response.AffinityGroups, "other")
}
