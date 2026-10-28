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

package affinity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

func targetServer(t *testing.T, status int, calls *int) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		if r.Method != http.MethodPost || r.URL.Path != groupTargetsPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req affinity.GroupTargetsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Targets) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		_ = json.NewEncoder(w).Encode(&affinity.GroupTargetsResponse{WrittenGroupIDs: []string{req.Targets[0].Expected.ID}})
	}))
	t.Cleanup(server.Close)
	return server
}

func TestTargetWriter(t *testing.T) {
	re := require.New(t)
	targets := []affinity.GroupTarget{{
		Expected:      affinity.Group{ID: "g", CreateTimestamp: 1},
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}}
	var okCalls, badCalls, failCalls int
	ok := targetServer(t, http.StatusOK, &okCalls).URL
	bad := targetServer(t, http.StatusBadRequest, &badCalls).URL
	fail := targetServer(t, http.StatusInternalServerError, &failCalls).URL
	down := httptest.NewServer(http.NotFoundHandler())
	downURL := down.URL
	down.Close()

	// A closed endpoint and a server error are skipped.
	writer := NewTargetWriter(http.DefaultClient, strings.Join([]string{downURL, fail + "/", ok}, ", "))
	written, err := writer.WriteGroupTargets(t.Context(), targets)
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(1, failCalls)
	re.Equal(1, okCalls)

	// A rejected request is not sent to other endpoints.
	writer = NewTargetWriter(http.DefaultClient, bad+","+ok)
	_, err = writer.WriteGroupTargets(t.Context(), targets)
	re.ErrorContains(err, "400")
	re.Equal(1, badCalls)
	re.Equal(1, okCalls)

	_, err = NewTargetWriter(http.DefaultClient, "").WriteGroupTargets(t.Context(), targets)
	re.Error(err)
}
