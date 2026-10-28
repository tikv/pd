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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

func targetServer(t *testing.T, status int, calls *atomic.Int32) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req affinity.GroupTargetsRequest
		if r.Method != http.MethodPost || r.URL.Path != groupTargetsPath || json.NewDecoder(r.Body).Decode(&req) != nil || len(req.Targets) == 0 {
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
	return server.URL
}

func TestTargetWriter(t *testing.T) {
	re := require.New(t)
	targets := []affinity.GroupTarget{{Expected: affinity.Group{ID: "g"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}}
	var okCalls, badCalls, failCalls, hangCalls atomic.Int32
	ok := targetServer(t, http.StatusOK, &okCalls)
	bad := targetServer(t, http.StatusBadRequest, &badCalls)
	fail := targetServer(t, http.StatusInternalServerError, &failCalls)
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		hangCalls.Add(1)
		// The server notices that the client is gone only after the body is read.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(hang.Close)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()

	// A closed endpoint, a timeout and a server error move on to the next
	// endpoint, and the endpoint that handled the request is tried first afterwards.
	writer := NewTargetWriter(http.DefaultClient, strings.Join([]string{down.URL, hang.URL, fail + "/", ok}, ", "))
	writer.timeout = 100 * time.Millisecond
	for range 2 {
		written, err := writer.WriteGroupTargets(t.Context(), targets)
		re.NoError(err)
		re.Equal([]string{"g"}, written)
	}
	re.Equal([]int32{1, 1, 2}, []int32{hangCalls.Load(), failCalls.Load(), okCalls.Load()})

	// A rejected request is not sent to other endpoints.
	_, err := NewTargetWriter(http.DefaultClient, bad+","+ok).WriteGroupTargets(t.Context(), targets)
	re.ErrorContains(err, "400")
	re.Equal([]int32{1, 2}, []int32{badCalls.Load(), okCalls.Load()})

	// Once the caller's context is done, no other endpoint is tried.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = NewTargetWriter(http.DefaultClient, hang.URL+","+ok).WriteGroupTargets(ctx, targets)
	re.ErrorIs(err, context.DeadlineExceeded)
	re.Equal([]int32{2, 2}, []int32{hangCalls.Load(), okCalls.Load()})

	_, err = NewTargetWriter(http.DefaultClient, "").WriteGroupTargets(t.Context(), targets)
	re.Error(err)
}
