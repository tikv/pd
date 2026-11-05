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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/etcdutil"
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
			_, _ = io.WriteString(w, "target write rejected")
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
	var okCalls, nextCalls atomic.Int32
	ok := targetServer(t, http.StatusOK, &okCalls)
	next := targetServer(t, http.StatusOK, &nextCalls)
	leaderURL := ok + "/"
	var failures int
	writer := NewTargetWriter(http.DefaultClient, func() (string, error) { return leaderURL, nil }, func() { failures++ })
	for _, url := range []string{ok + "/", next} {
		leaderURL = url
		written, err := writer.WriteGroupTargets(t.Context(), targets)
		re.NoError(err)
		re.Equal([]string{"g"}, written)
	}
	re.Equal(int32(1), okCalls.Load())
	re.Equal(int32(1), nextCalls.Load())
	re.Zero(failures)
}

func TestTargetWriterFailures(t *testing.T) {
	targets := []affinity.GroupTarget{{Expected: affinity.Group{ID: "g"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}}
	var badCalls, failCalls atomic.Int32
	bad := targetServer(t, http.StatusBadRequest, &badCalls)
	fail := targetServer(t, http.StatusServiceUnavailable, &failCalls)
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// The server notices that the client is gone only after the body is read.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(hang.Close)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	invalid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "invalid JSON")
	}))
	t.Cleanup(invalid.Close)
	for _, tc := range []struct {
		name      string
		url       string
		leaderErr error
		wantError string
	}{
		{name: "no leader", leaderErr: errors.New("PD leader is not found"), wantError: "PD leader is not found"},
		{name: "unreachable", url: down.URL, wantError: "connect"},
		{name: "timeout", url: hang.URL, wantError: "context deadline exceeded"},
		{name: "bad request", url: bad, wantError: "400 Bad Request target write rejected"},
		{name: "unavailable", url: fail, wantError: "503 Service Unavailable target write rejected"},
		{name: "invalid response", url: invalid.URL, wantError: "decode affinity group targets response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var failures int
			writer := NewTargetWriter(http.DefaultClient, func() (string, error) { return tc.url, tc.leaderErr }, func() { failures++ })
			writer.timeout = 100 * time.Millisecond
			written, err := writer.WriteGroupTargets(t.Context(), targets)
			re.ErrorContains(err, tc.wantError)
			re.Empty(written)
			re.Equal(1, failures)
		})
	}
	t.Run("canceled", func(t *testing.T) {
		re := require.New(t)
		var failures int
		writer := NewTargetWriter(http.DefaultClient, func() (string, error) { return hang.URL, nil }, func() { failures++ })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := writer.WriteGroupTargets(ctx, targets)
		re.ErrorIs(err, context.Canceled)
		re.Equal(1, failures)
	})
	require.Equal(t, int32(1), badCalls.Load())
	require.Equal(t, int32(1), failCalls.Load())
}

// TestTargetWriterSplitsRequests checks that the writer sends targets in requests
// that each fit in one etcd transaction, and that a failed request returns the
// groups written by the earlier ones.
func TestTargetWriterSplitsRequests(t *testing.T) {
	re := require.New(t)
	var (
		mu    sync.Mutex
		sizes []int
		fail  atomic.Int32 // the request that fails, counted from 1, or 0 for none
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req affinity.GroupTargetsRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		sizes = append(sizes, len(req.Targets))
		n := len(sizes)
		mu.Unlock()
		if int32(n) == fail.Load() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ids := make([]string, 0, len(req.Targets))
		for _, target := range req.Targets {
			ids = append(ids, target.Expected.ID)
		}
		_ = json.NewEncoder(w).Encode(&affinity.GroupTargetsResponse{WrittenGroupIDs: ids})
	}))
	t.Cleanup(server.Close)

	targets := make([]affinity.GroupTarget, etcdutil.MaxEtcdTxnOps+2)
	ids := make([]string, len(targets))
	for i := range targets {
		ids[i] = fmt.Sprintf("g%03d", i)
		targets[i] = affinity.GroupTarget{Expected: affinity.Group{ID: ids[i]}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}
	}
	var failures int
	writer := NewTargetWriter(http.DefaultClient, func() (string, error) { return server.URL, nil }, func() { failures++ })
	written, err := writer.WriteGroupTargets(t.Context(), targets)
	re.NoError(err)
	re.Equal(ids, written)
	re.Equal([]int{etcdutil.MaxEtcdTxnOps, 2}, sizes)
	re.Zero(failures)

	mu.Lock()
	sizes = nil
	mu.Unlock()
	fail.Store(2)
	written, err = writer.WriteGroupTargets(t.Context(), targets)
	re.ErrorContains(err, "400")
	re.Equal(ids[:etcdutil.MaxEtcdTxnOps], written)
	re.Equal(1, failures)
}
