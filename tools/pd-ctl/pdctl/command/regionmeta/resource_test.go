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

package regionmeta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHTTPResponseAndRedirectLimits(t *testing.T) {
	limiter := &rateLimiter{}
	t.Run("response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"padding":"`))
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes)))
			_, _ = w.Write([]byte(`"}`))
		}))
		defer server.Close()
		client := &httpClient{
			endpoint: server.URL, client: newSharedHTTPClient(nil), limiter: limiter, timeout: time.Second,
		}
		var destination any
		err := client.getJSON(context.Background(), "/", nil, true, false, &destination)
		require.ErrorContains(t, err, "response exceeds 8 MiB")
	})

	t.Run("redirect", func(t *testing.T) {
		var followed bool
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
		defer target.Close()
		server := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusTemporaryRedirect))
		defer server.Close()
		client := &httpClient{
			endpoint: server.URL, client: newSharedHTTPClient(nil), limiter: limiter, timeout: time.Second,
		}
		var destination any
		err := client.getJSON(context.Background(), "/", nil, true, false, &destination)
		require.ErrorContains(t, err, "unexpected HTTP 307")
		require.False(t, followed)
	})
}

func TestRateLimiterChargesEveryConcurrentRequest(t *testing.T) {
	limiter := &rateLimiter{interval: 20 * time.Millisecond}
	require.NoError(t, limiter.wait(context.Background(), 3))
	started := time.Now()
	require.NoError(t, limiter.wait(context.Background(), 1))
	require.GreaterOrEqual(t, time.Since(started), 45*time.Millisecond)
}

func TestBatchRequesterPreservesRequestErrorAfterCancel(t *testing.T) {
	blocked := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer blocked.Close()
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer failed.Close()

	limiter := &rateLimiter{}
	client := newSharedHTTPClient(nil)
	defer client.CloseIdleConnections()
	clients := []*httpClient{
		{endpoint: blocked.URL, client: client, limiter: limiter, timeout: time.Second},
		{endpoint: failed.URL, client: client, limiter: limiter, timeout: time.Second},
	}
	destinations := make([]any, len(clients))
	calls := make([]requestCall, 0, len(clients))
	for i := range clients {
		calls = append(calls, requestCall{client: clients[i], path: "/", destination: &destinations[i]})
	}
	err := (&batchRequester{limiter: limiter}).getJSON(context.Background(), calls)
	require.ErrorContains(t, err, failed.URL)
	require.ErrorContains(t, err, "unexpected HTTP 500")
}

func TestSortedRowsUseBoundedExternalMerge(t *testing.T) {
	directory := t.TempDir()
	budget := newDiskBudget(128 * 1024 * 1024)
	rows := newSortedRows(directory, budget)
	largeKey := strings.Repeat("AA", 512)
	for id := uint64(20000); id > 0; id-- {
		err := rows.add(id, int(id%3), regionMeta{StartKey: largeKey, EndKey: largeKey})
		require.NoError(t, err)
	}
	require.NoError(t, rows.finish())
	require.Greater(t, budget.peak, int64(sortBufferBytes))
	var previous uint64
	count := 0
	require.NoError(t, rows.iterate(func(row diskRow) error {
		if count > 0 {
			require.GreaterOrEqual(t, row.RegionID, previous)
		}
		previous = row.RegionID
		count++
		return nil
	}))
	require.Equal(t, 20000, count)
	rows.cleanup()
	require.Zero(t, budget.current)
}
