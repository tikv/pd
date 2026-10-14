// Copyright 2023 TiKV Project Authors.
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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/pkg/retry"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestConfigWithTrailingSlash(t *testing.T) {
	for _, suffix := range []string{"", "/", "///", "/proxy%2Ftenant/"} {
		t.Run(suffix, func(t *testing.T) {
			re := require.New(t)
			as := assert.New(t)
			requestPath := ConfigWithTTLSeconds(5)
			if suffix == "/proxy%2Ftenant/" {
				requestPath = "/proxy%2Ftenant" + requestPath
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				as.Equal(http.MethodPost, r.Method)
				as.Equal(requestPath, r.RequestURI)
				body, err := io.ReadAll(r.Body)
				as.NoError(err)
				as.JSONEq(`{"schedule.leader-schedule-limit":8}`, string(body))
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			c := newClientWithMockServiceDiscovery("test-trailing-slash", []string{server.URL + suffix})
			defer c.Close()
			re.NoError(c.SetConfig(context.Background(), map[string]any{"schedule.leader-schedule-limit": 8}, 5))
		})
	}
}

func TestRebalanceAffinityGroup(t *testing.T) {
	re := require.New(t)
	as := assert.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		as.Equal(http.MethodPost, r.Method)
		as.Equal("/pd/api/v2/affinity-groups/_tidb_t_42/rebalance", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		as.NoError(err)
		as.JSONEq(`{"apply":true}`, string(body))
		_, _ = io.WriteString(w, `[{"group_id":"_tidb_t_42","leader_store_id":2,"voter_store_ids":[1,2,3],"changed":true}]`)
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("test-affinity-rebalance", []string{server.URL})
	defer c.Close()

	plans, err := c.RebalanceAffinityGroup(context.Background(), "_tidb_t_42", true)
	re.NoError(err)
	re.Equal([]AffinityBalancePlan{{
		GroupID:       "_tidb_t_42",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
		Changed:       true,
	}}, plans)
}

func TestPDAllowFollowerHandleHeader(t *testing.T) {
	re := require.New(t)
	checked := 0
	httpClient := NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		var expectedVal string
		if req.URL.Path == HotHistory {
			expectedVal = "true"
		}
		val := req.Header.Get(pdAllowFollowerHandleKey)
		if val != expectedVal {
			re.Failf("PD allow follower handler header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		checked++
		return nil
	})
	c := newClientWithMockServiceDiscovery("test-header", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	defer c.Close()
	_, err := c.GetRegions(context.Background())
	re.NoError(err)
	_, err = c.GetHistoryHotRegions(context.Background(), &HistoryHotRegionsRequest{})
	re.NoError(err)
	re.Equal(2, checked)
}

func TestWithCallerID(t *testing.T) {
	re := require.New(t)
	checked := 0
	var expectedVal atomic.Value
	expectedVal.Store(defaultCallerID)
	httpClient := NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		val := req.Header.Get(xCallerIDKey)
		// Exclude the request sent by the inner client.
		if !strings.Contains(val, defaultInnerCallerID) && val != expectedVal.Load() {
			re.Failf("Caller ID header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		checked++
		return nil
	})
	c := newClientWithMockServiceDiscovery("test-caller-id", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	defer c.Close()
	_, err := c.GetRegions(context.Background())
	re.NoError(err)
	expectedVal.Store("test")
	_, err = c.WithCallerID(expectedVal.Load().(string)).GetRegions(context.Background())
	re.NoError(err)
	re.Equal(2, checked)
}

func TestWithBackoffer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newClientWithMockServiceDiscovery("test-with-backoffer", []string{"http://127.0.0.1"})
	defer c.Close()

	base := 100 * time.Millisecond
	max := 500 * time.Millisecond
	total := time.Second
	bo := retry.InitialBackoffer(base, max, total)
	// Test the time cost of the backoff.
	start := time.Now()
	_, err := c.WithBackoffer(bo).GetPDVersion(ctx)
	re.InDelta(total, time.Since(start), float64(250*time.Millisecond))
	re.Error(err)
	// Test if the infinite retry works.
	bo = retry.InitialBackoffer(base, max, 0)
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	start = time.Now()
	_, err = c.WithBackoffer(bo).GetPDVersion(timeoutCtx)
	re.InDelta(3*time.Second, time.Since(start), float64(250*time.Millisecond))
	re.ErrorIs(err, context.DeadlineExceeded)
}

func TestWithTargetURL(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newClientWithMockServiceDiscovery("test-with-target-url", []string{"http://127.0.0.1", "http://127.0.0.2", "http://127.0.0.3"})
	defer c.Close()

	_, err := c.WithTargetURL("http://127.0.0.4").GetStatus(ctx)
	re.ErrorIs(err, errs.ErrClientNoTargetMember)
	_, err = c.WithTargetURL("http://127.0.0.2").GetStatus(ctx)
	re.ErrorContains(err, "connect: connection refused")
}

func TestUpdateAffinityGroupPolicy(t *testing.T) {
	re := require.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		re.Equal(http.MethodPatch, r.Method)
		re.Equal("/pd/api/v2/affinity-groups/_tidb_t_1/policy", r.RequestURI)
		body, err := io.ReadAll(r.Body)
		re.NoError(err)
		re.JSONEq(`{"balance_policy":"fixed"}`, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, err = io.WriteString(w, `{"id":"_tidb_t_1","balance_policy":"fixed"}`)
		re.NoError(err)
	}))
	defer server.Close()

	c := newClientWithMockServiceDiscovery("test-affinity-policy", []string{server.URL})
	defer c.Close()
	state, err := c.UpdateAffinityGroupPolicy(context.Background(), "_tidb_t_1", "fixed")
	re.NoError(err)
	re.Equal("fixed", state.BalancePolicy)
}

func TestAffinityApplyUsesContextBeyondDefaultClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(30 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	defer server.Close()
	c := newClientWithMockServiceDiscovery("affinity-apply-timeout", []string{server.URL}).(*client)
	defer c.Close()
	c.inner.cli.Timeout = time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := c.RebalanceAffinityGroup(ctx, "_tidb_t_42", true)
	require.NoError(t, err)
	require.Equal(t, time.Millisecond, c.inner.cli.Timeout)

	cancelled, stop := context.WithCancel(t.Context())
	stop()
	_, err = c.RebalanceAffinityGroup(cancelled, "_tidb_t_42", true)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAffinityApplyFailureIsNotReplayedAndRetainsProgress(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":"plan changed","plans":[{"group_id":"p1","status":"applied"},{"group_id":"p2","status":"submitted","reason":"range changed"},{"group_id":"p3","status":"not_submitted"}]}`)
			})
			first, second := httptest.NewServer(handler), httptest.NewServer(handler)
			defer first.Close()
			defer second.Close()
			c := newClientWithMockServiceDiscovery("affinity-apply-no-replay", []string{first.URL, second.URL})
			defer c.Close()
			bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, 100*time.Millisecond)
			plans, err := c.WithBackoffer(bo).RebalanceAffinityGroup(t.Context(), "p1", true)
			require.Error(t, err)
			require.Equal(t, int32(1), requests.Load(), "neither failover nor backoff may replay apply")
			require.Len(t, plans, 3)
			require.Equal(t, "applied", plans[0].Status)
			require.Equal(t, "submitted", plans[1].Status)
			require.Equal(t, "range changed", plans[1].Reason)
			require.Equal(t, "not_submitted", plans[2].Status)
		})
	}
}

func TestAffinityApplyNetworkFailureIsNotReplayed(t *testing.T) {
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		assert.NoError(t, err)
		if err == nil {
			assert.NoError(t, connection.Close())
		}
	})
	first, second := httptest.NewServer(handler), httptest.NewServer(handler)
	defer first.Close()
	defer second.Close()
	c := newClientWithMockServiceDiscovery("affinity-apply-network-error", []string{first.URL, second.URL})
	defer c.Close()
	bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, 100*time.Millisecond)
	_, err := c.WithBackoffer(bo).RebalanceAffinityGroup(t.Context(), "p1", true)
	require.Error(t, err)
	require.Equal(t, int32(1), requests.Load())
}

func TestAffinityPreviewCanRetry(t *testing.T) {
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body RebalanceAffinityGroupRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.False(t, body.Apply)
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `"not ready"`)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	})
	first, second := httptest.NewServer(handler), httptest.NewServer(handler)
	defer first.Close()
	defer second.Close()
	c := newClientWithMockServiceDiscovery("affinity-preview-retry", []string{first.URL, second.URL})
	defer c.Close()
	_, err := c.RebalanceAffinityGroup(t.Context(), "p1", false)
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load())
}
