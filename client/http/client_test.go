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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/retry"
)

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
	c.GetRegions(context.Background())
	c.GetHistoryHotRegions(context.Background(), &HistoryHotRegionsRequest{})
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
	c.GetRegions(context.Background())
	expectedVal.Store("test")
	c.WithCallerID(expectedVal.Load().(string)).GetRegions(context.Background())
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
