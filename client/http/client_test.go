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
	"crypto/tls"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPDAddrNormalization(t *testing.T) {
	re := require.New(t)
	c := NewClient("test-http-pd-addr", []string{"127.0.0.1"})
	pdAddrs, leaderAddrIdx := c.(*client).inner.getPDAddrs()
	re.Len(pdAddrs, 1)
	re.Equal(-1, leaderAddrIdx)
	re.Contains(pdAddrs[0], httpScheme)
	c = NewClient("test-https-pd-addr", []string{"127.0.0.1"}, WithTLSConfig(&tls.Config{}))
	pdAddrs, leaderAddrIdx = c.(*client).inner.getPDAddrs()
	re.Len(pdAddrs, 1)
	re.Equal(-1, leaderAddrIdx)
	re.Contains(pdAddrs[0], httpsScheme)
}

// requestChecker is used to check the HTTP request sent by the client.
type requestChecker struct {
	checker func(req *http.Request) error
}

// RoundTrip implements the `http.RoundTripper` interface.
func (rc *requestChecker) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	return &http.Response{StatusCode: http.StatusOK}, rc.checker(req)
}

func newHTTPClientWithRequestChecker(checker func(req *http.Request) error) *http.Client {
	return &http.Client{
		Transport: &requestChecker{checker: checker},
	}
}

func TestPDAllowFollowerHandleHeader(t *testing.T) {
	re := require.New(t)
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		var expectedVal string
		if req.URL.Path == HotHistory {
			expectedVal = "true"
		}
		val := req.Header.Get(pdAllowFollowerHandleKey)
		if val != expectedVal {
			re.Failf("PD allow follower handler header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient("test-header", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	c.GetHistoryHotRegions(context.Background(), &HistoryHotRegionsRequest{})
	c.Close()
}

func TestCallerID(t *testing.T) {
	re := require.New(t)
	expectedVal := atomic.NewString(defaultCallerID)
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		val := req.Header.Get(xCallerIDKey)
		// Exclude the request sent by the inner client.
		if !strings.Contains(val, defaultInnerCallerID) && val != expectedVal.Load() {
			re.Failf("Caller ID header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient("test-caller-id", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	expectedVal.Store("test")
	c.WithCallerID(expectedVal.Load()).GetRegions(context.Background())
	c.Close()
}
