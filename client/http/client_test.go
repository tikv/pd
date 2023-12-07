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
	"testing"

	"github.com/stretchr/testify/require"
)

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
	var expectedVal string
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		val := req.Header.Get(pdAllowFollowerHandleKey)
		if val != expectedVal {
			re.Failf("PD allow follower handler header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient([]string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	expectedVal = "true"
	c.GetHistoryHotRegions(context.Background(), &HistoryHotRegionsRequest{})
}

func TestCallerID(t *testing.T) {
	re := require.New(t)
	expectedVal := defaultCallerID
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		val := req.Header.Get(componentSignatureKey)
		if val != expectedVal {
			re.Failf("Caller ID header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient([]string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	expectedVal = "test"
	c.WithCallerID(expectedVal).GetRegions(context.Background())
}
