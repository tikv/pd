// Copyright 2025 TiKV Project Authors.
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
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	sd "github.com/tikv/pd/client/servicediscovery"
)

/* The following functions are only for test */
// requestChecker is used to check the HTTP request sent by the client.
type requestChecker struct {
	checker func(req *http.Request) error
}

// RoundTrip implements the `http.RoundTripper` interface.
func (rc requestChecker) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	if err := rc.checker(req); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("{}")),
		Header:     make(http.Header),
	}, nil
}

// NewHTTPClientWithRequestChecker returns a http client with checker.
func NewHTTPClientWithRequestChecker(checker func(req *http.Request) error) *http.Client {
	return &http.Client{
		Transport: requestChecker{checker},
	}
}

// newClientWithMockServiceDiscovery creates a new PD HTTP client with a mock service discovery.
func newClientWithMockServiceDiscovery(
	source string,
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	sd := sd.NewMockServiceDiscovery(pdAddrs, c.inner.tlsConf)
	if err := sd.Init(); err != nil {
		log.Error("[pd] init mock service discovery failed",
			zap.String("source", source), zap.Strings("pd-addrs", pdAddrs), zap.Error(err))
		return nil
	}
	c.inner.init(sd)
	return c
}
