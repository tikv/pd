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

package tempurl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestTryAllocTestURLUsesConfiguredAllocator(t *testing.T) {
	expected := "http://127.0.0.1:12345"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expected))
	}))
	t.Cleanup(server.Close)
	t.Setenv(AllocURLFromUT, server.URL)

	// Check connection cleanup while the allocator is still running, as it is
	// when pd-ut subprocesses finish their tests.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	require.Equal(t, expected, tryAllocTestURL())
}

func TestTryAllocTestURLDoesNotFallBackFromConfiguredAllocator(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	t.Setenv(AllocURLFromUT, server.URL)

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	require.Empty(t, tryAllocTestURL())
}
