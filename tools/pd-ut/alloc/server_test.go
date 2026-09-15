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

package alloc

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/tempurl"
)

func TestRunHTTPServerPublishesReachableAddress(t *testing.T) {
	re := require.New(t)
	t.Setenv(tempurl.AllocURLFromUT, "")
	originalAddress := *statusAddress
	*statusAddress = "127.0.0.1:0"
	t.Cleanup(func() { *statusAddress = originalAddress })

	srv := RunHTTPServer(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		re.NoError(srv.Shutdown(ctx))
	})

	allocatorURL := os.Getenv(tempurl.AllocURLFromUT)
	parsedAllocatorURL, err := url.Parse(allocatorURL)
	re.NoError(err)
	re.NotEqual("0", parsedAllocatorURL.Port())

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, allocatorURL, nil)
	re.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	defer func() { re.NoError(resp.Body.Close()) }()
	re.Equal(http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	re.NoError(err)
	allocatedURL, err := url.Parse(string(body))
	re.NoError(err)
	re.NotEmpty(allocatedURL.Port())
}
