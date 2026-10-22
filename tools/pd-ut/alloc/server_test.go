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
	"fmt"
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, allocatorURL, nil) // #nosec G704 -- allocator URL is set by the local test harness.
	re.NoError(err)
	resp, err := http.DefaultClient.Do(req) // #nosec G704 -- allocator URL is set by the local test harness.
	re.NoError(err)
	defer func() { re.NoError(resp.Body.Close()) }()
	re.Equal(http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	re.NoError(err)
	allocatedURL, err := url.Parse(string(body))
	re.NoError(err)
	re.NotEmpty(allocatedURL.Port())
}

// TestReclaimTestAddrs pins the P2 fix: once the shared allocator holds more
// than maxTestAddrMapLen live addresses, it re-issues the oldest ones instead
// of retaining every address it has ever handed out, so a long run does not
// drain the ephemeral port namespace into log.Fatal.
func TestReclaimTestAddrs(t *testing.T) {
	re := require.New(t)

	testAddrMutex.Lock()
	saved, savedSeq := testAddrMap, testAddrSeq
	testAddrMap, testAddrSeq = make(map[string]int), 0
	t.Cleanup(func() {
		testAddrMutex.Lock()
		testAddrMap, testAddrSeq = saved, savedSeq
		testAddrMutex.Unlock()
	})
	testAddrMutex.Unlock()

	// Seed the map with more live addresses than the bound, in insertion
	// order a1 (oldest) ... aN (newest).
	total := maxTestAddrMapLen + 8
	for i := 1; i <= total; i++ {
		testAddrMutex.Lock()
		testAddrSeq++
		testAddrMap[fmt.Sprintf("a%d", i)] = testAddrSeq
		testAddrMutex.Unlock()
	}

	reclaimTestAddrs(maxTestAddrMapLen)

	re.LessOrEqual(len(testAddrMap), maxTestAddrMapLen,
		"map must be reclaimed down to the bound")
	// The oldest addresses (a1 .. a8) must have been evicted first.
	testAddrMutex.Lock()
	defer testAddrMutex.Unlock()
	for i := 1; i <= total-maxTestAddrMapLen; i++ {
		_, stillHeld := testAddrMap[fmt.Sprintf("a%d", i)]
		re.False(stillHeld, "oldest address a%d should have been reclaimed", i)
	}
}
