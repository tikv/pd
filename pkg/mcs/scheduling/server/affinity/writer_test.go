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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

func TestTargetWriterFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantError string
		status                int
	}{
		{name: "bad request", status: http.StatusBadRequest, body: "rejected", wantError: "400 Bad Request rejected"},
		{name: "unavailable", status: http.StatusServiceUnavailable, body: "rejected", wantError: "503 Service Unavailable rejected"},
		{name: "invalid response", status: http.StatusOK, body: "invalid JSON", wantError: "decode affinity group targets response"},
		{name: "timeout", wantError: "context deadline exceeded"},
		{name: "unreachable", wantError: "connect"},
		{name: "no leader", wantError: "PD leader is not found"},
		{name: "canceled", wantError: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if tc.name == "timeout" || tc.name == "canceled" {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)
			if tc.name == "unreachable" {
				server.Close()
			}
			failures := 0
			writer := NewTargetWriter(http.DefaultClient, func() (string, error) {
				if tc.name == "no leader" {
					return "", errors.New(tc.wantError)
				}
				return server.URL + "/", nil
			}, func() { failures++ })
			writer.timeout = 100 * time.Millisecond
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.name == "canceled" {
				cancel()
			}
			written, err := writer.WriteGroupTargets(ctx, []affinity.GroupTarget{{Expected: affinity.Group{ID: "g"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}})
			re.ErrorContains(err, tc.wantError)
			re.Empty(written)
			re.Equal(1, failures)
		})
	}
}
