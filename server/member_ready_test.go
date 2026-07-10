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

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

const (
	targetPDVersionPath = apiutil.CorePath + "/version"
	targetPDReadyPath   = apiutil.CoreV2Path + "/ready"
)

func TestCheckMemberReadyURL(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		versionCode int
		readyCode   int
		expectPaths []string
		expectError string
	}{
		{
			name:        "old version skips ready",
			version:     "v8.5.1",
			versionCode: http.StatusOK,
			readyCode:   http.StatusServiceUnavailable,
			expectPaths: []string{targetPDVersionPath},
		},
		{
			name:        "supported version checks ready",
			version:     "v8.5.2",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
		},
		{
			name:        "version request failure",
			versionCode: http.StatusInternalServerError,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath},
			expectError: "failed to get target pd member 1 version",
		},
		{
			name:        "invalid version does not check ready",
			version:     "invalid-version",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath},
			expectError: "failed to parse target pd member 1 version",
		},
		{
			name:        "ready request failure",
			version:     "v8.5.2",
			versionCode: http.StatusOK,
			readyCode:   http.StatusServiceUnavailable,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
			expectError: "target pd member 1 is not ready",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)

			var mu sync.Mutex
			paths := make([]string, 0, len(test.expectPaths))
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()

				switch r.URL.Path {
				case targetPDVersionPath:
					w.WriteHeader(test.versionCode)
					if test.versionCode == http.StatusOK {
						_, _ = fmt.Fprintf(w, `{"version":%q}`, test.version)
					}
				case targetPDReadyPath:
					w.WriteHeader(test.readyCode)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer testServer.Close()

			s := &Server{httpClient: testServer.Client()}
			err := s.checkMemberReadyURL(context.Background(), 1, testServer.URL)
			if test.expectError == "" {
				re.NoError(err)
			} else {
				re.ErrorContains(err, test.expectError)
			}

			mu.Lock()
			gotPaths := append([]string(nil), paths...)
			mu.Unlock()
			re.Equal(test.expectPaths, gotPaths)
		})
	}
}
