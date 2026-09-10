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
	targetPDReadyZPath  = apiutil.CoreV2Path + "/readyz/leader-promotion"
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
			name:        "latest v8.5 version still checks ready",
			version:     "v8.5.7",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
		},
		{
			name:        "readyz version checks readyz",
			version:     "v8.6.0",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath, targetPDReadyZPath},
		},
		{
			name:        "version request failure",
			versionCode: http.StatusInternalServerError,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath},
			expectError: "failed to get target pd member 1 version",
		},
		{
			name:        "invalid version checks ready",
			version:     "invalid-version",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
		},
		{
			name:        "none version checks ready",
			version:     "None",
			versionCode: http.StatusOK,
			readyCode:   http.StatusOK,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
		},
		{
			name:        "invalid version ready request failure",
			version:     "invalid-version",
			versionCode: http.StatusOK,
			readyCode:   http.StatusInternalServerError,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
			expectError: "target pd member 1 is not ready",
		},
		{
			name:        "invalid version without ready api",
			version:     "invalid-version",
			versionCode: http.StatusOK,
			readyCode:   http.StatusNotFound,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
		},
		{
			name:        "supported version without ready api",
			version:     "v8.5.2",
			versionCode: http.StatusOK,
			readyCode:   http.StatusNotFound,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
			expectError: "target pd member 1 is not ready",
		},
		{
			name:        "ready request failure",
			version:     "v8.5.2",
			versionCode: http.StatusOK,
			readyCode:   http.StatusServiceUnavailable,
			expectPaths: []string{targetPDVersionPath, targetPDReadyPath},
			expectError: "target pd member 1 is not ready",
		},
		{
			name:        "readyz request failure",
			version:     "v8.6.0",
			versionCode: http.StatusOK,
			readyCode:   http.StatusNotFound,
			expectPaths: []string{targetPDVersionPath, targetPDReadyZPath},
			expectError: "target pd member 1 is not ready",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)

			var mu sync.Mutex
			paths := make([]string, 0, len(test.expectPaths))
			allowFollowerHeaders := make([]string, 0, len(test.expectPaths))
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				allowFollowerHeaders = append(allowFollowerHeaders, r.Header.Get(apiutil.PDAllowFollowerHandleHeader))
				mu.Unlock()

				switch r.URL.Path {
				case targetPDVersionPath:
					w.WriteHeader(test.versionCode)
					if test.versionCode == http.StatusOK {
						_, _ = fmt.Fprintf(w, `{"version":%q}`, test.version)
					}
				case targetPDReadyPath, targetPDReadyZPath:
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
			gotAllowFollowerHeaders := append([]string(nil), allowFollowerHeaders...)
			mu.Unlock()
			re.Equal(test.expectPaths, gotPaths)
			re.Len(gotAllowFollowerHeaders, len(test.expectPaths))
			for _, header := range gotAllowFollowerHeaders {
				re.Equal("true", header)
			}
		})
	}
}

func TestCheckMemberReadyURLsContinuesAfterAttemptTimeout(t *testing.T) {
	re := require.New(t)

	slowServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slowServer.Close()

	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != targetPDVersionPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, `{"version":"v8.5.1"}`)
	}))
	defer readyServer.Close()

	s := &Server{httpClient: slowServer.Client()}
	clientURLs := []string{slowServer.URL, readyServer.URL}
	triedURLs, ready, err := s.checkMemberReadyURLs(context.Background(), 1, clientURLs)
	re.NoError(err)
	re.True(ready)
	re.Equal(clientURLs, triedURLs)
}

func TestExcludeClientURLs(t *testing.T) {
	re := require.New(t)
	re.Equal(
		[]string{"http://127.0.0.1:2379", "http://127.0.0.1:2381"},
		excludeClientURLs(
			[]string{"http://127.0.0.1:2379", "http://127.0.0.1:2380", "http://127.0.0.1:2381", "http://127.0.0.1:2381"},
			[]string{"http://127.0.0.1:2380"},
		),
	)
}
