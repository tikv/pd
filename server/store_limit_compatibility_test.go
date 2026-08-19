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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

func TestCheckDefaultStoreLimitPersistenceMember(t *testing.T) {
	testCases := []struct {
		name          string
		statusCode    int
		body          string
		expectedError string
	}{
		{
			name:       "supported",
			statusCode: http.StatusOK,
			body:       `{"schedule":{"default-store-limit":{"add-peer":15,"remove-peer":15}}}`,
		},
		{
			name:          "pre-feature member",
			statusCode:    http.StatusOK,
			body:          `{"schedule":{"store-limit":{}}}`,
			expectedError: "PD member pd-1 does not support persisted default store limits",
		},
		{
			name:          "invalid response",
			statusCode:    http.StatusOK,
			body:          `{`,
			expectedError: "failed to decode config from PD member pd-1",
		},
		{
			name:          "unavailable member",
			statusCode:    http.StatusServiceUnavailable,
			body:          `unavailable`,
			expectedError: "failed to load config from PD member pd-1: HTTP status 503",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "true", r.Header.Get(apiutil.PDAllowFollowerHandleHeader))
				assert.Equal(t, "true", r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader))
				w.WriteHeader(testCase.statusCode)
				_, err := w.Write([]byte(testCase.body))
				assert.NoError(t, err)
			}))
			defer server.Close()

			err := checkDefaultStoreLimitPersistenceMember(
				context.Background(), server.Client(), "PD", "pd-1", []string{server.URL}, "/pd/api/v1/config")
			if testCase.expectedError == "" {
				re.NoError(err)
				return
			}
			re.ErrorContains(err, testCase.expectedError)
		})
	}
}

func TestCheckDefaultStoreLimitPersistenceMemberTriesAllClientURLs(t *testing.T) {
	re := require.New(t)
	unavailableServer := httptest.NewServer(http.NotFoundHandler())
	unavailableURL := unavailableServer.URL
	unavailableServer.Close()
	supportedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{"schedule":{"default-store-limit":{"add-peer":15,"remove-peer":15}}}`))
		assert.NoError(t, err)
	}))
	defer supportedServer.Close()

	re.NoError(checkDefaultStoreLimitPersistenceMember(
		context.Background(), supportedServer.Client(), "PD", "pd-1",
		[]string{unavailableURL, supportedServer.URL}, "/pd/api/v1/config"))
}
