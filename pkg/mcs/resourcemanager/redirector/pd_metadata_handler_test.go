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

package redirector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	bs "github.com/tikv/pd/pkg/basicserver"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestShouldHandlePDMetadataLocally(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	tests := []struct {
		method string
		path   string
		expect bool
	}{
		{http.MethodPost, "/resource-manager/api/v1/config/group", false},
		{http.MethodPut, "/resource-manager/api/v1/config/group", false},
		{http.MethodGet, "/resource-manager/api/v1/config/groups", false},
		{http.MethodGet, "/resource-manager/api/v1/config/group/test", false},
		{http.MethodDelete, "/resource-manager/api/v1/config/group/test", false},
		{http.MethodPost, "/resource-manager/api/v1/config/keyspace/service-limit", false},
		{http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/test", false},
		{http.MethodGet, "/resource-manager/api/v1/config", false},
		{http.MethodPut, "/resource-manager/api/v1/admin/log", false},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		re.Equal(tc.expect, shouldHandlePDMetadataLocally(req), "method=%s path=%s", tc.method, tc.path)
	}
}

func TestPDMetadataFallbackHandlerRejectsForbiddenForwardHeader(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	localCalled := false
	localHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		localCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := newPDMetadataFallbackHandler(localHandler)
	req := httptest.NewRequest(http.MethodPost, "/resource-manager/api/v1/config/group", nil)
	req.Header.Set(apiutil.XForbiddenForwardToMicroserviceHeader, "true")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)
	re.Equal(http.StatusNotFound, resp.Code)
	re.False(localCalled)
}

func TestNewPDMetadataHandler(t *testing.T) {
	t.Parallel()

	provider := &mockManagerProvider{}
	manager := rmserver.NewManager[*mockManagerProvider](provider)
	handler := newPDMetadataHandler(manager)
	require.NotNil(t, handler)
}

type mockManagerProvider struct{ bs.Server }

func (*mockManagerProvider) GetControllerConfig() *rmserver.ControllerConfig {
	return &rmserver.ControllerConfig{}
}

func (*mockManagerProvider) GetMeteringWriter() *metering.Writer { return nil }

func (*mockManagerProvider) AddStartCallback(...func()) {}

func (*mockManagerProvider) AddServiceReadyCallback(...func(context.Context) error) {}
