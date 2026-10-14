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

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/tikv/pd/server"
)

func TestGetHealthStatusReturnsUnavailableWhenEtcdClientNotStarted(t *testing.T) {
	re := require.New(t)
	h := newHealthHandler(&server.Server{}, render.New())
	w := httptest.NewRecorder()

	h.GetHealthStatus(w, httptest.NewRequest(http.MethodGet, "/pd/api/v1/health", http.NoBody))

	re.Equal(http.StatusServiceUnavailable, w.Code)
	re.Contains(w.Body.String(), "server is started, but etcd not started")
}
