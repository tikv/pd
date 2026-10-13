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

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

func TestRegisterAffinityPolicyRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterAffinity(engine.Group("/pd/api/v2"))

	for _, route := range engine.Routes() {
		if route.Method == "PATCH" && route.Path == "/pd/api/v2/affinity-groups/:group_id/policy" {
			return
		}
	}
	require.Fail(t, "affinity policy route is not registered")
}

func TestAffinityBalanceErrorsHaveExplicitHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{
		{affinity.ErrBalanceBusy, http.StatusConflict},
		{affinity.ErrBalanceConflict, http.StatusConflict},
		{affinity.ErrBalanceNotReady, http.StatusServiceUnavailable},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			response := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(response)
			err := fmt.Errorf("already committed 1 group: %w", tc.err)
			require.True(t, handleAffinityError(ctx, err))
			require.Equal(t, tc.code, response.Code)
			require.Contains(t, response.Body.String(), "already committed 1 group")
		})
	}
}
