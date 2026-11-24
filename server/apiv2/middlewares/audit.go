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

package middlewares

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/server/config"
)

type auditServer interface {
	GetAuditBackend() []audit.Backend
	GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions
}

// Audit applies the shared HTTP audit pipeline to one route.
// Local-log backends capture consumed body bytes; metrics-only routes do not.
func Audit(serviceLabel string, backendLabels ...string) gin.HandlerFunc {
	labels := &audit.BackendLabels{Labels: backendLabels}
	return func(c *gin.Context) {
		if len(labels.Labels) == 0 {
			c.Next()
			return
		}
		svr := c.MustGet(ServerContextKey).(auditServer)
		if !svr.GetServiceMiddlewarePersistOptions().IsAuditEnabled() {
			c.Next()
			return
		}
		info, ok := requestutil.RequestInfoFrom(c.Request.Context())
		if !ok {
			info = requestutil.GetRequestInfoWithoutBody(c.Request)
		}
		info.ServiceLabel = serviceLabel
		// Keep user-controlled path parameters out of the audit log.
		info.Method = fmt.Sprintf("%s/%s:%s", c.Request.Proto, c.Request.Method, c.FullPath())
		r := c.Request.WithContext(requestutil.WithRequestInfo(c.Request.Context(), info))
		audit.ProcessHTTPRequest(c.Writer, r, labels, svr.GetAuditBackend(), func(_ http.ResponseWriter, r *http.Request) {
			c.Request = r
			c.Next()
		})
	}
}
