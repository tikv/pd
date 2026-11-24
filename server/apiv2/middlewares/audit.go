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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/logutil"
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
		backends := svr.GetAuditBackend()
		metricsOnly := len(backendLabels) == 1 && backendLabels[0] == audit.PrometheusHistogram
		if metricsOnly {
			for _, backend := range backends {
				if _, ok := backend.(*audit.PrometheusBackend); !ok && backend.Match(labels) {
					// Custom backends may consume fields that Prometheus ignores.
					metricsOnly = false
					break
				}
			}
		}
		info, ok := requestutil.RequestInfoFrom(c.Request.Context())
		if !ok {
			if metricsOnly {
				info.CallerID = apiutil.GetCallerIDOnHTTP(c.Request)
				info.StartTimeStamp = time.Now().Unix()
			} else {
				info = requestutil.GetRequestInfoWithoutBody(c.Request)
			}
		}
		info.ServiceLabel = serviceLabel
		if !metricsOnly {
			// Keep the route template separate from redacted target parameters.
			info.Method = fmt.Sprintf("%s/%s:%s", c.Request.Proto, c.Request.Method, c.FullPath())
			if len(c.Params) > 0 {
				if logutil.IsInfoLogFullyRedacted() {
					info.PathParam = "?"
				} else {
					// Gin parameters contain only strings, so marshaling cannot fail.
					params, _ := json.Marshal(c.Params)
					info.PathParam = string(params)
				}
			}
		}
		r := c.Request.WithContext(requestutil.WithRequestInfo(c.Request.Context(), info))
		audit.ProcessHTTPRequest(c.Writer, r, labels, backends, func(_ http.ResponseWriter, r *http.Request) {
			c.Request = r
			c.Next()
		})
	}
}
