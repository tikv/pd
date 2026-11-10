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
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/server/config"
)

type auditServer interface {
	GetAuditBackend() []audit.Backend
	GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions
}

// Audit selects the audit backends for one HTTP route. It shares the API v1
// audit switch, request format, redaction, and backend ordering. Routes without
// this middleware or without backend labels are not audited.
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
		info := requestutil.GetRequestInfoWithoutBody(c.Request)
		info.ServiceLabel = serviceLabel
		if slices.Contains(labels.Labels, audit.LocalLogLabel) {
			info.CaptureBody(c.Request)
		}
		c.Request = c.Request.WithContext(requestutil.WithRequestInfo(c.Request.Context(), info))
		for _, backend := range backends {
			if backend.ProcessBeforeHandler() && backend.Match(labels) {
				backend.ProcessHTTPRequest(c.Request)
			}
		}

		c.Next()

		c.Request = c.Request.WithContext(requestutil.WithEndTime(c.Request.Context(), time.Now().Unix()))
		for _, backend := range backends {
			if !backend.ProcessBeforeHandler() && backend.Match(labels) {
				backend.ProcessHTTPRequest(c.Request)
			}
		}
	}
}
