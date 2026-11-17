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
	"bytes"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
)

type auditServer interface {
	GetAuditBackend() []audit.Backend
	IsHTTPAuditEnabled() (bool, error)
}

// Limit each logged body to 4 KiB without limiting the handler's input.
const maxAuditBodyBytes = 4 << 10

// Audit selects the audit backends for one HTTP route. It shares the API v1
// audit switch, request format, redaction, and backend ordering. It does not read
// the request body. Routes without backend labels are not audited.
func Audit(serviceLabel string, backendLabels ...string) gin.HandlerFunc {
	return newAudit(serviceLabel, false, backendLabels...)
}

// AuditWithBody also captures a bounded body prefix for routes that consume it.
// Fully redacted bodies are left unread, and handlers retain the complete stream.
func AuditWithBody(serviceLabel string, backendLabels ...string) gin.HandlerFunc {
	return newAudit(serviceLabel, true, backendLabels...)
}

func newAudit(serviceLabel string, captureBody bool, backendLabels ...string) gin.HandlerFunc {
	labels := &audit.BackendLabels{Labels: backendLabels}
	return func(c *gin.Context) {
		if len(labels.Labels) == 0 {
			c.Next()
			return
		}
		svr := c.MustGet(ServerContextKey).(auditServer)
		enabled, err := svr.IsHTTPAuditEnabled()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		if !enabled {
			c.Next()
			return
		}

		backends := svr.GetAuditBackend()
		info := requestutil.GetRequestInfoWithoutBody(c.Request)
		info.ServiceLabel = serviceLabel
		if captureBody && slices.Contains(labels.Labels, audit.LocalLogLabel) {
			err = captureAuditBody(c.Request, &info)
		}
		c.Request = c.Request.WithContext(requestutil.WithRequestInfo(c.Request.Context(), info))
		hasAfterBackend := false
		for _, backend := range backends {
			if !backend.Match(labels) {
				continue
			}
			if backend.ProcessBeforeHandler() {
				backend.ProcessHTTPRequest(c.Request)
			} else {
				hasAfterBackend = true
			}
		}

		if err != nil {
			// Never turn a failed body read into a successful mutation.
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		} else {
			c.Next()
		}

		if !hasAfterBackend {
			return
		}
		c.Request = c.Request.WithContext(requestutil.WithEndTime(c.Request.Context(), time.Now().Unix()))
		for _, backend := range backends {
			if !backend.ProcessBeforeHandler() && backend.Match(labels) {
				backend.ProcessHTTPRequest(c.Request)
			}
		}
	}
}

type auditRequestBody struct {
	io.Reader
	io.Closer
}

func captureAuditBody(r *http.Request, info *requestutil.RequestInfo) error {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	if logutil.IsInfoLogFullyRedacted() {
		info.BodyParam = "?"
		return nil
	}
	body := r.Body
	prefix, err := io.ReadAll(io.LimitReader(body, maxAuditBodyBytes+1))
	r.Body = &auditRequestBody{Reader: io.MultiReader(bytes.NewReader(prefix), body), Closer: body}
	if len(prefix) > maxAuditBodyBytes {
		info.BodyParam = string(prefix[:maxAuditBodyBytes]) + " [truncated]"
	} else {
		info.BodyParam = string(prefix)
	}
	return err
}
