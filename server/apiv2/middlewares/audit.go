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
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
)

type auditServer interface {
	GetAuditBackend() []audit.Backend
	IsHTTPAuditEnabled() (bool, error)
}

// Audit selects the audit backends for one HTTP route. It shares the API v1
// audit switch, request format, redaction, and backend ordering. It does not read
// the request body. Routes without backend labels are not audited.
func Audit(serviceLabel string, backendLabels ...string) gin.HandlerFunc {
	return newAudit(serviceLabel, false, backendLabels...)
}

// AuditWithBody also captures the request body for routes that consume it.
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
			// Optional auditing must not replace the route's existing response.
			log.Warn("skip HTTP audit because configuration could not be loaded", errs.ZapError(err))
			c.Next()
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
			captureAuditBody(c.Request, &info)
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

		c.Next()

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
	io.ReadCloser
	captured *strings.Reader
	readErr  error
}

func (body *auditRequestBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if body.captured.Len() > 0 {
		return body.captured.Read(p)
	}
	if body.readErr != nil {
		err := body.readErr
		body.readErr = nil
		return 0, err
	}
	return body.ReadCloser.Read(p)
}

func captureAuditBody(r *http.Request, info *requestutil.RequestInfo) {
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	if logutil.IsInfoLogFullyRedacted() {
		info.BodyParam = "?"
		return
	}
	data, err := io.ReadAll(r.Body)
	info.BodyParam = string(data)
	// Leave read errors to the handler so auditing preserves its response and
	// prevents mutations based on an incomplete body.
	r.Body = &auditRequestBody{
		ReadCloser: r.Body,
		captured:   strings.NewReader(info.BodyParam),
		readErr:    err,
	}
}
