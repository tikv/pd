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
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/server/config"
)

type auditServer interface {
	GetAuditBackend() []audit.Backend
	GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions
}

// Audit selects the audit backends for one HTTP route. It shares the API v1
// audit switch, request format, redaction, and backend ordering. It does not read
// the request body. Routes without backend labels are not audited.
func Audit(serviceLabel string, backendLabels ...string) gin.HandlerFunc {
	return newAudit(serviceLabel, false, backendLabels...)
}

// AuditWithBody also records the bytes read by the handler or proxy. Body logs
// are emitted after processing so auditing never reads ahead of the consumer.
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
		if !svr.GetServiceMiddlewarePersistOptions().IsAuditEnabled() {
			c.Next()
			return
		}

		backends := svr.GetAuditBackend()
		info := requestutil.GetRequestInfoWithoutBody(c.Request)
		info.ServiceLabel = serviceLabel
		var body *auditRequestBody
		if captureBody && slices.Contains(labels.Labels, audit.LocalLogLabel) {
			body = captureAuditBody(c.Request, &info)
		}
		ctx := requestutil.WithRequestInfo(c.Request.Context(), info)
		// Keep ingress identity even if downstream routing rewrites the request.
		auditRequest := c.Request.WithContext(ctx)
		c.Request = c.Request.WithContext(ctx)
		hasAfterBackend := false
		for _, backend := range backends {
			if !backend.Match(labels) {
				continue
			}
			if backend.ProcessBeforeHandler() {
				if body == nil {
					backend.ProcessHTTPRequest(auditRequest)
				}
			} else {
				hasAfterBackend = true
			}
		}

		defer func() {
			if body != nil {
				info.BodyParam = body.String()
				auditRequest = auditRequest.WithContext(requestutil.WithRequestInfo(auditRequest.Context(), info))
				c.Request = c.Request.WithContext(requestutil.WithRequestInfo(c.Request.Context(), info))
				for _, backend := range backends {
					if backend.ProcessBeforeHandler() && backend.Match(labels) {
						backend.ProcessHTTPRequest(auditRequest)
					}
				}
			}

			if !hasAfterBackend {
				return
			}
			end := time.Now().Unix()
			c.Request = c.Request.WithContext(requestutil.WithEndTime(c.Request.Context(), end))
			auditRequest = auditRequest.WithContext(requestutil.WithEndTime(auditRequest.Context(), end))
			for _, backend := range backends {
				if !backend.ProcessBeforeHandler() && backend.Match(labels) {
					backend.ProcessHTTPRequest(auditRequest)
				}
			}
		}()
		c.Next()
	}
}

type auditRequestBody struct {
	io.ReadCloser
	mu   sync.Mutex
	data bytes.Buffer
}

func (body *auditRequestBody) Read(p []byte) (int, error) {
	n, err := body.ReadCloser.Read(p)
	if n > 0 {
		body.mu.Lock()
		body.data.Write(p[:n])
		body.mu.Unlock()
	}
	return n, err
}

func (body *auditRequestBody) String() string {
	// An HTTP transport may still be reading when an early response arrives.
	// Never hold this lock across the underlying Read, which can block.
	body.mu.Lock()
	defer body.mu.Unlock()
	return body.data.String()
}

func captureAuditBody(r *http.Request, info *requestutil.RequestInfo) *auditRequestBody {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	if logutil.IsInfoLogFullyRedacted() {
		info.BodyParam = "?"
		return nil
	}
	body := &auditRequestBody{ReadCloser: r.Body}
	r.Body = body
	return body
}
