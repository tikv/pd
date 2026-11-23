// Copyright 2022 TiKV Project Authors.
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

package audit

import (
	"bytes"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
)

const (
	// PrometheusHistogram is label name of PrometheusCounterBackend
	PrometheusHistogram = "prometheus-histogram"
	// LocalLogLabel is label name of LocalLogBackend
	LocalLogLabel = "local-log"
)

// BackendLabels is used to store some audit backend labels.
type BackendLabels struct {
	Labels []string
}

// LabelMatcher is used to help backend implement audit.Backend
type LabelMatcher struct {
	backendLabel string
}

// Match is used to check whether backendLabel is in the labels
func (m *LabelMatcher) Match(labels *BackendLabels) bool {
	return slices.Contains(labels.Labels, m.backendLabel)
}

// Sequence is used to help backend implement audit.Backend
type Sequence struct {
	before bool
}

// ProcessBeforeHandler is used to identify whether this backend should execute before handler
func (s *Sequence) ProcessBeforeHandler() bool {
	return s.before
}

// Backend defines what function audit backend should hold
type Backend interface {
	// ProcessHTTPRequest is used to perform HTTP audit process
	ProcessHTTPRequest(req *http.Request) bool
	// Match is used to determine if the backend matches
	Match(*BackendLabels) bool
	ProcessBeforeHandler() bool
}

// PrometheusBackend is an implementation of audit.Backend
// and it uses Prometheus histogram data type to implement audit.
// Note: histogram.WithLabelValues will degrade performance.
// Please don't use it in the hot path.
type PrometheusBackend struct {
	*LabelMatcher
	*Sequence
	histogramVec *prometheus.HistogramVec
	counterVec   *prometheus.CounterVec
}

// NewPrometheusBackend returns a PrometheusBackend
func NewPrometheusBackend(histogramVec *prometheus.HistogramVec, counterVec *prometheus.CounterVec, before bool) Backend {
	return &PrometheusBackend{
		LabelMatcher: &LabelMatcher{backendLabel: PrometheusHistogram},
		Sequence:     &Sequence{before: before},
		histogramVec: histogramVec,
		counterVec:   counterVec,
	}
}

// ProcessHTTPRequest is used to implement audit.Backend
func (b *PrometheusBackend) ProcessHTTPRequest(req *http.Request) bool {
	requestInfo, ok := requestutil.RequestInfoFrom(req.Context())
	if !ok {
		return false
	}
	endTime, ok := requestutil.EndTimeFrom(req.Context())
	if !ok {
		return false
	}

	duration := float64(endTime - requestInfo.StartTimeStamp)
	if elapsed, ok := requestutil.RequestDurationFrom(req.Context()); ok {
		duration = elapsed.Seconds()
	}
	b.histogramVec.WithLabelValues(requestInfo.ServiceLabel, "HTTP").Observe(duration)

	b.counterVec.WithLabelValues(requestInfo.ServiceLabel, "HTTP", requestInfo.CallerID).Inc()

	return true
}

// LocalLogBackend is an implementation of audit.Backend
// and it uses `github.com/pingcap/log` to implement audit
type LocalLogBackend struct {
	*LabelMatcher
	*Sequence
}

// NewLocalLogBackend returns a LocalLogBackend
func NewLocalLogBackend(before bool) Backend {
	return &LocalLogBackend{
		LabelMatcher: &LabelMatcher{backendLabel: LocalLogLabel},
		Sequence:     &Sequence{before: before},
	}
}

// ProcessHTTPRequest is used to implement audit.Backend
func (*LocalLogBackend) ProcessHTTPRequest(r *http.Request) bool {
	requestInfo, ok := requestutil.RequestInfoFrom(r.Context())
	if !ok {
		return false
	}
	clientCN := ""
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		clientCN = r.TLS.VerifiedChains[0][0].Subject.CommonName
	}
	// Header-derived IP and caller ID in service-info are diagnostic claims.
	// Keep the transport peer and verified certificate identity in separate fields
	// so a caller cannot replace them with forwarding headers. Behind a proxy,
	// these fields identify the proxy connection, not the original client.
	log.Info("audit log",
		zap.String("service-info", requestInfo.String()),
		zap.String("remote-addr", r.RemoteAddr),
		zap.String("tls-client-cn", clientCN))
	return true
}

// ProcessHTTPRequest runs the selected HTTP audit backends around next.
// Local logs capture only bytes consumed by next and run after processing when
// a body is captured. Auditing never reads ahead, drains input, or replaces a
// body-read error. Fully redacted and metrics-only requests skip body capture.
func ProcessHTTPRequest(w http.ResponseWriter, r *http.Request, labels *BackendLabels, backends []Backend, next http.HandlerFunc) {
	if labels == nil || len(labels.Labels) == 0 {
		next(w, r)
		return
	}
	start := time.Now()
	info, ok := requestutil.RequestInfoFrom(r.Context())
	if !ok {
		info = requestutil.GetRequestInfoWithoutBody(r)
	}
	var body *capturedBody
	if info.BodyParam == "" && slices.Contains(labels.Labels, LocalLogLabel) {
		body = captureBody(r, &info)
	}
	ctx := requestutil.WithRequestInfo(r.Context(), info)
	// Keep ingress identity even if downstream routing rewrites the request.
	auditRequest := r.WithContext(ctx)
	r = r.WithContext(ctx)
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
			for _, backend := range backends {
				if backend.ProcessBeforeHandler() && backend.Match(labels) {
					backend.ProcessHTTPRequest(auditRequest)
				}
			}
		}

		if !hasAfterBackend {
			return
		}
		end := time.Now()
		ctx := requestutil.WithEndTime(auditRequest.Context(), end.Unix())
		ctx = requestutil.WithRequestDuration(ctx, end.Sub(start))
		auditRequest = auditRequest.WithContext(ctx)
		for _, backend := range backends {
			if !backend.ProcessBeforeHandler() && backend.Match(labels) {
				backend.ProcessHTTPRequest(auditRequest)
			}
		}
	}()
	next(w, r)
}

type capturedBody struct {
	io.ReadCloser
	mu   sync.Mutex
	data bytes.Buffer
}

func (body *capturedBody) Read(p []byte) (int, error) {
	n, err := body.ReadCloser.Read(p)
	if n > 0 {
		body.mu.Lock()
		body.data.Write(p[:n])
		body.mu.Unlock()
	}
	return n, err
}

func (body *capturedBody) String() string {
	// An HTTP transport may still be reading when an early response arrives.
	// Never hold this lock across the underlying Read, which can block.
	body.mu.Lock()
	defer body.mu.Unlock()
	return body.data.String()
}

func captureBody(r *http.Request, info *requestutil.RequestInfo) *capturedBody {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	if logutil.IsInfoLogFullyRedacted() {
		info.BodyParam = "?"
		return nil
	}
	body := &capturedBody{ReadCloser: r.Body}
	r.Body = body
	return body
}
