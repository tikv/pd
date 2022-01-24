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
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/requestutil"
)

const (
	// PrometheusHistogram is label name of PrometheusCounterBackend
	PrometheusHistogram = "prometheus-histogram"
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
	for _, item := range labels.Labels {
		if m.backendLabel == item {
			return true
		}
	}
	return false
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

type PrometheusHistogramBackend struct {
	*LabelMatcher
	*Sequence
	histogramVec *prometheus.HistogramVec
}

// NewPrometheusHistogramBackend returns a PrometheusHistogramBackend
func NewPrometheusHistogramBackend(histogramVec *prometheus.HistogramVec, before bool) Backend {
	return &PrometheusHistogramBackend{
		LabelMatcher: &LabelMatcher{backendLabel: PrometheusHistogram},
		Sequence:     &Sequence{before: before},
		histogramVec: histogramVec,
	}
}

// ProcessHTTPRequest is used to implement audit.Backend
func (b *PrometheusHistogramBackend) ProcessHTTPRequest(req *http.Request) bool {
	requestInfo := requestutil.GetRequestInfo(req)
	executionInfo := requestutil.GetExecutionInfo(req)
	b.histogramVec.WithLabelValues(requestInfo.ServiceLabel, "HTTP", requestInfo.Component).Observe(float64(executionInfo.EndTimeStamp - requestInfo.StartTimeStamp))
	return true
}
