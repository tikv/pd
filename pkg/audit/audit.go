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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/requestutil"
	"go.uber.org/zap"
)

const (
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
func (l *LocalLogBackend) ProcessHTTPRequest(r *http.Request) bool {
	requestInfo, ok := requestutil.RequestInfoFrom(r.Context())
	if !ok {
		return false
	}
	log.Info("Audit Log", zap.String("service-info", requestInfo.String()))
	return true
}
