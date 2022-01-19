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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/requestutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// BackendLabels is used to store some audit backend labels.
type BackendLabels struct {
	Labels []string
}

// BackendMatcher is used in interface `Backend`.
type BackendMatcher interface {
	Match(*BackendLabels) bool
}

// LabelMatcher implements AuditBackendMatcher
type LabelMatcher struct {
	backendLabel string
}

// Match is used to implement AuditBackendMatcher
func (m *LabelMatcher) Match(labels *BackendLabels) bool {
	for _, item := range labels.Labels {
		if m.backendLabel == item {
			return true
		}
	}
	return false
}

// Backend defines what function audit backend should hold
type Backend interface {
	// ProcessHTTPRequest is used to perform HTTP audit process
	ProcessHTTPRequest(event requestutil.RequestInfo) bool
	// AuditBackendMatcher is used to determine if the backend matches
	BackendMatcher
}

type MainLogSink struct {
	TypeMatcher
}

func (l *MainLogSink) ProcessRequest(event requestutil.RequestInfo) bool {
	fields, _ := convertRequestEventToZapFields(event)
	log.Info("Audit Log", fields...)
	return true
}

func convertRequestEventToZapFields(event requestutil.RequestInfo) ([]zapcore.Field, error) {
	fields := make([]zapcore.Field, 0, 7)
	fields = append(fields, zap.String("service-label", event.ServiceLabel))
	fields = append(fields, zap.String("method", event.Method))
	fields = append(fields, zap.String("component", event.Component))
	fields = append(fields, zap.String("ip", event.IP))
	fields = append(fields, zap.String("ts", event.TimeStamp))
	fields = append(fields, zap.String("url-param", event.URLParam))
	fields = append(fields, zap.String("body-param", event.BodyParm))
	return fields, nil
}
