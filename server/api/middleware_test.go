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

package api

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/requestutil"
)

func TestCaptureRequestBodyForAuditReadsBodyOnlyWhenNeeded(t *testing.T) {
	testCases := []struct {
		name       string
		labels     *audit.BackendLabels
		bodyParam  string
		expectBody string
		expectRead bool
	}{
		{
			name:   "prometheus-only",
			labels: &audit.BackendLabels{Labels: []string{audit.PrometheusHistogram}},
		},
		{
			name: "no-audit-backend",
		},
		{
			name:       "local-log",
			labels:     &audit.BackendLabels{Labels: []string{audit.LocalLogLabel, audit.PrometheusHistogram}},
			expectBody: "request-body",
			expectRead: true,
		},
		{
			name:       "already-captured",
			labels:     &audit.BackendLabels{Labels: []string{audit.LocalLogLabel}},
			bodyParam:  "captured-body",
			expectBody: "captured-body",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			body := &trackingReadCloser{Reader: strings.NewReader("request-body")}
			req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", body)
			re.NoError(err)

			info := requestutil.GetRequestInfoWithoutBody(req)
			info.BodyParam = testCase.bodyParam
			re.Equal(testCase.expectRead, captureRequestBodyForAudit(req, &info, testCase.labels))
			re.Equal(testCase.expectBody, info.BodyParam)
			re.Equal(testCase.expectRead, body.reads > 0)
			re.Equal(testCase.expectRead, body.closed)

			data, err := io.ReadAll(req.Body)
			re.NoError(err)
			re.Equal("request-body", string(data))
		})
	}
}

func TestPrepareRequestForAuditCapturesBodyForExistingRequestInfo(t *testing.T) {
	re := require.New(t)
	body := &trackingReadCloser{Reader: strings.NewReader("request-body")}
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", body)
	re.NoError(err)

	requestInfo := requestutil.GetRequestInfoWithoutBody(req)
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), requestInfo))
	requestInfo, ok := requestutil.RequestInfoFrom(req.Context())
	re.True(ok)

	labels := &audit.BackendLabels{Labels: []string{audit.LocalLogLabel}}
	req = prepareRequestForAudit(req, requestInfo, ok, labels)
	requestInfo, ok = requestutil.RequestInfoFrom(req.Context())
	re.True(ok)
	re.Equal("request-body", requestInfo.BodyParam)
	re.Greater(body.reads, 0)
	re.True(body.closed)

	data, err := io.ReadAll(req.Body)
	re.NoError(err)
	re.Equal("request-body", string(data))
}
