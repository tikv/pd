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

func TestCaptureRequestBodyForAuditReadsBodyOnlyForLocalLog(t *testing.T) {
	testCases := []struct {
		name         string
		auditEnabled bool
		labels       *audit.BackendLabels
		expectBody   bool
	}{
		{
			name:         "audit-disabled",
			auditEnabled: false,
			labels:       &audit.BackendLabels{Labels: []string{audit.LocalLogLabel}},
		},
		{
			name:         "prometheus-only",
			auditEnabled: true,
			labels:       &audit.BackendLabels{Labels: []string{audit.PrometheusHistogram}},
		},
		{
			name:         "no-audit-backend",
			auditEnabled: true,
		},
		{
			name:         "local-log",
			auditEnabled: true,
			labels:       &audit.BackendLabels{Labels: []string{audit.LocalLogLabel, audit.PrometheusHistogram}},
			expectBody:   true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", strings.NewReader("request-body"))
			re.NoError(err)

			info := requestutil.GetRequestInfoWithoutBody(req)
			captureRequestBodyForAudit(req, &info, testCase.auditEnabled, testCase.labels)
			if testCase.expectBody {
				re.Equal("request-body", info.BodyParam)
			} else {
				re.Empty(info.BodyParam)
			}

			data, err := io.ReadAll(req.Body)
			re.NoError(err)
			re.Equal("request-body", string(data))
		})
	}
}
