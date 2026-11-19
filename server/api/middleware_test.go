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
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/urfave/negroni/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
	v2 "github.com/tikv/pd/server/apiv2/middlewares"
	"github.com/tikv/pd/server/config"
)

type testHTTPAuditServer struct {
	options  *config.ServiceMiddlewarePersistOptions
	labels   *audit.BackendLabels
	backends []audit.Backend
}

func (s *testHTTPAuditServer) GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions {
	return s.options
}

func (s *testHTTPAuditServer) GetAuditBackend() []audit.Backend {
	return s.backends
}

func (s *testHTTPAuditServer) GetServiceAuditBackendLabels(label string) *audit.BackendLabels {
	if label != "Selected" {
		return nil
	}
	return s.labels
}

type testHTTPAuditBackend struct {
	label  string
	before bool
	run    func(*http.Request)
}

func (b *testHTTPAuditBackend) Match(labels *audit.BackendLabels) bool {
	return slices.Contains(labels.Labels, b.label)
}

func (b *testHTTPAuditBackend) ProcessBeforeHandler() bool {
	return b.before
}

func (b *testHTTPAuditBackend) ProcessHTTPRequest(r *http.Request) bool {
	b.run(r)
	return true
}

type testHTTPAuditBody struct {
	io.Reader
	reads  int
	closed bool
}

func (b *testHTTPAuditBody) Read(p []byte) (int, error) {
	b.reads++
	return b.Reader.Read(p)
}

func (b *testHTTPAuditBody) Close() error {
	b.closed = true
	return io.ErrClosedPipe
}

func TestHTTPAuditParity(t *testing.T) {
	both := []string{audit.LocalLogLabel, audit.PrometheusHistogram}
	for _, version := range []string{"v1", "v2"} {
		for _, tc := range []struct {
			name         string
			labels       []string
			enabled      bool
			mode         logutil.RedactInfoLogType
			consume      int
			readErr      error
			panic        bool
			existingInfo bool
			captured     string
		}{
			{name: "unselected", enabled: true, consume: -1},
			{name: "disabled", labels: both, consume: -1},
			{name: "metrics only", labels: []string{audit.PrometheusHistogram}, enabled: true, consume: -1},
			{name: "local log", labels: []string{audit.LocalLogLabel}, enabled: true, consume: -1},
			{name: "both", labels: both, enabled: true, consume: -1},
			{name: "existing metadata", labels: both, enabled: true, consume: -1, existingInfo: true},
			{name: "already captured", labels: both, enabled: true, consume: -1, existingInfo: true, captured: "earlier-capture"},
			{name: "early rejection", labels: both, enabled: true},
			{name: "partial consumption", labels: both, enabled: true, consume: 3},
			{name: "read error", labels: both, enabled: true, consume: -1, readErr: io.ErrUnexpectedEOF},
			{name: "fully redacted", labels: both, enabled: true, mode: logutil.RedactInfoLogON, consume: -1, readErr: io.ErrUnexpectedEOF},
			{name: "marker", labels: both, enabled: true, mode: logutil.RedactInfoLogMarker, consume: -1, readErr: io.ErrUnexpectedEOF},
			{name: "panic", labels: both, enabled: true, consume: -1, panic: true},
		} {
			t.Run(version+"/"+tc.name, func(t *testing.T) {
				re := require.New(t)
				var logger *zap.Logger
				var props *log.ZapProperties
				re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, tc.mode))
				t.Cleanup(func() { re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, logutil.RedactInfoLogOFF)) })
				const payload = "0123456789"
				var reader io.Reader = strings.NewReader(payload)
				if tc.readErr != nil {
					reader = io.MultiReader(reader, iotest.ErrReader(tc.readErr))
				}
				body := &testHTTPAuditBody{Reader: reader}
				var events []string
				var loggedBody string
				backend := func(label string, before bool) audit.Backend {
					return &testHTTPAuditBackend{label: label, before: before, run: func(r *http.Request) {
						info, ok := requestutil.RequestInfoFrom(r.Context())
						re.True(ok)
						re.Equal("Selected", info.ServiceLabel)
						_, hasEnd := requestutil.EndTimeFrom(r.Context())
						re.Equal(!before, hasEnd)
						if before {
							loggedBody = info.BodyParam
						}
						events = append(events, label)
					}}
				}
				svr := &testHTTPAuditServer{
					options:  config.NewServiceMiddlewarePersistOptions(config.NewServiceMiddlewareConfig()),
					labels:   &audit.BackendLabels{Labels: tc.labels},
					backends: []audit.Backend{backend(audit.LocalLogLabel, true), backend(audit.PrometheusHistogram, false)},
				}
				svr.options.SetAuditConfig(&config.AuditConfig{EnableAudit: tc.enabled})
				handler := func(w http.ResponseWriter, r *http.Request) {
					re.Zero(body.reads, "auditing must not read ahead of the handler")
					re.False(body.closed)
					if tc.consume < 0 {
						data, err := io.ReadAll(r.Body)
						re.Equal(payload, string(data))
						if tc.readErr == nil {
							re.NoError(err)
						} else {
							re.ErrorIs(err, tc.readErr)
						}
					} else if tc.consume > 0 {
						data := make([]byte, tc.consume)
						_, err := io.ReadFull(r.Body, data)
						re.NoError(err)
						re.Equal(payload[:tc.consume], string(data))
					}
					re.ErrorIs(r.Body.Close(), io.ErrClosedPipe)
					re.True(body.closed)
					events = append(events, "handler")
					if tc.panic {
						panic("handler failed")
					}
					w.WriteHeader(http.StatusBadRequest)
				}
				var router http.Handler
				if version == "v1" {
					r := mux.NewRouter()
					r.Handle("/selected", negroni.New(&auditMiddleware{svr: svr}, negroni.WrapFunc(handler))).Name("Selected")
					router = r
				} else {
					r := gin.New()
					r.Use(func(c *gin.Context) { c.Set(v2.ServerContextKey, svr) })
					r.POST("/selected", v2.Audit("Selected", tc.labels...), func(c *gin.Context) { handler(c.Writer, c.Request) })
					router = r
				}
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodPost, "/selected", body)
				if tc.existingInfo {
					info := requestutil.GetRequestInfoWithoutBody(request)
					info.ServiceLabel = "Selected"
					info.BodyParam = tc.captured
					request = request.WithContext(requestutil.WithRequestInfo(request.Context(), info))
				}
				serve := func() { router.ServeHTTP(recorder, request) }
				if tc.panic {
					re.PanicsWithValue("handler failed", serve)
				} else {
					serve()
					re.Equal(http.StatusBadRequest, recorder.Code)
				}
				expected := []string{"handler"}
				if tc.enabled && slices.Contains(tc.labels, audit.LocalLogLabel) {
					if tc.captured != "" {
						expected = append([]string{audit.LocalLogLabel}, expected...)
						re.Equal(tc.captured, loggedBody)
					} else if tc.mode == logutil.RedactInfoLogON {
						expected = append([]string{audit.LocalLogLabel}, expected...)
						re.Equal("?", loggedBody)
					} else {
						expected = append(expected, audit.LocalLogLabel)
						consumed := tc.consume
						if consumed < 0 {
							consumed = len(payload)
						}
						re.Equal(payload[:consumed], loggedBody)
					}
				}
				if tc.enabled && slices.Contains(tc.labels, audit.PrometheusHistogram) {
					expected = append(expected, audit.PrometheusHistogram)
				}
				re.Equal(expected, events)
			})
		}
	}
}
