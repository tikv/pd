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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type testAuditServer struct {
	options  *config.ServiceMiddlewarePersistOptions
	backends []audit.Backend
}

func (s *testAuditServer) GetAuditBackend() []audit.Backend {
	return s.backends
}

func (s *testAuditServer) GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions {
	return s.options
}

func newTestAuditServer(backends ...audit.Backend) *testAuditServer {
	return &testAuditServer{
		options:  config.NewServiceMiddlewarePersistOptions(config.NewServiceMiddlewareConfig()),
		backends: backends,
	}
}

type recordingAuditBackend struct {
	label  string
	before bool
	run    func(*http.Request)
}

func (b *recordingAuditBackend) Match(labels *audit.BackendLabels) bool {
	return slices.Contains(labels.Labels, b.label)
}

func (b *recordingAuditBackend) ProcessBeforeHandler() bool {
	return b.before
}

func (b *recordingAuditBackend) ProcessHTTPRequest(r *http.Request) bool {
	b.run(r)
	return true
}

type auditReadCloser struct {
	io.Reader
	beforeRead func()
	reads      int
	bytesRead  int
	closed     bool
}

func (r *auditReadCloser) Read(p []byte) (int, error) {
	if r.beforeRead != nil {
		r.beforeRead()
	}
	r.reads++
	n, err := r.Reader.Read(p)
	r.bytesRead += n
	return n, err
}

func (r *auditReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestAuditBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		labels     []string
		enabled    bool
		capture    bool
		expectInfo bool
		events     []string
	}{
		{name: "no labels", enabled: true, events: []string{"handler"}},
		{name: "disabled", labels: []string{audit.LocalLogLabel}, events: []string{"handler"}},
		{name: "metrics only", labels: []string{audit.PrometheusHistogram}, enabled: true, expectInfo: true, events: []string{"handler", "metrics"}},
		{name: "local log", labels: []string{audit.LocalLogLabel}, enabled: true, capture: true, expectInfo: true, events: []string{"handler", "log"}},
		{name: "both", labels: []string{audit.LocalLogLabel, audit.PrometheusHistogram}, enabled: true, capture: true, expectInfo: true, events: []string{"handler", "log", "metrics"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var events []string
			body := &auditReadCloser{Reader: strings.NewReader("request-body")}
			checkInfo := func(r *http.Request) {
				info, ok := requestutil.RequestInfoFrom(r.Context())
				re.True(ok)
				re.Equal("CreateKeyspace", info.ServiceLabel)
				re.Contains(info.Method, "POST:/pd/api/v2/keyspaces")
				if tc.capture {
					re.Equal("request-body", info.BodyParam)
				} else {
					re.Empty(info.BodyParam)
				}
			}
			svr := newTestAuditServer(
				&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
					checkInfo(r)
					_, ok := requestutil.EndTimeFrom(r.Context())
					re.False(ok)
					events = append(events, "log")
				}},
				&recordingAuditBackend{label: audit.PrometheusHistogram, run: func(r *http.Request) {
					checkInfo(r)
					end, ok := requestutil.EndTimeFrom(r.Context())
					re.True(ok)
					info, _ := requestutil.RequestInfoFrom(r.Context())
					re.GreaterOrEqual(end, info.StartTimeStamp)
					events = append(events, "metrics")
				}},
			)
			svr.options.SetAuditConfig(&config.AuditConfig{EnableAudit: tc.enabled})
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			router.POST("/pd/api/v2/keyspaces", Audit("CreateKeyspace", tc.labels...), func(c *gin.Context) {
				re.Zero(body.reads)
				re.False(body.closed)
				_, ok := requestutil.RequestInfoFrom(c.Request.Context())
				re.Equal(tc.expectInfo, ok)
				data, err := io.ReadAll(c.Request.Body)
				re.NoError(err)
				re.Equal("request-body", string(data))
				re.NoError(c.Request.Body.Close())
				events = append(events, "handler")
				c.Status(http.StatusBadRequest)
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/pd/api/v2/keyspaces", body))
			re.Equal(http.StatusBadRequest, recorder.Code)
			re.Equal(tc.events, events)
		})
	}
}

func TestAuditOptionalRoutesAndLiveSwitch(t *testing.T) {
	re := require.New(t)
	calls := 0
	svr := newTestAuditServer(&recordingAuditBackend{
		label: audit.LocalLogLabel, before: true,
		run: func(*http.Request) { calls++ },
	})
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
	handler := func(c *gin.Context) { c.Status(http.StatusNoContent) }
	router.POST("/selected", Audit("Selected", audit.LocalLogLabel), handler)
	router.POST("/plain", handler)
	for _, tc := range []struct {
		path    string
		enabled bool
		calls   int
	}{
		{path: "/plain", enabled: true},
		{path: "/selected"},
		{path: "/selected", enabled: true, calls: 1},
		{path: "/selected", calls: 1},
		{path: "/plain", enabled: true, calls: 1},
	} {
		svr.options.SetAuditConfig(&config.AuditConfig{EnableAudit: tc.enabled})
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, tc.path, http.NoBody))
		re.Equal(http.StatusNoContent, recorder.Code)
		re.Equal(tc.calls, calls)
	}
}

func TestAuditWithoutLabelsDoesNotRequireServer(t *testing.T) {
	router := gin.New()
	router.GET("/plain", Audit("Plain"), func(c *gin.Context) { c.Status(http.StatusNoContent) })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/plain", http.NoBody))
	require.Equal(t, http.StatusNoContent, recorder.Code)
}

func TestAuditUsesV1FormatAndRedaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode logutil.RedactInfoLogType
	}{
		{name: "disabled", mode: logutil.RedactInfoLogOFF},
		{name: "enabled", mode: logutil.RedactInfoLogON},
		{name: "marker", mode: logutil.RedactInfoLogMarker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var logger *zap.Logger
			var props *log.ZapProperties
			re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, tc.mode))
			t.Cleanup(func() {
				re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, logutil.RedactInfoLogOFF))
			})
			output := &bytes.Buffer{}
			syncer := zapcore.Lock(zapcore.AddSync(output))
			logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Format: "json"}, syncer, syncer)
			re.NoError(err)
			t.Cleanup(log.ReplaceGlobals(logger, props))

			backend := audit.NewLocalLogBackend(true)
			svr := newTestAuditServer(backend)
			body := []byte(`{"name":"private-value"}`)
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			var recordedInfo requestutil.RequestInfo
			router.POST("/selected", Audit("CreateKeyspace", audit.LocalLogLabel), func(c *gin.Context) {
				var ok bool
				recordedInfo, ok = requestutil.RequestInfoFrom(c.Request.Context())
				re.True(ok)
				data, err := io.ReadAll(c.Request.Body)
				re.NoError(err)
				re.Equal(body, data)
				recordedInfo.BodyParam = string(data)
				c.Request.RemoteAddr = "192.0.2.20:54321"
				c.Request.TLS = nil
				c.Request.URL.Path = "/rewritten"
				c.AbortWithStatus(http.StatusBadRequest)
			})
			request := httptest.NewRequest(http.MethodPost, "/selected?key=private-value", bytes.NewReader(body))
			request.RemoteAddr = "192.0.2.10:12345"
			request.Header.Set(apiutil.XForwardedForHeader, "203.0.113.99")
			request.Header.Set(apiutil.XCallerIDHeader, "claimed-caller")
			certificate := &x509.Certificate{Subject: pkix.Name{CommonName: "verified-client"}}
			request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certificate}}}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			re.Equal(http.StatusBadRequest, recorder.Code)

			re.True(backend.ProcessHTTPRequest(request.WithContext(requestutil.WithRequestInfo(request.Context(), recordedInfo))))
			lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
			re.Len(lines, 2)
			var v2, v1 map[string]any
			re.NoError(json.Unmarshal(lines[0], &v2))
			re.NoError(json.Unmarshal(lines[1], &v1))
			for _, field := range []string{"message", "service-info", "remote-addr", "tls-client-cn"} {
				re.Equal(v1[field], v2[field], field)
			}
			re.Equal("audit log", v2["message"])
			re.Equal("192.0.2.10:12345", v2["remote-addr"])
			re.Equal("verified-client", v2["tls-client-cn"])
			re.Contains(v2["service-info"], "IP:203.0.113.99")
			re.Contains(v2["service-info"], "CallerID:claimed-caller")
			if tc.mode == logutil.RedactInfoLogON {
				re.NotContains(string(lines[0]), "private-value")
			} else {
				re.Contains(string(lines[0]), "private-value")
			}
		})
	}
}

func TestAuditLocalLogDoesNotSetEndTime(t *testing.T) {
	re := require.New(t)
	svr := newTestAuditServer(audit.NewLocalLogBackend(true))
	router := gin.New()
	var handlerRequest *http.Request
	router.Use(func(c *gin.Context) {
		c.Set(ServerContextKey, svr)
		c.Next()
		re.Same(handlerRequest, c.Request)
		_, ok := requestutil.EndTimeFrom(c.Request.Context())
		re.False(ok)
	})
	router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
		handlerRequest = c.Request
		c.Status(http.StatusNoContent)
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", http.NoBody))
	re.Equal(http.StatusNoContent, recorder.Code)
}

func TestAuditCompleteBody(t *testing.T) {
	for _, size := range []int{0, 10, 4 << 20} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			re := require.New(t)
			payload := strings.Repeat("x", size)
			body := &auditReadCloser{Reader: strings.NewReader(payload)}
			var recorded string
			svr := newTestAuditServer(&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
				info, ok := requestutil.RequestInfoFrom(r.Context())
				re.True(ok)
				recorded = info.BodyParam
			}})
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
				re.Zero(body.bytesRead)
				re.False(body.closed)
				info, ok := requestutil.RequestInfoFrom(c.Request.Context())
				re.True(ok)
				re.Empty(info.BodyParam)
				data, err := io.ReadAll(c.Request.Body)
				re.NoError(err)
				re.Equal(payload, string(data))
				re.NoError(c.Request.Body.Close())
				re.True(body.closed)
				c.Status(http.StatusNoContent)
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", body))
			re.Equal(http.StatusNoContent, recorder.Code)
			re.Equal(payload, recorded)
		})
	}
}

func TestAuditBodyReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader func(string) io.Reader
		err    error
	}{
		{name: "unexpected EOF", reader: func(payload string) io.Reader {
			return io.MultiReader(strings.NewReader(payload), iotest.ErrReader(io.ErrUnexpectedEOF))
		}, err: io.ErrUnexpectedEOF},
		{name: "one-shot error", reader: func(payload string) io.Reader {
			return iotest.TimeoutReader(strings.NewReader(payload))
		}, err: iotest.ErrTimeout},
	} {
		for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
			t.Run(tc.name+"/"+strconv.Itoa(status), func(t *testing.T) {
				re := require.New(t)
				payload := "partial body"
				body := &auditReadCloser{Reader: tc.reader(payload)}
				var recorded string
				svr := newTestAuditServer(&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
					info, ok := requestutil.RequestInfoFrom(r.Context())
					re.True(ok)
					recorded = info.BodyParam
				}})
				router := gin.New()
				router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
				called := false
				router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
					called = true
					re.False(body.closed)
					info, ok := requestutil.RequestInfoFrom(c.Request.Context())
					re.True(ok)
					re.Empty(info.BodyParam)
					data, err := io.ReadAll(c.Request.Body)
					re.Equal(payload, string(data))
					re.ErrorIs(err, tc.err)
					re.NoError(c.Request.Body.Close())
					re.True(body.closed)
					c.AbortWithStatus(status)
				})
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", body))
				re.Equal(status, recorder.Code)
				re.True(called)
				re.Equal(payload, recorded)
			})
		}
	}
}

func TestAuditSkipsUnusedAndRedactedBodies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    logutil.RedactInfoLogType
		capture bool
	}{
		{name: "metrics only", mode: logutil.RedactInfoLogOFF},
		{name: "fully redacted", mode: logutil.RedactInfoLogON, capture: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var logger *zap.Logger
			var props *log.ZapProperties
			re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, tc.mode))
			t.Cleanup(func() { re.NoError(logutil.SetupLogger(&log.Config{}, &logger, &props, logutil.RedactInfoLogOFF)) })
			body := &auditReadCloser{Reader: strings.NewReader(strings.Repeat("x", 4<<20))}
			svr := newTestAuditServer()
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			middleware := Audit("Selected", audit.PrometheusHistogram)
			if tc.capture {
				middleware = Audit("Selected", audit.LocalLogLabel)
			}
			router.POST("/selected", middleware, func(c *gin.Context) {
				re.Zero(body.reads)
				re.False(body.closed)
				data, err := io.ReadAll(c.Request.Body)
				re.NoError(err)
				re.Len(data, 4<<20)
				re.NoError(c.Request.Body.Close())
				c.Status(http.StatusNoContent)
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", body))
			re.Equal(http.StatusNoContent, recorder.Code)
		})
	}
}

func TestAuditOnlyCapturesConsumedBody(t *testing.T) {
	for _, consumed := range []int{0, 3, 10} {
		t.Run(strconv.Itoa(consumed), func(t *testing.T) {
			re := require.New(t)
			const payload = "0123456789"
			body := &auditReadCloser{Reader: strings.NewReader(payload)}
			var recorded string
			svr := newTestAuditServer(&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
				info, ok := requestutil.RequestInfoFrom(r.Context())
				re.True(ok)
				recorded = info.BodyParam
			}})
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
				re.Zero(body.reads)
				data := make([]byte, consumed)
				_, err := io.ReadFull(c.Request.Body, data)
				re.NoError(err)
				re.Equal(payload[:consumed], string(data))
				c.AbortWithStatus(http.StatusBadRequest)
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", body))
			re.Equal(http.StatusBadRequest, recorder.Code)
			re.Equal(consumed, body.bytesRead)
			re.Equal(payload[:consumed], recorded)
			remaining, err := io.ReadAll(body)
			re.NoError(err)
			re.Equal(payload[consumed:], string(remaining))
		})
	}
}

func TestAuditBodyReadDoesNotDelayResponse(t *testing.T) {
	re := require.New(t)
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		re.NoError(reader.Close())
		re.NoError(writer.Close())
	})
	started := make(chan struct{})
	finished := make(chan error, 1)
	body := &auditReadCloser{Reader: reader, beforeRead: func() { close(started) }}
	logged := false
	svr := newTestAuditServer(&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
		info, ok := requestutil.RequestInfoFrom(r.Context())
		re.True(ok)
		re.Empty(info.BodyParam)
		logged = true
	}})
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
	router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
		requestBody := c.Request.Body
		go func() {
			_, err := requestBody.Read(make([]byte, 1))
			finished <- err
		}()
		<-started
		c.AbortWithStatus(http.StatusBadRequest)
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", body))
	re.Equal(http.StatusBadRequest, recorder.Code)
	re.True(logged)
	// An early proxy response must not wait for its asynchronous body reader.
	re.NoError(reader.Close())
	re.ErrorIs(<-finished, io.ErrClosedPipe)
}

func TestAuditBodyLogOnPanic(t *testing.T) {
	re := require.New(t)
	var recorded string
	svr := newTestAuditServer(&recordingAuditBackend{label: audit.LocalLogLabel, before: true, run: func(r *http.Request) {
		info, ok := requestutil.RequestInfoFrom(r.Context())
		re.True(ok)
		recorded = info.BodyParam
	}})
	router := gin.New()
	router.Use(gin.CustomRecovery(func(c *gin.Context, _ any) { c.AbortWithStatus(http.StatusInternalServerError) }))
	router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
	router.POST("/selected", Audit("Selected", audit.LocalLogLabel), func(c *gin.Context) {
		data, err := io.ReadAll(c.Request.Body)
		re.NoError(err)
		re.Equal("request-body", string(data))
		panic("handler failed")
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/selected", strings.NewReader("request-body")))
	re.Equal(http.StatusInternalServerError, recorder.Code)
	re.Equal("request-body", recorded)
}

func BenchmarkAuditSelection(b *testing.B) {
	gin.SetMode(gin.ReleaseMode)
	for _, tc := range []struct {
		name    string
		enabled bool
		labels  []string
	}{
		{name: "without-middleware", enabled: true},
		{name: "no-labels", enabled: true, labels: []string{}},
		{name: "disabled", labels: []string{audit.LocalLogLabel}},
		{name: "metrics-only", enabled: true, labels: []string{audit.PrometheusHistogram}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			svr := newTestAuditServer()
			svr.options.SetAuditConfig(&config.AuditConfig{EnableAudit: tc.enabled})
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(ServerContextKey, svr) })
			handlers := gin.HandlersChain{func(c *gin.Context) { c.Status(http.StatusNoContent) }}
			if tc.labels != nil {
				handlers = append(gin.HandlersChain{Audit("Benchmark", tc.labels...)}, handlers...)
			}
			router.POST("/test", handlers...)
			request := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(strings.Repeat("x", 1<<20)))
			recorder := httptest.NewRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				router.ServeHTTP(recorder, request)
			}
		})
	}
}
