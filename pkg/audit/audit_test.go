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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestLabelMatcher(t *testing.T) {
	re := require.New(t)
	matcher := &LabelMatcher{"testSuccess"}
	labels1 := &BackendLabels{Labels: []string{"testFail", "testSuccess"}}
	re.True(matcher.Match(labels1))
	labels2 := &BackendLabels{Labels: []string{"testFail"}}
	re.False(matcher.Match(labels2))
}

func TestPrometheusBackend(t *testing.T) {
	re := require.New(t)
	serviceAuditHistogramTest := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds_test",
			Help:      "PD server service handling audit",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method"})

	serviceAuditCounterTest := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_requests_total_test",
			Help:      "Total number of service requests for audit test",
		}, []string{"service", "method", "caller_id"})

	registry := prometheus.NewRegistry()
	registry.MustRegister(serviceAuditHistogramTest)
	registry.MustRegister(serviceAuditCounterTest)

	ts := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	defer ts.Close()

	backend := NewPrometheusBackend(serviceAuditHistogramTest, serviceAuditCounterTest, true)
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", http.NoBody)
	re.NoError(err)
	info := requestutil.GetRequestInfo(req)
	info.ServiceLabel = "test"
	info.CallerID = "user1"
	info.IP = "localhost"
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	re.False(backend.ProcessHTTPRequest(req))

	endTime := time.Now().Unix() + 20
	req = req.WithContext(requestutil.WithEndTime(req.Context(), endTime))

	re.True(backend.ProcessHTTPRequest(req))
	re.True(backend.ProcessHTTPRequest(req))

	info.CallerID = "user2"
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	re.True(backend.ProcessHTTPRequest(req))

	req, err = http.NewRequest(http.MethodGet, ts.URL, http.NoBody)
	re.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	re.NoError(err)
	output := string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_test_count{method=\"HTTP\",service=\"test\"}")
	re.Contains(output, "pd_service_audit_requests_total_test{caller_id=\"user1\",method=\"HTTP\",service=\"test\"}")
}

func TestLocalLogBackendUsingFile(t *testing.T) {
	re := require.New(t)
	backend := NewLocalLogBackend(true)
	fname := testutil.InitTempFileLogger(t, "info")
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	re.NoError(err)
	re.False(backend.ProcessHTTPRequest(req))
	info := requestutil.GetRequestInfo(req)
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	re.True(backend.ProcessHTTPRequest(req))
	b, err := os.ReadFile(fname)
	re.NoError(err)
	output := strings.SplitN(string(b), "]", 4)
	re.Equal(
		fmt.Sprintf(" [\"audit log\"] [service-info=\"{ServiceLabel:, Method:HTTP/1.1/GET:/test, CallerID:anonymous, IP:, Port:, "+
			"StartTime:%s, URLParam:{\\\"test\\\":[\\\"test\\\"]}, BodyParam:testBody}\"] [remote-addr=] [tls-client-cn=]\n",
			time.Unix(info.StartTimeStamp, 0).String()),
		output[3],
	)
}

func TestLocalLogBackendRedact(t *testing.T) {
	re := require.New(t)
	var (
		lg    *zap.Logger
		props *log.ZapProperties
	)
	re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogON))
	defer func() {
		re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogOFF))
	}()
	backend := NewLocalLogBackend(true)
	fname := testutil.InitTempFileLogger(t, "info")
	body := `{"id":"test","data":[{"start_key":"7480000000000000ff","end_key":"7480000000000001ff"}]}`
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:2379/test?key=7480000000000000ff", strings.NewReader(body))
	re.NoError(err)
	info := requestutil.GetRequestInfo(req)
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	re.True(backend.ProcessHTTPRequest(req))
	b, err := os.ReadFile(fname)
	re.NoError(err)
	output := strings.SplitN(string(b), "]", 4)
	re.Equal(
		fmt.Sprintf(" [\"audit log\"] [service-info=\"{ServiceLabel:, Method:HTTP/1.1/POST:/test, CallerID:anonymous, IP:, Port:, "+
			"StartTime:%s, URLParam:?, BodyParam:?}\"] [remote-addr=] [tls-client-cn=]\n",
			time.Unix(info.StartTimeStamp, 0).String()),
		output[3],
	)
	re.NotContains(string(b), "7480000000000000ff")
}

func captureAuditJSON(t *testing.T) *bytes.Buffer {
	t.Helper()
	output := &bytes.Buffer{}
	syncer := zapcore.Lock(zapcore.AddSync(output))
	logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Format: "json"}, syncer, syncer)
	require.NoError(t, err)
	t.Cleanup(log.ReplaceGlobals(logger, props))
	return output
}

func TestLocalLogBackendConnectionOrigin(t *testing.T) {
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: "component-client"}}
	for _, testCase := range []struct {
		name       string
		remoteAddr string
		tlsState   *tls.ConnectionState
		clientCN   string
	}{
		{name: "plain HTTP", remoteAddr: "192.0.2.10:12345"},
		{name: "IPv6", remoteAddr: "[2001:db8::10]:12345"},
		{name: "no peer address"},
		{
			name:       "unverified certificate",
			remoteAddr: "192.0.2.10:12345",
			tlsState:   &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}},
		},
		{
			name:       "verified certificate",
			remoteAddr: "192.0.2.10:12345",
			tlsState: &tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{certificate},
				VerifiedChains:   [][]*x509.Certificate{{certificate}},
			},
			clientCN: "component-client",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			output := captureAuditJSON(t)
			req := httptest.NewRequest(http.MethodGet, "http://pd/test", http.NoBody)
			req.RemoteAddr, req.TLS = testCase.remoteAddr, testCase.tlsState
			req.Header.Set(apiutil.XForwardedForHeader, "203.0.113.99, 203.0.113.100")
			req.Header.Set(apiutil.XRealIPHeader, "203.0.113.101")
			req.Header.Set(apiutil.XCallerIDHeader, `forged","remote-addr":"attacker","tls-client-cn":"admin`)
			info := requestutil.GetRequestInfo(req)
			re.Equal("203.0.113.99", info.IP)
			req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
			re.True(NewLocalLogBackend(true).ProcessHTTPRequest(req))
			var entry map[string]any
			re.NoError(json.Unmarshal(output.Bytes(), &entry))
			re.Equal(testCase.remoteAddr, entry["remote-addr"])
			re.Equal(testCase.clientCN, entry["tls-client-cn"])
			re.Contains(entry["service-info"], "IP:203.0.113.99")
		})
	}
}

func TestLocalLogBackendForwardedConnection(t *testing.T) {
	re := require.New(t)
	output := captureAuditJSON(t)
	peer := make(chan string, 1)
	backend := NewLocalLogBackend(true)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := requestutil.GetRequestInfo(r)
		r = r.WithContext(requestutil.WithRequestInfo(r.Context(), info))
		if !backend.ProcessHTTPRequest(r) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		peer <- r.RemoteAddr
	}))
	defer target.Close()
	targetURL, err := url.Parse(target.URL)
	re.NoError(err)
	proxy := httptest.NewServer(httputil.NewSingleHostReverseProxy(targetURL))
	defer proxy.Close()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxy.URL, http.NoBody)
	re.NoError(err)
	req.Header.Set(apiutil.XForwardedForHeader, "203.0.113.99")
	resp, err := proxy.Client().Do(req)
	re.NoError(err)
	re.NoError(resp.Body.Close())
	re.Equal(http.StatusOK, resp.StatusCode)
	var entry map[string]any
	re.NoError(json.Unmarshal(output.Bytes(), &entry))
	re.Equal(<-peer, entry["remote-addr"])
	re.NotEmpty(entry["remote-addr"])
	re.NotEqual("203.0.113.99", entry["remote-addr"])
	re.Contains(entry["service-info"], "IP:203.0.113.99")
}

func BenchmarkLocalLogAuditUsingTerminal(b *testing.B) {
	re := require.New(b)

	backend := NewLocalLogBackend(true)
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	re.NoError(err)

	for b.Loop() {
		info := requestutil.GetRequestInfo(req)
		req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
		backend.ProcessHTTPRequest(req)
	}
}

func BenchmarkLocalLogAuditUsingFile(b *testing.B) {
	re := require.New(b)

	backend := NewLocalLogBackend(true)
	testutil.InitTempFileLogger(b, "info")
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	re.NoError(err)

	for b.Loop() {
		info := requestutil.GetRequestInfo(req)
		req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
		re.True(backend.ProcessHTTPRequest(req))
	}
}
