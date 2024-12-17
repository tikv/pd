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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestLabelMatcher(t *testing.T) {
	re := require.New(t)
	matcher := &LabelMatcher{"testSuccess"}
	labels1 := &BackendLabels{Labels: []string{"testFail", "testSuccess"}}
	re.True(matcher.Match(labels1))
	labels2 := &BackendLabels{Labels: []string{"testFail"}}
	re.False(matcher.Match(labels2))
}

func TestPrometheusHistogramBackend(t *testing.T) {
	re := require.New(t)
	serviceAuditHistogramTest := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds_test",
			Help:      "PD server service handling audit",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method", "caller_id", "ip"})

	prometheus.MustRegister(serviceAuditHistogramTest)

	ts := httptest.NewServer(promhttp.Handler())
	defer ts.Close()

	backend := NewPrometheusHistogramBackend(serviceAuditHistogramTest, true)
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", http.NoBody)
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

	// For test, sleep time needs longer than the push interval
	time.Sleep(time.Second)
	req, _ = http.NewRequest(http.MethodGet, ts.URL, http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	output := string(content)
	re.Contains(output, "pd_service_audit_handling_seconds_test_count{caller_id=\"user1\",ip=\"localhost\",method=\"HTTP\",service=\"test\"} 2")
	re.Contains(output, "pd_service_audit_handling_seconds_test_count{caller_id=\"user2\",ip=\"localhost\",method=\"HTTP\",service=\"test\"} 1")
}

func TestLocalLogBackendUsingFile(t *testing.T) {
	re := require.New(t)
	backend := NewLocalLogBackend(true)
	fname := testutil.InitTempFileLogger("info")
	defer os.RemoveAll(fname)
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	re.False(backend.ProcessHTTPRequest(req))
	info := requestutil.GetRequestInfo(req)
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	re.True(backend.ProcessHTTPRequest(req))
	b, _ := os.ReadFile(fname)
	output := strings.SplitN(string(b), "]", 4)
	re.Equal(
		fmt.Sprintf(" [\"audit log\"] [service-info=\"{ServiceLabel:, Method:HTTP/1.1/GET:/test, CallerID:anonymous, IP:, Port:, "+
			"StartTime:%s, URLParam:{\\\"test\\\":[\\\"test\\\"]}, BodyParam:testBody}\"]\n",
			time.Unix(info.StartTimeStamp, 0).String()),
		output[3],
	)
}

func BenchmarkLocalLogAuditUsingTerminal(b *testing.B) {
	b.StopTimer()
	backend := NewLocalLogBackend(true)
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		info := requestutil.GetRequestInfo(req)
		req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
		backend.ProcessHTTPRequest(req)
	}
}

func BenchmarkLocalLogAuditUsingFile(b *testing.B) {
	b.StopTimer()
	backend := NewLocalLogBackend(true)
	fname := testutil.InitTempFileLogger("info")
	defer os.RemoveAll(fname)
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		info := requestutil.GetRequestInfo(req)
		req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
		backend.ProcessHTTPRequest(req)
	}
}
