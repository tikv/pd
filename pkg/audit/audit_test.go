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
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/metricutil"
	"github.com/tikv/pd/pkg/requestutil"
	"github.com/tikv/pd/pkg/typeutil"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuditSuite{})

type testAuditSuite struct {
}

func (s *testAuditSuite) TestLabelMatcher(c *C) {
	matcher := &LabelMatcher{"testSuccess"}
	labels1 := &BackendLabels{Labels: []string{"testFail", "testSuccess"}}
	c.Assert(matcher.Match(labels1), Equals, true)

	labels2 := &BackendLabels{Labels: []string{"testFail"}}
	c.Assert(matcher.Match(labels2), Equals, false)
}

// TestPrometheusHistogramBackend is used to test result manually
func (s *testAuditSuite) TestPrometheusHistogramBackend(c *C) {
	serviceAuditHistogramTest := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds_test",
			Help:      "PD server service handling audit",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method", "component"})

	prometheus.MustRegister(serviceAuditHistogramTest)
	cfg := &metricutil.MetricConfig{
		PushJob:     "prometheus",
		PushAddress: "127.0.0.1:9091",
		PushInterval: typeutil.Duration{
			Duration: time.Second,
		},
	}
	metricutil.Push(cfg)

	backend := NewPrometheusHistogramBackend(serviceAuditHistogramTest, true)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", nil)
	info := requestutil.GetRequestInfo(req)
	info.ServiceLabel = "test"
	info.Component = "user1"
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	c.Assert(backend.ProcessHTTPRequest(req), Equals, false)

	endTime := time.Now().Unix() + 20
	req = req.WithContext(requestutil.WithEndTime(req.Context(), endTime))

	c.Assert(backend.ProcessHTTPRequest(req), Equals, true)
	c.Assert(backend.ProcessHTTPRequest(req), Equals, true)

	info.Component = "user2"
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	c.Assert(backend.ProcessHTTPRequest(req), Equals, true)
	// For test, sleep time needs longer than the push interval
	time.Sleep(2 * time.Second)
}

func (s *testAuditSuite) TestLocalLogBackendUsingFile(c *C) {
	backend := NewLocalLogBackend(true)
	fname := initLog()
	defer os.Remove(fname)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	c.Assert(backend.ProcessHTTPRequest(req), Equals, false)
	info := requestutil.GetRequestInfo(req)
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	c.Assert(backend.ProcessHTTPRequest(req), Equals, true)
	b, _ := os.ReadFile(fname)
	output := strings.SplitN(string(b), "]", 4)
	c.Assert(output[3], Equals, fmt.Sprintf(" [\"Audit Log\"] [service-info=\"{ServiceLabel:, Method:HTTP/1.1/GET:/test, Component:anonymous, IP:, "+
		"StartTime:%s, URLParam:{\\\"test\\\":[\\\"test\\\"]}, BodyParam:testBody}\"]\n",
		time.Unix(info.StartTimeStamp, 0).String()))
}

func BenchmarkLocalLogAuditUsingTerminal(b *testing.B) {
	b.StopTimer()
	backend := NewLocalLogBackend(true)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
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
	fname := initLog()
	defer os.Remove(fname)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", strings.NewReader("testBody"))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		info := requestutil.GetRequestInfo(req)
		req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
		backend.ProcessHTTPRequest(req)
	}
}

func initLog() string {
	cfg := &log.Config{}
	f, _ := os.CreateTemp("/tmp", "pd_tests")
	fname := f.Name()
	f.Close()
	cfg.File.Filename = fname
	cfg.Level = "info"
	lg, p, _ := log.InitLogger(cfg)
	log.ReplaceGlobals(lg, p)
	return fname
}
