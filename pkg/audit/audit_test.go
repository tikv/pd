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
	"testing"
	"time"

	. "github.com/pingcap/check"
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

func (s *testAuditSuite) TestPrometheusHistogramBackend(c *C) {

	serviceAuditHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds",
			Help:      "PD server service handling audit",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method", "component"})

	prometheus.MustRegister(serviceAuditHistogram)
	cfg := &metricutil.MetricConfig{
		PushJob:     "prometheus",
		PushAddress: "127.0.0.1:9091",
		PushInterval: typeutil.Duration{
			Duration: time.Second,
		},
	}
	metricutil.Push(cfg)

	backend := NewPrometheusHistogramBackend(serviceAuditHistogram, true)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:2379/test?test=test", nil)
	info := requestutil.GetRequestInfo(req)
	info.ServiceLabel = "test"
	req = req.WithContext(requestutil.WithRequestInfo(req.Context(), info))
	endTime := time.Now().Unix()
	req = req.WithContext(requestutil.WithEndTime(req.Context(), endTime))

	c.Assert(backend.ProcessHTTPRequest(req), Equals, true)
}
