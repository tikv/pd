// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/retry"
	"go.uber.org/atomic"
)

func TestPDAddrNormalization(t *testing.T) {
	re := require.New(t)
	c := NewClient("test-http-pd-addr", []string{"127.0.0.1"})
	pdAddrs, leaderAddrIdx := c.(*client).inner.getPDAddrs()
	re.Len(pdAddrs, 1)
	re.Equal(-1, leaderAddrIdx)
	re.Contains(pdAddrs[0], httpScheme)
	c.Close()
	c = NewClient("test-https-pd-addr", []string{"127.0.0.1"}, WithTLSConfig(&tls.Config{}))
	pdAddrs, leaderAddrIdx = c.(*client).inner.getPDAddrs()
	re.Len(pdAddrs, 1)
	re.Equal(-1, leaderAddrIdx)
	re.Contains(pdAddrs[0], httpsScheme)
	c.Close()
}

// requestChecker is used to check the HTTP request sent by the client.
type requestChecker struct {
	checker func(req *http.Request) error
}

// RoundTrip implements the `http.RoundTripper` interface.
func (rc *requestChecker) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	return &http.Response{StatusCode: http.StatusOK}, rc.checker(req)
}

func newHTTPClientWithRequestChecker(checker func(req *http.Request) error) *http.Client {
	return &http.Client{
		Transport: &requestChecker{checker: checker},
	}
}

func TestPDAllowFollowerHandleHeader(t *testing.T) {
	re := require.New(t)
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		var expectedVal string
		if req.URL.Path == HotHistory {
			expectedVal = "true"
		}
		val := req.Header.Get(pdAllowFollowerHandleKey)
		if val != expectedVal {
			re.Failf("PD allow follower handler header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient("test-header", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	c.GetHistoryHotRegions(context.Background(), &HistoryHotRegionsRequest{})
	c.Close()
}

func TestCallerID(t *testing.T) {
	re := require.New(t)
	expectedVal := atomic.NewString(defaultCallerID)
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		val := req.Header.Get(xCallerIDKey)
		// Exclude the request sent by the inner client.
		if !strings.Contains(val, defaultInnerCallerID) && val != expectedVal.Load() {
			re.Failf("Caller ID header check failed",
				"should be %s, but got %s", expectedVal, val)
		}
		return nil
	})
	c := NewClient("test-caller-id", []string{"http://127.0.0.1"}, WithHTTPClient(httpClient))
	c.GetRegions(context.Background())
	expectedVal.Store("test")
	c.WithCallerID(expectedVal.Load()).GetRegions(context.Background())
	c.Close()
}

func TestRedirectWithMetrics(t *testing.T) {
	re := require.New(t)

	pdAddrs := []string{"127.0.0.1", "172.0.0.1", "192.0.0.1"}
	metricCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "check",
		}, []string{"name", ""})

	// 1. Test all followers failed, need to send all followers.
	httpClient := newHTTPClientWithRequestChecker(func(req *http.Request) error {
		if req.URL.Path == Schedulers {
			return errors.New("mock error")
		}
		return nil
	})
	c := NewClient("test-http-pd-redirect", pdAddrs, WithHTTPClient(httpClient), WithMetrics(metricCnt, nil))
	pdAddrs, leaderAddrIdx := c.(*client).inner.getPDAddrs()
	re.Equal(-1, leaderAddrIdx)
	c.CreateScheduler(context.Background(), "test", 0)
	var out dto.Metric
	failureCnt, err := c.(*client).inner.requestCounter.GetMetricWithLabelValues([]string{createSchedulerName, networkErrorStatus}...)
	re.NoError(err)
	failureCnt.Write(&out)
	re.Equal(float64(3), out.Counter.GetValue())
	c.Close()

	// 2. Test the Leader success, just need to send to leader.
	httpClient = newHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader success.
		if !strings.Contains(pdAddrs[0], req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = NewClient("test-http-pd-redirect", pdAddrs, WithHTTPClient(httpClient), WithMetrics(metricCnt, nil))
	// force to update members info.
	c.(*client).setLeaderAddrIdx(0)
	c.CreateScheduler(context.Background(), "test", 0)
	successCnt, err := c.(*client).inner.requestCounter.GetMetricWithLabelValues([]string{createSchedulerName, ""}...)
	re.NoError(err)
	successCnt.Write(&out)
	re.Equal(float64(1), out.Counter.GetValue())
	c.Close()

	// 3. Test when the leader fails, needs to be sent to the follower in order,
	// and returns directly if one follower succeeds
	httpClient = newHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader failure.
		if strings.Contains(pdAddrs[0], req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = NewClient("test-http-pd-redirect", pdAddrs, WithHTTPClient(httpClient), WithMetrics(metricCnt, nil))
	// force to update members info.
	c.(*client).setLeaderAddrIdx(0)
	c.CreateScheduler(context.Background(), "test", 0)
	successCnt, err = c.(*client).inner.requestCounter.GetMetricWithLabelValues([]string{createSchedulerName, ""}...)
	re.NoError(err)
	successCnt.Write(&out)
	// only one follower success
	re.Equal(float64(2), out.Counter.GetValue())
	failureCnt, err = c.(*client).inner.requestCounter.GetMetricWithLabelValues([]string{createSchedulerName, networkErrorStatus}...)
	re.NoError(err)
	failureCnt.Write(&out)
	// leader failure
	re.Equal(float64(4), out.Counter.GetValue())
	c.Close()
}

func TestWithBackoffer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewClient("test-with-backoffer", []string{"http://127.0.0.1"})

	base := 100 * time.Millisecond
	max := 500 * time.Millisecond
	total := time.Second
	bo := retry.InitialBackoffer(base, max, total)
	// Test the time cost of the backoff.
	start := time.Now()
	_, err := c.WithBackoffer(bo).GetPDVersion(ctx)
	re.InDelta(total, time.Since(start), float64(250*time.Millisecond))
	re.Error(err)
	// Test if the infinite retry works.
	bo = retry.InitialBackoffer(base, max, 0)
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	start = time.Now()
	_, err = c.WithBackoffer(bo).GetPDVersion(timeoutCtx)
	re.InDelta(3*time.Second, time.Since(start), float64(250*time.Millisecond))
	re.ErrorIs(err, context.DeadlineExceeded)
	c.Close()
}
