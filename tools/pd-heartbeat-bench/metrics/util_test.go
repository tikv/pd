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

package metrics

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/pkg/v3/report"
)

func TestCollectUsesIndependentWorkloadRoundCount(t *testing.T) {
	avgRegionStats = report.Stats{}
	avgStoreTime = 0
	workloadStatsRounds = 0

	collect(report.Stats{Total: time.Second, Average: 1, RPS: 100}, 10)
	collect(report.Stats{Total: 3 * time.Second, Average: 3, RPS: 300}, 30)

	require.Equal(t, 2*time.Second, avgRegionStats.Total)
	require.Equal(t, 2.0, avgRegionStats.Average)
	require.Equal(t, 200.0, avgRegionStats.RPS)
	require.Equal(t, 20.0, avgStoreTime)
	require.Equal(t, 2, workloadStatsRounds)
}

func TestCollectSkipsEmptyReportStats(t *testing.T) {
	avgRegionStats = report.Stats{}
	avgStoreTime = 0
	workloadStatsRounds = 0

	rep := report.NewReport("%.4f")
	statsCh := rep.Stats()
	close(rep.Results())
	stats := <-statsCh
	require.True(t, math.IsNaN(stats.Average))

	storeTime := 1.0
	CollectRegionAndStoreStats(&stats, &storeTime)
	require.Equal(t, 0, workloadStatsRounds)
	require.Equal(t, report.Stats{}, avgRegionStats)
	require.Zero(t, avgStoreTime)

	fields := RegionFields(stats)
	require.Equal(t, "0.0000s", fields[3].String)
	require.Equal(t, "0.0000s", fields[4].String)
}

func TestPrometheusLatencyQueriesReturnMilliseconds(t *testing.T) {
	require.Contains(t, hbLatency99Metric, "* 1000")
	require.Contains(t, hbLatencyAvgMetric, "* 1000")
	breakdown := hbBreakdownMetricByName("RegionGuide")
	require.Contains(t, breakdown, "_count")
	require.Contains(t, breakdown, "* 1000")
}

func TestCollectMetricsPreservesPeaks(t *testing.T) {
	var mu sync.Mutex
	requests := make(map[string]int)
	values := []float64{1, 5, 1, 1, 1, 2, 8, 2, 2, 2}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		query := r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		if query == "missing" {
			_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
			return
		}
		value := values[requests[query]]
		requests[query]++
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"%.1f"]}]}}`, value)
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := newPrometheusClient(*endpoint)
	require.NoError(t, err)

	oldClient, oldMetrics, oldFinal, oldRounds := prometheusCli, metrics2Collect, finalMetrics2Collect, metricCollectRounds
	t.Cleanup(func() {
		prometheusCli, metrics2Collect, finalMetrics2Collect, metricCollectRounds = oldClient, oldMetrics, oldFinal, oldRounds
	})
	prometheusCli = client
	metrics2Collect = []metric{
		{promSQL: "peak", max: true},
		{promSQL: "average"},
		{promSQL: "missing", max: true},
	}
	finalMetrics2Collect = append([]metric(nil), metrics2Collect...)
	metricCollectRounds = 0

	CollectMetrics(WarmUpRound, 0)
	require.Equal(t, 5.0, finalMetrics2Collect[0].value)
	require.InDelta(t, 1.8, finalMetrics2Collect[1].value, 1e-10)
	CollectMetrics(WarmUpRound+1, 0)
	require.Equal(t, 8.0, finalMetrics2Collect[0].value)
	require.InDelta(t, 2.5, finalMetrics2Collect[1].value, 1e-10)
	require.Equal(t, 2, finalMetrics2Collect[0].samples)
	require.Zero(t, finalMetrics2Collect[2].samples)
}
