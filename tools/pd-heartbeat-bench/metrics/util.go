// Copyright 2024 TiKV Project Authors.
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
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"go.etcd.io/etcd/pkg/v3/report"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

var (
	prometheusCli        api.Client
	finalMetrics2Collect []metric
	avgRegionStats       report.Stats
	avgStoreTime         float64
	metricCollectRounds  int
	workloadStatsRounds  int
	collectMu            sync.Mutex

	baseMetrics = []metric{
		{promSQL: cpuMetric, name: "max cpu usage(%)", max: true},
		{promSQL: memoryMetric, name: "max memory usage(G)", max: true},
		{promSQL: goRoutineMetric, name: "max go routines", max: true},
		{promSQL: hbLatency99Metric, name: "p99 heartbeat latency(ms)"},
		{promSQL: hbLatencyAvgMetric, name: "avg heartbeat latency(ms)"},
	}
	metrics2Collect []metric

	// Prometheus SQL
	cpuMetric          = `max(rate(process_cpu_seconds_total{job=~".*pd.*",job!~".*heartbeat-bench.*"}[30s])) * 100`
	memoryMetric       = `max(go_memstats_heap_inuse_bytes{job=~".*pd.*",job!~".*heartbeat-bench.*"}) / 1024 / 1024 / 1024`
	goRoutineMetric    = `max(go_goroutines{job=~".*pd.*",job!~".*heartbeat-bench.*"})`
	hbLatency99Metric  = `histogram_quantile(0.99, sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_bucket[1m])) by (le)) * 1000`
	hbLatencyAvgMetric = `sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_sum[1m])) / sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_count[1m])) * 1000`

	// Heartbeat Performance Duration BreakDown
	breakdownNames = []string{
		"AsyncHotStatsDuration",
		"CollectRegionStats",
		"Other",
		"PreCheck",
		"RegionGuide",
		"SaveCache_CheckOverlaps",
		"SaveCache_InvalidRegion",
		"SaveCache_SetRegion",
		"SaveCache_UpdateSubTree",
	}
	hbBreakdownMetricByName = func(name string) string {
		return fmt.Sprintf(
			`sum(rate(pd_core_region_heartbeat_breakdown_handle_duration_seconds_sum{name="%s"}[1m])) / sum(rate(pd_core_region_heartbeat_breakdown_handle_duration_seconds_count{name="%s"}[1m])) * 1000`,
			name,
			name,
		)
	}
)

type metric struct {
	promSQL string
	name    string
	value   float64
	samples int
	// max indicates whether the metric is a max value
	max bool
}

// InitMetric2Collect initializes the metrics to collect
func InitMetric2Collect(endpoint string) (withMetric bool) {
	collectMu.Lock()
	defer collectMu.Unlock()
	metrics2Collect = append([]metric(nil), baseMetrics...)
	for _, name := range breakdownNames {
		metrics2Collect = append(metrics2Collect, metric{
			promSQL: hbBreakdownMetricByName(name),
			name:    name + " avg latency(ms)",
		})
	}
	finalMetrics2Collect = append([]metric(nil), metrics2Collect...)
	metricCollectRounds = 0
	workloadStatsRounds = 0
	avgRegionStats = report.Stats{}
	avgStoreTime = 0

	if j := strings.Index(endpoint, "//"); j == -1 {
		endpoint = "http://" + endpoint
	}
	cu, err := url.Parse(endpoint)
	if err != nil {
		log.Error("parse prometheus url error", zap.Error(err))
		return false
	}
	prometheusCli, err = newPrometheusClient(*cu)
	if err != nil {
		log.Error("create prometheus client error", zap.Error(err))
		return false
	}
	// check whether the prometheus is available
	_, err = getMetric(prometheusCli, goRoutineMetric, time.Now())
	if err != nil {
		log.Error("check prometheus availability error, please check the prometheus address", zap.Error(err))
		return false
	}
	return true
}

func newPrometheusClient(prometheusURL url.URL) (api.Client, error) {
	client, err := api.NewClient(api.Config{
		Address: prometheusURL.String(),
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

// WarmUpRound wait for the first round to warm up
const WarmUpRound = 1

// CollectMetrics collects the metrics
func CollectMetrics(curRound int, wait time.Duration) {
	if curRound < WarmUpRound {
		return
	}
	collectMu.Lock()
	defer collectMu.Unlock()
	for i := range metrics2Collect {
		metrics2Collect[i].value = 0
		metrics2Collect[i].samples = 0
	}
	// Sample five times, preserving maxima for resource metrics.
	res := make([]struct {
		sum   float64
		max   float64
		count int
	}, len(metrics2Collect))
	for sample := range 5 {
		for j, m := range metrics2Collect {
			r, err := getMetric(prometheusCli, m.promSQL, time.Now())
			if err != nil {
				log.Error("get metric error", zap.String("name", m.name), zap.String("prom sql", m.promSQL), zap.Error(err))
			} else if len(r) > 0 {
				res[j].sum += r[0]
				if res[j].count == 0 || r[0] > res[j].max {
					res[j].max = r[0]
				}
				res[j].count += 1
			}
		}
		if sample < 4 {
			time.Sleep(wait)
		}
	}
	getRes := func(index int) float64 {
		if res[index].count == 0 {
			return 0
		}
		if metrics2Collect[index].max {
			return res[index].max
		}
		return res[index].sum / float64(res[index].count)
	}
	for i := range metrics2Collect {
		if res[i].count == 0 {
			continue
		}
		metrics2Collect[i].value = getRes(i)
		metrics2Collect[i].samples = 1
		if metrics2Collect[i].max {
			if finalMetrics2Collect[i].samples == 0 {
				finalMetrics2Collect[i].value = metrics2Collect[i].value
			} else {
				finalMetrics2Collect[i].value = max(finalMetrics2Collect[i].value, metrics2Collect[i].value)
			}
		} else {
			finalMetrics2Collect[i].value = runningAverage(
				finalMetrics2Collect[i].value,
				finalMetrics2Collect[i].samples,
				metrics2Collect[i].value,
			)
		}
		finalMetrics2Collect[i].samples++
	}

	metricCollectRounds++
	log.Info("metrics collected", zap.Int("round", metricCollectRounds), zap.String("metrics", formatMetrics(metrics2Collect)))
}

func getMetric(cli api.Client, query string, ts time.Time) ([]float64, error) {
	httpAPI := v1.NewAPI(cli)
	val, _, err := httpAPI.Query(context.Background(), query, ts)
	if err != nil {
		return nil, err
	}
	valMatrix, ok := val.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("unexpected prometheus result type %s", val.Type())
	}
	if len(valMatrix) == 0 {
		return nil, nil
	}
	var value []float64
	for i := range valMatrix {
		value = append(value, float64(valMatrix[i].Value))
		// judge whether exceeded float maximum value
		if math.IsNaN(value[i]) || math.IsInf(value[i], 0) {
			return nil, fmt.Errorf("prometheus query returned a non-finite value, result=%s", valMatrix[i].String())
		}
	}
	return value, nil
}

func formatMetrics(ms []metric) string {
	if len(ms) == 0 {
		return ""
	}
	var builder strings.Builder
	for _, m := range ms {
		if m.samples == 0 {
			continue
		}
		fmt.Fprintf(&builder, "[%s] %.10f ", m.name, m.value)
	}
	return builder.String()
}

// CollectRegionAndStoreStats collects the region and store stats
func CollectRegionAndStoreStats(regionStats *report.Stats, storeTime *float64) {
	if regionStats == nil || storeTime == nil || !isFinite(*storeTime) || !regionStatsAreFinite(*regionStats) {
		return
	}
	collectMu.Lock()
	defer collectMu.Unlock()
	collect(*regionStats, *storeTime)
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func regionStatsAreFinite(stats report.Stats) bool {
	return isFinite(stats.Average) && isFinite(stats.Stddev) &&
		isFinite(stats.Fastest) && isFinite(stats.Slowest) && isFinite(stats.RPS)
}

func finiteOrZero(value float64) float64 {
	if !isFinite(value) {
		return 0
	}
	return value
}

func collect(regionStats report.Stats, storeTime float64) {
	avgRegionStats.Total = time.Duration(runningAverage(float64(avgRegionStats.Total), workloadStatsRounds, float64(regionStats.Total)))
	avgRegionStats.Average = runningAverage(avgRegionStats.Average, workloadStatsRounds, regionStats.Average)
	avgRegionStats.Stddev = runningAverage(avgRegionStats.Stddev, workloadStatsRounds, regionStats.Stddev)
	avgRegionStats.Fastest = runningAverage(avgRegionStats.Fastest, workloadStatsRounds, regionStats.Fastest)
	avgRegionStats.Slowest = runningAverage(avgRegionStats.Slowest, workloadStatsRounds, regionStats.Slowest)
	avgRegionStats.RPS = runningAverage(avgRegionStats.RPS, workloadStatsRounds, regionStats.RPS)
	avgStoreTime = runningAverage(avgStoreTime, workloadStatsRounds, storeTime)
	workloadStatsRounds++
}

func runningAverage(average float64, samples int, value float64) float64 {
	return (average*float64(samples) + value) / float64(samples+1)
}

// OutputConclusion outputs the final conclusion
func OutputConclusion() {
	collectMu.Lock()
	defer collectMu.Unlock()
	if workloadStatsRounds == 0 && metricCollectRounds == 0 {
		return
	}
	logFields := RegionFields(avgRegionStats,
		zap.Float64("avg heartbeat round time", avgStoreTime),
		zap.Int("workload rounds", workloadStatsRounds),
		zap.Int("metric rounds", metricCollectRounds),
		zap.String("metrics", formatMetrics(finalMetrics2Collect)))
	log.Info("final metrics collected", logFields...)
}

// RegionFields returns the fields for region stats
func RegionFields(stats report.Stats, fields ...zap.Field) []zap.Field {
	return append([]zap.Field{
		zap.String("total", fmt.Sprintf("%.4fs", stats.Total.Seconds())),
		zap.String("slowest", fmt.Sprintf("%.4fs", finiteOrZero(stats.Slowest))),
		zap.String("fastest", fmt.Sprintf("%.4fs", finiteOrZero(stats.Fastest))),
		zap.String("average", fmt.Sprintf("%.4fs", finiteOrZero(stats.Average))),
		zap.String("stddev", fmt.Sprintf("%.4fs", finiteOrZero(stats.Stddev))),
		zap.String("rps", fmt.Sprintf("%.4f", finiteOrZero(stats.RPS))),
	}, fields...)
}
