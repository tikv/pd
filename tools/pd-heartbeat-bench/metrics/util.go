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
	collectRound         = 1.0

	metrics2Collect = []metric{
		{promSQL: cpuMetric, name: "max cpu usage(%)", max: true},
		{promSQL: memoryMetric, name: "max memory usage(G)", max: true},
		{promSQL: goRoutineMetric, name: "max go routines", max: true},
		{promSQL: hbLatency99Metric, name: "99% Heartbeat Latency(ms)"},
		{promSQL: hbLatencyAvgMetric, name: "Avg Heartbeat Latency(ms)"},
	}

	// Prometheus SQL
	cpuMetric          = `max_over_time(irate(process_cpu_seconds_total{job=~".*pd.*"}[30s])[1h:30s]) * 100`
	memoryMetric       = `max_over_time(go_memstats_heap_inuse_bytes{job=~".*pd.*"}[1h])/1024/1024/1024`
	goRoutineMetric    = `max_over_time(go_goroutines{job=~".*pd.*"}[1h])`
	hbLatency99Metric  = `histogram_quantile(0.99, sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_bucket{}[1m])) by (le))`
	hbLatencyAvgMetric = `sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_sum{}[1m])) / sum(rate(pd_scheduler_handle_region_heartbeat_duration_seconds_count{}[1m]))`

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
		return fmt.Sprintf(`sum(rate(pd_core_region_heartbeat_breakdown_handle_duration_seconds_sum{name="%s"}[1m]))`, name)
	}
)

type metric struct {
	promSQL string
	name    string
	value   float64
	// max indicates whether the metric is a max value
	max bool
}

// InitMetric2Collect initializes the metrics to collect
func InitMetric2Collect(endpoint string) (withMetric bool) {
	for _, name := range breakdownNames {
		metrics2Collect = append(metrics2Collect, metric{
			promSQL: hbBreakdownMetricByName(name),
			name:    name,
		})
	}
	finalMetrics2Collect = metrics2Collect

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
	// retry 5 times to get average value
	res := make([]struct {
		sum   float64
		count int
	}, len(metrics2Collect))
	for range 5 {
		for j, m := range metrics2Collect {
			r, err := getMetric(prometheusCli, m.promSQL, time.Now())
			if err != nil {
				log.Error("get metric error", zap.String("name", m.name), zap.String("prom sql", m.promSQL), zap.Error(err))
			} else if len(r) > 0 {
				res[j].sum += r[0]
				res[j].count += 1
			}
		}
		time.Sleep(wait)
	}
	getRes := func(index int) float64 {
		if res[index].count == 0 {
			return 0
		}
		return res[index].sum / float64(res[index].count)
	}
	for i := range metrics2Collect {
		metrics2Collect[i].value = getRes(i)
		if metrics2Collect[i].max {
			finalMetrics2Collect[i].value = max(finalMetrics2Collect[i].value, metrics2Collect[i].value)
		} else {
			finalMetrics2Collect[i].value = (finalMetrics2Collect[i].value*collectRound + metrics2Collect[i].value) / (collectRound + 1)
		}
	}

	collectRound += 1
	log.Info("metrics collected", zap.Float64("round", collectRound), zap.String("metrics", formatMetrics(metrics2Collect)))
}

func getMetric(cli api.Client, query string, ts time.Time) ([]float64, error) {
	httpAPI := v1.NewAPI(cli)
	val, _, err := httpAPI.Query(context.Background(), query, ts)
	if err != nil {
		return nil, err
	}
	valMatrix := val.(model.Vector)
	if len(valMatrix) == 0 {
		return nil, nil
	}
	var value []float64
	for i := range valMatrix {
		value = append(value, float64(valMatrix[i].Value))
		// judge whether exceeded float maximum value
		if math.IsNaN(value[i]) {
			return nil, fmt.Errorf("prometheus query result exceeded float maximum value, result=%s", valMatrix[i].String())
		}
	}
	return value, nil
}

func formatMetrics(ms []metric) string {
	res := ""
	for _, m := range ms {
		res += "[" + m.name + "]" + " " + fmt.Sprintf("%.10f", m.value) + " "
	}
	return res
}

// CollectRegionAndStoreStats collects the region and store stats
func CollectRegionAndStoreStats(regionStats *report.Stats, storeTime *float64) {
	if regionStats != nil && storeTime != nil {
		collect(*regionStats, *storeTime)
	}
}

func collect(regionStats report.Stats, storeTime float64) {
	average := func(avg, new float64) float64 {
		return (avg*collectRound + new) / (collectRound + 1)
	}

	avgRegionStats.Total = time.Duration(average(float64(avgRegionStats.Total), float64(regionStats.Total)))
	avgRegionStats.Average = average(avgRegionStats.Average, regionStats.Average)
	avgRegionStats.Stddev = average(avgRegionStats.Stddev, regionStats.Stddev)
	avgRegionStats.Fastest = average(avgRegionStats.Fastest, regionStats.Fastest)
	avgRegionStats.Slowest = average(avgRegionStats.Slowest, regionStats.Slowest)
	avgRegionStats.RPS = average(avgRegionStats.RPS, regionStats.RPS)
	avgStoreTime = average(avgStoreTime, storeTime)
}

// OutputConclusion outputs the final conclusion
func OutputConclusion() {
	logFields := RegionFields(avgRegionStats,
		zap.Float64("avg store time", avgStoreTime),
		zap.Float64("current round", collectRound),
		zap.String("metrics", formatMetrics(finalMetrics2Collect)))
	log.Info("final metrics collected", logFields...)
}

// RegionFields returns the fields for region stats
func RegionFields(stats report.Stats, fields ...zap.Field) []zap.Field {
	return append([]zap.Field{
		zap.String("total", fmt.Sprintf("%.4fs", stats.Total.Seconds())),
		zap.String("slowest", fmt.Sprintf("%.4fs", stats.Slowest)),
		zap.String("fastest", fmt.Sprintf("%.4fs", stats.Fastest)),
		zap.String("average", fmt.Sprintf("%.4fs", stats.Average)),
		zap.String("stddev", fmt.Sprintf("%.4fs", stats.Stddev)),
		zap.String("rps", fmt.Sprintf("%.4f", stats.RPS)),
	}, fields...)
}
