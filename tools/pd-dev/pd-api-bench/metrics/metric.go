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

import "github.com/prometheus/client_golang/prometheus"

var (
	PDAPIExecutionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "api_bench",
			Name:      "pd_api_execution_duration_seconds",
			Help:      "Bucketed histogram of all pd api execution time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{"type"})

	PDAPIRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "api_bench",
			Name:      "pd_api_request_total",
			Help:      "Counter of the pd http api requests",
		}, []string{"type", "result"})
)

func RegisterMetrics() {
	prometheus.MustRegister(PDAPIExecutionHistogram)
	prometheus.MustRegister(PDAPIRequestCounter)
}
