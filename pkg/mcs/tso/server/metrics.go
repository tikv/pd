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

package server

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "tso"
)

var (
	// TODO: pre-allocate gauge metrics
	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})

	metadataGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "cluster",
			Name:      "metadata",
			Help:      "Record critical metadata.",
		}, []string{"type"})

	serverInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "info",
			Help:      "Indicate the tso server info, and the value is the start timestamp (s).",
		}, []string{"version", "hash"})

	tsoProxyHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "handle_tso_proxy_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	tsoProxyBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "handle_tso_proxy_batch_size",
			Help:      "Bucketed histogram of the batch size of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		})

	tsoHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "handle_tso_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
)

func init() {
	prometheus.MustRegister(timeJumpBackCounter)
	prometheus.MustRegister(metadataGauge)
	prometheus.MustRegister(serverInfo)
	prometheus.MustRegister(tsoProxyHandleDuration)
	prometheus.MustRegister(tsoProxyBatchSize)
	prometheus.MustRegister(tsoHandleDuration)
}
