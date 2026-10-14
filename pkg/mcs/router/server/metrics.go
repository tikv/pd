// Copyright 2025 TiKV Project Authors.
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

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace       = "router"
	serverSubsystem = "server"
)

var (
	queryRegionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "query_region_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of region query requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	regionRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_request_cnt",
			Help:      "Counter of region request.",
		}, []string{"request", "caller_id", "caller_component", "event"})

	regionSyncerStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_syncer_status",
			Help:      "Inner status of the region syncer.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(regionRequestCounter)
	prometheus.MustRegister(queryRegionDuration)
	prometheus.MustRegister(regionSyncerStatus)
}
