// Copyright 2019 TiKV Project Authors.
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

package cluster

import "github.com/prometheus/client_golang/prometheus"

var (
	healthStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "health_status",
			Help:      "Status of the cluster.",
		}, []string{"name"})

	regionEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "region_event",
			Help:      "Counter of the region event",
		}, []string{"event"})

	bucketEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "bucket_event",
			Help:      "Counter of the bucket event",
		}, []string{"event"})

	updateStoreStatsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "update_stores_stats_time",
			Help:      "Time spent of updating store stats.",
		})

	clusterStateCPUGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "cluster_state_cpu_usage",
			Help:      "CPU usage to determine the cluster state",
		})
	clusterStateCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "cluster_state_current",
			Help:      "Current state of the cluster",
		}, []string{"state"})

	storesProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "progress",
			Help:      "The current progress of corresponding action",
		}, []string{"address", "store", "action"})

	storesSpeedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "speed",
			Help:      "The current speed of corresponding action",
		}, []string{"address", "store", "action"})

	storesETAGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "eta",
			Help:      "The ETA of corresponding action",
		}, []string{"address", "store", "action"})

	storeSyncConfigEvent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "store_sync",
			Help:      "The state of store sync config",
		}, []string{"address", "state"})
)

func init() {
	prometheus.MustRegister(regionEventCounter)
	prometheus.MustRegister(healthStatusGauge)
	prometheus.MustRegister(clusterStateCPUGauge)
	prometheus.MustRegister(clusterStateCurrent)
	prometheus.MustRegister(bucketEventCounter)
	prometheus.MustRegister(storesProgressGauge)
	prometheus.MustRegister(storesSpeedGauge)
	prometheus.MustRegister(storesETAGauge)
	prometheus.MustRegister(storeSyncConfigEvent)
	prometheus.MustRegister(updateStoreStatsGauge)
}
