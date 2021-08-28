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

	schedulerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "status",
			Help:      "Status of the scheduler.",
		}, []string{"kind", "type"})

	hotSpotStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "hotspot",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"address", "store", "type"})

	patrolCheckRegionsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "patrol_regions_time",
			Help:      "Time spent of patrol checks region.",
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

	regionListGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "region_list",
			Help:      "Number of region in waiting list",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(regionEventCounter)
	prometheus.MustRegister(healthStatusGauge)
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(hotSpotStatusGauge)
	prometheus.MustRegister(patrolCheckRegionsGauge)
	prometheus.MustRegister(clusterStateCPUGauge)
	prometheus.MustRegister(clusterStateCurrent)
	prometheus.MustRegister(regionListGauge)
}
