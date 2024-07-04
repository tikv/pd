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

package checker

import "github.com/prometheus/client_golang/prometheus"

var (
	checkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "event_count",
			Help:      "Counter of checker events.",
		}, []string{"type", "name"})
	regionListGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "region_list",
			Help:      "Number of region about different type.",
		}, []string{"type"})

	patrolCheckRegionsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "patrol_regions_time",
			Help:      "Time spent of patrol checks region.",
		})
)

func init() {
	prometheus.MustRegister(checkerCounter)
	prometheus.MustRegister(regionListGauge)
	prometheus.MustRegister(patrolCheckRegionsGauge)
}
