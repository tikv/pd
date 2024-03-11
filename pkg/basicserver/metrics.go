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

var (
	// ServerMaxProcsGauge records the maxprocs.
	ServerMaxProcsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "maxprocs",
			Help:      "The value of GOMAXPROCS.",
		})

	// ServerMemoryLimit records the cgroup memory limit.
	ServerMemoryLimit = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "memory_quota_bytes",
			Help:      "The value of memory quota bytes.",
		})

	// ServerInfoGauge indicates the pd server info including version and git hash.
	ServerInfoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "info",
			Help:      "Indicate the pd server info, and the value is the start timestamp (s).",
		}, []string{"version", "hash"})
)

func init() {
	prometheus.MustRegister(ServerMaxProcsGauge)
	prometheus.MustRegister(ServerMemoryLimit)
	prometheus.MustRegister(ServerInfoGauge)
}
