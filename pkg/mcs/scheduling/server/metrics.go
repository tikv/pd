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

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace       = "scheduling"
	serverSubsystem = "server"
)

var (
	// Store heartbeat metrics
	storeHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "handle_store_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled store heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	storeHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "store_heartbeat_total",
			Help:      "Counter of store heartbeat requests.",
		}, []string{"address", "store", "status"})

	// Region heartbeat metrics
	regionHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "handle_region_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled region heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	regionHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_heartbeat_total",
			Help:      "Counter of region heartbeat requests.",
		}, []string{"address", "store", "status"})
)

func init() {
	prometheus.MustRegister(storeHeartbeatHandleDuration)
	prometheus.MustRegister(storeHeartbeatCounter)
	prometheus.MustRegister(regionHeartbeatHandleDuration)
	prometheus.MustRegister(regionHeartbeatCounter)
}
