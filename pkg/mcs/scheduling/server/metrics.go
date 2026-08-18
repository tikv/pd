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

	regionBucketsHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "handle_region_buckets_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled region buckets requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	regionBucketsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_buckets_total",
			Help:      "Counter of region buckets requests.",
		}, []string{"address", "store", "status"})

	regionBucketsReportInterval = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_buckets_report_interval_seconds",
			Help:      "Bucketed histogram of processing time (s) of region buckets report intervals.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20), // 1s ~ 17m
		}, []string{"address", "store"})
)

func init() {
	prometheus.MustRegister(storeHeartbeatHandleDuration)
	prometheus.MustRegister(storeHeartbeatCounter)
	prometheus.MustRegister(regionHeartbeatHandleDuration)
	prometheus.MustRegister(regionHeartbeatCounter)
	prometheus.MustRegister(regionBucketsHandleDuration)
	prometheus.MustRegister(regionBucketsCounter)
	prometheus.MustRegister(regionBucketsReportInterval)
}

// DeleteStoreMetrics deletes the per-store heartbeat/bucket metrics of a store.
// Matches on the store label alone, not address: PD allows an existing store ID to
// change address (e.g. after a TiKV restart with a new IP), so requiring the current
// address to match as well would permanently leak any series recorded under a
// previous address.
func DeleteStoreMetrics(id string) {
	labels := prometheus.Labels{"store": id}
	storeHeartbeatHandleDuration.DeletePartialMatch(labels)
	storeHeartbeatCounter.DeletePartialMatch(labels)
	regionHeartbeatHandleDuration.DeletePartialMatch(labels)
	regionHeartbeatCounter.DeletePartialMatch(labels)
	regionBucketsHandleDuration.DeletePartialMatch(labels)
	regionBucketsCounter.DeletePartialMatch(labels)
	regionBucketsReportInterval.DeletePartialMatch(labels)
}

// ResetMetrics resets the per-store heartbeat/bucket metrics declared in this
// file. DeleteStoreMetrics only ever removes one store's series at a time, so
// on a primary handoff or cluster shutdown it can't be used to clear every
// store still known to the outgoing Cluster instance; this wipes the vectors
// wholesale instead, the same way the other packages' ResetXxxMetrics do.
func ResetMetrics() {
	storeHeartbeatHandleDuration.Reset()
	storeHeartbeatCounter.Reset()
	regionHeartbeatHandleDuration.Reset()
	regionHeartbeatCounter.Reset()
	regionBucketsHandleDuration.Reset()
	regionBucketsCounter.Reset()
	regionBucketsReportInterval.Reset()
}
