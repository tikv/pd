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

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace              = "resource_manager"
	ruSubsystem            = "resource_unit"
	resourceSubsystem      = "resource"
	resourceGroupNameLabel = "name"
	typeLabel              = "type"
	readTypeLabel          = "read"
	writeTypeLabel         = "write"
)

var (
	// RU cost metrics.
	readRequestUnitCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit",
			Help:      "Bucketed histogram of the read request unit cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 10, 5), // 1 ~ 100000
		}, []string{resourceGroupNameLabel})
	writeRequestUnitCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit",
			Help:      "Bucketed histogram of the write request unit cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(3, 10, 5), // 3 ~ 300000
		}, []string{resourceGroupNameLabel})
	sqlLayerRequestUnitCost = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "sql_layer_request_unit",
			Help:      "The number of the sql layer request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel})

	// Resource cost metrics.
	readByteCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "read_byte",
			Help:      "Bucketed histogram of the read byte cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		}, []string{resourceGroupNameLabel})
	writeByteCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "write_byte",
			Help:      "Bucketed histogram of the write byte cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		}, []string{resourceGroupNameLabel})
	kvCPUCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "kv_cpu_time_ms",
			Help:      "Bucketed histogram of the KV CPU time cost in milliseconds for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 10, 3), // 1 ~ 1000
		}, []string{resourceGroupNameLabel})
	sqlCPUCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "sql_cpu_time_ms",
			Help:      "Bucketed histogram of the SQL CPU time cost in milliseconds for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 10, 3), // 1 ~ 1000
		}, []string{resourceGroupNameLabel})
	requestCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "request_count",
			Help:      "The number of read/write requests for all resource groups.",
		}, []string{resourceGroupNameLabel, typeLabel})
)

func init() {
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
	prometheus.MustRegister(sqlLayerRequestUnitCost)
	prometheus.MustRegister(readByteCost)
	prometheus.MustRegister(writeByteCost)
	prometheus.MustRegister(kvCPUCost)
	prometheus.MustRegister(sqlCPUCost)
	prometheus.MustRegister(requestCount)
}
