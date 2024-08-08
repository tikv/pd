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
	namespace                 = "resource_manager"
	serverSubsystem           = "server"
	ruSubsystem               = "resource_unit"
	resourceSubsystem         = "resource"
	resourceGroupNameLabel    = "name"
	typeLabel                 = "type"
	readTypeLabel             = "read"
	writeTypeLabel            = "write"
	newResourceGroupNameLabel = "resource_group"

	// Labels for the config.
	ruPerSecLabel   = "ru_per_sec"
	ruCapacityLabel = "ru_capacity"
	priorityLabel   = "priority"
)

var (
	// Meta & Server info.
	serverInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "info",
			Help:      "Indicate the resource manager server info, and the value is the start timestamp (s).",
		}, []string{"version", "hash"})
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
	readRequestUnitMaxPerSecCost = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit_max_per_sec",
			Help:      "Gauge of the max read request unit per second for all resource groups.",
		}, []string{newResourceGroupNameLabel})
	writeRequestUnitMaxPerSecCost = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit_max_per_sec",
			Help:      "Gauge of the max write request unit per second for all resource groups.",
		}, []string{newResourceGroupNameLabel})

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

	availableRUCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "available_ru",
			Help:      "Counter of the available RU for all resource groups.",
<<<<<<< HEAD:pkg/mcs/resource_manager/server/metrics.go
		}, []string{resourceGroupNameLabel})
=======
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	resourceGroupConfigGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "group_config",
			Help:      "Config of the resource group.",
		}, []string{newResourceGroupNameLabel, typeLabel})
>>>>>>> 56f082db6 (resource_manager: add metrics to show resource group configuration (#8478)):pkg/mcs/resourcemanager/server/metrics.go
)

func init() {
	prometheus.MustRegister(serverInfo)
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
	prometheus.MustRegister(sqlLayerRequestUnitCost)
	prometheus.MustRegister(readByteCost)
	prometheus.MustRegister(writeByteCost)
	prometheus.MustRegister(kvCPUCost)
	prometheus.MustRegister(sqlCPUCost)
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(availableRUCounter)
<<<<<<< HEAD:pkg/mcs/resource_manager/server/metrics.go
=======
	prometheus.MustRegister(readRequestUnitMaxPerSecCost)
	prometheus.MustRegister(writeRequestUnitMaxPerSecCost)
	prometheus.MustRegister(resourceGroupConfigGauge)
>>>>>>> 56f082db6 (resource_manager: add metrics to show resource group configuration (#8478)):pkg/mcs/resourcemanager/server/metrics.go
}
