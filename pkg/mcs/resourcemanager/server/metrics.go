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
	namespace                 = "resource_manager"
	serverSubsystem           = "server"
	ruSubsystem               = "resource_unit"
	resourceSubsystem         = "resource"
	resourceGroupNameLabel    = "name"
	typeLabel                 = "type"
	readTypeLabel             = "read"
	writeTypeLabel            = "write"
	backgroundTypeLabel       = "background"
	tiflashTypeLabel          = "ap"
	defaultTypeLabel          = "tp"
	newResourceGroupNameLabel = "resource_group"
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
	// `sum` is added to the name to maintain compatibility with the previous use of histogram.
	readRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit_sum",
			Help:      "Counter of the read request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	writeRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit_sum",
			Help:      "Counter of the write request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	sqlLayerRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "sql_layer_request_unit_sum",
			Help:      "The number of the sql layer request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	// Resource cost metrics.
	readByteCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "read_byte_sum",
			Help:      "Counter of the read byte cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	writeByteCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "write_byte_sum",
			Help:      "Counter of the write byte cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	kvCPUCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "kv_cpu_time_ms_sum",
			Help:      "Counter of the KV CPU time cost in milliseconds for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	sqlCPUCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "sql_cpu_time_ms_sum",
			Help:      "Counter of the SQL CPU time cost in milliseconds for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "request_count",
			Help:      "The number of read/write requests for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})

	availableRUCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "available_ru",
			Help:      "Counter of the available RU for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})
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
}
