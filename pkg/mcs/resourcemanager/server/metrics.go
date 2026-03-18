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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/utils/grpcutil"
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
	backgroundTypeLabel       = "background"
	tiflashTypeLabel          = "ap"
	defaultTypeLabel          = "tp"
	newResourceGroupNameLabel = "resource_group"

	// Labels for the config.
	ruPerSecLabel   = "ru_per_sec"
	ruCapacityLabel = "ru_capacity"
	priorityLabel   = "priority"
)

var (
	grpcStreamSendDuration = grpcutil.NewGRPCStreamSendDuration(namespace, serverSubsystem)

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

	// RUv2 metrics (experimental v2 RU calculation, recording only).
	requestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "request_unit_v2_sum",
			Help:      "Counter of the total experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	tikvRequestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "tikv_request_unit_v2_sum",
			Help:      "Counter of the TiKV-side experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})
	tidbRequestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "tidb_request_unit_v2_sum",
			Help:      "Counter of the TiDB-side experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel})

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

	resourceGroupConfigGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "group_config",
			Help:      "Config of the resource group.",
		}, []string{newResourceGroupNameLabel, typeLabel})

	pushRUMetricsDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "push_ru_metrics_duration_seconds",
			Help:      "The duration of pushing RU metrics to Prometheus.",
			Buckets:   prometheus.DefBuckets,
		})

	requestUnitSumPerSec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "request_unit_sum_per_sec",
			Help:      "The number of the request unit cost per second for all resource groups.",
		}, []string{newResourceGroupNameLabel})

	requestUnitConsumeRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "request_unit_consume_rate",
			Help:      "request_unit_per_second/fill_rate for all resource groups.",
		}, []string{newResourceGroupNameLabel})

	resourceUnitServiceLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "service_limit",
			Help:      "Fill rate value for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	syncLoadGroupCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "sync_load_group_counter",
			Help:      "The number of the sync load group.",
		})

	asyncLoadGroupDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "async_load_group_duration_seconds",
			Help:      "The duration of the async load group.",
		})
)

func init() {
	prometheus.MustRegister(grpcStreamSendDuration)
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
	prometheus.MustRegister(requestUnitV2Cost)
	prometheus.MustRegister(tikvRequestUnitV2Cost)
	prometheus.MustRegister(tidbRequestUnitV2Cost)
	prometheus.MustRegister(sqlLayerRequestUnitCost)
	prometheus.MustRegister(readByteCost)
	prometheus.MustRegister(writeByteCost)
	prometheus.MustRegister(kvCPUCost)
	prometheus.MustRegister(sqlCPUCost)
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(availableRUCounter)
	prometheus.MustRegister(readRequestUnitMaxPerSecCost)
	prometheus.MustRegister(writeRequestUnitMaxPerSecCost)
	prometheus.MustRegister(resourceGroupConfigGauge)
	prometheus.MustRegister(pushRUMetricsDuration)
	prometheus.MustRegister(requestUnitSumPerSec)
	prometheus.MustRegister(requestUnitConsumeRate)
	prometheus.MustRegister(resourceUnitServiceLimit)
	prometheus.MustRegister(syncLoadGroupCounter)
	prometheus.MustRegister(asyncLoadGroupDuration)
}

func newAcquireTokenBucketsMetricsStream(stream rmpb.ResourceManager_AcquireTokenBucketsServer) rmpb.ResourceManager_AcquireTokenBucketsServer {
	return grpcutil.NewMetricsStream(stream, stream.Send, stream.Recv, grpcStreamSendDuration, "acquire-token-buckets")
}
