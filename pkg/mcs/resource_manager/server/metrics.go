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
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

// TODO: add some Prometheus metrics.

// HistoryResourceGroupMetricsList wraps HistoryResourceGroupMetrics, which will be returned to TiDB.
//
// NOTE: This type will be exported by HTTP API. Please pay more attention when modifying it.
type HistoryResourceGroupMetricsList struct {
	HistoryResourceGroupMetricsList []*HistoryResourceGroupMetrics `json:"history_resource_group_metrics_list"`
}

// HistoryResourceGroupMetrics contains the metrics info for a resource group to tell how many RUs and resources
// it cost.
type HistoryResourceGroupMetrics struct {
	ResourceGroup string   `json:"resource_group"`
	StartTime     int64    `json:"start_time"`
	Duration      uint64   `json:"duration"`
	Metrics       *Metrics `json:"metrics"`
}

// TODO: implement the API method to retrieve the history resource group metrics info.

// Metrics contains the RU and resource metrics info.
type Metrics struct {
	ResourceUnit *ResourceUnitMetrics `json:"resource_unit_metrics,omitempty"`
	Resource     *ResourceMetrics     `json:"resource_metrics,omitempty"`
	// Metrics will be reset every time after it's persisted. These two time points will
	// help us to track its lifetime.
	createTime time.Time
	updateTime time.Time
}

// ResourceUnitMetrics contains the RU metrics info for the RU mode, which provides the RU cost info.
type ResourceUnitMetrics struct {
	RRUPerSec        float64 `json:"rru_per_sec"`
	WRUPerSec        float64 `json:"wru_per_sec"`
	MaxRRUPerSec     float64 `json:"max_rru_per_sec"`
	MaxWRUPerSec     float64 `json:"max_wru_per_sec"`
	AverageRRUPerSec float64 `json:"average_rru_per_sec"`
	AverageWRUPerSec float64 `json:"average_wru_per_sec"`
	TotalRRU         float64 `json:"total_rru"`
	TotalWRU         float64 `json:"total_wru"`
}

// ResourceMetrics contains the native resource metrics info.
type ResourceMetrics struct {
	TotalCPUTimeMs  float64 `json:"total_cpu_time_ms"`
	SQLCPUTimeMs    float64 `json:"sql_cpu_time_ms"`
	KVCPUTimeMs     float64 `json:"kv_cpu_time_ms"`
	KVReadRPCCount  float64 `json:"kv_read_rpc_count"`
	KVWriteRPCCount float64 `json:"kv_write_rpc_count"`
	KVReadBytes     float64 `json:"kv_read_bytes"`
	KVWriteBytes    float64 `json:"kv_write_bytes"`
}

// NewMetrics creates a new Metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		ResourceUnit: &ResourceUnitMetrics{},
		Resource:     &ResourceMetrics{},
		createTime:   time.Now(),
		updateTime:   time.Now(),
	}
}

// Update updates the metrics with the consumption info.
func (m *Metrics) Update(consumption *rmpb.Consumption) {
	if consumption == nil {
		return
	}
	now := time.Now()
	updateInterval := now.Sub(m.updateTime).Seconds()
	if updateInterval == 0 {
		return
	}
	m.updateTime = now
	createInterval := now.Sub(m.createTime).Seconds()
	// RU info.
	if consumption.RRU != 0 {
		m.ResourceUnit.TotalRRU += consumption.RRU
		m.ResourceUnit.RRUPerSec = consumption.RRU / updateInterval
		if m.ResourceUnit.RRUPerSec > m.ResourceUnit.MaxRRUPerSec {
			m.ResourceUnit.MaxRRUPerSec = m.ResourceUnit.RRUPerSec
		}
		m.ResourceUnit.AverageRRUPerSec = m.ResourceUnit.TotalRRU / createInterval
	}
	if consumption.WRU != 0 {
		m.ResourceUnit.TotalWRU += consumption.WRU
		m.ResourceUnit.WRUPerSec = consumption.WRU / updateInterval
		if m.ResourceUnit.WRUPerSec > m.ResourceUnit.MaxWRUPerSec {
			m.ResourceUnit.MaxWRUPerSec = m.ResourceUnit.WRUPerSec
		}
		m.ResourceUnit.AverageWRUPerSec = m.ResourceUnit.TotalWRU / createInterval
	}
	// Byte info.
	if consumption.ReadBytes != 0 {
		m.Resource.KVReadBytes += consumption.ReadBytes
	}
	if consumption.WriteBytes != 0 {
		m.Resource.KVWriteBytes += consumption.WriteBytes
	}
	// CPU time info.
	if consumption.TotalCpuTimeMs != 0 {
		m.Resource.TotalCPUTimeMs += consumption.TotalCpuTimeMs
	}
	if consumption.SqlLayerCpuTimeMs != 0 {
		m.Resource.SQLCPUTimeMs += consumption.SqlLayerCpuTimeMs
	}
	m.Resource.KVCPUTimeMs = m.Resource.TotalCPUTimeMs - m.Resource.SQLCPUTimeMs
	// RPC count info.
	if consumption.KvReadRpcCount != 0 {
		m.Resource.KVReadRPCCount += consumption.KvReadRpcCount
	}
	if consumption.KvWriteRpcCount != 0 {
		m.Resource.KVWriteRPCCount += consumption.KvWriteRpcCount
	}
}

// Reset will reset each filed manually to prevent from allocating new memory.
func (m *Metrics) Reset() {
	m.ResourceUnit.RRUPerSec = 0
	m.ResourceUnit.WRUPerSec = 0
	m.ResourceUnit.MaxRRUPerSec = 0
	m.ResourceUnit.MaxWRUPerSec = 0
	m.ResourceUnit.AverageRRUPerSec = 0
	m.ResourceUnit.AverageWRUPerSec = 0
	m.ResourceUnit.TotalRRU = 0
	m.ResourceUnit.TotalWRU = 0
	m.Resource.TotalCPUTimeMs = 0
	m.Resource.SQLCPUTimeMs = 0
	m.Resource.KVCPUTimeMs = 0
	m.Resource.KVReadRPCCount = 0
	m.Resource.KVWriteRPCCount = 0
	m.Resource.KVReadBytes = 0
	m.Resource.KVWriteBytes = 0
	m.createTime = time.Now()
	m.updateTime = time.Now()
}
