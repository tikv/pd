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
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
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
	keyspaceNameLabel         = "keyspace_name"

	// Labels for the config.
	ruPerSecLabel   = "ru_per_sec"
	ruCapacityLabel = "ru_capacity"
	priorityLabel   = "priority"
)

var (
	// RU cost metrics.
	// `sum` is added to the name to maintain compatibility with the previous use of histogram.
	readRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit_sum",
			Help:      "Counter of the read request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	writeRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit_sum",
			Help:      "Counter of the write request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})

	readRequestUnitMaxPerSecCost = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit_max_per_sec",
			Help:      "Gauge of the max read request unit per second for all resource groups.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
	writeRequestUnitMaxPerSecCost = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit_max_per_sec",
			Help:      "Gauge of the max write request unit per second for all resource groups.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})

	sqlLayerRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "sql_layer_request_unit_sum",
			Help:      "The number of the sql layer request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, keyspaceNameLabel})

	// Resource cost metrics.
	readByteCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "read_byte_sum",
			Help:      "Counter of the read byte cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	writeByteCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "write_byte_sum",
			Help:      "Counter of the write byte cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	kvCPUCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "kv_cpu_time_ms_sum",
			Help:      "Counter of the KV CPU time cost in milliseconds for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	sqlCPUCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "sql_cpu_time_ms_sum",
			Help:      "Counter of the SQL CPU time cost in milliseconds for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "request_count",
			Help:      "The number of read/write requests for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})

	availableRUCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "available_ru",
			Help:      "Counter of the available RU for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, keyspaceNameLabel})

	resourceGroupConfigGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "group_config",
			Help:      "Config of the resource group.",
		}, []string{newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})

	sampledRequestUnitPerSec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "sampled_request_unit_per_sec",
			Help:      "Gauge of the sampled RU/s for all resource groups.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
)

type metrics struct {
	// record update time of each resource group
	consumptionRecordMap map[consumptionRecordKey]time.Time
	// max per sec trackers for each keyspace and resource group.
	maxPerSecTrackerMap map[trackerKey]*maxPerSecCostTracker
	// cached counter metrics for each keyspace, resource group and RU type.
	counterMetricsMap map[metricsKey]*counterMetrics
	// cached gauge metrics for each keyspace, resource group and RU type.
	gaugeMetricsMap map[metricsKey]*gaugeMetrics
}

type consumptionRecordKey struct {
	keyspaceID uint32
	groupName  string
	ruType     string
}

type metricsKey struct {
	keyspaceID uint32
	groupName  string
	ruType     string
}

type trackerKey struct {
	keyspaceID uint32
	groupName  string
}

func init() {
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
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
	prometheus.MustRegister(sampledRequestUnitPerSec)
}

func newMetrics() *metrics {
	return &metrics{
		consumptionRecordMap: make(map[consumptionRecordKey]time.Time),
		maxPerSecTrackerMap:  make(map[trackerKey]*maxPerSecCostTracker),
		counterMetricsMap:    make(map[metricsKey]*counterMetrics),
		gaugeMetricsMap:      make(map[metricsKey]*gaugeMetrics),
	}
}

// insertConsumptionRecord inserts the consumption record and returns the duration since the last record.
// If the record is not found, it returns 0 as the duration.
func (m *metrics) insertConsumptionRecord(keyspaceID uint32, groupName string, ruType string, now time.Time) time.Duration {
	key := consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     ruType,
	}
	last, ok := m.consumptionRecordMap[key]
	m.consumptionRecordMap[key] = now
	if !ok {
		return 0
	}
	return now.Sub(last)
}

func (m *metrics) deleteConsumptionRecord(record consumptionRecordKey) {
	delete(m.consumptionRecordMap, record)
}

func (m *metrics) getMaxPerSecTracker(keyspaceID uint32, keyspaceName, groupName string) *maxPerSecCostTracker {
	tracker := m.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}]
	if tracker == nil {
		tracker = newMaxPerSecCostTracker(keyspaceName, groupName, defaultCollectIntervalSec)
		m.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}] = tracker
	}
	return tracker
}

func (m *metrics) deleteMaxPerSecTracker(keyspaceID uint32, groupName string) {
	delete(m.maxPerSecTrackerMap, trackerKey{keyspaceID, groupName})
}

func (m *metrics) getCounterMetrics(keyspaceID uint32, keyspaceName, groupName, ruType string) *counterMetrics {
	key := metricsKey{keyspaceID, groupName, ruType}
	if m.counterMetricsMap[key] == nil {
		m.counterMetricsMap[key] = newCounterMetrics(keyspaceName, groupName, ruType)
	}
	return m.counterMetricsMap[key]
}

func (m *metrics) getGaugeMetrics(keyspaceID uint32, keyspaceName, groupName string) *gaugeMetrics {
	key := metricsKey{keyspaceID, groupName, ""}
	if m.gaugeMetricsMap[key] == nil {
		m.gaugeMetricsMap[key] = newGaugeMetrics(keyspaceName, groupName)
	}
	return m.gaugeMetricsMap[key]
}

func (m *metrics) deleteMetrics(keyspaceID uint32, keyspaceName, groupName, ruType string) {
	delete(m.counterMetricsMap, metricsKey{keyspaceID, groupName, ruType})
	delete(m.gaugeMetricsMap, metricsKey{keyspaceID, groupName, ""})
	deleteLabelValues(keyspaceName, groupName, ruType)
}

func (m *metrics) recordConsumption(
	consumptionInfo *consumptionItem,
	keyspaceName string,
	controllerConfig *ControllerConfig,
	now time.Time,
) time.Duration {
	keyspaceID := consumptionInfo.keyspaceID
	groupName := consumptionInfo.resourceGroupName
	ruLabelType := defaultTypeLabel
	if consumptionInfo.isBackground {
		ruLabelType = backgroundTypeLabel
	}
	if consumptionInfo.isTiFlash {
		ruLabelType = tiflashTypeLabel
	}
	consumption := consumptionInfo.Consumption
	m.getMaxPerSecTracker(keyspaceID, keyspaceName, groupName).collect(consumption)
	m.getCounterMetrics(keyspaceID, keyspaceName, groupName, ruLabelType).add(consumption, controllerConfig)
	return m.insertConsumptionRecord(keyspaceID, groupName, ruLabelType, now)
}

func (m *metrics) cleanupAllMetrics(r consumptionRecordKey, keyspaceName string) {
	m.deleteConsumptionRecord(r)
	m.deleteMetrics(r.keyspaceID, keyspaceName, r.groupName, r.ruType)
	m.deleteMaxPerSecTracker(r.keyspaceID, r.groupName)
}

type counterMetrics struct {
	RRUMetrics               prometheus.Counter
	WRUMetrics               prometheus.Counter
	SQLLayerRUMetrics        prometheus.Counter
	ReadByteMetrics          prometheus.Counter
	WriteByteMetrics         prometheus.Counter
	KvCPUMetrics             prometheus.Counter
	SQLCPUMetrics            prometheus.Counter
	ReadRequestCountMetrics  prometheus.Counter
	WriteRequestCountMetrics prometheus.Counter
}

func newCounterMetrics(keyspaceName, groupName, ruLabelType string) *counterMetrics {
	return &counterMetrics{
		RRUMetrics:               readRequestUnitCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		WRUMetrics:               writeRequestUnitCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		SQLLayerRUMetrics:        sqlLayerRequestUnitCost.WithLabelValues(groupName, groupName, keyspaceName),
		ReadByteMetrics:          readByteCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		WriteByteMetrics:         writeByteCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		KvCPUMetrics:             kvCPUCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		SQLCPUMetrics:            sqlCPUCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		ReadRequestCountMetrics:  requestCount.WithLabelValues(groupName, groupName, readTypeLabel, keyspaceName),
		WriteRequestCountMetrics: requestCount.WithLabelValues(groupName, groupName, writeTypeLabel, keyspaceName),
	}
}

func (m *counterMetrics) add(consumption *rmpb.Consumption, controllerConfig *ControllerConfig) {
	// RU info.
	if consumption.RRU > 0 {
		m.RRUMetrics.Add(consumption.RRU)
	}
	if consumption.WRU > 0 {
		m.WRUMetrics.Add(consumption.WRU)
	}
	// Byte info.
	if consumption.ReadBytes > 0 {
		m.ReadByteMetrics.Add(consumption.ReadBytes)
	}
	if consumption.WriteBytes > 0 {
		m.WriteByteMetrics.Add(consumption.WriteBytes)
	}
	// CPU time info.
	if consumption.TotalCpuTimeMs > 0 {
		if consumption.SqlLayerCpuTimeMs > 0 {
			m.SQLLayerRUMetrics.Add(consumption.SqlLayerCpuTimeMs * controllerConfig.RequestUnit.CPUMsCost)
			m.SQLCPUMetrics.Add(consumption.SqlLayerCpuTimeMs)
		}
		m.KvCPUMetrics.Add(consumption.TotalCpuTimeMs - consumption.SqlLayerCpuTimeMs)
	}
	// RPC count info.
	if consumption.KvReadRpcCount > 0 {
		m.ReadRequestCountMetrics.Add(consumption.KvReadRpcCount)
	}
	if consumption.KvWriteRpcCount > 0 {
		m.WriteRequestCountMetrics.Add(consumption.KvWriteRpcCount)
	}
}

type gaugeMetrics struct {
	availableRUCounter                 prometheus.Gauge
	priorityResourceGroupConfigGauge   prometheus.Gauge
	ruPerSecResourceGroupConfigGauge   prometheus.Gauge
	ruCapacityResourceGroupConfigGauge prometheus.Gauge
	sampledRequestUnitPerSecGauge      prometheus.Gauge
}

func newGaugeMetrics(keyspaceName, groupName string) *gaugeMetrics {
	return &gaugeMetrics{
		availableRUCounter:                 availableRUCounter.WithLabelValues(groupName, groupName, keyspaceName),
		priorityResourceGroupConfigGauge:   resourceGroupConfigGauge.WithLabelValues(groupName, priorityLabel, keyspaceName),
		ruPerSecResourceGroupConfigGauge:   resourceGroupConfigGauge.WithLabelValues(groupName, ruPerSecLabel, keyspaceName),
		ruCapacityResourceGroupConfigGauge: resourceGroupConfigGauge.WithLabelValues(groupName, ruCapacityLabel, keyspaceName),
		sampledRequestUnitPerSecGauge:      sampledRequestUnitPerSec.WithLabelValues(groupName, keyspaceName),
	}
}

func (m *gaugeMetrics) setGroup(group *ResourceGroup) {
	ru := math.Max(group.getRUToken(), 0)
	m.availableRUCounter.Set(ru)
	m.priorityResourceGroupConfigGauge.Set(group.getPriority())
	m.ruPerSecResourceGroupConfigGauge.Set(group.getFillRate())
	m.ruCapacityResourceGroupConfigGauge.Set(group.getBurstLimit())
}

func (m *gaugeMetrics) setSampledRUPerSec(ruPerSec float64) {
	m.sampledRequestUnitPerSecGauge.Set(ruPerSec)
}

func deleteLabelValues(keyspaceName, groupName, ruLabelType string) {
	readRequestUnitCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	writeRequestUnitCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	sqlLayerRequestUnitCost.DeleteLabelValues(groupName, groupName, keyspaceName)
	readByteCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	writeByteCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	kvCPUCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	sqlCPUCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	requestCount.DeleteLabelValues(groupName, groupName, readTypeLabel, keyspaceName)
	requestCount.DeleteLabelValues(groupName, groupName, writeTypeLabel, keyspaceName)
	availableRUCounter.DeleteLabelValues(groupName, groupName, keyspaceName)
	readRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
	writeRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
	readRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
	writeRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
	sampledRequestUnitPerSec.DeleteLabelValues(groupName, keyspaceName)
	resourceGroupConfigGauge.DeletePartialMatch(prometheus.Labels{newResourceGroupNameLabel: groupName, keyspaceNameLabel: keyspaceName})
}

type maxPerSecCostTracker struct {
	keyspaceName  string
	groupName     string
	maxPerSecRRU  float64
	maxPerSecWRU  float64
	rruSum        float64
	wruSum        float64
	lastRRUSum    float64
	lastWRUSum    float64
	flushPeriod   int
	cnt           int
	rruMaxMetrics prometheus.Gauge
	wruMaxMetrics prometheus.Gauge
}

func newMaxPerSecCostTracker(keyspaceName, groupName string, flushPeriod int) *maxPerSecCostTracker {
	return &maxPerSecCostTracker{
		keyspaceName:  keyspaceName,
		groupName:     groupName,
		flushPeriod:   flushPeriod,
		rruMaxMetrics: readRequestUnitMaxPerSecCost.WithLabelValues(groupName, keyspaceName),
		wruMaxMetrics: writeRequestUnitMaxPerSecCost.WithLabelValues(groupName, keyspaceName),
	}
}

func (t *maxPerSecCostTracker) collect(consume *rmpb.Consumption) {
	t.rruSum += consume.RRU
	t.wruSum += consume.WRU
}

func (t *maxPerSecCostTracker) flushMetrics() {
	if t.lastRRUSum == 0 && t.lastWRUSum == 0 {
		t.lastRRUSum = t.rruSum
		t.lastWRUSum = t.wruSum
		return
	}
	deltaRRU := t.rruSum - t.lastRRUSum
	deltaWRU := t.wruSum - t.lastWRUSum
	t.lastRRUSum = t.rruSum
	t.lastWRUSum = t.wruSum
	if deltaRRU > t.maxPerSecRRU {
		t.maxPerSecRRU = deltaRRU
	}
	if deltaWRU > t.maxPerSecWRU {
		t.maxPerSecWRU = deltaWRU
	}
	t.cnt++
	// flush to metrics in every flushPeriod.
	if t.cnt%t.flushPeriod == 0 {
		t.rruMaxMetrics.Set(t.maxPerSecRRU)
		t.wruMaxMetrics.Set(t.maxPerSecWRU)
		t.maxPerSecRRU = 0
		t.maxPerSecWRU = 0
	}
}
