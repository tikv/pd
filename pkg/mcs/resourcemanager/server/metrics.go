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
	"sync"
	"sync/atomic"
	"time"

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
	keyspaceNameLabel         = "keyspace_name"
	fillRateLabel             = "fill_rate"
	burstLimitLabel           = "burst_limit"
	slotEventLabel            = "event"
	slotCreatedEventLabel     = "created"
	slotDeletedEventLabel     = "deleted"
	slotExpiredEventLabel     = "expired"
	kindLabel                 = "kind"
	throttleKindLabel         = "throttling"
	trickleKindLabel          = "trickle"
	serviceLimitCauseLabel    = "service_limit"
	groupCauseLabel           = "group_fill_rate_or_burst"

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
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	writeRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit_sum",
			Help:      "Counter of the write request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	activeRequestUnitCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "active_request_unit_sum",
			Help:      "Counter of the active request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})

	// RUv2 metrics (experimental v2 RU calculation, recording only).
	requestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "request_unit_v2_sum",
			Help:      "Counter of the total experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	tikvRequestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "tikv_request_unit_v2_sum",
			Help:      "Counter of the TiKV-side experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	tidbRequestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "tidb_request_unit_v2_sum",
			Help:      "Counter of the TiDB-side experimental v2 request unit cost for all resource groups.",
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
	tiflashRequestUnitV2Cost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "tiflash_request_unit_v2_sum",
			Help:      "Counter of the TiFlash-side experimental v2 request unit cost for all resource groups.",
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
	crossAZTrafficCost = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: resourceSubsystem,
			Name:      "cross_az_traffic_byte_sum",
			Help:      "Counter of the cross AZ traffic byte cost for all resource groups.",
		}, []string{newResourceGroupNameLabel, typeLabel, keyspaceNameLabel})
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

	overrideSettings = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "override_settings",
			Help:      "Gauge of the override settings for all resource groups.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel, typeLabel})

	serviceLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "service_limit",
			Help:      "Gauge of the total RU limit of specific keyspace.",
		}, []string{keyspaceNameLabel})

	grantedTokensHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "granted_tokens",
			Help:      "Histogram of tokens granted per request.",
			Buckets:   []float64{0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000},
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
	activeSlotCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "active_slot_count",
			Help:      "Number of active token slots per resource group.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
	slotEventsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "slot_events_total",
			Help:      "Counter of slot lifecycle events (created, deleted, expired).",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel, slotEventLabel})
	tokenLoanGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "token_loan",
			Help:      "Current total token loan amount per resource group.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
	trickleDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "trickle_duration_ms",
			Help:      "Histogram of trickle duration in milliseconds per request.",
			Buckets:   []float64{0, 100, 500, 1000, 2000, 3000, 4000, 5000, 10000},
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel})
	requestCauseCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "request_cause_total",
			Help:      "Counter of throttling and trickle contributing causes per resource group.",
		}, []string{newResourceGroupNameLabel, keyspaceNameLabel, kindLabel, "cause"})
)

type metrics struct {
	mu sync.RWMutex
	// record update time of each resource group metrics.
	consumptionRecordMap map[consumptionRecordKey]time.Time
	// max per sec trackers for each keyspace and resource group.
	maxPerSecTrackerMap map[trackerKey]*maxPerSecCostTracker
	// cached counter metrics for each keyspace, resource group and RU type.
	counterMetricsMap map[metricsKey]*counterMetrics
	// cached gauge metrics for each keyspace, resource group and RU type.
	gaugeMetricsMap map[metricsKey]*gaugeMetrics
	// cached request metrics for each keyspace and resource group.
	requestMetricsMap map[requestMetricsKey]*requestMetrics
	// cached service limit metric labels for each keyspace.
	serviceLimitMetricsMap map[uint32]string
}

type consumptionRecordKey struct {
	keyspaceID uint32
	groupName  string
	ruType     string
}

type staleConsumptionRecord struct {
	key        consumptionRecordKey
	lastUpdate time.Time
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

type requestMetricsKey struct {
	keyspaceID uint32
	groupName  string
}

func init() {
	prometheus.MustRegister(grpcStreamSendDuration)
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
	prometheus.MustRegister(activeRequestUnitCost)
	prometheus.MustRegister(requestUnitV2Cost)
	prometheus.MustRegister(tikvRequestUnitV2Cost)
	prometheus.MustRegister(tidbRequestUnitV2Cost)
	prometheus.MustRegister(tiflashRequestUnitV2Cost)
	prometheus.MustRegister(sqlLayerRequestUnitCost)
	prometheus.MustRegister(readByteCost)
	prometheus.MustRegister(writeByteCost)
	prometheus.MustRegister(crossAZTrafficCost)
	prometheus.MustRegister(kvCPUCost)
	prometheus.MustRegister(sqlCPUCost)
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(availableRUCounter)
	prometheus.MustRegister(readRequestUnitMaxPerSecCost)
	prometheus.MustRegister(writeRequestUnitMaxPerSecCost)
	prometheus.MustRegister(resourceGroupConfigGauge)
	prometheus.MustRegister(sampledRequestUnitPerSec)
	prometheus.MustRegister(overrideSettings)
	prometheus.MustRegister(serviceLimit)
	prometheus.MustRegister(grantedTokensHistogram)
	prometheus.MustRegister(activeSlotCountGauge)
	prometheus.MustRegister(slotEventsCounter)
	prometheus.MustRegister(tokenLoanGauge)
	prometheus.MustRegister(trickleDurationHistogram)
	prometheus.MustRegister(requestCauseCounter)
}

func newMetrics() *metrics {
	return &metrics{
		consumptionRecordMap:   make(map[consumptionRecordKey]time.Time),
		maxPerSecTrackerMap:    make(map[trackerKey]*maxPerSecCostTracker),
		counterMetricsMap:      make(map[metricsKey]*counterMetrics),
		gaugeMetricsMap:        make(map[metricsKey]*gaugeMetrics),
		requestMetricsMap:      make(map[requestMetricsKey]*requestMetrics),
		serviceLimitMetricsMap: make(map[uint32]string),
	}
}

// insertConsumptionRecord inserts the consumption record.
func (m *metrics) insertConsumptionRecord(keyspaceID uint32, groupName string, ruType string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertConsumptionRecordLocked(keyspaceID, groupName, ruType, now)
}

func (m *metrics) insertConsumptionRecordLocked(keyspaceID uint32, groupName string, ruType string, now time.Time) {
	key := consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     ruType,
	}
	m.consumptionRecordMap[key] = now
}

func (m *metrics) getMaxPerSecTracker(keyspaceID uint32, keyspaceName, groupName string) *maxPerSecCostTracker {
	m.mu.Lock()
	defer m.mu.Unlock()
	tracker := m.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}]
	if tracker == nil {
		tracker = newMaxPerSecCostTracker(keyspaceName, groupName, defaultCollectIntervalSec)
		m.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}] = tracker
	} else if !tracker.matchLabels(keyspaceName, groupName) {
		tracker.rebindLabels(keyspaceName, groupName)
	}
	return tracker
}

func (m *metrics) getCounterMetrics(keyspaceID uint32, keyspaceName, groupName, ruType string) *counterMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := metricsKey{keyspaceID, groupName, ruType}
	cm := m.counterMetricsMap[key]
	if cm == nil || !cm.matchLabels(keyspaceName, groupName, ruType) {
		if cm != nil {
			cm.deleteTypeLabelValues()
			if !m.hasOtherCounterMetricsWithLabelsLocked(key, cm.keyspaceName, cm.groupName) {
				deleteSharedCounterMetricLabelValues(cm.keyspaceName, cm.groupName)
			}
		}
		cm = newCounterMetrics(keyspaceName, groupName, ruType)
		m.counterMetricsMap[key] = cm
	}
	return cm
}

func (m *metrics) getGaugeMetrics(keyspaceID uint32, keyspaceName, groupName string) *gaugeMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := metricsKey{keyspaceID, groupName, ""}
	gm := m.gaugeMetricsMap[key]
	if gm == nil || !gm.matchLabels(keyspaceName, groupName) {
		if gm != nil {
			gm.deleteLabelValues()
		}
		gm = newGaugeMetrics(keyspaceName, groupName)
		m.gaugeMetricsMap[key] = gm
	}
	return gm
}

func (m *metrics) getRequestMetrics(keyspaceID uint32, keyspaceName, groupName string, now time.Time) *requestMetrics {
	key := requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}
	m.mu.RLock()
	rm := m.requestMetricsMap[key]
	if rm != nil && rm.matchLabels(keyspaceName, groupName) {
		if !rm.shouldTouchRecord(now) {
			m.mu.RUnlock()
			return rm
		}
		m.mu.RUnlock()
	} else {
		m.mu.RUnlock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	rm = m.requestMetricsMap[key]
	if rm == nil || !rm.matchLabels(keyspaceName, groupName) {
		if rm != nil {
			rm.deleteLabelValues()
		}
		rm = newRequestMetrics(keyspaceName, groupName)
		m.requestMetricsMap[key] = rm
	}
	if rm.shouldTouchRecord(now) {
		rm.lastRecordUnix.Store(now.UnixNano())
		m.insertConsumptionRecordLocked(keyspaceID, groupName, defaultTypeLabel, now)
	}
	return rm
}

func (m *metrics) deleteMetricsLocked(keyspaceID uint32, keyspaceName, groupName, ruType string, deleteGroupMetrics bool) {
	counterKey := metricsKey{keyspaceID, groupName, ruType}
	cm := m.counterMetricsMap[counterKey]
	delete(m.counterMetricsMap, counterKey)
	if cm != nil {
		cm.deleteTypeLabelValues()
		if !m.hasCounterMetricsWithLabelsLocked(keyspaceID, groupName, cm.keyspaceName, cm.groupName) {
			deleteSharedCounterMetricLabelValues(cm.keyspaceName, cm.groupName)
		}
	} else {
		deleteTypeCounterMetricLabelValues(keyspaceName, groupName, ruType)
	}

	deleteRequestMetrics := ruType == defaultTypeLabel
	if deleteRequestMetrics {
		m.deleteRequestMetricsLocked(keyspaceID, keyspaceName, groupName)
	}

	if !deleteGroupMetrics {
		return
	}
	if cm != nil {
		deleteSharedCounterMetricLabelValues(cm.keyspaceName, cm.groupName)
	} else {
		deleteSharedCounterMetricLabelValues(keyspaceName, groupName)
	}
	for key, counter := range m.counterMetricsMap {
		if key.keyspaceID != keyspaceID || key.groupName != groupName {
			continue
		}
		delete(m.counterMetricsMap, key)
		if counter != nil {
			counter.deleteLabelValues()
		} else {
			deleteCounterMetricLabelValues(keyspaceName, groupName, key.ruType)
		}
	}

	gaugeKey := metricsKey{keyspaceID, groupName, ""}
	gm := m.gaugeMetricsMap[gaugeKey]
	delete(m.gaugeMetricsMap, gaugeKey)
	tKey := trackerKey{keyspaceID, groupName}
	tracker := m.maxPerSecTrackerMap[tKey]
	delete(m.maxPerSecTrackerMap, tKey)

	if gm != nil {
		gm.deleteLabelValues()
	} else {
		deleteGaugeMetricLabelValues(keyspaceName, groupName)
	}
	if !deleteRequestMetrics {
		m.deleteRequestMetricsLocked(keyspaceID, keyspaceName, groupName)
	}
	if tracker != nil {
		tracker.deleteLabelValues()
	} else {
		deleteMaxPerSecTrackerLabelValues(keyspaceName, groupName)
	}
}

func (m *metrics) deleteRequestMetricsLocked(keyspaceID uint32, keyspaceName, groupName string) {
	requestKey := requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}
	rm := m.requestMetricsMap[requestKey]
	delete(m.requestMetricsMap, requestKey)
	if rm != nil {
		rm.deleteLabelValues()
		return
	}
	deleteRequestMetricLabelValues(keyspaceName, groupName)
}

func (m *metrics) getStaleConsumptionRecords(now time.Time) []staleConsumptionRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	records := make([]staleConsumptionRecord, 0)
	for r, lastTime := range m.consumptionRecordMap {
		if now.Sub(lastTime) > metricsCleanupTimeout {
			records = append(records, staleConsumptionRecord{
				key:        r,
				lastUpdate: lastTime,
			})
		}
	}
	return records
}

func (m *metrics) recordConsumption(
	consumptionInfo *consumptionItem,
	controllerConfig *ControllerConfig,
	now time.Time,
) {
	keyspaceID := consumptionInfo.keyspaceID
	keyspaceName := consumptionInfo.keyspaceName
	groupName := consumptionInfo.resourceGroupName
	ruLabelType := defaultTypeLabel
	if consumptionInfo.isBackground {
		ruLabelType = backgroundTypeLabel
	}
	if consumptionInfo.isTiFlash {
		ruLabelType = tiflashTypeLabel
	}
	consumption := consumptionInfo.Consumption
	if consumption == nil {
		return
	}
	m.getMaxPerSecTracker(keyspaceID, keyspaceName, groupName).collect(consumption)
	m.getCounterMetrics(keyspaceID, keyspaceName, groupName, ruLabelType).add(consumption, controllerConfig, keyspaceID)
	m.insertConsumptionRecord(keyspaceID, groupName, ruLabelType, now)
}

func (m *metrics) cleanupAllMetrics(r consumptionRecordKey, keyspaceName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupAllMetricsLocked(r, keyspaceName)
}

func (m *metrics) cleanupStaleMetrics(r staleConsumptionRecord, keyspaceName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	lastUpdate, ok := m.consumptionRecordMap[r.key]
	if !ok || !lastUpdate.Equal(r.lastUpdate) {
		return false
	}
	m.cleanupAllMetricsLocked(r.key, keyspaceName)
	return true
}

func (m *metrics) cleanupAllMetricsLocked(r consumptionRecordKey, keyspaceName string) {
	delete(m.consumptionRecordMap, r)
	deleteGroupMetrics := !m.hasConsumptionRecordLocked(r.keyspaceID, r.groupName)
	m.deleteMetricsLocked(r.keyspaceID, keyspaceName, r.groupName, r.ruType, deleteGroupMetrics)
}

func (m *metrics) hasConsumptionRecordLocked(keyspaceID uint32, groupName string) bool {
	for r := range m.consumptionRecordMap {
		if r.keyspaceID == keyspaceID && r.groupName == groupName {
			return true
		}
	}
	return false
}

func (m *metrics) hasCounterMetricsWithLabelsLocked(keyspaceID uint32, groupName, keyspaceName, labelGroupName string) bool {
	for key, counter := range m.counterMetricsMap {
		if key.keyspaceID != keyspaceID || key.groupName != groupName {
			continue
		}
		if counter != nil && counter.keyspaceName == keyspaceName && counter.groupName == labelGroupName {
			return true
		}
	}
	return false
}

func (m *metrics) hasOtherCounterMetricsWithLabelsLocked(excludedKey metricsKey, keyspaceName, labelGroupName string) bool {
	for key, counter := range m.counterMetricsMap {
		if key == excludedKey || key.keyspaceID != excludedKey.keyspaceID || key.groupName != excludedKey.groupName {
			continue
		}
		if counter != nil && counter.keyspaceName == keyspaceName && counter.groupName == labelGroupName {
			return true
		}
	}
	return false
}

type counterMetrics struct {
	keyspaceName               string
	groupName                  string
	ruLabelType                string
	RRUMetrics                 prometheus.Counter
	WRUMetrics                 prometheus.Counter
	ActiveRUMetrics            prometheus.Counter
	TotalRUV2Metrics           prometheus.Counter
	TiKVRUV2Metrics            prometheus.Counter
	TiDBRUV2Metrics            prometheus.Counter
	TiFlashRUV2Metrics         prometheus.Counter
	SQLLayerRUMetrics          prometheus.Counter
	ReadByteMetrics            prometheus.Counter
	WriteByteMetrics           prometheus.Counter
	ReadCrossAZTrafficMetrics  prometheus.Counter
	WriteCrossAZTrafficMetrics prometheus.Counter
	KvCPUMetrics               prometheus.Counter
	SQLCPUMetrics              prometheus.Counter
	ReadRequestCountMetrics    prometheus.Counter
	WriteRequestCountMetrics   prometheus.Counter
}

func newCounterMetrics(keyspaceName, groupName, ruLabelType string) *counterMetrics {
	return &counterMetrics{
		keyspaceName:               keyspaceName,
		groupName:                  groupName,
		ruLabelType:                ruLabelType,
		RRUMetrics:                 readRequestUnitCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		WRUMetrics:                 writeRequestUnitCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		ActiveRUMetrics:            activeRequestUnitCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		TotalRUV2Metrics:           requestUnitV2Cost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		TiKVRUV2Metrics:            tikvRequestUnitV2Cost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		TiDBRUV2Metrics:            tidbRequestUnitV2Cost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		TiFlashRUV2Metrics:         tiflashRequestUnitV2Cost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		SQLLayerRUMetrics:          sqlLayerRequestUnitCost.WithLabelValues(groupName, groupName, keyspaceName),
		ReadByteMetrics:            readByteCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		WriteByteMetrics:           writeByteCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		ReadCrossAZTrafficMetrics:  crossAZTrafficCost.WithLabelValues(groupName, "read", keyspaceName),
		WriteCrossAZTrafficMetrics: crossAZTrafficCost.WithLabelValues(groupName, "write", keyspaceName),
		KvCPUMetrics:               kvCPUCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		SQLCPUMetrics:              sqlCPUCost.WithLabelValues(groupName, groupName, ruLabelType, keyspaceName),
		ReadRequestCountMetrics:    requestCount.WithLabelValues(groupName, groupName, readTypeLabel, keyspaceName),
		WriteRequestCountMetrics:   requestCount.WithLabelValues(groupName, groupName, writeTypeLabel, keyspaceName),
	}
}

func (m *counterMetrics) matchLabels(keyspaceName, groupName, ruLabelType string) bool {
	return m != nil && m.keyspaceName == keyspaceName && m.groupName == groupName && m.ruLabelType == ruLabelType
}

func (m *counterMetrics) deleteLabelValues() {
	if m == nil {
		return
	}
	deleteCounterMetricLabelValues(m.keyspaceName, m.groupName, m.ruLabelType)
}

func (m *counterMetrics) deleteTypeLabelValues() {
	if m == nil {
		return
	}
	deleteTypeCounterMetricLabelValues(m.keyspaceName, m.groupName, m.ruLabelType)
}

func calculateSQLRU(consumption *rmpb.Consumption, controllerConfig *ControllerConfig) float64 {
	if consumption.TotalCpuTimeMs <= 0 || consumption.SqlLayerCpuTimeMs <= 0 {
		return 0
	}
	if controllerConfig == nil {
		return 0
	}
	return consumption.SqlLayerCpuTimeMs * controllerConfig.RequestUnit.CPUMsCost
}

func calculateActiveRU(consumption *rmpb.Consumption, controllerConfig *ControllerConfig, keyspaceID uint32) float64 {
	if controllerConfig.getKeyspaceRUVersion(keyspaceID) == RUVersionV2 {
		return consumption.TikvRUV2 + consumption.TidbRUV2 + consumption.TiflashRUV2
	}
	return consumption.RRU + consumption.WRU + calculateSQLRU(consumption, controllerConfig)
}

func (m *counterMetrics) add(consumption *rmpb.Consumption, controllerConfig *ControllerConfig, keyspaceID uint32) {
	if consumption == nil {
		return
	}
	// RU info.
	if consumption.RRU > 0 {
		m.RRUMetrics.Add(consumption.RRU)
	}
	if consumption.WRU > 0 {
		m.WRUMetrics.Add(consumption.WRU)
	}
	if activeRU := calculateActiveRU(consumption, controllerConfig, keyspaceID); activeRU > 0 {
		m.ActiveRUMetrics.Add(activeRU)
	}
	// RUv2 info (experimental).
	if consumption.TikvRUV2 > 0 {
		m.TiKVRUV2Metrics.Add(consumption.TikvRUV2)
	}
	if consumption.TidbRUV2 > 0 {
		m.TiDBRUV2Metrics.Add(consumption.TidbRUV2)
	}
	if consumption.TiflashRUV2 > 0 {
		m.TiFlashRUV2Metrics.Add(consumption.TiflashRUV2)
	}
	if ruv2 := consumption.TikvRUV2 + consumption.TidbRUV2 + consumption.TiflashRUV2; ruv2 > 0 {
		m.TotalRUV2Metrics.Add(ruv2)
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
		if sqlRU := calculateSQLRU(consumption, controllerConfig); sqlRU > 0 {
			m.SQLLayerRUMetrics.Add(sqlRU)
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
	if consumption.ReadCrossAzTrafficBytes > 0 {
		m.ReadCrossAZTrafficMetrics.Add(float64(consumption.ReadCrossAzTrafficBytes))
	}
	if consumption.WriteCrossAzTrafficBytes > 0 {
		m.WriteCrossAZTrafficMetrics.Add(float64(consumption.WriteCrossAzTrafficBytes))
	}
}

type requestMetrics struct {
	keyspaceName                string
	groupName                   string
	grantedTokensObserver       prometheus.Observer
	trickleDurationObserver     prometheus.Observer
	serviceLimitThrottleCounter prometheus.Counter
	groupThrottleCounter        prometheus.Counter
	serviceLimitTrickleCounter  prometheus.Counter
	groupTrickleCounter         prometheus.Counter
	lastRecordUnix              atomic.Int64
}

type requestMetricsObservation struct {
	grantedTokens   float64
	trickleTimeMs   int64
	serviceLimited  bool
	groupLimited    bool
	serviceTrickled bool
	groupTrickled   bool
}

func newRequestMetrics(keyspaceName, groupName string) *requestMetrics {
	return &requestMetrics{
		keyspaceName:                keyspaceName,
		groupName:                   groupName,
		grantedTokensObserver:       grantedTokensHistogram.WithLabelValues(groupName, keyspaceName),
		trickleDurationObserver:     trickleDurationHistogram.WithLabelValues(groupName, keyspaceName),
		serviceLimitThrottleCounter: requestCauseCounter.WithLabelValues(groupName, keyspaceName, throttleKindLabel, serviceLimitCauseLabel),
		groupThrottleCounter:        requestCauseCounter.WithLabelValues(groupName, keyspaceName, throttleKindLabel, groupCauseLabel),
		serviceLimitTrickleCounter:  requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, serviceLimitCauseLabel),
		groupTrickleCounter:         requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, groupCauseLabel),
	}
}

func (m *requestMetrics) matchLabels(keyspaceName, groupName string) bool {
	return m != nil && m.keyspaceName == keyspaceName && m.groupName == groupName
}

func (m *requestMetrics) deleteLabelValues() {
	if m == nil {
		return
	}
	deleteRequestMetricLabelValues(m.keyspaceName, m.groupName)
}

func (m *requestMetrics) shouldTouchRecord(now time.Time) bool {
	last := m.lastRecordUnix.Load()
	return now.Sub(time.Unix(0, last)) >= metricsCleanupInterval
}

func (m *requestMetrics) observe(observation requestMetricsObservation) {
	if m == nil {
		return
	}
	m.grantedTokensObserver.Observe(observation.grantedTokens)
	m.trickleDurationObserver.Observe(float64(observation.trickleTimeMs))
	if observation.serviceLimited {
		m.serviceLimitThrottleCounter.Inc()
	}
	if observation.groupLimited {
		m.groupThrottleCounter.Inc()
	}
	if observation.serviceTrickled {
		m.serviceLimitTrickleCounter.Inc()
	}
	if observation.groupTrickled {
		m.groupTrickleCounter.Inc()
	}
}

type gaugeMetrics struct {
	keyspaceName                       string
	groupName                          string
	availableRUCounter                 prometheus.Gauge
	priorityResourceGroupConfigGauge   prometheus.Gauge
	ruPerSecResourceGroupConfigGauge   prometheus.Gauge
	ruCapacityResourceGroupConfigGauge prometheus.Gauge
	sampledRequestUnitPerSecGauge      prometheus.Gauge
	overrideFillRateGauge              prometheus.Gauge
	overrideBurstLimitGauge            prometheus.Gauge
	activeSlotCountGauge               prometheus.Gauge
	tokenLoanGauge                     prometheus.Gauge
	slotCreatedCounter                 prometheus.Counter
	slotDeletedCounter                 prometheus.Counter
	slotExpiredCounter                 prometheus.Counter
}

func newGaugeMetrics(keyspaceName, groupName string) *gaugeMetrics {
	return &gaugeMetrics{
		keyspaceName:                       keyspaceName,
		groupName:                          groupName,
		availableRUCounter:                 availableRUCounter.WithLabelValues(groupName, groupName, keyspaceName),
		priorityResourceGroupConfigGauge:   resourceGroupConfigGauge.WithLabelValues(groupName, priorityLabel, keyspaceName),
		ruPerSecResourceGroupConfigGauge:   resourceGroupConfigGauge.WithLabelValues(groupName, ruPerSecLabel, keyspaceName),
		ruCapacityResourceGroupConfigGauge: resourceGroupConfigGauge.WithLabelValues(groupName, ruCapacityLabel, keyspaceName),
		sampledRequestUnitPerSecGauge:      sampledRequestUnitPerSec.WithLabelValues(groupName, keyspaceName),
		overrideFillRateGauge:              overrideSettings.WithLabelValues(groupName, keyspaceName, fillRateLabel),
		overrideBurstLimitGauge:            overrideSettings.WithLabelValues(groupName, keyspaceName, burstLimitLabel),
		activeSlotCountGauge:               activeSlotCountGauge.WithLabelValues(groupName, keyspaceName),
		tokenLoanGauge:                     tokenLoanGauge.WithLabelValues(groupName, keyspaceName),
		slotCreatedCounter:                 slotEventsCounter.WithLabelValues(groupName, keyspaceName, slotCreatedEventLabel),
		slotDeletedCounter:                 slotEventsCounter.WithLabelValues(groupName, keyspaceName, slotDeletedEventLabel),
		slotExpiredCounter:                 slotEventsCounter.WithLabelValues(groupName, keyspaceName, slotExpiredEventLabel),
	}
}

func (m *gaugeMetrics) matchLabels(keyspaceName, groupName string) bool {
	return m != nil && m.keyspaceName == keyspaceName && m.groupName == groupName
}

func (m *gaugeMetrics) deleteLabelValues() {
	if m == nil {
		return
	}
	deleteGaugeMetricLabelValues(m.keyspaceName, m.groupName)
}

func (m *gaugeMetrics) setGroup(group *ResourceGroup, keyspaceName string) {
	// skip basic metrics for the default group.
	if group.Name != DefaultResourceGroupName {
		ru := math.Max(group.getRUToken(), 0)
		m.availableRUCounter.Set(ru)
		m.priorityResourceGroupConfigGauge.Set(group.getPriority())
		m.ruPerSecResourceGroupConfigGauge.Set(group.getFillRate(true))
		m.ruCapacityResourceGroupConfigGauge.Set(float64(group.getBurstLimit(true)))
	}

	// Set the override fill rate and burst limit and delete the metrics if the override is not set.
	overrideFillRate := group.getOverrideFillRate()
	if overrideFillRate == -1 {
		overrideSettings.DeleteLabelValues(group.Name, keyspaceName, fillRateLabel)
	} else {
		m.overrideFillRateGauge.Set(overrideFillRate)
	}
	overrideBurstLimit := group.getOverrideBurstLimit()
	if overrideBurstLimit == -1 {
		overrideSettings.DeleteLabelValues(group.Name, keyspaceName, burstLimitLabel)
	} else {
		m.overrideBurstLimitGauge.Set(float64(overrideBurstLimit))
	}

	slotMetrics := group.GetSlotMetrics()
	m.activeSlotCountGauge.Set(float64(slotMetrics.SlotCount))
	m.tokenLoanGauge.Set(slotMetrics.TokenLoan)
	if created, deleted, expired := group.DrainSlotEvents(); created+deleted+expired > 0 {
		m.slotCreatedCounter.Add(float64(created))
		m.slotDeletedCounter.Add(float64(deleted))
		m.slotExpiredCounter.Add(float64(expired))
	}
}

func (m *gaugeMetrics) setSampledRUPerSec(ruPerSec float64) {
	m.sampledRequestUnitPerSecGauge.Set(ruPerSec)
}

func (m *metrics) setOrRemoveServiceLimitMetrics(keyspaceID uint32, keyspaceName string, limit float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if oldKeyspaceName, ok := m.serviceLimitMetricsMap[keyspaceID]; ok && oldKeyspaceName != keyspaceName {
		serviceLimit.DeleteLabelValues(oldKeyspaceName)
		delete(m.serviceLimitMetricsMap, keyspaceID)
	}
	if limit > 0 {
		serviceLimit.WithLabelValues(keyspaceName).Set(limit)
		m.serviceLimitMetricsMap[keyspaceID] = keyspaceName
	} else {
		serviceLimit.DeleteLabelValues(keyspaceName)
		delete(m.serviceLimitMetricsMap, keyspaceID)
	}
}

func deleteCounterMetricLabelValues(keyspaceName, groupName, ruLabelType string) {
	deleteTypeCounterMetricLabelValues(keyspaceName, groupName, ruLabelType)
	deleteSharedCounterMetricLabelValues(keyspaceName, groupName)
}

func deleteTypeCounterMetricLabelValues(keyspaceName, groupName, ruLabelType string) {
	readRequestUnitCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	writeRequestUnitCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	activeRequestUnitCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	requestUnitV2Cost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	tikvRequestUnitV2Cost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	tidbRequestUnitV2Cost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	tiflashRequestUnitV2Cost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	readByteCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	writeByteCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	kvCPUCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
	sqlCPUCost.DeleteLabelValues(groupName, groupName, ruLabelType, keyspaceName)
}

func deleteSharedCounterMetricLabelValues(keyspaceName, groupName string) {
	sqlLayerRequestUnitCost.DeleteLabelValues(groupName, groupName, keyspaceName)
	crossAZTrafficCost.DeleteLabelValues(groupName, readTypeLabel, keyspaceName)
	crossAZTrafficCost.DeleteLabelValues(groupName, writeTypeLabel, keyspaceName)
	requestCount.DeleteLabelValues(groupName, groupName, readTypeLabel, keyspaceName)
	requestCount.DeleteLabelValues(groupName, groupName, writeTypeLabel, keyspaceName)
}

func deleteGaugeMetricLabelValues(keyspaceName, groupName string) {
	availableRUCounter.DeleteLabelValues(groupName, groupName, keyspaceName)
	resourceGroupConfigGauge.DeleteLabelValues(groupName, priorityLabel, keyspaceName)
	resourceGroupConfigGauge.DeleteLabelValues(groupName, ruPerSecLabel, keyspaceName)
	resourceGroupConfigGauge.DeleteLabelValues(groupName, ruCapacityLabel, keyspaceName)
	sampledRequestUnitPerSec.DeleteLabelValues(groupName, keyspaceName)
	overrideSettings.DeleteLabelValues(groupName, keyspaceName, fillRateLabel)
	overrideSettings.DeleteLabelValues(groupName, keyspaceName, burstLimitLabel)
	activeSlotCountGauge.DeleteLabelValues(groupName, keyspaceName)
	tokenLoanGauge.DeleteLabelValues(groupName, keyspaceName)
	slotEventsCounter.DeleteLabelValues(groupName, keyspaceName, slotCreatedEventLabel)
	slotEventsCounter.DeleteLabelValues(groupName, keyspaceName, slotDeletedEventLabel)
	slotEventsCounter.DeleteLabelValues(groupName, keyspaceName, slotExpiredEventLabel)
}

func deleteMaxPerSecTrackerLabelValues(keyspaceName, groupName string) {
	readRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
	writeRequestUnitMaxPerSecCost.DeleteLabelValues(groupName, keyspaceName)
}

func deleteRequestMetricLabelValues(keyspaceName, groupName string) {
	grantedTokensHistogram.DeleteLabelValues(groupName, keyspaceName)
	trickleDurationHistogram.DeleteLabelValues(groupName, keyspaceName)
	requestCauseCounter.DeleteLabelValues(groupName, keyspaceName, throttleKindLabel, serviceLimitCauseLabel)
	requestCauseCounter.DeleteLabelValues(groupName, keyspaceName, throttleKindLabel, groupCauseLabel)
	requestCauseCounter.DeleteLabelValues(groupName, keyspaceName, trickleKindLabel, serviceLimitCauseLabel)
	requestCauseCounter.DeleteLabelValues(groupName, keyspaceName, trickleKindLabel, groupCauseLabel)
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

func (t *maxPerSecCostTracker) matchLabels(keyspaceName, groupName string) bool {
	return t != nil && t.keyspaceName == keyspaceName && t.groupName == groupName
}

func (t *maxPerSecCostTracker) deleteLabelValues() {
	if t == nil {
		return
	}
	deleteMaxPerSecTrackerLabelValues(t.keyspaceName, t.groupName)
}

func (t *maxPerSecCostTracker) rebindLabels(keyspaceName, groupName string) {
	t.deleteLabelValues()
	t.keyspaceName = keyspaceName
	t.groupName = groupName
	t.rruMaxMetrics = readRequestUnitMaxPerSecCost.WithLabelValues(groupName, keyspaceName)
	t.wruMaxMetrics = writeRequestUnitMaxPerSecCost.WithLabelValues(groupName, keyspaceName)
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

func newAcquireTokenBucketsMetricsStream(stream rmpb.ResourceManager_AcquireTokenBucketsServer) rmpb.ResourceManager_AcquireTokenBucketsServer {
	return grpcutil.NewMetricsStream(stream, stream.Send, stream.Recv, grpcStreamSendDuration, "acquire-token-buckets")
}
