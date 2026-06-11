// Copyright 2024 TiKV Project Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

func TestActiveRequestUnitMetricUsesV1ByDefault(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "billing-v1-keyspace"
		groupName    = "billing-v1-group"
		keyspaceID   = uint32(101)
	)

	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})

	metrics := newCounterMetrics(keyspaceName, groupName, defaultTypeLabel)
	metrics.add(&rmpb.Consumption{
		RRU:               10,
		WRU:               20,
		TotalCpuTimeMs:    4,
		SqlLayerCpuTimeMs: 3,
		TikvRUV2:          100,
		TidbRUV2:          200,
		TiflashRUV2:       300,
	}, &ControllerConfig{
		RequestUnit: RequestUnitConfig{CPUMsCost: 2},
	}, keyspaceID)

	re.Equal(float64(36), testutil.ToFloat64(metrics.ActiveRUMetrics))
}

func TestActiveRequestUnitMetricUsesV2ForOverriddenKeyspace(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "billing-v2-keyspace"
		groupName    = "billing-v2-group"
		keyspaceID   = uint32(202)
	)

	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})

	metrics := newCounterMetrics(keyspaceName, groupName, defaultTypeLabel)
	metrics.add(&rmpb.Consumption{
		RRU:               10,
		WRU:               20,
		TotalCpuTimeMs:    4,
		SqlLayerCpuTimeMs: 3,
		TikvRUV2:          7,
		TidbRUV2:          11,
		TiflashRUV2:       13,
	}, &ControllerConfig{
		RequestUnit: RequestUnitConfig{CPUMsCost: 2},
		RUVersionPolicy: &RUVersionPolicy{
			Default: RUVersionV1,
			Overrides: map[uint32]RUVersion{
				keyspaceID: RUVersionV2,
			},
		},
	}, keyspaceID)

	re.Equal(float64(31), testutil.ToFloat64(metrics.ActiveRUMetrics))
}

func TestMaxPerSecCostTracker(t *testing.T) {
	re := require.New(t)
	tracker := newMaxPerSecCostTracker("test", "test", defaultCollectIntervalSec)

	// Define the expected max values for each flushPeriod
	expectedMaxRU := []float64{19, 39, 59}
	expectedSum := []float64{190, 780, 1770}

	for i := range 60 {
		// Record data
		consumption := &rmpb.Consumption{
			RRU: float64(i),
			WRU: float64(i),
		}
		tracker.collect(consumption)
		tracker.flushMetrics()

		// Check the max values at the end of each flushPeriod
		if (i+1)%20 == 0 {
			period := i / 20
			re.Equalf(tracker.maxPerSecRRU, expectedMaxRU[period], "maxPerSecRRU in period %d is incorrect", period+1)
			re.Equalf(tracker.maxPerSecWRU, expectedMaxRU[period], "maxPerSecWRU in period %d is incorrect", period+1)
			re.Equal(tracker.rruSum, expectedSum[period])
			re.Equal(tracker.rruSum, expectedSum[period])
		}
	}
}

func TestObserveTokenGrantRecordsZeroTrickle(t *testing.T) {
	re := require.New(t)
	groupName := "observe_token_group"
	keyspaceName := "observe_token_keyspace"
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})

	metrics := newRequestMetrics(keyspaceName, groupName)
	metrics.observe(requestMetricsObservation{
		grantedTokens: 42,
		trickleTimeMs: 0,
	})

	granted := findHistogramMetric(re, "resource_manager_server_granted_tokens", map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	})
	re.Equal(uint64(1), granted.GetSampleCount())
	re.Equal(42.0, granted.GetSampleSum())

	trickle := findHistogramMetric(re, "resource_manager_server_trickle_duration_ms", map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	})
	re.Equal(uint64(1), trickle.GetSampleCount())
	re.Zero(trickle.GetSampleSum())
}

func TestObserveRequestCause(t *testing.T) {
	re := require.New(t)
	groupName := "observe_request_group"
	keyspaceName := "observe_request_keyspace"
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})
	throttleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, throttleKindLabel, serviceLimitCauseLabel)
	trickleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, groupCauseLabel)
	metrics := newRequestMetrics(keyspaceName, groupName)

	metrics.observe(requestMetricsObservation{serviceLimited: true})
	metrics.observe(requestMetricsObservation{groupTrickled: true})

	re.Equal(1.0, testutil.ToFloat64(throttleCounter))
	re.Equal(1.0, testutil.ToFloat64(trickleCounter))
}

func TestServiceLimitMetricsDeletesCachedFallbackLabelWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10879)
		fallbackName = "keyspace-10879"
		loadedName   = "loaded-service-limit-keyspace"
		metricName   = "resource_manager_resource_unit_service_limit"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		serviceLimit.DeleteLabelValues(fallbackName)
		serviceLimit.DeleteLabelValues(loadedName)
	})

	metrics.setOrRemoveServiceLimitMetrics(keyspaceID, fallbackName, 100)
	re.True(hasMetric(metricName, map[string]string{keyspaceNameLabel: fallbackName}))

	metrics.setOrRemoveServiceLimitMetrics(keyspaceID, loadedName, 100)
	re.False(hasMetric(metricName, map[string]string{keyspaceNameLabel: fallbackName}))
	re.True(hasMetric(metricName, map[string]string{keyspaceNameLabel: loadedName}))

	metrics.setOrRemoveServiceLimitMetrics(keyspaceID, loadedName, 0)
	re.False(hasMetric(metricName, map[string]string{keyspaceNameLabel: loadedName}))
}

func TestRequestMetricsCachedAndCleanedUp(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10867)
		keyspaceName = "request_metrics_keyspace"
		groupName    = "request_metrics_group"
	)
	now := time.Now()
	metrics := newMetrics()

	first := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, now)
	second := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, now.Add(time.Second))
	re.Same(first, second)

	recordKey := consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}
	metrics.mu.RLock()
	re.Equal(now, metrics.consumptionRecordMap[recordKey])
	_, ok := metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	metrics.mu.RUnlock()
	re.True(ok)

	touchTime := now.Add(metricsCleanupInterval + time.Second)
	third := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, touchTime)
	re.Same(first, third)
	metrics.mu.RLock()
	re.Equal(touchTime, metrics.consumptionRecordMap[recordKey])
	metrics.mu.RUnlock()

	metrics.cleanupAllMetrics(recordKey, keyspaceName)

	metrics.mu.RLock()
	_, ok = metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	_, recordExists := metrics.consumptionRecordMap[recordKey]
	metrics.mu.RUnlock()
	re.False(ok)
	re.False(recordExists)
}

func TestCleanupStaleRecordSkipsRefreshedRecord(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID         = uint32(10876)
		keyspaceName       = "refreshed_request_metrics_keyspace"
		groupName          = "refreshed_request_metrics_group"
		grantedTokenMetric = "resource_manager_server_granted_tokens"
	)
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})
	staleTime := time.Now().Add(-metricsCleanupTimeout - time.Second)
	cleanupTime := staleTime.Add(metricsCleanupTimeout + time.Second)
	refreshTime := cleanupTime.Add(time.Second)
	metrics := newMetrics()

	first := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, staleTime)
	first.observe(requestMetricsObservation{grantedTokens: 1})
	staleRecords := metrics.getStaleConsumptionRecords(cleanupTime)
	re.Len(staleRecords, 1)

	second := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, refreshTime)
	re.Same(first, second)
	metrics.cleanupStaleMetrics(staleRecords[0], keyspaceName)

	labels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	recordKey := consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}
	metrics.mu.RLock()
	_, requestExists := metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	recordTime, recordExists := metrics.consumptionRecordMap[recordKey]
	metrics.mu.RUnlock()
	re.True(requestExists)
	re.True(recordExists)
	re.Equal(refreshTime, recordTime)
	re.True(hasMetric(grantedTokenMetric, labels))
}

func TestCleanupStaleDefaultRUTypeDeletesRequestMetricsWithActiveBackground(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID         = uint32(10878)
		keyspaceName       = "stale_default_active_background_keyspace"
		groupName          = "stale_default_active_background_group"
		grantedTokenMetric = "resource_manager_server_granted_tokens"
		backgroundRUMetric = "resource_manager_resource_unit_read_request_unit_sum"
		availableRUMetric  = "resource_manager_resource_unit_available_ru"
		maxPerSecMetric    = "resource_manager_resource_unit_read_request_unit_max_per_sec"
		requestCauseMetric = "resource_manager_server_request_cause_total"
	)
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
		deleteCounterMetricLabelValues(keyspaceName, groupName, backgroundTypeLabel)
	})
	staleTime := time.Now().Add(-metricsCleanupTimeout - time.Second)
	cleanupTime := staleTime.Add(metricsCleanupTimeout + time.Second)
	freshTime := cleanupTime
	metrics := newMetrics()

	requestMetrics := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, staleTime)
	requestMetrics.observe(requestMetricsObservation{
		grantedTokens:  1,
		serviceLimited: true,
	})
	backgroundMetrics := metrics.getCounterMetrics(keyspaceID, keyspaceName, groupName, backgroundTypeLabel)
	backgroundMetrics.add(&rmpb.Consumption{RRU: 2}, &ControllerConfig{}, keyspaceID)
	metrics.insertConsumptionRecord(keyspaceID, groupName, backgroundTypeLabel, freshTime)
	gaugeMetrics := metrics.getGaugeMetrics(keyspaceID, keyspaceName, groupName)
	gaugeMetrics.availableRUCounter.Set(3)
	tracker := metrics.getMaxPerSecTracker(keyspaceID, keyspaceName, groupName)
	tracker.rruMaxMetrics.Set(4)

	staleRecords := metrics.getStaleConsumptionRecords(cleanupTime)
	re.Len(staleRecords, 1)
	re.True(metrics.cleanupStaleMetrics(staleRecords[0], keyspaceName))

	requestLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	backgroundRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 backgroundTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	groupLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	groupKeyspaceLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	re.False(hasMetric(grantedTokenMetric, requestLabels))
	re.False(hasMetric(requestCauseMetric, map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
		kindLabel:                 throttleKindLabel,
		"cause":                   serviceLimitCauseLabel,
	}))
	re.True(hasMetric(backgroundRUMetric, backgroundRULabels))
	re.True(hasMetric(availableRUMetric, groupLabels))
	re.True(hasMetric(maxPerSecMetric, groupKeyspaceLabels))
	metrics.mu.RLock()
	_, requestExists := metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	_, defaultRecordExists := metrics.consumptionRecordMap[consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}]
	_, backgroundRecordExists := metrics.consumptionRecordMap[consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     backgroundTypeLabel,
	}]
	_, backgroundCounterExists := metrics.counterMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: backgroundTypeLabel}]
	_, gaugeExists := metrics.gaugeMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: ""}]
	_, trackerExists := metrics.maxPerSecTrackerMap[trackerKey{keyspaceID: keyspaceID, groupName: groupName}]
	metrics.mu.RUnlock()
	re.False(requestExists)
	re.False(defaultRecordExists)
	re.True(backgroundRecordExists)
	re.True(backgroundCounterExists)
	re.True(gaugeExists)
	re.True(trackerExists)
}

func TestRequestMetricsDeletesCreatedLabelsWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID       = uint32(10868)
		fallbackName     = "keyspace-10868"
		loadedName       = "loaded-keyspace"
		groupName        = "request_metrics_label_group"
		grantedMetric    = "resource_manager_server_granted_tokens"
		trickleMetric    = "resource_manager_server_trickle_duration_ms"
		requestCauseName = "resource_manager_server_request_cause_total"
	)
	now := time.Now()
	metrics := newMetrics()

	first := metrics.getRequestMetrics(keyspaceID, fallbackName, groupName, now)
	first.observe(requestMetricsObservation{
		grantedTokens:  3,
		trickleTimeMs:  100,
		serviceLimited: true,
		groupTrickled:  true,
	})

	fallbackLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(grantedMetric, fallbackLabels))
	re.True(hasMetric(trickleMetric, fallbackLabels))
	re.True(hasMetric(requestCauseName, map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
		kindLabel:                 throttleKindLabel,
		"cause":                   serviceLimitCauseLabel,
	}))

	second := metrics.getRequestMetrics(keyspaceID, loadedName, groupName, now.Add(metricsCleanupInterval+time.Second))
	re.NotSame(first, second)
	re.False(hasMetric(grantedMetric, fallbackLabels))
	re.False(hasMetric(trickleMetric, fallbackLabels))
	re.False(hasMetric(requestCauseName, map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
		kindLabel:                 throttleKindLabel,
		"cause":                   serviceLimitCauseLabel,
	}))

	second.observe(requestMetricsObservation{grantedTokens: 5})
	loadedLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         loadedName,
	}
	re.True(hasMetric(grantedMetric, loadedLabels))

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}, loadedName)
	re.False(hasMetric(grantedMetric, loadedLabels))
}

func TestCounterMetricsDeletesCreatedLabelsWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10869)
		fallbackName = "keyspace-10869"
		loadedName   = "loaded-counter-keyspace"
		groupName    = "counter_metrics_label_group"
		metricName   = "resource_manager_resource_unit_read_request_unit_sum"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
	})

	first := metrics.getCounterMetrics(keyspaceID, fallbackName, groupName, defaultTypeLabel)
	first.add(&rmpb.Consumption{RRU: 1}, &ControllerConfig{}, keyspaceID)
	fallbackLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(metricName, fallbackLabels))

	second := metrics.getCounterMetrics(keyspaceID, loadedName, groupName, defaultTypeLabel)
	re.NotSame(first, second)
	re.False(hasMetric(metricName, fallbackLabels))

	second.add(&rmpb.Consumption{RRU: 2}, &ControllerConfig{}, keyspaceID)
	loadedLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         loadedName,
	}
	re.True(hasMetric(metricName, loadedLabels))
}

func TestCounterMetricsRebindKeepsSharedLabelsUsedByOtherRUType(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID         = uint32(10877)
		fallbackName       = "keyspace-10877"
		loadedName         = "loaded-counter-shared-keyspace"
		groupName          = "counter_metrics_shared_label_group"
		readRUMetric       = "resource_manager_resource_unit_read_request_unit_sum"
		crossAZMetric      = "resource_manager_resource_cross_az_traffic_byte_sum"
		requestCountMetric = "resource_manager_resource_request_count"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
		deleteCounterMetricLabelValues(fallbackName, groupName, backgroundTypeLabel)
	})

	defaultMetrics := metrics.getCounterMetrics(keyspaceID, fallbackName, groupName, defaultTypeLabel)
	defaultMetrics.add(&rmpb.Consumption{
		RRU:                     1,
		KvReadRpcCount:          1,
		ReadCrossAzTrafficBytes: 1,
	}, &ControllerConfig{}, keyspaceID)
	backgroundMetrics := metrics.getCounterMetrics(keyspaceID, fallbackName, groupName, backgroundTypeLabel)
	backgroundMetrics.add(&rmpb.Consumption{
		RRU:                     2,
		KvReadRpcCount:          2,
		ReadCrossAzTrafficBytes: 2,
	}, &ControllerConfig{}, keyspaceID)

	defaultFallbackLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	backgroundFallbackLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 backgroundTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	fallbackCrossAZLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	fallbackRequestCountLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(readRUMetric, defaultFallbackLabels))
	re.True(hasMetric(readRUMetric, backgroundFallbackLabels))
	re.True(hasMetric(crossAZMetric, fallbackCrossAZLabels))
	re.True(hasMetric(requestCountMetric, fallbackRequestCountLabels))

	defaultLoadedMetrics := metrics.getCounterMetrics(keyspaceID, loadedName, groupName, defaultTypeLabel)
	defaultLoadedMetrics.add(&rmpb.Consumption{RRU: 3}, &ControllerConfig{}, keyspaceID)

	re.False(hasMetric(readRUMetric, defaultFallbackLabels))
	re.True(hasMetric(readRUMetric, backgroundFallbackLabels))
	re.True(hasMetric(crossAZMetric, fallbackCrossAZLabels))
	re.True(hasMetric(requestCountMetric, fallbackRequestCountLabels))
}

func TestGaugeMetricsDeletesCreatedLabelsWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10870)
		fallbackName = "keyspace-10870"
		loadedName   = "loaded-gauge-keyspace"
		groupName    = "gauge_metrics_label_group"
		metricName   = "resource_manager_resource_unit_available_ru"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
	})

	first := metrics.getGaugeMetrics(keyspaceID, fallbackName, groupName)
	first.availableRUCounter.Set(1)
	fallbackLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(metricName, fallbackLabels))

	second := metrics.getGaugeMetrics(keyspaceID, loadedName, groupName)
	re.NotSame(first, second)
	re.False(hasMetric(metricName, fallbackLabels))

	second.availableRUCounter.Set(2)
	loadedLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         loadedName,
	}
	re.True(hasMetric(metricName, loadedLabels))
}

func TestMaxPerSecTrackerRebindsLabelsWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10871)
		fallbackName = "keyspace-10871"
		loadedName   = "loaded-tracker-keyspace"
		groupName    = "tracker_metrics_label_group"
		metricName   = "resource_manager_resource_unit_read_request_unit_max_per_sec"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
	})

	first := metrics.getMaxPerSecTracker(keyspaceID, fallbackName, groupName)
	first.rruMaxMetrics.Set(1)
	first.rruSum = 10
	first.lastRRUSum = 4
	first.maxPerSecRRU = 6
	first.cnt = 3
	fallbackLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(metricName, fallbackLabels))

	second := metrics.getMaxPerSecTracker(keyspaceID, loadedName, groupName)
	re.Same(first, second)
	re.Equal(10.0, second.rruSum)
	re.Equal(4.0, second.lastRRUSum)
	re.Equal(6.0, second.maxPerSecRRU)
	re.Equal(3, second.cnt)
	re.False(hasMetric(metricName, fallbackLabels))

	second.rruMaxMetrics.Set(2)
	loadedLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         loadedName,
	}
	re.True(hasMetric(metricName, loadedLabels))
}

func TestCleanupAllMetricsDeletesCachedLabelsWhenKeyspaceNameChanges(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID          = uint32(10872)
		fallbackName        = "keyspace-10872"
		loadedName          = "loaded-cleanup-keyspace"
		groupName           = "cleanup_metrics_label_group"
		readRUMetric        = "resource_manager_resource_unit_read_request_unit_sum"
		crossAZMetric       = "resource_manager_resource_cross_az_traffic_byte_sum"
		availableRUMetric   = "resource_manager_resource_unit_available_ru"
		maxPerSecMetric     = "resource_manager_resource_unit_read_request_unit_max_per_sec"
		grantedTokensMetric = "resource_manager_server_granted_tokens"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
	})

	counterMetrics := metrics.getCounterMetrics(keyspaceID, fallbackName, groupName, defaultTypeLabel)
	counterMetrics.add(&rmpb.Consumption{
		RRU:                     1,
		ReadCrossAzTrafficBytes: 2,
	}, &ControllerConfig{}, keyspaceID)
	gaugeMetrics := metrics.getGaugeMetrics(keyspaceID, fallbackName, groupName)
	gaugeMetrics.availableRUCounter.Set(3)
	tracker := metrics.getMaxPerSecTracker(keyspaceID, fallbackName, groupName)
	tracker.rruMaxMetrics.Set(4)
	requestMetrics := metrics.getRequestMetrics(keyspaceID, fallbackName, groupName, time.Now())
	requestMetrics.observe(requestMetricsObservation{grantedTokens: 5})

	readRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	crossAZLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	availableRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
	}
	groupKeyspaceLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         fallbackName,
	}
	re.True(hasMetric(readRUMetric, readRULabels))
	re.True(hasMetric(crossAZMetric, crossAZLabels))
	re.True(hasMetric(availableRUMetric, availableRULabels))
	re.True(hasMetric(maxPerSecMetric, groupKeyspaceLabels))
	re.True(hasMetric(grantedTokensMetric, groupKeyspaceLabels))

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}, loadedName)

	re.False(hasMetric(readRUMetric, readRULabels))
	re.False(hasMetric(crossAZMetric, crossAZLabels))
	re.False(hasMetric(availableRUMetric, availableRULabels))
	re.False(hasMetric(maxPerSecMetric, groupKeyspaceLabels))
	re.False(hasMetric(grantedTokensMetric, groupKeyspaceLabels))
	metrics.mu.RLock()
	_, counterExists := metrics.counterMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: defaultTypeLabel}]
	_, gaugeExists := metrics.gaugeMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: ""}]
	_, trackerExists := metrics.maxPerSecTrackerMap[trackerKey{keyspaceID: keyspaceID, groupName: groupName}]
	_, requestExists := metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	metrics.mu.RUnlock()
	re.False(counterExists)
	re.False(gaugeExists)
	re.False(trackerExists)
	re.False(requestExists)
}

func TestCleanupStaleRUTypeKeepsActiveGroupMetrics(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID          = uint32(10874)
		keyspaceName        = "mixed_cleanup_keyspace"
		groupName           = "mixed_cleanup_group"
		readRUMetric        = "resource_manager_resource_unit_read_request_unit_sum"
		crossAZMetric       = "resource_manager_resource_cross_az_traffic_byte_sum"
		requestCountMetric  = "resource_manager_resource_request_count"
		availableRUMetric   = "resource_manager_resource_unit_available_ru"
		maxPerSecMetric     = "resource_manager_resource_unit_read_request_unit_max_per_sec"
		grantedTokensMetric = "resource_manager_server_granted_tokens"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
		deleteCounterMetricLabelValues(keyspaceName, groupName, backgroundTypeLabel)
	})

	defaultMetrics := metrics.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	defaultMetrics.add(&rmpb.Consumption{
		RRU:                     1,
		KvReadRpcCount:          1,
		ReadCrossAzTrafficBytes: 1,
	}, &ControllerConfig{}, keyspaceID)
	backgroundMetrics := metrics.getCounterMetrics(keyspaceID, keyspaceName, groupName, backgroundTypeLabel)
	backgroundMetrics.add(&rmpb.Consumption{
		RRU:                     2,
		KvReadRpcCount:          2,
		ReadCrossAzTrafficBytes: 2,
	}, &ControllerConfig{}, keyspaceID)
	gaugeMetrics := metrics.getGaugeMetrics(keyspaceID, keyspaceName, groupName)
	gaugeMetrics.availableRUCounter.Set(3)
	tracker := metrics.getMaxPerSecTracker(keyspaceID, keyspaceName, groupName)
	tracker.rruMaxMetrics.Set(4)
	requestMetrics := metrics.getRequestMetrics(keyspaceID, keyspaceName, groupName, time.Now())
	requestMetrics.observe(requestMetricsObservation{grantedTokens: 5})
	metrics.insertConsumptionRecord(keyspaceID, groupName, backgroundTypeLabel, time.Now())

	defaultReadRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	backgroundReadRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 backgroundTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	crossAZLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	requestCountLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	availableRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	groupKeyspaceLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         keyspaceName,
	}
	re.True(hasMetric(readRUMetric, defaultReadRULabels))
	re.True(hasMetric(readRUMetric, backgroundReadRULabels))
	re.True(hasMetric(crossAZMetric, crossAZLabels))
	re.True(hasMetric(requestCountMetric, requestCountLabels))
	re.True(hasMetric(availableRUMetric, availableRULabels))
	re.True(hasMetric(maxPerSecMetric, groupKeyspaceLabels))
	re.True(hasMetric(grantedTokensMetric, groupKeyspaceLabels))

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     backgroundTypeLabel,
	}, keyspaceName)

	re.True(hasMetric(readRUMetric, defaultReadRULabels))
	re.False(hasMetric(readRUMetric, backgroundReadRULabels))
	re.True(hasMetric(crossAZMetric, crossAZLabels))
	re.True(hasMetric(requestCountMetric, requestCountLabels))
	re.True(hasMetric(availableRUMetric, availableRULabels))
	re.True(hasMetric(maxPerSecMetric, groupKeyspaceLabels))
	re.True(hasMetric(grantedTokensMetric, groupKeyspaceLabels))
	metrics.mu.RLock()
	_, defaultCounterExists := metrics.counterMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: defaultTypeLabel}]
	_, backgroundCounterExists := metrics.counterMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: backgroundTypeLabel}]
	_, gaugeExists := metrics.gaugeMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: ""}]
	_, trackerExists := metrics.maxPerSecTrackerMap[trackerKey{keyspaceID: keyspaceID, groupName: groupName}]
	_, requestExists := metrics.requestMetricsMap[requestMetricsKey{keyspaceID: keyspaceID, groupName: groupName}]
	metrics.mu.RUnlock()
	re.True(defaultCounterExists)
	re.False(backgroundCounterExists)
	re.True(gaugeExists)
	re.True(trackerExists)
	re.True(requestExists)
}

func TestCleanupStaleRUTypeDeletesItsFallbackSharedCounterLabels(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID         = uint32(10875)
		fallbackName       = "keyspace-10875"
		loadedName         = "loaded-mixed-cleanup-keyspace"
		groupName          = "mixed_fallback_cleanup_group"
		readRUMetric       = "resource_manager_resource_unit_read_request_unit_sum"
		crossAZMetric      = "resource_manager_resource_cross_az_traffic_byte_sum"
		requestCountMetric = "resource_manager_resource_request_count"
		availableRUMetric  = "resource_manager_resource_unit_available_ru"
	)
	metrics := newMetrics()
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(fallbackName, groupName)
		deleteDefaultLabelValuesForTest(loadedName, groupName)
		deleteCounterMetricLabelValues(fallbackName, groupName, backgroundTypeLabel)
	})

	backgroundMetrics := metrics.getCounterMetrics(keyspaceID, fallbackName, groupName, backgroundTypeLabel)
	backgroundMetrics.add(&rmpb.Consumption{
		RRU:                     1,
		KvReadRpcCount:          1,
		ReadCrossAzTrafficBytes: 1,
	}, &ControllerConfig{}, keyspaceID)
	metrics.insertConsumptionRecord(keyspaceID, groupName, backgroundTypeLabel, time.Now())
	defaultMetrics := metrics.getCounterMetrics(keyspaceID, loadedName, groupName, defaultTypeLabel)
	defaultMetrics.add(&rmpb.Consumption{
		RRU:                     2,
		KvReadRpcCount:          2,
		ReadCrossAzTrafficBytes: 2,
	}, &ControllerConfig{}, keyspaceID)
	gaugeMetrics := metrics.getGaugeMetrics(keyspaceID, loadedName, groupName)
	gaugeMetrics.availableRUCounter.Set(3)
	requestMetrics := metrics.getRequestMetrics(keyspaceID, loadedName, groupName, time.Now())
	requestMetrics.observe(requestMetricsObservation{grantedTokens: 4})

	fallbackReadRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 backgroundTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	fallbackCrossAZLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	fallbackRequestCountLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         fallbackName,
	}
	loadedCrossAZLabels := map[string]string{
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         loadedName,
	}
	loadedRequestCountLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 readTypeLabel,
		keyspaceNameLabel:         loadedName,
	}
	loadedAvailableRULabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		keyspaceNameLabel:         loadedName,
	}
	re.True(hasMetric(readRUMetric, fallbackReadRULabels))
	re.True(hasMetric(crossAZMetric, fallbackCrossAZLabels))
	re.True(hasMetric(requestCountMetric, fallbackRequestCountLabels))
	re.True(hasMetric(crossAZMetric, loadedCrossAZLabels))
	re.True(hasMetric(requestCountMetric, loadedRequestCountLabels))
	re.True(hasMetric(availableRUMetric, loadedAvailableRULabels))

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     backgroundTypeLabel,
	}, loadedName)

	re.False(hasMetric(readRUMetric, fallbackReadRULabels))
	re.False(hasMetric(crossAZMetric, fallbackCrossAZLabels))
	re.False(hasMetric(requestCountMetric, fallbackRequestCountLabels))
	re.True(hasMetric(crossAZMetric, loadedCrossAZLabels))
	re.True(hasMetric(requestCountMetric, loadedRequestCountLabels))
	re.True(hasMetric(availableRUMetric, loadedAvailableRULabels))
}

func TestDeleteCounterMetricLabelValuesUsesRUType(t *testing.T) {
	re := require.New(t)
	const (
		keyspaceID   = uint32(10873)
		keyspaceName = "counter_cleanup_keyspace"
		groupName    = "counter_cleanup_group"
		metricName   = "resource_manager_resource_unit_read_request_unit_sum"
	)
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
		deleteCounterMetricLabelValues(keyspaceName, groupName, backgroundTypeLabel)
	})

	defaultMetrics := newCounterMetrics(keyspaceName, groupName, defaultTypeLabel)
	defaultMetrics.add(&rmpb.Consumption{RRU: 1}, &ControllerConfig{}, keyspaceID)
	backgroundMetrics := newCounterMetrics(keyspaceName, groupName, backgroundTypeLabel)
	backgroundMetrics.add(&rmpb.Consumption{RRU: 2}, &ControllerConfig{}, keyspaceID)
	defaultLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 defaultTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	backgroundLabels := map[string]string{
		resourceGroupNameLabel:    groupName,
		newResourceGroupNameLabel: groupName,
		typeLabel:                 backgroundTypeLabel,
		keyspaceNameLabel:         keyspaceName,
	}
	re.True(hasMetric(metricName, defaultLabels))
	re.True(hasMetric(metricName, backgroundLabels))

	deleteCounterMetricLabelValues(keyspaceName, groupName, backgroundTypeLabel)

	re.True(hasMetric(metricName, defaultLabels))
	re.False(hasMetric(metricName, backgroundLabels))
}

func TestRequestRUSeparatesServiceAndGroupTrickleCause(t *testing.T) {
	re := require.New(t)
	groupName := "request_ru_trickle_group"
	keyspaceName := "request_ru_trickle_keyspace"
	t.Cleanup(func() {
		deleteDefaultLabelValuesForTest(keyspaceName, groupName)
	})

	serviceTrickleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, serviceLimitCauseLabel)
	groupTrickleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, groupCauseLabel)
	now := time.Now()
	rg := &ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: NewRequestUnitSettings(groupName, &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   100,
				BurstLimit: 1000,
			},
		}),
	}
	sl := newServiceLimiter(1, 10, nil)
	sl.AvailableTokens = 10
	sl.LastUpdate = now
	metrics := newRequestMetrics(keyspaceName, groupName)

	tokens := rg.RequestRU(now, 10, 1000, 1, keyspaceName, nil, sl, metrics)

	re.NotNil(tokens)
	re.Equal(10.0, tokens.GetGrantedTokens().GetTokens())
	re.Equal(int64(1000), tokens.GetTrickleTimeMs())
	re.Equal(1.0, testutil.ToFloat64(serviceTrickleCounter))
	re.Zero(testutil.ToFloat64(groupTrickleCounter))
}

func TestGaugeMetricsSetGroupSlotMetrics(t *testing.T) {
	re := require.New(t)
	groupName := "slot_group"
	keyspaceName := "slot_keyspace"
	rg := &ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: NewRequestUnitSettings(groupName, &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   100,
				BurstLimit: 200,
			},
		}),
	}
	slot := newTokenSlot(1, time.Now())
	rg.RUSettings.RU.tokenSlots[1] = slot
	rg.RUSettings.RU.setSlotTokenCapacity(slot, -12.5)
	rg.RUSettings.RU.slotsCreated = 2
	rg.RUSettings.RU.slotsDeleted = 1
	rg.RUSettings.RU.slotsExpired = 3

	gm := newGaugeMetrics(keyspaceName, groupName)
	gm.setGroup(rg, keyspaceName)

	re.Equal(1.0, testutil.ToFloat64(gm.activeSlotCountGauge))
	re.Equal(12.5, testutil.ToFloat64(gm.tokenLoanGauge))
	re.Equal(2.0, testutil.ToFloat64(gm.slotCreatedCounter))
	re.Equal(1.0, testutil.ToFloat64(gm.slotDeletedCounter))
	re.Equal(3.0, testutil.ToFloat64(gm.slotExpiredCounter))
	created, deleted, expired := rg.DrainSlotEvents()
	re.Zero(created)
	re.Zero(deleted)
	re.Zero(expired)
}

func deleteDefaultLabelValuesForTest(keyspaceName, groupName string) {
	deleteCounterMetricLabelValues(keyspaceName, groupName, defaultTypeLabel)
	deleteGaugeMetricLabelValues(keyspaceName, groupName)
	deleteMaxPerSecTrackerLabelValues(keyspaceName, groupName)
	deleteRequestMetricLabelValues(keyspaceName, groupName)
}

func findHistogramMetric(re *require.Assertions, metricName string, labels map[string]string) *dto.Histogram {
	metrics, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)
	for _, mf := range metrics {
		if mf.GetName() != metricName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if matchMetricLabels(metric.GetLabel(), labels) {
				return metric.GetHistogram()
			}
		}
	}
	re.FailNow(fmt.Sprintf("metric %s with labels %v not found", metricName, labels))
	return nil
}

func hasMetric(metricName string, labels map[string]string) bool {
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return false
	}
	for _, mf := range metrics {
		if mf.GetName() != metricName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if matchMetricLabels(metric.GetLabel(), labels) {
				return true
			}
		}
	}
	return false
}

func matchMetricLabels(actual []*dto.LabelPair, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, label := range actual {
		if expected[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}
