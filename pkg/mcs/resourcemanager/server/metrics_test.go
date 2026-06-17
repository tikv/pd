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
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
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
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
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

func TestCounterMetricsIgnoresNegativeRequestUnitConsumption(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "negative-ru-keyspace"
		groupName    = "negative-ru-group"
		keyspaceID   = uint32(303)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	metrics := newCounterMetrics(keyspaceName, groupName, defaultTypeLabel)
	metrics.add(&rmpb.Consumption{
		RRU: 10,
		WRU: 3,
	}, &ControllerConfig{}, keyspaceID)
	metrics.add(&rmpb.Consumption{
		RRU: -4,
		WRU: -1,
	}, &ControllerConfig{}, keyspaceID)

	re.Equal(float64(10), testutil.ToFloat64(metrics.RRUMetrics))
	re.Equal(float64(3), testutil.ToFloat64(metrics.WRUMetrics))
	re.Equal(float64(13), testutil.ToFloat64(metrics.ActiveRUMetrics))
}

func TestRecordConsumptionUsesActualRequestUnitCounters(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "actual-counter-keyspace"
		groupName    = "actual-counter-group"
		keyspaceID   = uint32(404)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption: &rmpb.Consumption{
			RRU:        12,
			WRU:        8,
			WriteBytes: 1024,
		},
	}, &ControllerConfig{}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Equal(float64(12), testutil.ToFloat64(counter.RRUMetrics))
	re.Equal(float64(8), testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(1024), testutil.ToFloat64(counter.WriteByteMetrics))
	re.Contains(m.consumptionRecordMap, consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	})
}

func TestRecordConsumptionKeepsRecordForEmptyConsumption(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "empty-consumption-keyspace"
		groupName    = "empty-consumption-group"
		keyspaceID   = uint32(407)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	now := time.Now()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption:       &rmpb.Consumption{},
	}, &ControllerConfig{}, now)

	key := consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}
	re.Equal(now, m.consumptionRecordMap[key])

	counter := m.counterMetricsMap[metricsKey{keyspaceID: keyspaceID, groupName: groupName, ruType: defaultTypeLabel}]
	re.NotNil(counter)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Zero(testutil.ToFloat64(counter.ActiveRUMetrics))
}

func TestRecordConsumptionIgnoresNilConsumption(t *testing.T) {
	re := require.New(t)

	m := newMetrics()
	re.NotPanics(func() {
		m.recordConsumption(&consumptionItem{
			keyspaceID:        408,
			keyspaceName:      "nil-consumption-keyspace",
			resourceGroupName: "nil-consumption-group",
		}, &ControllerConfig{}, time.Now())
	})
	re.Empty(m.counterMetricsMap)
	re.Empty(m.consumptionRecordMap)
}

func TestRecordConsumptionDoesNotRefundNegativeConsumption(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "negative-consumption-keyspace"
		groupName    = "negative-consumption-group"
		keyspaceID   = uint32(405)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption: &rmpb.Consumption{
			RRU: -12,
			WRU: -8,
		},
	}, &ControllerConfig{}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
}

func TestRecordConsumptionUsesActualForActiveRequestUnit(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "actual-active-ru-keyspace"
		groupName    = "actual-active-ru-group"
		keyspaceID   = uint32(505)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption: &rmpb.Consumption{
			RRU:               12,
			WRU:               8,
			TotalCpuTimeMs:    4,
			SqlLayerCpuTimeMs: 3,
		},
	}, &ControllerConfig{
		RequestUnit: RequestUnitConfig{CPUMsCost: 2},
	}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Equal(float64(12), testutil.ToFloat64(counter.RRUMetrics))
	re.Equal(float64(8), testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(26), testutil.ToFloat64(counter.ActiveRUMetrics))
}

func TestRecordConsumptionUsesActualRUV2ForActiveRequestUnit(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "actual-active-ruv2-keyspace"
		groupName    = "actual-active-ruv2-group"
		keyspaceID   = uint32(606)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption: &rmpb.Consumption{
			TikvRUV2:    7,
			TidbRUV2:    11,
			TiflashRUV2: 13,
		},
	}, &ControllerConfig{
		RUVersionPolicy: &RUVersionPolicy{
			Default: RUVersionV1,
			Overrides: map[uint32]RUVersion{
				keyspaceID: RUVersionV2,
			},
		},
	}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(31), testutil.ToFloat64(counter.ActiveRUMetrics))
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

func TestMetricsCleanupUsesCreationTimeKeyspaceName(t *testing.T) {
	testCases := []struct {
		name                 string
		keyspaceID           uint32
		creationKeyspaceName string
		cleanupKeyspaceName  string
		groupName            string
	}{
		{
			name:                 "fallback name at creation",
			keyspaceID:           123,
			creationKeyspaceName: "keyspace-123",
			cleanupKeyspaceName:  "tenant-a",
			groupName:            "cleanup-fallback-name-group",
		},
		{
			name:                 "same name at creation and cleanup",
			keyspaceID:           124,
			creationKeyspaceName: "tenant-b",
			cleanupKeyspaceName:  "tenant-b",
			groupName:            "cleanup-same-name-group",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			cleanup := func(keyspaceName string) {
				deleteLabelValues(keyspaceName, testCase.groupName, defaultTypeLabel)
				readRequestUnitMaxPerSecCost.DeleteLabelValues(testCase.groupName, keyspaceName)
				writeRequestUnitMaxPerSecCost.DeleteLabelValues(testCase.groupName, keyspaceName)
			}
			cleanup(testCase.creationKeyspaceName)
			cleanup(testCase.cleanupKeyspaceName)
			t.Cleanup(func() {
				cleanup(testCase.creationKeyspaceName)
				cleanup(testCase.cleanupKeyspaceName)
			})

			m := newMetrics()
			m.getCounterMetrics(
				testCase.keyspaceID,
				testCase.creationKeyspaceName,
				testCase.groupName,
				defaultTypeLabel,
			).add(&rmpb.Consumption{
				RRU: 1,
				WRU: 1,
			}, nil, testCase.keyspaceID)

			tracker := m.getMaxPerSecTracker(
				testCase.keyspaceID,
				testCase.creationKeyspaceName,
				testCase.groupName,
			)
			for range defaultCollectIntervalSec + 1 {
				tracker.collect(&rmpb.Consumption{RRU: 1, WRU: 1})
				tracker.flushMetrics()
			}

			m.getGaugeMetrics(
				testCase.keyspaceID,
				testCase.creationKeyspaceName,
				testCase.groupName,
			)

			counterLabels := map[string]string{
				resourceGroupNameLabel:    testCase.groupName,
				newResourceGroupNameLabel: testCase.groupName,
				typeLabel:                 defaultTypeLabel,
				keyspaceNameLabel:         testCase.creationKeyspaceName,
			}
			maxPerSecLabels := map[string]string{
				newResourceGroupNameLabel: testCase.groupName,
				keyspaceNameLabel:         testCase.creationKeyspaceName,
			}
			gaugeLabels := map[string]string{
				resourceGroupNameLabel:    testCase.groupName,
				newResourceGroupNameLabel: testCase.groupName,
				keyspaceNameLabel:         testCase.creationKeyspaceName,
			}

			re.Greater(testutil.CollectAndCount(collectorWithLabels(readRequestUnitCost, counterLabels)), 0)
			re.Greater(testutil.CollectAndCount(collectorWithLabels(readRequestUnitMaxPerSecCost, maxPerSecLabels)), 0)
			re.Greater(testutil.CollectAndCount(collectorWithLabels(availableRUCounter, gaugeLabels)), 0)

			m.cleanupAllMetrics(consumptionRecordKey{
				keyspaceID: testCase.keyspaceID,
				groupName:  testCase.groupName,
				ruType:     defaultTypeLabel,
			}, testCase.cleanupKeyspaceName)

			re.Zero(testutil.CollectAndCount(collectorWithLabels(readRequestUnitCost, counterLabels)))
			re.Zero(testutil.CollectAndCount(collectorWithLabels(readRequestUnitMaxPerSecCost, maxPerSecLabels)))
			re.Zero(testutil.CollectAndCount(collectorWithLabels(availableRUCounter, gaugeLabels)))
		})
	}
}

func collectorWithLabels(collector prometheus.Collector, labels map[string]string) prometheus.Collector {
	return labelFilterCollector{collector: collector, labels: labels}
}

type labelFilterCollector struct {
	collector prometheus.Collector
	labels    map[string]string
}

func (c labelFilterCollector) Describe(chan<- *prometheus.Desc) {}

func (c labelFilterCollector) Collect(ch chan<- prometheus.Metric) {
	metricCh := make(chan prometheus.Metric)
	go func() {
		c.collector.Collect(metricCh)
		close(metricCh)
	}()
	for metric := range metricCh {
		if metricHasLabels(metric, c.labels) {
			ch <- metric
		}
	}
}

func metricHasLabels(metric prometheus.Metric, labels map[string]string) bool {
	dtoMetric := &dto.Metric{}
	if err := metric.Write(dtoMetric); err != nil {
		return false
	}
	for name, value := range labels {
		if !metricHasLabel(dtoMetric, name, value) {
			return false
		}
	}
	return true
}

func metricHasLabel(metric *dto.Metric, name, value string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name && label.GetValue() == value {
			return true
		}
	}
	return false
}
