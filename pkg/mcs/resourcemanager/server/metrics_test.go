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

func TestCounterMetricsRecordsRequestUnitRefund(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "refund-keyspace"
		groupName    = "refund-group"
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
	re.Equal(float64(4), testutil.ToFloat64(metrics.RRURefundMetrics))
	re.Equal(float64(3), testutil.ToFloat64(metrics.WRUMetrics))
	re.Equal(float64(1), testutil.ToFloat64(metrics.WRURefundMetrics))
	re.Equal(float64(13), testutil.ToFloat64(metrics.ActiveRUMetrics))
}

func TestActiveRequestUnitRefundMetricIsNotExposed(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "no-active-refund-keyspace"
		groupName    = "no-active-refund-group"
		keyspaceID   = uint32(313)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	metrics := newCounterMetrics(keyspaceName, groupName, defaultTypeLabel)
	metrics.add(&rmpb.Consumption{
		RRU: -4,
		WRU: -1,
	}, &ControllerConfig{}, keyspaceID)

	families, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)
	for _, family := range families {
		re.NotEqual("resource_manager_resource_unit_active_request_unit_refund_sum", family.GetName())
	}
}

func TestRecordConsumptionUsesSettlementForRequestUnitCounters(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "settlement-counter-keyspace"
		groupName    = "settlement-counter-group"
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
		Settlement: &rmpb.Consumption{
			RRU: -4,
			WRU: -2,
		},
	}, &ControllerConfig{}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Equal(float64(4), testutil.ToFloat64(counter.RRURefundMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(2), testutil.ToFloat64(counter.WRURefundMetrics))
	re.Equal(float64(1024), testutil.ToFloat64(counter.WriteByteMetrics),
		"byte and other non-RU metrics should still come from actual consumption")
}

func TestRecordConsumptionTouchesActivityForSettlementOnlyRefund(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "settlement-only-keyspace"
		groupName    = "settlement-only-group"
		keyspaceID   = uint32(406)
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
		Settlement: &rmpb.Consumption{
			RRU: -4,
			WRU: -2,
		},
	}, &ControllerConfig{}, now)

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Equal(float64(4), testutil.ToFloat64(counter.RRURefundMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(2), testutil.ToFloat64(counter.WRURefundMetrics))
	re.Contains(m.metricsActivityRecordMap, metricsActivityRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	})
	re.Equal(now, m.metricsActivityRecordMap[metricsActivityRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}])
}

func TestRecordConsumptionIgnoresEmptySettlementOnlyReport(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "empty-settlement-keyspace"
		groupName    = "empty-settlement-group"
		keyspaceID   = uint32(407)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})

	m := newMetrics()
	m.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Settlement:        &rmpb.Consumption{},
	}, &ControllerConfig{}, time.Now())

	re.Empty(m.counterMetricsMap)
	re.Empty(m.metricsActivityRecordMap)
}

func TestRecordConsumptionFallsBackToActualRequestUnitCountersWithoutSettlement(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "actual-counter-keyspace"
		groupName    = "actual-counter-group"
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
			RRU:        12,
			WRU:        8,
			WriteBytes: 1024,
		},
	}, &ControllerConfig{}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Equal(float64(12), testutil.ToFloat64(counter.RRUMetrics))
	re.Zero(testutil.ToFloat64(counter.RRURefundMetrics))
	re.Equal(float64(8), testutil.ToFloat64(counter.WRUMetrics))
	re.Zero(testutil.ToFloat64(counter.WRURefundMetrics))
	re.Equal(float64(1024), testutil.ToFloat64(counter.WriteByteMetrics))
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
		Settlement: &rmpb.Consumption{
			RRU: -4,
			WRU: -2,
		},
	}, &ControllerConfig{
		RequestUnit: RequestUnitConfig{CPUMsCost: 2},
	}, time.Now())

	counter := m.getCounterMetrics(keyspaceID, keyspaceName, groupName, defaultTypeLabel)
	re.Zero(testutil.ToFloat64(counter.RRUMetrics))
	re.Equal(float64(4), testutil.ToFloat64(counter.RRURefundMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(2), testutil.ToFloat64(counter.WRURefundMetrics))
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
		Settlement: &rmpb.Consumption{
			RRU: -4,
			WRU: -2,
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
	re.Equal(float64(4), testutil.ToFloat64(counter.RRURefundMetrics))
	re.Zero(testutil.ToFloat64(counter.WRUMetrics))
	re.Equal(float64(2), testutil.ToFloat64(counter.WRURefundMetrics))
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
