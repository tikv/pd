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

func TestRCUTracker(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "rcu-test-keyspace"
		groupName    = "rcu-test-group"
	)

	t.Cleanup(func() {
		requestUnitSumPerSec.DeleteLabelValues(groupName, keyspaceName)
		requestUnitConsumeRate.DeleteLabelValues(groupName, keyspaceName)
	})

	tracker := newRCUTracker(keyspaceName, groupName)
	tracker.collect(&rmpb.Consumption{
		RRU:            10,
		WRU:            5,
		TotalCpuTimeMs: 2,
	})
	tracker.flushMetrics(100, 2)

	tracker.collect(&rmpb.Consumption{
		RRU:            20,
		WRU:            10,
		TotalCpuTimeMs: 5,
	})
	tracker.lastFlushTime = time.Now().Add(-2 * time.Second)
	tracker.flushMetrics(100, 2)

	re.InEpsilon(float64(20), testutil.ToFloat64(tracker.rcuMetrics), 0.01)
	re.InEpsilon(float64(0.2), testutil.ToFloat64(tracker.consumeRateMetrics), 0.01)

	tracker.collect(&rmpb.Consumption{
		RRU:            20,
		WRU:            10,
		TotalCpuTimeMs: 5,
	})
	tracker.lastFlushTime = time.Now().Add(-2 * time.Second)
	tracker.flushMetrics(0, 2)

	re.InEpsilon(float64(20), testutil.ToFloat64(tracker.rcuMetrics), 0.01)
	re.Zero(testutil.ToFloat64(tracker.consumeRateMetrics))
}

func TestCleanupAllMetricsKeepsRCUTrackerForActiveGroup(t *testing.T) {
	re := require.New(t)

	const (
		keyspaceName = "cleanup-rcu-keyspace"
		groupName    = "cleanup-rcu-group"
		keyspaceID   = uint32(303)
	)

	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
		deleteLabelValues(keyspaceName, groupName, backgroundTypeLabel)
		requestUnitSumPerSec.DeleteLabelValues(groupName, keyspaceName)
		requestUnitConsumeRate.DeleteLabelValues(groupName, keyspaceName)
	})

	metrics := newMetrics()
	metrics.getRCUTracker(keyspaceID, keyspaceName, groupName)
	metrics.insertConsumptionRecord(keyspaceID, groupName, defaultTypeLabel, time.Now())
	metrics.insertConsumptionRecord(keyspaceID, groupName, backgroundTypeLabel, time.Now())

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     defaultTypeLabel,
	}, keyspaceName)

	re.Contains(metrics.rcuTrackerMap, trackerKey{keyspaceID: keyspaceID, groupName: groupName})

	metrics.cleanupAllMetrics(consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  groupName,
		ruType:     backgroundTypeLabel,
	}, keyspaceName)

	re.NotContains(metrics.rcuTrackerMap, trackerKey{keyspaceID: keyspaceID, groupName: groupName})
}
