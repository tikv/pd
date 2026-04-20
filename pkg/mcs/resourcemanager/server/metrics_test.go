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

	observeTokenGrant(groupName, keyspaceName, &rmpb.GrantedRUTokenBucket{
		GrantedTokens: &rmpb.TokenBucket{Tokens: 42},
		TrickleTimeMs: 0,
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
	throttleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, throttleKindLabel, serviceLimitCauseLabel)
	trickleCounter := requestCauseCounter.WithLabelValues(groupName, keyspaceName, trickleKindLabel, groupCauseLabel)

	observeRequestCause(groupName, keyspaceName, throttleKindLabel, serviceLimitCauseLabel)
	observeRequestCause(groupName, keyspaceName, trickleKindLabel, groupCauseLabel)

	re.Equal(1.0, testutil.ToFloat64(throttleCounter))
	re.Equal(1.0, testutil.ToFloat64(trickleCounter))
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
	rg.RUSettings.RU.tokenSlots[1] = &tokenSlot{curTokenCapacity: -12.5}
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
