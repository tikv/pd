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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestActiveRequestUnitMetricUsesV1ByDefault(t *testing.T) {
	re := require.New(t)

	const (
		groupName  = "billing-v1-group"
		keyspaceID = uint32(101)
	)

	counter := activeRequestUnitCost.WithLabelValues(groupName, groupName, defaultTypeLabel)
	t.Cleanup(func() {
		activeRequestUnitCost.DeleteLabelValues(groupName, groupName, defaultTypeLabel)
	})

	activeRU := calculateActiveRU(&rmpb.Consumption{
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
	counter.Add(activeRU)

	re.Equal(float64(36), testutil.ToFloat64(counter))
}

func TestActiveRequestUnitMetricUsesV2ForOverriddenKeyspace(t *testing.T) {
	re := require.New(t)

	const (
		groupName  = "billing-v2-group"
		keyspaceID = uint32(202)
	)

	counter := activeRequestUnitCost.WithLabelValues(groupName, groupName, defaultTypeLabel)
	t.Cleanup(func() {
		activeRequestUnitCost.DeleteLabelValues(groupName, groupName, defaultTypeLabel)
	})

	activeRU := calculateActiveRU(&rmpb.Consumption{
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
	counter.Add(activeRU)

	re.Equal(float64(31), testutil.ToFloat64(counter))
}

func TestMaxPerSecCostTracker(t *testing.T) {
	tracker := newMaxPerSecCostTracker("test", defaultCollectIntervalSec)
	re := require.New(t)

	// Define the expected max values for each flushPeriod
	expectedMaxRU := []float64{19, 39, 59}
	expectedSum := []float64{190, 780, 1770}

	for i := range 60 {
		// Record data
		consumption := &rmpb.Consumption{
			RRU: float64(i),
			WRU: float64(i),
		}
		tracker.CollectConsumption(consumption)
		tracker.FlushMetrics()

		// Check the max values at the end of each flushPeriod
		if (i+1)%20 == 0 {
			period := i / 20
			re.Equal(tracker.maxPerSecRRU, expectedMaxRU[period], "maxPerSecRRU in period %d is incorrect", period+1)
			re.Equal(tracker.maxPerSecWRU, expectedMaxRU[period], "maxPerSecWRU in period %d is incorrect", period+1)
			re.Equal(tracker.rruSum, expectedSum[period])
			re.Equal(tracker.rruSum, expectedSum[period])
		}
	}
}
