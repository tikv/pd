// Copyright 2026 TiKV Project Authors.
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

package core

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// TestAddDurationSumClampsNegative ensures a negative duration never reaches
// prometheus.Counter.Add (which panics on negative input).
func TestAddDurationSumClampsNegative(t *testing.T) {
	re := require.New(t)
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_add_duration_sum"})

	re.NotPanics(func() {
		addDurationSum(c, -time.Second)
	})
	re.Equal(float64(0), testCounterValue(c), "negative duration must not be added")

	addDurationSum(c, time.Second)
	re.Equal(float64(1), testCounterValue(c), "positive duration must be added")

	addDurationSum(c, 0)
	re.Equal(float64(1), testCounterValue(c), "zero duration must be a no-op")
}

// TestOnAllStageFinishedBackwardClock reproduces the original panic: when the OS
// monotonic clock moves backwards, now.Sub(lastCheckTime) is negative. The tracer
// stage callbacks must not crash pd-server in that case.
func TestTracerStagesToleranceBackwardClock(t *testing.T) {
	re := require.New(t)
	// lastCheckTime in the future simulates a backward clock step between two
	// time.Now() reads; the monotonic reading is preserved so Sub goes negative.
	future := time.Now().Add(time.Hour)
	h := &regionHeartbeatProcessTracer{
		startTime:     future,
		lastCheckTime: future,
	}
	h.saveCacheStats.startTime = future
	h.saveCacheStats.lastCheckTime = future

	re.NotPanics(func() {
		h.OnPreCheckFinished()
		h.OnAsyncHotStatsFinished()
		h.OnRegionGuideFinished()
		h.OnCheckOverlapsFinished()
		h.OnValidateRegionFinished()
		h.OnSetRegionFinished()
		h.OnUpdateSubTreeFinished()
		h.OnCollectRegionStatsFinished()
		h.OnAllStageFinished()
	})
}

func testCounterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}
