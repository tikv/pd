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

package core

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// recordingStage couples a tracer stage method with the counters it feeds, so a
// single table can drive the tests over every measuring stage uniformly.
type recordingStage struct {
	name   string
	finish func(*regionHeartbeatProcessTracer)
	sum    prometheus.Counter
	count  prometheus.Counter
}

func recordingStages() []recordingStage {
	return []recordingStage{
		{"PreCheck", (*regionHeartbeatProcessTracer).OnPreCheckFinished, preCheckDurationSum, preCheckCount},
		{"AsyncHotStats", (*regionHeartbeatProcessTracer).OnAsyncHotStatsFinished, asyncHotStatsDurationSum, asyncHotStatsCount},
		{"RegionGuide", (*regionHeartbeatProcessTracer).OnRegionGuideFinished, regionGuideDurationSum, regionGuideCount},
		{"CheckOverlaps", (*regionHeartbeatProcessTracer).OnCheckOverlapsFinished, checkOverlapsDurationSum, checkOverlapsCount},
		{"ValidateRegion", (*regionHeartbeatProcessTracer).OnValidateRegionFinished, validateRegionDurationSum, validateRegionCount},
		{"SetRegion", (*regionHeartbeatProcessTracer).OnSetRegionFinished, setRegionDurationSum, setRegionCount},
		{"UpdateSubTree", (*regionHeartbeatProcessTracer).OnUpdateSubTreeFinished, updateSubTreeDurationSum, updateSubTreeCount},
		{"CollectRegionStats", (*regionHeartbeatProcessTracer).OnCollectRegionStatsFinished, regionCollectDurationSum, regionCollectCount},
		{"Other", (*regionHeartbeatProcessTracer).OnAllStageFinished, otherDurationSum, otherCount},
	}
}

// TestRegionHeartbeatTracerToleratesNonMonotonicClock checks the tracer is
// robust to a backwards jump of the OS monotonic clock, which can happen on
// virtualized hosts (an unsynchronized TSC across vCPUs, or a live migration).
// For every stage, a checkpoint left in the future must not make the stage panic
// or push a negative value into its duration counter (prometheus.Counter.Add
// panics on a negative value).
func TestRegionHeartbeatTracerToleratesNonMonotonicClock(t *testing.T) {
	re := require.New(t)
	for _, s := range recordingStages() {
		h := &regionHeartbeatProcessTracer{}
		h.lastCheckTime = time.Now().Add(time.Hour) // the clock appears to run backwards
		before := testutil.ToFloat64(s.sum)
		re.NotPanics(func() { s.finish(h) }, s.name)
		re.GreaterOrEqual(testutil.ToFloat64(s.sum), before, s.name) // the counter never decreases
	}
}

// TestRegionHeartbeatTracerRecordsEachStageIndependently checks that every stage
// reports the time spent in that stage -- and only that stage -- into its own
// counters, exactly once. Each stage is given a distinct, controlled interval by
// rewinding the checkpoint, then matched against its own sum and count counters.
func TestRegionHeartbeatTracerRecordsEachStageIndependently(t *testing.T) {
	re := require.New(t)
	h := &regionHeartbeatProcessTracer{}
	for i, s := range recordingStages() {
		interval := time.Duration(i+1) * 100 * time.Millisecond // distinct per stage
		sumBefore := testutil.ToFloat64(s.sum)
		countBefore := testutil.ToFloat64(s.count)
		// No real time is spent: rewinding the checkpoint makes the stage measure
		// exactly `interval`.
		h.lastCheckTime = time.Now().Add(-interval)
		s.finish(h)
		re.InDelta(interval.Seconds(), testutil.ToFloat64(s.sum)-sumBefore, 0.05, s.name)
		re.Equal(1.0, testutil.ToFloat64(s.count)-countBefore, s.name)
	}
}
