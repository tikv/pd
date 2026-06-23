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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// HeartbeatBreakdownHandleDurationSum is the summary of the processing time of handle the heartbeat stage.
	HeartbeatBreakdownHandleDurationSum = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "core",
			Name:      "region_heartbeat_breakdown_handle_duration_seconds_sum",
			Help:      "Bucketed histogram of processing time (s) of handle the heartbeat stage.",
		}, []string{"name"})

	// HeartbeatBreakdownHandleCount is the summary of the processing count of handle the heartbeat stage.
	HeartbeatBreakdownHandleCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "core",
			Name:      "region_heartbeat_breakdown_handle_duration_seconds_count",
			Help:      "Bucketed histogram of processing count of handle the heartbeat stage.",
		}, []string{"name"})
	// AcquireRegionsLockWaitDurationSum is the summary of the processing time of waiting for acquiring regions lock.
	AcquireRegionsLockWaitDurationSum = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "core",
			Name:      "acquire_regions_lock_wait_duration_seconds_sum",
			Help:      "Bucketed histogram of processing time (s) of waiting for acquiring regions lock.",
		}, []string{"type"})
	// AcquireRegionsLockWaitCount is the summary of the processing count of waiting for acquiring regions lock.
	AcquireRegionsLockWaitCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "core",
			Name:      "acquire_regions_lock_wait_duration_seconds_count",
			Help:      "Bucketed histogram of processing count of waiting for acquiring regions lock.",
		}, []string{"name"})

	// lock statistics
	waitRegionsLockDurationSum    = AcquireRegionsLockWaitDurationSum.WithLabelValues("WaitRegionsLock")
	waitRegionsLockCount          = AcquireRegionsLockWaitCount.WithLabelValues("WaitRegionsLock")
	waitSubRegionsLockDurationSum = AcquireRegionsLockWaitDurationSum.WithLabelValues("WaitSubRegionsLock")
	waitSubRegionsLockCount       = AcquireRegionsLockWaitCount.WithLabelValues("WaitSubRegionsLock")

	// heartbeat breakdown statistics
	preCheckDurationSum       = HeartbeatBreakdownHandleDurationSum.WithLabelValues("PreCheck")
	preCheckCount             = HeartbeatBreakdownHandleCount.WithLabelValues("PreCheck")
	asyncHotStatsDurationSum  = HeartbeatBreakdownHandleDurationSum.WithLabelValues("AsyncHotStatsDuration")
	asyncHotStatsCount        = HeartbeatBreakdownHandleCount.WithLabelValues("AsyncHotStatsDuration")
	regionGuideDurationSum    = HeartbeatBreakdownHandleDurationSum.WithLabelValues("RegionGuide")
	regionGuideCount          = HeartbeatBreakdownHandleCount.WithLabelValues("RegionGuide")
	checkOverlapsDurationSum  = HeartbeatBreakdownHandleDurationSum.WithLabelValues("SaveCache_CheckOverlaps")
	checkOverlapsCount        = HeartbeatBreakdownHandleCount.WithLabelValues("SaveCache_CheckOverlaps")
	validateRegionDurationSum = HeartbeatBreakdownHandleDurationSum.WithLabelValues("SaveCache_InvalidRegion")
	validateRegionCount       = HeartbeatBreakdownHandleCount.WithLabelValues("SaveCache_InvalidRegion")
	setRegionDurationSum      = HeartbeatBreakdownHandleDurationSum.WithLabelValues("SaveCache_SetRegion")
	setRegionCount            = HeartbeatBreakdownHandleCount.WithLabelValues("SaveCache_SetRegion")
	updateSubTreeDurationSum  = HeartbeatBreakdownHandleDurationSum.WithLabelValues("SaveCache_UpdateSubTree")
	updateSubTreeCount        = HeartbeatBreakdownHandleCount.WithLabelValues("SaveCache_UpdateSubTree")
	regionCollectDurationSum  = HeartbeatBreakdownHandleDurationSum.WithLabelValues("CollectRegionStats")
	regionCollectCount        = HeartbeatBreakdownHandleCount.WithLabelValues("CollectRegionStats")
	otherDurationSum          = HeartbeatBreakdownHandleDurationSum.WithLabelValues("Other")
	otherCount                = HeartbeatBreakdownHandleCount.WithLabelValues("Other")
)

func init() {
	prometheus.MustRegister(HeartbeatBreakdownHandleDurationSum)
	prometheus.MustRegister(HeartbeatBreakdownHandleCount)
	prometheus.MustRegister(AcquireRegionsLockWaitDurationSum)
	prometheus.MustRegister(AcquireRegionsLockWaitCount)
}

var tracerPool = &sync.Pool{
	New: func() any {
		return &regionHeartbeatProcessTracer{}
	},
}

// RegionHeartbeatProcessTracer is used to trace the process of handling region heartbeat.
type RegionHeartbeatProcessTracer interface {
	// Begin starts the tracing.
	Begin()
	// OnPreCheckFinished will be called when the pre-check is finished.
	OnPreCheckFinished()
	// OnAsyncHotStatsFinished will be called when the async hot stats is finished.
	OnAsyncHotStatsFinished()
	// OnRegionGuideFinished will be called when the region guide is finished.
	OnRegionGuideFinished()
	// OnSaveCacheBegin will be called when the save cache begins.
	OnSaveCacheBegin()
	// OnSaveCacheFinished will be called when the save cache is finished.
	OnSaveCacheFinished()
	// OnCheckOverlapsFinished will be called when the check overlaps is finished.
	OnCheckOverlapsFinished()
	// OnValidateRegionFinished will be called when the validate region is finished.
	OnValidateRegionFinished()
	// OnSetRegionFinished will be called when the set region is finished.
	OnSetRegionFinished()
	// OnUpdateSubTreeFinished will be called when the update sub tree is finished.
	OnUpdateSubTreeFinished()
	// OnCollectRegionStatsFinished will be called when the collect region stats is finished.
	OnCollectRegionStatsFinished()
	// OnAllStageFinished will be called when all stages are finished.
	OnAllStageFinished()
	// Release releases the tracer.
	Release()
}

type noopHeartbeatProcessTracer struct{}

// NewNoopHeartbeatProcessTracer returns a noop heartbeat process tracer.
func NewNoopHeartbeatProcessTracer() RegionHeartbeatProcessTracer {
	return &noopHeartbeatProcessTracer{}
}

// Begin implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) Begin() {}

// OnPreCheckFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnPreCheckFinished() {}

// OnAsyncHotStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnAsyncHotStatsFinished() {}

// OnRegionGuideFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnRegionGuideFinished() {}

// OnSaveCacheBegin implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnSaveCacheBegin() {}

// OnSaveCacheFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnSaveCacheFinished() {}

// OnCheckOverlapsFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnCheckOverlapsFinished() {}

// OnValidateRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnValidateRegionFinished() {}

// OnSetRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnSetRegionFinished() {}

// OnUpdateSubTreeFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnUpdateSubTreeFinished() {}

// OnCollectRegionStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnCollectRegionStatsFinished() {}

// OnAllStageFinished implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) OnAllStageFinished() {}

// Release implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) Release() {}

// regionHeartbeatProcessTracer measures the duration of each region-heartbeat
// processing stage and reports it to the breakdown metrics. It keeps a single
// checkpoint: a stage's duration is the time elapsed since the previous stage
// advanced the checkpoint.
type regionHeartbeatProcessTracer struct {
	lastCheckTime time.Time
}

// NewHeartbeatProcessTracer returns a heartbeat process tracer.
func NewHeartbeatProcessTracer() RegionHeartbeatProcessTracer {
	return tracerPool.Get().(*regionHeartbeatProcessTracer)
}

// Begin implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) Begin() {
	h.lastCheckTime = time.Now()
}

// measure records one processing stage: it reads the clock once, computes the
// duration since the previous checkpoint, advances the checkpoint, and updates
// the stage's duration-sum and count counters.
//
// A non-positive duration is not added to the sum counter. time.Now() normally
// carries a monotonic reading, so two consecutive reads never go backwards; but
// on some virtualized hosts the OS monotonic clock (CLOCK_MONOTONIC) can briefly
// regress -- e.g. an unsynchronized TSC across vCPUs, or a live migration.
// Without this guard the negative value would reach prometheus.Counter.Add,
// which panics with "counter cannot decrease in value" and crashes pd-server.
// Guarding turns that rare, external clock glitch into a negligibly
// under-counted metric instead of a fatal panic.
func (h *regionHeartbeatProcessTracer) measure(durationSum, count prometheus.Counter) {
	now := time.Now()
	duration := now.Sub(h.lastCheckTime)
	h.lastCheckTime = now
	if duration > 0 {
		durationSum.Add(duration.Seconds())
	}
	count.Inc()
}

// resetCheckpoint starts a fresh measurement window at the current time. It is
// used at the SaveCache boundaries, whose enclosed sub-stages are timed on their
// own and should not be attributed to the surrounding stages.
func (h *regionHeartbeatProcessTracer) resetCheckpoint() {
	h.lastCheckTime = time.Now()
}

// OnPreCheckFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnPreCheckFinished() {
	h.measure(preCheckDurationSum, preCheckCount)
}

// OnAsyncHotStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnAsyncHotStatsFinished() {
	h.measure(asyncHotStatsDurationSum, asyncHotStatsCount)
}

// OnRegionGuideFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnRegionGuideFinished() {
	h.measure(regionGuideDurationSum, regionGuideCount)
}

// OnSaveCacheBegin implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSaveCacheBegin() {
	h.resetCheckpoint()
}

// OnSaveCacheFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSaveCacheFinished() {
	h.resetCheckpoint()
}

// OnCheckOverlapsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnCheckOverlapsFinished() {
	h.measure(checkOverlapsDurationSum, checkOverlapsCount)
}

// OnValidateRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnValidateRegionFinished() {
	h.measure(validateRegionDurationSum, validateRegionCount)
}

// OnSetRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSetRegionFinished() {
	h.measure(setRegionDurationSum, setRegionCount)
}

// OnUpdateSubTreeFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnUpdateSubTreeFinished() {
	h.measure(updateSubTreeDurationSum, updateSubTreeCount)
}

// OnCollectRegionStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnCollectRegionStatsFinished() {
	h.measure(regionCollectDurationSum, regionCollectCount)
}

// OnAllStageFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnAllStageFinished() {
	h.measure(otherDurationSum, otherCount)
}

// Release implements the RegionHeartbeatProcessTracer interface.
// Release puts the tracer back into the pool.
func (h *regionHeartbeatProcessTracer) Release() {
	// Reset the fields of h to their zero values.
	*h = regionHeartbeatProcessTracer{}
	tracerPool.Put(h)
}
