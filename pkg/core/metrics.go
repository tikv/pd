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
	"go.uber.org/zap"
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

type saveCacheStats struct {
	startTime              time.Time
	lastCheckTime          time.Time
	checkOverlapsDuration  time.Duration
	validateRegionDuration time.Duration
	setRegionDuration      time.Duration
	updateSubTreeDuration  time.Duration
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
	// LogFields returns the log fields.
	LogFields() []zap.Field
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

// LogFields implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) LogFields() []zap.Field {
	return nil
}

// Release implements the RegionHeartbeatProcessTracer interface.
func (*noopHeartbeatProcessTracer) Release() {}

type regionHeartbeatProcessTracer struct {
	startTime             time.Time
	lastCheckTime         time.Time
	preCheckDuration      time.Duration
	asyncHotStatsDuration time.Duration
	regionGuideDuration   time.Duration
	saveCacheStats        saveCacheStats
	OtherDuration         time.Duration
}

// NewHeartbeatProcessTracer returns a heartbeat process tracer.
func NewHeartbeatProcessTracer() RegionHeartbeatProcessTracer {
	return tracerPool.Get().(*regionHeartbeatProcessTracer)
}

// Begin implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) Begin() {
	now := time.Now()
	h.startTime = now
	h.lastCheckTime = now
}

// OnPreCheckFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnPreCheckFinished() {
	now := time.Now()
	h.preCheckDuration = now.Sub(h.lastCheckTime)
	h.lastCheckTime = now
	preCheckDurationSum.Add(h.preCheckDuration.Seconds())
	preCheckCount.Inc()
}

// OnAsyncHotStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnAsyncHotStatsFinished() {
	now := time.Now()
	h.asyncHotStatsDuration = now.Sub(h.lastCheckTime)
	h.lastCheckTime = now
	asyncHotStatsDurationSum.Add(h.preCheckDuration.Seconds())
	asyncHotStatsCount.Inc()
}

// OnRegionGuideFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnRegionGuideFinished() {
	now := time.Now()
	h.regionGuideDuration = now.Sub(h.lastCheckTime)
	h.lastCheckTime = now
	regionGuideDurationSum.Add(h.regionGuideDuration.Seconds())
	regionGuideCount.Inc()
}

// OnSaveCacheBegin implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSaveCacheBegin() {
	now := time.Now()
	h.saveCacheStats.startTime = now
	h.saveCacheStats.lastCheckTime = now
	h.lastCheckTime = now
}

// OnSaveCacheFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSaveCacheFinished() {
	// update the outer checkpoint time
	h.lastCheckTime = time.Now()
}

// OnCollectRegionStatsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnCollectRegionStatsFinished() {
	now := time.Now()
	regionCollectDurationSum.Add(now.Sub(h.lastCheckTime).Seconds())
	regionCollectCount.Inc()
	h.lastCheckTime = now
}

// OnCheckOverlapsFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnCheckOverlapsFinished() {
	now := time.Now()
	h.saveCacheStats.checkOverlapsDuration = now.Sub(h.lastCheckTime)
	h.saveCacheStats.lastCheckTime = now
	checkOverlapsDurationSum.Add(h.saveCacheStats.checkOverlapsDuration.Seconds())
	checkOverlapsCount.Inc()
}

// OnValidateRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnValidateRegionFinished() {
	now := time.Now()
	h.saveCacheStats.validateRegionDuration = now.Sub(h.saveCacheStats.lastCheckTime)
	h.saveCacheStats.lastCheckTime = now
	validateRegionDurationSum.Add(h.saveCacheStats.validateRegionDuration.Seconds())
	validateRegionCount.Inc()
}

// OnSetRegionFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnSetRegionFinished() {
	now := time.Now()
	h.saveCacheStats.setRegionDuration = now.Sub(h.saveCacheStats.lastCheckTime)
	h.saveCacheStats.lastCheckTime = now
	setRegionDurationSum.Add(h.saveCacheStats.setRegionDuration.Seconds())
	setRegionCount.Inc()
}

// OnUpdateSubTreeFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnUpdateSubTreeFinished() {
	now := time.Now()
	h.saveCacheStats.updateSubTreeDuration = now.Sub(h.saveCacheStats.lastCheckTime)
	h.saveCacheStats.lastCheckTime = now
	updateSubTreeDurationSum.Add(h.saveCacheStats.updateSubTreeDuration.Seconds())
	updateSubTreeCount.Inc()
}

// OnAllStageFinished implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) OnAllStageFinished() {
	now := time.Now()
	h.OtherDuration = now.Sub(h.lastCheckTime)
	otherDurationSum.Add(h.OtherDuration.Seconds())
	otherCount.Inc()
}

// LogFields implements the RegionHeartbeatProcessTracer interface.
func (h *regionHeartbeatProcessTracer) LogFields() []zap.Field {
	return []zap.Field{
		zap.Duration("pre-check-duration", h.preCheckDuration),
		zap.Duration("async-hot-stats-duration", h.asyncHotStatsDuration),
		zap.Duration("region-guide-duration", h.regionGuideDuration),
		zap.Duration("check-overlaps-duration", h.saveCacheStats.checkOverlapsDuration),
		zap.Duration("validate-region-duration", h.saveCacheStats.validateRegionDuration),
		zap.Duration("set-region-duration", h.saveCacheStats.setRegionDuration),
		zap.Duration("update-sub-tree-duration", h.saveCacheStats.updateSubTreeDuration),
		zap.Duration("other-duration", h.OtherDuration),
	}
}

// Release implements the RegionHeartbeatProcessTracer interface.
// Release puts the tracer back into the pool.
func (h *regionHeartbeatProcessTracer) Release() {
	// Reset the fields of h to their zero values.
	*h = regionHeartbeatProcessTracer{}
	tracerPool.Put(h)
}
