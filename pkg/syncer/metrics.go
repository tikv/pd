// Copyright 2018 TiKV Project Authors.
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

package syncer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	fullSyncResultSuccess = "success"
	fullSyncResultFailure = "failure"

	fullSyncReasonInitial            = "initial"
	fullSyncReasonHistoryGap         = "history_gap"
	fullSyncReasonStartIndexAhead    = "start_index_ahead"
	fullSyncReasonMaxHistoryExceeded = "max_history_exceeded"
	fullSyncReasonSendError          = "send_error"
	fullSyncReasonContextCanceled    = "context_canceled"
	fullSyncReasonStreamClosed       = "stream_closed"
	fullSyncReasonUnknown            = "unknown"

	historyBufferResizeGrow   = "grow"
	historyBufferResizeShrink = "shrink"

	historyBufferMissHistorySync     = "history_sync"
	historyBufferMissLiveDrain       = "live_drain"
	historyBufferMissFullSyncCatchUp = "full_sync_catch_up"

	streamEventBind            = "bind"
	streamEventUnbind          = "unbind"
	streamEventSendError       = "send_error"
	streamEventSendTimeout     = "send_timeout"
	streamEventContextCanceled = "context_canceled"
	streamEventStreamClosed    = "stream_closed"
)

var regionSyncerStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "region_syncer",
		Name:      "status",
		Help:      "Inner status of the region syncer.",
	}, []string{"type"})

var (
	regionSyncerClientReadyGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "client_ready",
			Help:      "Whether the region syncer client has completed synchronization and can serve follower region reads.",
		})

	regionSyncerFullSyncCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "full_sync_total",
			Help:      "Counter of region syncer full synchronization attempts by result and reason.",
		}, []string{"result", "reason"})

	regionSyncerFullSyncLastDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "full_sync_last_duration_seconds",
			Help:      "Duration in seconds of the latest region syncer full synchronization attempt by result.",
		}, []string{"result"})

	regionSyncerHistoryBufferCapacityGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_capacity",
			Help:      "Current capacity of the region syncer history buffer.",
		})

	regionSyncerHistoryBufferLengthGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_length",
			Help:      "Current number of records retained in the region syncer history buffer.",
		})

	regionSyncerHistoryBufferMaxCapacityGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_max_capacity",
			Help:      "Maximum capacity allowed for the region syncer history buffer.",
		})

	regionSyncerHistoryBufferResizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_resize_total",
			Help:      "Counter of region syncer history buffer resize events.",
		}, []string{"direction"})

	regionSyncerHistoryBufferMissCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_miss_total",
			Help:      "Counter of region syncer history buffer misses by phase.",
		}, []string{"phase"})

	regionSyncerDownstreamLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "downstream_lag",
			Help:      "Number of history records each downstream stream lags behind the leader-side region syncer history index.",
		}, []string{"downstream"})

	regionSyncerStreamEventsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "stream_events_total",
			Help:      "Counter of region syncer downstream stream lifecycle and send events.",
		}, []string{"event"})
)

var (
	regionSyncerFullSyncCounters            map[string]prometheus.Counter
	regionSyncerFullSyncLastDurationGauges  map[string]prometheus.Gauge
	regionSyncerHistoryBufferResizeCounters map[string]prometheus.Counter
	regionSyncerHistoryBufferMissCounters   map[string]prometheus.Counter
	regionSyncerStreamEventCounters         map[string]prometheus.Counter
)

func init() {
	prometheus.MustRegister(regionSyncerStatus)
	prometheus.MustRegister(regionSyncerClientReadyGauge)
	prometheus.MustRegister(regionSyncerFullSyncCounter)
	prometheus.MustRegister(regionSyncerFullSyncLastDurationGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferCapacityGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferLengthGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferMaxCapacityGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferResizeCounter)
	prometheus.MustRegister(regionSyncerHistoryBufferMissCounter)
	prometheus.MustRegister(regionSyncerDownstreamLagGauge)
	prometheus.MustRegister(regionSyncerStreamEventsCounter)

	initRegionSyncerMetrics()
}

func initRegionSyncerMetrics() {
	results := []string{fullSyncResultSuccess, fullSyncResultFailure}
	reasons := []string{
		fullSyncReasonInitial,
		fullSyncReasonHistoryGap,
		fullSyncReasonStartIndexAhead,
		fullSyncReasonMaxHistoryExceeded,
		fullSyncReasonSendError,
		fullSyncReasonContextCanceled,
		fullSyncReasonStreamClosed,
		fullSyncReasonUnknown,
	}
	regionSyncerFullSyncCounters = make(map[string]prometheus.Counter, len(results)*len(reasons))
	for _, result := range results {
		for _, reason := range reasons {
			regionSyncerFullSyncCounters[fullSyncMetricKey(result, reason)] =
				regionSyncerFullSyncCounter.WithLabelValues(result, reason)
		}
	}
	regionSyncerFullSyncLastDurationGauges = map[string]prometheus.Gauge{
		fullSyncResultSuccess: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultSuccess),
		fullSyncResultFailure: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultFailure),
	}
	regionSyncerHistoryBufferResizeCounters = map[string]prometheus.Counter{
		historyBufferResizeGrow:   regionSyncerHistoryBufferResizeCounter.WithLabelValues(historyBufferResizeGrow),
		historyBufferResizeShrink: regionSyncerHistoryBufferResizeCounter.WithLabelValues(historyBufferResizeShrink),
	}
	regionSyncerHistoryBufferMissCounters = map[string]prometheus.Counter{
		historyBufferMissHistorySync:     regionSyncerHistoryBufferMissCounter.WithLabelValues(historyBufferMissHistorySync),
		historyBufferMissLiveDrain:       regionSyncerHistoryBufferMissCounter.WithLabelValues(historyBufferMissLiveDrain),
		historyBufferMissFullSyncCatchUp: regionSyncerHistoryBufferMissCounter.WithLabelValues(historyBufferMissFullSyncCatchUp),
	}
	regionSyncerStreamEventCounters = map[string]prometheus.Counter{
		streamEventBind:            regionSyncerStreamEventsCounter.WithLabelValues(streamEventBind),
		streamEventUnbind:          regionSyncerStreamEventsCounter.WithLabelValues(streamEventUnbind),
		streamEventSendError:       regionSyncerStreamEventsCounter.WithLabelValues(streamEventSendError),
		streamEventSendTimeout:     regionSyncerStreamEventsCounter.WithLabelValues(streamEventSendTimeout),
		streamEventContextCanceled: regionSyncerStreamEventsCounter.WithLabelValues(streamEventContextCanceled),
		streamEventStreamClosed:    regionSyncerStreamEventsCounter.WithLabelValues(streamEventStreamClosed),
	}
}

func fullSyncMetricKey(result, reason string) string {
	return result + "/" + reason
}

func setRegionSyncerClientReadyMetrics(ready bool) {
	if ready {
		regionSyncerClientReadyGauge.Set(1)
		return
	}
	regionSyncerClientReadyGauge.Set(0)
}

func observeFullSyncMetrics(result, reason string, duration time.Duration) {
	counter, ok := regionSyncerFullSyncCounters[fullSyncMetricKey(result, reason)]
	if !ok {
		counter = regionSyncerFullSyncCounters[fullSyncMetricKey(result, fullSyncReasonUnknown)]
	}
	counter.Inc()
	regionSyncerFullSyncLastDurationGauges[result].Set(duration.Seconds())
}

func observeHistoryBufferMetrics(length, capacity, maxCapacity int) {
	regionSyncerHistoryBufferLengthGauge.Set(float64(length))
	regionSyncerHistoryBufferCapacityGauge.Set(float64(capacity))
	regionSyncerHistoryBufferMaxCapacityGauge.Set(float64(maxCapacity))
}

func incHistoryBufferResizeMetrics(direction string) {
	regionSyncerHistoryBufferResizeCounters[direction].Inc()
}

func incHistoryBufferMissMetrics(phase string) {
	regionSyncerHistoryBufferMissCounters[phase].Inc()
}

func incStreamEventMetrics(event string) {
	regionSyncerStreamEventCounters[event].Inc()
}
