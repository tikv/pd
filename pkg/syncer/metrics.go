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

	fullSyncTriggerInitial         = "initial"
	fullSyncTriggerHistoryGap      = "history_gap"
	fullSyncTriggerStartIndexAhead = "start_index_ahead"
	fullSyncTriggerUnknown         = "unknown"

	fullSyncFailureNone               = "none"
	fullSyncFailureMaxHistoryExceeded = "max_history_exceeded"
	fullSyncFailureSendError          = "send_error"
	fullSyncFailureContextCanceled    = "context_canceled"
	fullSyncFailureStreamClosed       = "stream_closed"
	fullSyncFailureUnknown            = "unknown"

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
			Help:      "Counter of region syncer full synchronization attempts by result, trigger, and failure reason.",
		}, []string{"result", "trigger", "failure_reason"})

	regionSyncerFullSyncLastDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "full_sync_last_duration_seconds",
			Help:      "Duration in seconds of the latest region syncer full synchronization attempt by result.",
		}, []string{"result"})

	regionSyncerHistoryBufferCapacityRecordsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_capacity_records",
			Help:      "Current capacity in records of the region syncer history buffer.",
		})

	regionSyncerHistoryBufferLengthRecordsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_length_records",
			Help:      "Current number of records retained in the region syncer history buffer.",
		})

	regionSyncerHistoryBufferMissCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_miss_total",
			Help:      "Counter of region syncer history buffer misses by phase.",
		}, []string{"phase"})

	regionSyncerDownstreamLagRecordsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "downstream_lag_records",
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
	regionSyncerFullSyncCounters           map[fullSyncMetricLabels]prometheus.Counter
	regionSyncerFullSyncLastDurationGauges map[string]prometheus.Gauge
	regionSyncerHistoryBufferMissCounters  map[string]prometheus.Counter
	regionSyncerStreamEventCounters        map[string]prometheus.Counter
)

type fullSyncMetricLabels struct {
	result        string
	trigger       string
	failureReason string
}

func init() {
	prometheus.MustRegister(regionSyncerStatus)
	prometheus.MustRegister(regionSyncerClientReadyGauge)
	prometheus.MustRegister(regionSyncerFullSyncCounter)
	prometheus.MustRegister(regionSyncerFullSyncLastDurationGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferCapacityRecordsGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferLengthRecordsGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferMissCounter)
	prometheus.MustRegister(regionSyncerDownstreamLagRecordsGauge)
	prometheus.MustRegister(regionSyncerStreamEventsCounter)

	initRegionSyncerMetrics()
}

func initRegionSyncerMetrics() {
	triggers := []string{
		fullSyncTriggerInitial,
		fullSyncTriggerHistoryGap,
		fullSyncTriggerStartIndexAhead,
		fullSyncTriggerUnknown,
	}
	failureReasons := []string{
		fullSyncFailureMaxHistoryExceeded,
		fullSyncFailureSendError,
		fullSyncFailureContextCanceled,
		fullSyncFailureStreamClosed,
		fullSyncFailureUnknown,
	}
	regionSyncerFullSyncCounters = make(map[fullSyncMetricLabels]prometheus.Counter, len(triggers)*(len(failureReasons)+1))
	for _, trigger := range triggers {
		successKey := fullSyncMetricKey(fullSyncResultSuccess, trigger, fullSyncFailureNone)
		regionSyncerFullSyncCounters[successKey] =
			regionSyncerFullSyncCounter.WithLabelValues(fullSyncResultSuccess, trigger, fullSyncFailureNone)
		for _, failureReason := range failureReasons {
			failureKey := fullSyncMetricKey(fullSyncResultFailure, trigger, failureReason)
			regionSyncerFullSyncCounters[failureKey] =
				regionSyncerFullSyncCounter.WithLabelValues(fullSyncResultFailure, trigger, failureReason)
		}
	}
	regionSyncerFullSyncLastDurationGauges = map[string]prometheus.Gauge{
		fullSyncResultSuccess: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultSuccess),
		fullSyncResultFailure: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultFailure),
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

func fullSyncMetricKey(result, trigger, failureReason string) fullSyncMetricLabels {
	return fullSyncMetricLabels{
		result:        result,
		trigger:       trigger,
		failureReason: failureReason,
	}
}

func setRegionSyncerClientReadyMetrics(ready bool) {
	if ready {
		regionSyncerClientReadyGauge.Set(1)
		return
	}
	regionSyncerClientReadyGauge.Set(0)
}

func observeFullSyncMetrics(result, trigger, failureReason string, duration time.Duration) {
	counter, ok := regionSyncerFullSyncCounters[fullSyncMetricKey(result, trigger, failureReason)]
	if !ok {
		if result == fullSyncResultSuccess {
			counter = regionSyncerFullSyncCounters[fullSyncMetricKey(
				fullSyncResultSuccess,
				fullSyncTriggerUnknown,
				fullSyncFailureNone,
			)]
		} else {
			counter = regionSyncerFullSyncCounters[fullSyncMetricKey(
				fullSyncResultFailure,
				fullSyncTriggerUnknown,
				fullSyncFailureUnknown,
			)]
		}
	}
	counter.Inc()
	regionSyncerFullSyncLastDurationGauges[result].Set(duration.Seconds())
}

func observeHistoryBufferMetrics(length, capacity int) {
	observeHistoryBufferLengthMetrics(length)
	regionSyncerHistoryBufferCapacityRecordsGauge.Set(float64(capacity))
}

func observeHistoryBufferLengthMetrics(length int) {
	regionSyncerHistoryBufferLengthRecordsGauge.Set(float64(length))
}

func incHistoryBufferMissMetrics(phase string) {
	regionSyncerHistoryBufferMissCounters[phase].Inc()
}

func incStreamEventMetrics(event string) {
	regionSyncerStreamEventCounters[event].Inc()
}
