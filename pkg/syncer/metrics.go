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

	fullSyncOutcomeSuccess            = "success"
	fullSyncOutcomeMaxHistoryExceeded = "max_history_exceeded"
	fullSyncOutcomeSendError          = "send_error"
	fullSyncOutcomeContextCanceled    = "context_canceled"
	fullSyncOutcomeStreamClosed       = "stream_closed"

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
			Help:      "Counter of region syncer full synchronization attempts by trigger and outcome.",
		}, []string{"trigger", "outcome"})

	regionSyncerFullSyncInProgressGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "full_sync_in_progress",
			Help:      "Current number of region syncer full synchronizations in progress.",
		})

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

	regionSyncerHistoryBufferLiveDrainMissCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "region_syncer",
			Name:      "history_buffer_live_drain_miss_total",
			Help:      "Counter of region syncer history buffer misses while draining live updates to a downstream.",
		})

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
	regionSyncerStreamEventCounters        map[string]prometheus.Counter
)

type fullSyncMetricLabels struct {
	trigger string
	outcome string
}

func init() {
	prometheus.MustRegister(regionSyncerStatus)
	prometheus.MustRegister(regionSyncerClientReadyGauge)
	prometheus.MustRegister(regionSyncerFullSyncCounter)
	prometheus.MustRegister(regionSyncerFullSyncInProgressGauge)
	prometheus.MustRegister(regionSyncerFullSyncLastDurationGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferCapacityRecordsGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferLengthRecordsGauge)
	prometheus.MustRegister(regionSyncerHistoryBufferLiveDrainMissCounter)
	prometheus.MustRegister(regionSyncerDownstreamLagRecordsGauge)
	prometheus.MustRegister(regionSyncerStreamEventsCounter)

	initRegionSyncerMetrics()
}

func initRegionSyncerMetrics() {
	triggers := []string{
		fullSyncTriggerInitial,
		fullSyncTriggerHistoryGap,
		fullSyncTriggerStartIndexAhead,
	}
	outcomes := []string{
		fullSyncOutcomeSuccess,
		fullSyncOutcomeMaxHistoryExceeded,
		fullSyncOutcomeSendError,
		fullSyncOutcomeContextCanceled,
		fullSyncOutcomeStreamClosed,
	}
	regionSyncerFullSyncCounters = make(map[fullSyncMetricLabels]prometheus.Counter, len(triggers)*len(outcomes))
	for _, trigger := range triggers {
		for _, outcome := range outcomes {
			key := fullSyncMetricKey(trigger, outcome)
			regionSyncerFullSyncCounters[key] =
				regionSyncerFullSyncCounter.WithLabelValues(trigger, outcome)
		}
	}
	regionSyncerFullSyncLastDurationGauges = map[string]prometheus.Gauge{
		fullSyncResultSuccess: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultSuccess),
		fullSyncResultFailure: regionSyncerFullSyncLastDurationGauge.WithLabelValues(fullSyncResultFailure),
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

func fullSyncMetricKey(trigger, outcome string) fullSyncMetricLabels {
	return fullSyncMetricLabels{
		trigger: trigger,
		outcome: outcome,
	}
}

func setRegionSyncerClientReadyMetrics(ready bool) {
	if ready {
		regionSyncerClientReadyGauge.Set(1)
		return
	}
	regionSyncerClientReadyGauge.Set(0)
}

func observeFullSyncMetrics(trigger, outcome string, duration time.Duration) {
	regionSyncerFullSyncCounters[fullSyncMetricKey(trigger, outcome)].Inc()
	result := fullSyncResultFailure
	if outcome == fullSyncOutcomeSuccess {
		result = fullSyncResultSuccess
	}
	regionSyncerFullSyncLastDurationGauges[result].Set(duration.Seconds())
}

func observeHistoryBufferMetrics(length, capacity int) {
	observeHistoryBufferLengthMetrics(length)
	regionSyncerHistoryBufferCapacityRecordsGauge.Set(float64(capacity))
}

func observeHistoryBufferLengthMetrics(length int) {
	regionSyncerHistoryBufferLengthRecordsGauge.Set(float64(length))
}

func incHistoryBufferLiveDrainMissMetrics() {
	regionSyncerHistoryBufferLiveDrainMissCounter.Inc()
}

func incStreamEventMetrics(event string) {
	regionSyncerStreamEventCounters[event].Inc()
}
