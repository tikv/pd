// Copyright 2016 TiKV Project Authors.
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

package tso

import "github.com/prometheus/client_golang/prometheus"

const (
	pdNamespace  = "pd"
	tsoNamespace = "tso"
	dcLabel      = "dc"
	typeLabel    = "type"
	groupLabel   = "group"
)

var (
	// TSO metrics
	tsoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: pdNamespace,
			Subsystem: "tso",
			Name:      "events",
			Help:      "Counter of tso events",
		}, []string{typeLabel, groupLabel, dcLabel})

	tsoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: pdNamespace,
			Subsystem: "cluster",
			Name:      "tso",
			Help:      "Record of tso metadata.",
		}, []string{typeLabel, groupLabel, dcLabel})

	tsoGap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: pdNamespace,
			Subsystem: "cluster",
			Name:      "tso_gap_millionseconds",
			Help:      "The minimal (non-zero) TSO gap for each DC.",
		}, []string{groupLabel, dcLabel})

	tsoOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: pdNamespace,
			Subsystem: "cluster",
			Name:      "tso_operation_duration_seconds",
			Help:      "Bucketed histogram of processing time(s) of the TSO operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{typeLabel, groupLabel, dcLabel})

	tsoAllocatorRole = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: pdNamespace,
			Subsystem: "tso",
			Name:      "role",
			Help:      "Indicate the PD server role info, whether it's a TSO allocator.",
		}, []string{groupLabel, dcLabel})

	// Keyspace Group metrics
	keyspaceGroupStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: tsoNamespace,
			Subsystem: "keyspace_group",
			Name:      "state",
			Help:      "Gauge of the Keyspace Group states.",
		}, []string{typeLabel})

	keyspaceGroupOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: tsoNamespace,
			Subsystem: "keyspace_group",
			Name:      "operation_duration_seconds",
			Help:      "Bucketed histogram of processing time(s) of the Keyspace Group operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{typeLabel})
)

func init() {
	prometheus.MustRegister(tsoCounter)
	prometheus.MustRegister(tsoGauge)
	prometheus.MustRegister(tsoGap)
	prometheus.MustRegister(tsoOpDuration)
	prometheus.MustRegister(tsoAllocatorRole)
	prometheus.MustRegister(keyspaceGroupStateGauge)
	prometheus.MustRegister(keyspaceGroupOpDuration)
}

type tsoMetrics struct {
	// timestampOracle event counter
	syncEvent                    prometheus.Counter
	skipSyncEvent                prometheus.Counter
	syncOKEvent                  prometheus.Counter
	errSaveSyncTSEvent           prometheus.Counter
	errLeaseResetTSEvent         prometheus.Counter
	errResetSmallPhysicalTSEvent prometheus.Counter
	errResetSmallLogicalTSEvent  prometheus.Counter
	errResetLargeTSEvent         prometheus.Counter
	errSaveResetTSEvent          prometheus.Counter
	resetTSOOKEvent              prometheus.Counter
	saveEvent                    prometheus.Counter
	slowSaveEvent                prometheus.Counter
	systemTimeSlowEvent          prometheus.Counter
	skipSaveEvent                prometheus.Counter
	errSaveUpdateTSEvent         prometheus.Counter
	notLeaderAnymoreEvent        prometheus.Counter
	logicalOverflowEvent         prometheus.Counter
	exceededMaxRetryEvent        prometheus.Counter
	// timestampOracle operation duration
	syncSaveDuration   prometheus.Observer
	resetSaveDuration  prometheus.Observer
	updateSaveDuration prometheus.Observer
	// allocator event counter
	notLeaderEvent               prometheus.Counter
	globalTSOSyncEvent           prometheus.Counter
	globalTSOEstimateEvent       prometheus.Counter
	globalTSOPersistEvent        prometheus.Counter
	precheckLogicalOverflowEvent prometheus.Counter
	errGlobalTSOPersistEvent     prometheus.Counter
	// others
	tsoPhysicalGauge      prometheus.Gauge
	tsoPhysicalGapGauge   prometheus.Gauge
	globalTSOSyncRTTGauge prometheus.Gauge
}

func newTSOMetrics(groupID, dcLocation string) *tsoMetrics {
	return &tsoMetrics{
		syncEvent:                    tsoCounter.WithLabelValues("sync", groupID, dcLocation),
		skipSyncEvent:                tsoCounter.WithLabelValues("skip_sync", groupID, dcLocation),
		syncOKEvent:                  tsoCounter.WithLabelValues("sync_ok", groupID, dcLocation),
		errSaveSyncTSEvent:           tsoCounter.WithLabelValues("err_save_sync_ts", groupID, dcLocation),
		errLeaseResetTSEvent:         tsoCounter.WithLabelValues("err_lease_reset_ts", groupID, dcLocation),
		errResetSmallPhysicalTSEvent: tsoCounter.WithLabelValues("err_reset_physical_small_ts", groupID, dcLocation),
		errResetSmallLogicalTSEvent:  tsoCounter.WithLabelValues("err_reset_logical_small_ts", groupID, dcLocation),
		errResetLargeTSEvent:         tsoCounter.WithLabelValues("err_reset_large_ts", groupID, dcLocation),
		errSaveResetTSEvent:          tsoCounter.WithLabelValues("err_save_reset_ts", groupID, dcLocation),
		resetTSOOKEvent:              tsoCounter.WithLabelValues("reset_tso_ok", groupID, dcLocation),
		saveEvent:                    tsoCounter.WithLabelValues("save", groupID, dcLocation),
		slowSaveEvent:                tsoCounter.WithLabelValues("slow_save", groupID, dcLocation),
		systemTimeSlowEvent:          tsoCounter.WithLabelValues("system_time_slow", groupID, dcLocation),
		skipSaveEvent:                tsoCounter.WithLabelValues("skip_save", groupID, dcLocation),
		errSaveUpdateTSEvent:         tsoCounter.WithLabelValues("err_save_update_ts", groupID, dcLocation),
		notLeaderAnymoreEvent:        tsoCounter.WithLabelValues("not_leader_anymore", groupID, dcLocation),
		logicalOverflowEvent:         tsoCounter.WithLabelValues("logical_overflow", groupID, dcLocation),
		exceededMaxRetryEvent:        tsoCounter.WithLabelValues("exceeded_max_retry", groupID, dcLocation),
		syncSaveDuration:             tsoOpDuration.WithLabelValues("sync_save", groupID, dcLocation),
		resetSaveDuration:            tsoOpDuration.WithLabelValues("reset_save", groupID, dcLocation),
		updateSaveDuration:           tsoOpDuration.WithLabelValues("update_save", groupID, dcLocation),
		notLeaderEvent:               tsoCounter.WithLabelValues("not_leader", groupID, dcLocation),
		globalTSOSyncEvent:           tsoCounter.WithLabelValues("global_tso_sync", groupID, dcLocation),
		globalTSOEstimateEvent:       tsoCounter.WithLabelValues("global_tso_estimate", groupID, dcLocation),
		globalTSOPersistEvent:        tsoCounter.WithLabelValues("global_tso_persist", groupID, dcLocation),
		errGlobalTSOPersistEvent:     tsoCounter.WithLabelValues("global_tso_persist_err", groupID, dcLocation),
		precheckLogicalOverflowEvent: tsoCounter.WithLabelValues("precheck_logical_overflow", groupID, dcLocation),
		tsoPhysicalGauge:             tsoGauge.WithLabelValues("tso", groupID, dcLocation),
		tsoPhysicalGapGauge:          tsoGap.WithLabelValues(groupLabel, dcLocation),
		globalTSOSyncRTTGauge:        tsoGauge.WithLabelValues("global_tso_sync_rtt", groupID, dcLocation),
	}
}

type keyspaceGroupMetrics struct {
	splitSourceGauge        prometheus.Gauge
	splitTargetGauge        prometheus.Gauge
	mergeSourceGauge        prometheus.Gauge
	mergeTargetGauge        prometheus.Gauge
	splitDuration           prometheus.Observer
	mergeDuration           prometheus.Observer
	finishSplitSendDuration prometheus.Observer
	finishSplitDuration     prometheus.Observer
	finishMergeSendDuration prometheus.Observer
	finishMergeDuration     prometheus.Observer
}

func newKeyspaceGroupMetrics() *keyspaceGroupMetrics {
	return &keyspaceGroupMetrics{
		splitSourceGauge:        keyspaceGroupStateGauge.WithLabelValues("split-source"),
		splitTargetGauge:        keyspaceGroupStateGauge.WithLabelValues("split-target"),
		mergeSourceGauge:        keyspaceGroupStateGauge.WithLabelValues("merge-source"),
		mergeTargetGauge:        keyspaceGroupStateGauge.WithLabelValues("merge-target"),
		splitDuration:           keyspaceGroupOpDuration.WithLabelValues("split"),
		mergeDuration:           keyspaceGroupOpDuration.WithLabelValues("merge"),
		finishSplitSendDuration: keyspaceGroupOpDuration.WithLabelValues("finish-split-send"),
		finishSplitDuration:     keyspaceGroupOpDuration.WithLabelValues("finish-split"),
		finishMergeSendDuration: keyspaceGroupOpDuration.WithLabelValues("finish-merge-send"),
		finishMergeDuration:     keyspaceGroupOpDuration.WithLabelValues("finish-merge"),
	}
}
