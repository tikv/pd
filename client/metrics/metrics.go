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

package metrics

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// make sure register metrics only once
var initialized int32

func init() {
	initMetrics(prometheus.Labels{})
	initCmdDurations()
	initRegisteredConsumers()
}

var consumersInitializers = struct {
	sync.Mutex
	value []func()
}{}

// RegisterConsumer registers a consumer to be initialized when the metrics are (re)initialized
func RegisterConsumer(initConsumer func()) {
	consumersInitializers.Lock()
	defer consumersInitializers.Unlock()
	consumersInitializers.value = append(consumersInitializers.value, initConsumer)
	initConsumer()
}

func initRegisteredConsumers() {
	consumersInitializers.Lock()
	defer consumersInitializers.Unlock()
	for _, initConsumer := range consumersInitializers.value {
		initConsumer()
	}
}

// InitAndRegisterMetrics initializes and registers the metrics manually.
func InitAndRegisterMetrics(constLabels prometheus.Labels) {
	if atomic.CompareAndSwapInt32(&initialized, 0, 1) {
		// init metrics with constLabels
		initMetrics(constLabels)
		initCmdDurations()
		initRegisteredConsumers()
		// register metrics
		registerMetrics()
	}
}

var (
	cmdDuration               *prometheus.HistogramVec
	cmdFailedDuration         *prometheus.HistogramVec
	internalCmdDuration       *prometheus.HistogramVec
	internalCmdFailedDuration *prometheus.HistogramVec
	requestDuration           *prometheus.HistogramVec

	// TSOBestBatchSize is the histogram of the best batch size of TSO requests.
	TSOBestBatchSize prometheus.Histogram
	// TSOBatchSize is the histogram of the batch size of TSO requests.
	TSOBatchSize prometheus.Histogram
	// TSOBatchSendLatency is the histogram of the latency of sending TSO requests.
	TSOBatchSendLatency prometheus.Histogram
	// RequestForwarded is the gauge to indicate if the request is forwarded.
	RequestForwarded *prometheus.GaugeVec
	// OngoingRequestCountGauge is the gauge to indicate the count of ongoing TSO requests.
	OngoingRequestCountGauge *prometheus.GaugeVec
	// EstimateTSOLatencyGauge is the gauge to indicate the estimated latency of TSO requests.
	EstimateTSOLatencyGauge *prometheus.GaugeVec
	// CircuitBreakerCounters is a vector for different circuit breaker counters
	CircuitBreakerCounters *prometheus.CounterVec
)

func initMetrics(constLabels prometheus.Labels) {
	cmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "cmd",
			Name:        "handle_cmds_duration_seconds",
			Help:        "Bucketed histogram of processing time (s) of handled success cmds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	cmdFailedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "cmd",
			Name:        "handle_failed_cmds_duration_seconds",
			Help:        "Bucketed histogram of processing time (s) of failed handled cmds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	internalCmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "internal_cmd",
			Name:        "handle_cmds_duration_seconds",
			Help:        "Bucketed histogram of processing time (s) of handled success internal cmds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	internalCmdFailedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "internal_cmd",
			Name:        "handle_failed_cmds_duration_seconds",
			Help:        "Bucketed histogram of processing time (s) of failed handled internal cmds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "handle_requests_duration_seconds",
			Help:        "Bucketed histogram of processing time (s) of handled requests.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	TSOBestBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "handle_tso_best_batch_size",
			Help:        "Bucketed histogram of the best batch size of handled requests.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(1, 2, 13),
		})

	TSOBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "handle_tso_batch_size",
			Help:        "Bucketed histogram of the batch size of handled requests.",
			ConstLabels: constLabels,
			Buckets:     []float64{1, 2, 4, 8, 10, 14, 18, 22, 26, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120, 140, 160, 180, 200, 500, 1000},
		})

	TSOBatchSendLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "tso_batch_send_latency",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 13),
			Help:        "tso batch send latency",
		})

	RequestForwarded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "forwarded_status",
			Help:        "The status to indicate if the request is forwarded",
			ConstLabels: constLabels,
		}, []string{"host", "delegate"})

	OngoingRequestCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "ongoing_requests_count",
			Help:        "Current count of ongoing batch tso requests",
			ConstLabels: constLabels,
		}, []string{"stream"})
	EstimateTSOLatencyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "estimate_tso_latency",
			Help:        "Estimated latency of an RTT of getting TSO",
			ConstLabels: constLabels,
		}, []string{"stream"})

	CircuitBreakerCounters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   "pd_client",
			Subsystem:   "request",
			Name:        "circuit_breaker_count",
			Help:        "Circuit breaker counters",
			ConstLabels: constLabels,
		}, []string{"name", "success"})
}

// CmdDurationXXX and CmdFailedDurationXXX are the durations of the client commands.
var (
	CmdDurationTSOWait                  prometheus.Observer
	CmdDurationTSO                      prometheus.Observer
	CmdDurationTSOAsyncWait             prometheus.Observer
	CmdDurationGetRegion                prometheus.Observer
	CmdDurationGetAllMembers            prometheus.Observer
	CmdDurationGetPrevRegion            prometheus.Observer
	CmdDurationGetRegionByID            prometheus.Observer
	CmdDurationScanRegions              prometheus.Observer
	CmdDurationBatchScanRegions         prometheus.Observer
	CmdDurationGetStore                 prometheus.Observer
	CmdDurationGetAllStores             prometheus.Observer
	CmdDurationUpdateGCSafePoint        prometheus.Observer
	CmdDurationUpdateServiceGCSafePoint prometheus.Observer
	CmdDurationScatterRegion            prometheus.Observer
	CmdDurationScatterRegions           prometheus.Observer
	CmdDurationGetOperator              prometheus.Observer
	CmdDurationSplitRegions             prometheus.Observer
	CmdDurationSplitAndScatterRegions   prometheus.Observer
	CmdDurationLoadKeyspace             prometheus.Observer
	CmdDurationUpdateKeyspaceState      prometheus.Observer
	CmdDurationGetAllKeyspaces          prometheus.Observer
	CmdDurationGet                      prometheus.Observer
	CmdDurationPut                      prometheus.Observer
	CmdDurationUpdateGCSafePointV2      prometheus.Observer
	CmdDurationUpdateServiceSafePointV2 prometheus.Observer

	CmdFailedDurationGetRegion                prometheus.Observer
	CmdFailedDurationTSOWait                  prometheus.Observer
	CmdFailedDurationTSO                      prometheus.Observer
	CmdFailedDurationGetAllMembers            prometheus.Observer
	CmdFailedDurationGetPrevRegion            prometheus.Observer
	CmdFailedDurationGetRegionByID            prometheus.Observer
	CmdFailedDurationScanRegions              prometheus.Observer
	CmdFailedDurationBatchScanRegions         prometheus.Observer
	CmdFailedDurationGetStore                 prometheus.Observer
	CmdFailedDurationGetAllStores             prometheus.Observer
	CmdFailedDurationUpdateGCSafePoint        prometheus.Observer
	CmdFailedDurationUpdateServiceGCSafePoint prometheus.Observer
	CmdFailedDurationLoadKeyspace             prometheus.Observer
	CmdFailedDurationUpdateKeyspaceState      prometheus.Observer
	CmdFailedDurationGet                      prometheus.Observer
	CmdFailedDurationPut                      prometheus.Observer
	CmdFailedDurationUpdateGCSafePointV2      prometheus.Observer
	CmdFailedDurationUpdateServiceSafePointV2 prometheus.Observer

	InternalCmdDurationGetClusterInfo prometheus.Observer
	InternalCmdDurationGetMembers     prometheus.Observer

	InternalCmdFailedDurationGetClusterInfo prometheus.Observer
	InternalCmdFailedDurationGetMembers     prometheus.Observer

	// RequestDurationTSO records the durations of the successful TSO requests.
	RequestDurationTSO prometheus.Observer
	// RequestFailedDurationTSO records the durations of the failed TSO requests.
	RequestFailedDurationTSO prometheus.Observer
)

func initCmdDurations() {
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	CmdDurationTSOWait = cmdDuration.WithLabelValues("wait")
	CmdDurationTSO = cmdDuration.WithLabelValues("tso")
	CmdDurationTSOAsyncWait = cmdDuration.WithLabelValues("tso_async_wait")
	CmdDurationGetRegion = cmdDuration.WithLabelValues("get_region")
	CmdDurationGetAllMembers = cmdDuration.WithLabelValues("get_member_info")
	CmdDurationGetPrevRegion = cmdDuration.WithLabelValues("get_prev_region")
	CmdDurationGetRegionByID = cmdDuration.WithLabelValues("get_region_byid")
	CmdDurationScanRegions = cmdDuration.WithLabelValues("scan_regions")
	CmdDurationBatchScanRegions = cmdDuration.WithLabelValues("batch_scan_regions")
	CmdDurationGetStore = cmdDuration.WithLabelValues("get_store")
	CmdDurationGetAllStores = cmdDuration.WithLabelValues("get_all_stores")
	CmdDurationUpdateGCSafePoint = cmdDuration.WithLabelValues("update_gc_safe_point")
	CmdDurationUpdateServiceGCSafePoint = cmdDuration.WithLabelValues("update_service_gc_safe_point")
	CmdDurationScatterRegion = cmdDuration.WithLabelValues("scatter_region")
	CmdDurationScatterRegions = cmdDuration.WithLabelValues("scatter_regions")
	CmdDurationGetOperator = cmdDuration.WithLabelValues("get_operator")
	CmdDurationSplitRegions = cmdDuration.WithLabelValues("split_regions")
	CmdDurationSplitAndScatterRegions = cmdDuration.WithLabelValues("split_and_scatter_regions")
	CmdDurationLoadKeyspace = cmdDuration.WithLabelValues("load_keyspace")
	CmdDurationUpdateKeyspaceState = cmdDuration.WithLabelValues("update_keyspace_state")
	CmdDurationGetAllKeyspaces = cmdDuration.WithLabelValues("get_all_keyspaces")
	CmdDurationGet = cmdDuration.WithLabelValues("get")
	CmdDurationPut = cmdDuration.WithLabelValues("put")
	CmdDurationUpdateGCSafePointV2 = cmdDuration.WithLabelValues("update_gc_safe_point_v2")
	CmdDurationUpdateServiceSafePointV2 = cmdDuration.WithLabelValues("update_service_safe_point_v2")

	CmdFailedDurationGetRegion = cmdFailedDuration.WithLabelValues("get_region")
	CmdFailedDurationTSOWait = cmdFailedDuration.WithLabelValues("wait")
	CmdFailedDurationTSO = cmdFailedDuration.WithLabelValues("tso")
	CmdFailedDurationGetAllMembers = cmdFailedDuration.WithLabelValues("get_member_info")
	CmdFailedDurationGetPrevRegion = cmdFailedDuration.WithLabelValues("get_prev_region")
	CmdFailedDurationGetRegionByID = cmdFailedDuration.WithLabelValues("get_region_byid")
	CmdFailedDurationScanRegions = cmdFailedDuration.WithLabelValues("scan_regions")
	CmdFailedDurationBatchScanRegions = cmdFailedDuration.WithLabelValues("batch_scan_regions")
	CmdFailedDurationGetStore = cmdFailedDuration.WithLabelValues("get_store")
	CmdFailedDurationGetAllStores = cmdFailedDuration.WithLabelValues("get_all_stores")
	CmdFailedDurationUpdateGCSafePoint = cmdFailedDuration.WithLabelValues("update_gc_safe_point")
	CmdFailedDurationUpdateServiceGCSafePoint = cmdFailedDuration.WithLabelValues("update_service_gc_safe_point")
	CmdFailedDurationLoadKeyspace = cmdFailedDuration.WithLabelValues("load_keyspace")
	CmdFailedDurationUpdateKeyspaceState = cmdFailedDuration.WithLabelValues("update_keyspace_state")
	CmdFailedDurationGet = cmdFailedDuration.WithLabelValues("get")
	CmdFailedDurationPut = cmdFailedDuration.WithLabelValues("put")
	CmdFailedDurationUpdateGCSafePointV2 = cmdFailedDuration.WithLabelValues("update_gc_safe_point_v2")
	CmdFailedDurationUpdateServiceSafePointV2 = cmdFailedDuration.WithLabelValues("update_service_safe_point_v2")

	InternalCmdDurationGetClusterInfo = internalCmdDuration.WithLabelValues("get_cluster_info")
	InternalCmdDurationGetMembers = internalCmdDuration.WithLabelValues("get_members")

	InternalCmdFailedDurationGetClusterInfo = internalCmdFailedDuration.WithLabelValues("get_cluster_info")
	InternalCmdFailedDurationGetMembers = internalCmdFailedDuration.WithLabelValues("get_members")

	RequestDurationTSO = requestDuration.WithLabelValues("tso")
	RequestFailedDurationTSO = requestDuration.WithLabelValues("tso-failed")
}

func registerMetrics() {
	prometheus.MustRegister(cmdDuration)
	prometheus.MustRegister(cmdFailedDuration)
	prometheus.MustRegister(internalCmdDuration)
	prometheus.MustRegister(internalCmdFailedDuration)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(TSOBestBatchSize)
	prometheus.MustRegister(TSOBatchSize)
	prometheus.MustRegister(TSOBatchSendLatency)
	prometheus.MustRegister(RequestForwarded)
	prometheus.MustRegister(EstimateTSOLatencyGauge)
	prometheus.MustRegister(CircuitBreakerCounters)
}
