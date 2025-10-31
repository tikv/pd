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

package server

import (
	"math/rand"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
)

var (
	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})
	bucketReportCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "bucket_report",
			Help:      "Counter of bucket report.",
		}, []string{"address", "store", "type", "status"})
	regionHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat",
			Help:      "Counter of region heartbeat.",
		}, []string{"address", "store", "type", "status"})

	regionHeartbeatLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat_latency_seconds",
			Help:      "Bucketed histogram of latency (s) of receiving heartbeat.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"address", "store"})

	metadataGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "metadata",
			Help:      "Record critical metadata.",
		}, []string{"type"})

	etcdStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "etcd_state",
			Help:      "Etcd raft states.",
		}, []string{"type"})

	tsoProxyHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_proxy_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	tsoProxyBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_proxy_batch_size",
			Help:      "Bucketed histogram of the batch size of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		})

	tsoProxyForwardTimeoutCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "tso_proxy_forward_timeout_total",
			Help:      "Counter of timeouts when tso proxy forwarding tso requests to tso service.",
		})

	tsoProxyChannelLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "tso_proxy_channel_latency_seconds",
			Help:      "Bucketed histogram of latency (s) of send/recv TSO from the tso dispatcher channel.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 104s
		})

	tsoHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	queryRegionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "query_region_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of region query requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	bucketReportLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_bucket_report_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled bucket report requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	bucketReportInterval = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "bucket_report_interval_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled bucket report requests.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20), // 1s ~ 17m
		}, []string{"address", "store"})

	regionHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "handle_region_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled region heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	// TODO: pre-allocate gauge metrics
	storeHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "handle_store_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled store heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	serviceAuditHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds",
			Help:      "PD server service handling performance metrics",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method"})

	serviceAuditCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_requests_total",
			Help:      "Total number of service requests for audit",
		}, []string{"service", "method", "caller_component"})

	apiConcurrencyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "api_concurrency",
			Help:      "Concurrency number of the api.",
		}, []string{"kind", "api"})

	forwardFailCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "forward_fail_total",
			Help:      "Counter of forward fail.",
		}, []string{"request", "type"})
	forwardTsoDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "forward_tso_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled forward tso requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	regionRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "region_request_cnt",
			Help:      "Counter of region request.",
		}, []string{"request", "caller_id", "caller_component", "event"})
)

func init() {
	prometheus.MustRegister(timeJumpBackCounter)
	prometheus.MustRegister(regionHeartbeatCounter)
	prometheus.MustRegister(regionHeartbeatLatency)
	prometheus.MustRegister(metadataGauge)
	prometheus.MustRegister(etcdStateGauge)
	prometheus.MustRegister(tsoProxyHandleDuration)
	prometheus.MustRegister(tsoProxyBatchSize)
	prometheus.MustRegister(tsoProxyForwardTimeoutCounter)
	prometheus.MustRegister(tsoProxyChannelLatency)
	prometheus.MustRegister(tsoHandleDuration)
	prometheus.MustRegister(queryRegionDuration)
	prometheus.MustRegister(regionHeartbeatHandleDuration)
	prometheus.MustRegister(storeHeartbeatHandleDuration)
	prometheus.MustRegister(bucketReportCounter)
	prometheus.MustRegister(bucketReportLatency)
	prometheus.MustRegister(serviceAuditHistogram)
	prometheus.MustRegister(serviceAuditCounter)
	prometheus.MustRegister(bucketReportInterval)
	prometheus.MustRegister(apiConcurrencyGauge)
	prometheus.MustRegister(forwardFailCounter)
	prometheus.MustRegister(forwardTsoDuration)
	prometheus.MustRegister(regionRequestCounter)
}

type requestEvent string

const (
	requestSuccess requestEvent = "success"
	requestFailed  requestEvent = "failed"
)

func incRegionRequestCounter(method string, header *pdpb.RequestHeader, err *pdpb.Error) {
	if err == nil && rand.Intn(100) != 0 {
		// sample 1% region requests to avoid high cardinality
		return
	}

	var (
		event           = requestSuccess
		callerID        = header.CallerId
		callerComponent = header.CallerComponent
	)
	if err != nil {
		log.Warn("region request encounter error",
			zap.String("method", method),
			zap.String("caller_id", callerID),
			zap.String("caller_component", callerComponent),
			zap.Stringer("error", err))
		event = requestFailed
	}
	if callerID == "" {
		callerID = "unknown"
	}
	if callerComponent == "" {
		callerComponent = "unknown"
	}
	regionRequestCounter.WithLabelValues(method, callerID, callerComponent, string(event)).Inc()
}
