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

package election

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace    = "pd"
	metricsSubsystem    = "lease"
	metricsLabelPurpose = "purpose"
	metricsLabelReason  = "reason"
)

const (
	reasonInvalidTTL      = "invalid_ttl"
	reasonLeaseExpired    = "lease_expired"
	reasonContextCanceled = "context_canceled"
)

var (
	keepAliveResponseInterval = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "keepalive_response_interval_seconds",
			Help:      "Interval between consecutive valid lease keepalive responses received from etcd, by purpose.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 16),
		},
		[]string{metricsLabelPurpose},
	)

	renewalTerminationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "renewal_termination_total",
			Help:      "Number of lease renewal stop or anomaly events, broken down by purpose and reason. Reason `context_canceled` is benign (caller-initiated shutdown); `lease_expired` indicates keepalive timeout; `invalid_ttl` indicates a successful keepalive response with non-positive TTL.",
		},
		[]string{metricsLabelPurpose, metricsLabelReason},
	)

	localTTLRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "local_ttl_remaining_seconds",
			Help:      "PD local estimate of remaining lease TTL, updated on every keepalive response and on keepalive timeout firing; not etcd authoritative TTL.",
		},
		[]string{metricsLabelPurpose},
	)
)

func init() {
	prometheus.MustRegister(keepAliveResponseInterval)
	prometheus.MustRegister(renewalTerminationTotal)
	prometheus.MustRegister(localTTLRemaining)
}

type leaseMetrics struct {
	responseInterval prometheus.Observer
	ttlRemaining     prometheus.Gauge
	invalidTTL       prometheus.Counter
	leaseExpired     prometheus.Counter
	contextCanceled  prometheus.Counter
}

func newLeaseMetrics(purpose string) leaseMetrics {
	return leaseMetrics{
		responseInterval: keepAliveResponseInterval.WithLabelValues(purpose),
		ttlRemaining:     localTTLRemaining.WithLabelValues(purpose),
		invalidTTL:       renewalTerminationTotal.WithLabelValues(purpose, reasonInvalidTTL),
		leaseExpired:     renewalTerminationTotal.WithLabelValues(purpose, reasonLeaseExpired),
		contextCanceled:  renewalTerminationTotal.WithLabelValues(purpose, reasonContextCanceled),
	}
}

// observeRemainingTTL records the lease's remaining TTL into the gauge,
// clamping non-positive durations (already expired) to 0.
func (m leaseMetrics) observeRemainingTTL(remaining time.Duration) {
	seconds := remaining.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	m.ttlRemaining.Set(seconds)
}
