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
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	metricsNamespace    = "pd"
	metricsSubsystem    = "lease"
	metricsLabelPurpose = "purpose"
	metricsLabelReason  = "reason"
	metricsLabelResult  = "result"
)

const (
	reasonInvalidTTL      = "invalid_ttl"
	reasonLeaseExpired    = "lease_expired"
	reasonContextCanceled = "context_canceled"

	metricsResultSuccess  = "success"
	metricsResultError    = "error"
	metricsResultCanceled = "canceled"

	// Per-Lease GaugeFunc registered at Grant time computes time.Until(expireTime)
	// on every scrape, so the metric reflects the actual decay of the lease
	// between renewals instead of being frozen at TTL.
	localTTLRemainingName = "local_ttl_remaining_seconds"
	localTTLRemainingHelp = "PD local estimate of remaining lease TTL, computed at scrape time as time.Until(expireTime), clamped to 0."
)

var (
	// keepAliveLatencyBuckets is shared by histograms that measure
	// etcd lease-operation latencies in the 10 ms–40 s range.
	keepAliveLatencyBuckets = prometheus.ExponentialBuckets(0.01, 2, 13)

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

	keepAliveRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "keepalive_request_duration_seconds",
			Help:      "Duration of etcd Lease.KeepAliveOnce requests observed by PD, by purpose and result.",
			Buckets:   keepAliveLatencyBuckets,
		},
		[]string{metricsLabelPurpose, metricsLabelResult},
	)

	keepAliveTickInterval = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "keepalive_tick_interval_seconds",
			Help:      "Interval between consecutive KeepAliveOnce call start times, observed inside each worker goroutine by purpose. Spikes indicate local scheduling delay (CPU starvation, GC pauses, goroutine pressure) and correlate with the `the interval between keeping alive lease is too long` warning.",
			Buckets:   keepAliveLatencyBuckets,
		},
		[]string{metricsLabelPurpose},
	)

	localTTLRemaining = newLocalTTLRemainingCollector()
)

func init() {
	prometheus.MustRegister(keepAliveResponseInterval)
	prometheus.MustRegister(renewalTerminationTotal)
	prometheus.MustRegister(localTTLRemaining)
	prometheus.MustRegister(keepAliveRequestDuration)
	prometheus.MustRegister(keepAliveTickInterval)
}

type leaseMetrics struct {
	responseInterval                prometheus.Observer
	keepAliveRequestSuccessLatency  prometheus.Observer
	keepAliveRequestErrorLatency    prometheus.Observer
	keepAliveRequestCanceledLatency prometheus.Observer
	tickInterval                    prometheus.Observer
	invalidTTL                      prometheus.Counter
	leaseExpired                    prometheus.Counter
	contextCanceled                 prometheus.Counter
}

func newLeaseMetrics(purpose string) leaseMetrics {
	return leaseMetrics{
		responseInterval:                keepAliveResponseInterval.WithLabelValues(purpose),
		keepAliveRequestSuccessLatency:  keepAliveRequestDuration.WithLabelValues(purpose, metricsResultSuccess),
		keepAliveRequestErrorLatency:    keepAliveRequestDuration.WithLabelValues(purpose, metricsResultError),
		keepAliveRequestCanceledLatency: keepAliveRequestDuration.WithLabelValues(purpose, metricsResultCanceled),
		tickInterval:                    keepAliveTickInterval.WithLabelValues(purpose),
		invalidTTL:                      renewalTerminationTotal.WithLabelValues(purpose, reasonInvalidTTL),
		leaseExpired:                    renewalTerminationTotal.WithLabelValues(purpose, reasonLeaseExpired),
		contextCanceled:                 renewalTerminationTotal.WithLabelValues(purpose, reasonContextCanceled),
	}
}

// localTTLRemainingCollector is a custom Prometheus collector that emits the
// remaining lease TTL per lease purpose at scrape time. Computing the value
// lazily means the curve naturally decays between renewals instead of being
// frozen at the configured TTL.
type localTTLRemainingCollector struct {
	desc   *prometheus.Desc
	mu     sync.Mutex
	leases map[string]*Lease // purpose -> active Lease
}

func newLocalTTLRemainingCollector() *localTTLRemainingCollector {
	return &localTTLRemainingCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, metricsSubsystem, localTTLRemainingName),
			localTTLRemainingHelp,
			[]string{metricsLabelPurpose},
			nil,
		),
		leases: make(map[string]*Lease),
	}
}

func (c *localTTLRemainingCollector) register(l *Lease) {
	c.mu.Lock()
	existing, ok := c.leases[l.purpose]
	if !ok {
		c.leases[l.purpose] = l
	}
	c.mu.Unlock()

	leaseID := int64(l.GetID())
	if ok {
		log.Warn("skipped registering a duplicate lease to the metrics collector",
			zap.String("purpose", l.purpose),
			zap.Int64("existing-lease-id", int64(existing.GetID())),
			zap.Int64("new-lease-id", leaseID))
	} else {
		log.Info("registered a new lease to the metrics collector",
			zap.String("purpose", l.purpose),
			zap.Int64("lease-id", leaseID))
	}
}

// unregister removes l only if it is the current registered lease for that
// purpose. This guards against the race where a new Lease has already taken
// over the slot before the old one's Close() runs.
func (c *localTTLRemainingCollector) unregister(l *Lease) {
	var deleted bool
	c.mu.Lock()
	if c.leases[l.purpose] == l {
		deleted = true
		delete(c.leases, l.purpose)
	}
	existing, ok := c.leases[l.purpose]
	c.mu.Unlock()
	leaseID := int64(l.GetID())
	if deleted {
		log.Info("unregistered a lease from the metrics collector",
			zap.String("purpose", l.purpose),
			zap.Int64("lease-id", leaseID))
	} else if ok {
		log.Warn("skipped unregistering a different lease from the metrics collector",
			zap.String("purpose", l.purpose),
			zap.Int64("existing-lease-id", int64(existing.GetID())),
			zap.Int64("lease-id", leaseID))
	} else {
		log.Warn("skipped unregistering a non-existent lease from the metrics collector",
			zap.String("purpose", l.purpose),
			zap.Int64("lease-id", leaseID))
	}
}

// Describe implements prometheus.Collector.
func (c *localTTLRemainingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector.
func (c *localTTLRemainingCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	snapshot := make(map[string]*Lease, len(c.leases))
	maps.Copy(snapshot, c.leases)
	c.mu.Unlock()
	now := time.Now()
	for purpose, l := range snapshot {
		remaining := l.loadExpireTime().Sub(now).Seconds()
		if remaining < 0 {
			remaining = 0
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, remaining, purpose)
	}
}

func (m leaseMetrics) observeKeepAliveRequestDurationMetrics(duration time.Duration, err error) {
	if err == nil {
		m.keepAliveRequestSuccessLatency.Observe(duration.Seconds())
		return
	}
	if errors.Is(err, context.Canceled) {
		m.keepAliveRequestCanceledLatency.Observe(duration.Seconds())
		return
	}
	m.keepAliveRequestErrorLatency.Observe(duration.Seconds())
}
