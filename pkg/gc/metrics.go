// Copyright 2023 TiKV Project Authors.
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

package gc

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Metrics and warnings share the same minimum barrier timestamp age.
	barrierObservationMinimumAge = 24 * time.Hour
	barrierWarningInterval       = 10 * time.Minute
)

// The registry owns one forwarding collector, never a list of GC managers.
// Its pointer is attached only while a production manager is leader.
type activeBarrierMetrics struct {
	current atomic.Pointer[barrierMetrics]
}

// Describe implements prometheus.Collector.
func (*activeBarrierMetrics) Describe(ch chan<- *prometheus.Desc) { ch <- barrierTimestampDesc }

// Collect forwards to the currently active leader without retaining old managers.
func (c *activeBarrierMetrics) Collect(ch chan<- prometheus.Metric) {
	if current := c.current.Load(); current != nil {
		current.Collect(ch)
	}
}

var (
	barrierTimestampDesc = prometheus.NewDesc(
		"pd_gc_barrier_timestamp_seconds",
		"Physical Unix timestamp of a valid GC barrier more than 24 hours old, observed by successful transaction safe point advancement.",
		[]string{"scope", "keyspace_id", "keyspace_name", "barrier_id"}, nil,
	)
	productionBarrierMetrics = &activeBarrierMetrics{}
	gcSafePointGauge         = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "gc",
			Name:      "gc_safepoint",
			Help:      "The ts of gc safepoint",
		}, []string{"type"})
	gcStateCacheAccessCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "gc",
			Name:      "gc_state_cache_access_total",
			Help:      "Counter of GC state cache accesses by result.",
		}, []string{"result"})

	gcStateCacheAccessHitCounter     = gcStateCacheAccessCounter.WithLabelValues("hit")
	gcStateCacheAccessSlowHitCounter = gcStateCacheAccessCounter.WithLabelValues("slow_hit")
	gcStateCacheAccessMissCounter    = gcStateCacheAccessCounter.WithLabelValues("miss")
)

func init() {
	prometheus.MustRegister(productionBarrierMetrics)
	prometheus.MustRegister(gcSafePointGauge)
	prometheus.MustRegister(gcStateCacheAccessCounter)
}
