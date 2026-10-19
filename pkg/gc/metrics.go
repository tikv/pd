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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
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
)

func init() {
	prometheus.MustRegister(productionBarrierMetrics)
	prometheus.MustRegister(gcSafePointGauge)
}

type barrierMetricScope struct {
	keyspaceID uint32
	global     bool
}

func (s barrierMetricScope) labels() (scope, keyspaceID string) {
	if s.global {
		return "global", ""
	}
	return "keyspace", strconv.FormatUint(uint64(s.keyspaceID), 10)
}

type barrierMetricEntry struct {
	keyspaceName string
	barrier      endpoint.GCBarrier
	lastWarning  time.Time
	metric       prometheus.Metric
}

// barrierMetrics retains only reported barriers. Its lock protects publication,
// scrape-time expiry and lifecycle invalidation; scrapes never access storage.
// A single generation fences in-flight reads without retaining keyspace IDs
// after removal. An unrelated keyspace removal may defer an observation until
// the next successful advancement request.
type barrierMetrics struct {
	mu      syncutil.Mutex
	epoch   uint64
	entries map[barrierMetricScope]map[string]barrierMetricEntry
	now     func() time.Time
}

func newBarrierMetrics(now func() time.Time) *barrierMetrics {
	return &barrierMetrics{entries: make(map[barrierMetricScope]map[string]barrierMetricEntry), now: now}
}

func (m *barrierMetrics) generation() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch
}

func (m *barrierMetrics) clearMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	clear(m.entries)
}

func (m *barrierMetrics) invalidateKeyspaceMetrics(keyspaceID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	delete(m.entries, barrierMetricScope{keyspaceID: keyspaceID})
}

// Describe implements prometheus.Collector.
func (*barrierMetrics) Describe(ch chan<- *prometheus.Desc) { ch <- barrierTimestampDesc }

// Collect implements prometheus.Collector with expiry filtering and no storage IO.
func (m *barrierMetrics) Collect(ch chan<- prometheus.Metric) {
	now := m.now()
	m.mu.Lock()
	var snapshot []prometheus.Metric
	for scope, entries := range m.entries {
		for id, entry := range entries {
			if entry.barrier.IsExpired(now) {
				delete(entries, id)
				continue
			}
			snapshot = append(snapshot, entry.metric)
		}
		if len(entries) == 0 {
			delete(m.entries, scope)
		}
	}
	m.mu.Unlock()
	// Never hold the observation lock while waiting for a scraper.
	for _, metric := range snapshot {
		ch <- metric
	}
}

type barrierWarning struct {
	scope   barrierMetricScope
	barrier endpoint.GCBarrier
	now     time.Time
}

func (w barrierWarning) log() {
	scope, keyspaceID := w.scope.labels()
	physical, _ := tsoutil.ParseTS(w.barrier.BarrierTS)
	expiration := "never"
	if w.barrier.ExpirationTime != nil {
		expiration = w.barrier.ExpirationTime.UTC().Format(time.RFC3339Nano)
	}
	log.Warn("GC barrier timestamp is too old",
		zap.String("scope", scope), zap.String("keyspace-id", keyspaceID),
		zap.String("barrier-id", w.barrier.BarrierID), zap.Uint64("barrier-ts", w.barrier.BarrierTS),
		zap.Time("barrier-time", physical), zap.Duration("lag", w.now.Sub(physical)), zap.String("expiration-time", expiration))
}

func (*barrierMetrics) entryMetrics(scope barrierMetricScope, keyspaceName string, barrier *endpoint.GCBarrier, previous barrierMetricEntry, now time.Time) (barrierMetricEntry, bool) {
	physical, _ := tsoutil.ParseTS(barrier.BarrierTS)
	if (!scope.global && barrier.BarrierID == keypath.GCWorkerServiceSafePointID) || barrier.IsExpired(now) || now.Sub(physical) <= barrierObservationMinimumAge {
		return barrierMetricEntry{}, false
	}
	if scope.global || scope.keyspaceID == constant.NullKeyspaceID {
		keyspaceName = ""
	}
	entry := barrierMetricEntry{keyspaceName: keyspaceName, barrier: *barrier, lastWarning: previous.lastWarning}
	if previous.barrier.IsExpired(now) {
		entry.lastWarning = time.Time{}
	}
	if barrier.ExpirationTime != nil {
		expiration := *barrier.ExpirationTime
		entry.barrier.ExpirationTime = &expiration
	}
	scopeLabel, keyspaceLabel := scope.labels()
	entry.metric = prometheus.MustNewConstMetric(barrierTimestampDesc, prometheus.GaugeValue, float64(physical.UnixMilli())/1000, scopeLabel, keyspaceLabel, entry.keyspaceName, barrier.BarrierID)
	return entry, true
}

func (m *barrierMetrics) observeMetrics(generation uint64, keyspaceID uint32, keyspaceName string, barriers []*endpoint.GCBarrier, globals []*endpoint.GlobalGCBarrier, now time.Time) []barrierWarning {
	m.mu.Lock()
	defer m.mu.Unlock()
	if generation != m.epoch {
		return nil
	}
	var warnings []barrierWarning
	observe := func(scope barrierMetricScope, visit func(func(*endpoint.GCBarrier))) {
		previous := m.entries[scope]
		var next map[string]barrierMetricEntry
		visit(func(barrier *endpoint.GCBarrier) {
			entry, ok := m.entryMetrics(scope, keyspaceName, barrier, previous[barrier.BarrierID], now)
			if !ok {
				return
			}
			if entry.lastWarning.IsZero() || now.Sub(entry.lastWarning) >= barrierWarningInterval {
				entry.lastWarning = now
				warnings = append(warnings, barrierWarning{scope: scope, barrier: entry.barrier, now: now})
			}
			if next == nil {
				next = make(map[string]barrierMetricEntry)
			}
			next[barrier.BarrierID] = entry
		})
		if len(next) == 0 {
			delete(m.entries, scope)
		} else {
			m.entries[scope] = next
		}
	}
	observe(barrierMetricScope{keyspaceID: keyspaceID}, func(accept func(*endpoint.GCBarrier)) {
		for _, barrier := range barriers {
			accept(barrier)
		}
	})
	observe(barrierMetricScope{global: true}, func(accept func(*endpoint.GCBarrier)) {
		for _, barrier := range globals {
			accept(&endpoint.GCBarrier{BarrierID: barrier.BarrierID, BarrierTS: barrier.BarrierTS, ExpirationTime: barrier.ExpirationTime.Time})
		}
	})
	return warnings
}

// updateMetrics updates an existing observation after a successful write. Writes
// cannot discover barriers or emit warnings; discovery belongs to advancement.
func (m *barrierMetrics) updateMetrics(scope barrierMetricScope, barrier *endpoint.GCBarrier, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[scope]
	previous, ok := entries[barrier.BarrierID]
	if !ok {
		return
	}
	if previous.barrier.IsExpired(now) {
		delete(entries, barrier.BarrierID)
	} else if entry, valid := m.entryMetrics(scope, previous.keyspaceName, barrier, previous, now); valid {
		entries[barrier.BarrierID] = entry
	} else {
		delete(entries, barrier.BarrierID)
	}
	if len(entries) == 0 {
		delete(m.entries, scope)
	}
}

func (m *barrierMetrics) deleteMetrics(scope barrierMetricScope, barrierID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[scope]
	delete(entries, barrierID)
	if len(entries) == 0 {
		delete(m.entries, scope)
	}
}

// barrierObservation travels with the existing diagnostic context. Warnings are
// selected under serialization, then emitted after the GC manager unlocks.
type barrierObservationKey struct{}
type barrierObservation struct {
	generation uint64
	warnings   []barrierWarning
}

func (o *barrierObservation) logWarnings() {
	for _, warning := range o.warnings {
		warning.log()
	}
}

// CloseBarrierMetrics releases production collection during server shutdown.
// Call it after leadership callbacks have stopped.
// Cleanup of an old manager cannot detach a replacement leader's collector.
func (m *GCStateManager) CloseBarrierMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()
	productionBarrierMetrics.current.CompareAndSwap(m.barrierMetrics, nil)
	m.barrierMetrics.clearMetrics()
}
