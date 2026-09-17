// Copyright 2025 TiKV Project Authors.
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

package controller

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

// ruMaxSampler owns both the sampling state and all peak GaugeVec
// operations. No peak metric lookup or deletion runs on the request or token
// loop paths, where a concurrent Prometheus collection could block it.
// Like the other controller metrics, the labels assume one active controller
// per process.
type ruMaxSampler map[string]*ruMaxGroup

type ruMaxGroup struct {
	gc      *groupCostController
	tracker ruMaxTracker
	seen    bool
}

func (c *ResourceGroupsController) runRUMaxSampler(ctx context.Context) {
	defer c.wg.Done()
	logger := log.L().With(zap.Uint64("client-id", c.clientUniqueID), zap.Uint32("keyspace-id", c.keyspaceID))
	sampler := make(ruMaxSampler)
	defer func() {
		start := time.Now()
		sampler.clear()
		logger.Info("[resource group controller] RU max sampler stopped", zap.Duration("cleanup-duration", time.Since(start)))
	}()
	ticker := time.NewTicker(defaultGroupStateUpdateInterval)
	defer ticker.Stop()
	logger.Info("[resource group controller] RU max sampler started",
		zap.Duration("sample-interval", defaultGroupStateUpdateInterval), zap.Duration("window", ruMaxWindow))
	for {
		select {
		case <-ctx.Done():
			logger.Info("[resource group controller] RU max sampler stopping",
				zap.Error(ctx.Err()), zap.Int("tracked-groups", len(sampler)))
			return
		case <-ticker.C:
			sampler.sample(c)
		}
	}
}

func (s ruMaxSampler) sample(c *ResourceGroupsController) {
	c.groupsController.Range(func(key, value any) bool {
		// Use the cache key: a tombstone uses default's config but still belongs
		// to the original resource group.
		name, gc := key.(string), value.(*groupCostController)
		state := s[name]
		if state == nil {
			state = &ruMaxGroup{gc: gc, tracker: newRUMaxTracker(name, gc.createdAt)}
			s[name] = state
		} else if state.gc != gc {
			// Reuse the live gauges, but start a new window and zero consumption
			// baseline for this controller, including requests before this sweep.
			state.gc = gc
			state.tracker.base = gc.createdAt
			state.tracker.last = 0
			state.tracker.prevRRU, state.tracker.prevWRU = 0, 0
			state.tracker.samples = state.tracker.samples[:0]
		}
		state.seen = true
		// Take the time inside the consumption lock so lock waiting cannot skew
		// the interval. Window scans and metric publication stay outside it.
		gc.mu.Lock()
		now := time.Now()
		rru, wru := gc.mu.consumption.RRU, gc.mu.consumption.WRU
		gc.mu.Unlock()
		state.tracker.observe(rru, wru, now)
		return true
	})
	// Range is not a snapshot. Concurrent cache changes converge in later
	// sweeps; runtime deletion is asynchronous, with no fixed latency bound.
	for name, state := range s {
		if !state.seen {
			deleteRUMaxMetricLabels(name)
			delete(s, name)
		} else {
			state.seen = false
		}
	}
}

func (s ruMaxSampler) clear() {
	for name := range s {
		deleteRUMaxMetricLabels(name)
		delete(s, name)
	}
}

const ruMaxWindow = 60 * time.Second

type ruRateSample struct {
	at       time.Duration
	rru, wru float64
}

// ruMaxTracker publishes the maximum sampled rate whose sampling interval
// ended within the last 60 seconds. All times are relative to the controller
// creation time, preserving the monotonic clock while keeping samples compact.
type ruMaxTracker struct {
	rruGauge, wruGauge, ruGauge prometheus.Gauge
	base                        time.Time
	last                        time.Duration
	prevRRU, prevWRU            float64
	samples                     []ruRateSample
}

func newRUMaxTracker(name string, now time.Time) ruMaxTracker {
	return ruMaxTracker{
		rruGauge: metrics.RUMaxPerSecGauge.WithLabelValues(name, requestSourceRUTypeRRU),
		wruGauge: metrics.RUMaxPerSecGauge.WithLabelValues(name, requestSourceRUTypeWRU),
		ruGauge:  metrics.RUMaxPerSecGauge.WithLabelValues(name, ruTypeTotal),
		base:     now,
	}
}

func (t *ruMaxTracker) observe(curRRU, curWRU float64, now time.Time) {
	elapsed := now.Sub(t.base)
	duration := elapsed - t.last
	if duration <= 0 {
		return
	}
	// Creation can fall just before a tick. Normalize the first sample over at
	// least one sampling interval so its peak does not depend on that phase.
	// Keep the actual timestamp below for subsequent deltas and window expiry.
	if t.last == 0 && duration < defaultGroupStateUpdateInterval {
		duration = defaultGroupStateUpdateInterval
	}
	// Failed requests can roll back consumption. Clamp each type before summing
	// so a negative net increment cannot cancel the other type's consumption.
	rru := math.Max(0, curRRU-t.prevRRU) / duration.Seconds()
	wru := math.Max(0, curWRU-t.prevWRU) / duration.Seconds()
	t.last, t.prevRRU, t.prevWRU = elapsed, curRRU, curWRU

	maxRRU, maxWRU, maxRU := rru, wru, rru+wru
	kept := t.samples[:0]
	for _, sample := range t.samples {
		if elapsed-sample.at >= ruMaxWindow {
			continue
		}
		kept = append(kept, sample)
		maxRRU = math.Max(maxRRU, sample.rru)
		maxWRU = math.Max(maxWRU, sample.wru)
		maxRU = math.Max(maxRU, sample.rru+sample.wru)
	}
	// Zero samples cannot raise a maximum, but idle ticks still expire samples
	// and publish zero once the window empties. Reuse the storage across ticks.
	if rru > 0 || wru > 0 {
		kept = append(kept, ruRateSample{at: elapsed, rru: rru, wru: wru})
	}
	t.samples = kept
	t.rruGauge.Set(maxRRU)
	t.wruGauge.Set(maxWRU)
	t.ruGauge.Set(maxRU)
}

func deleteRUMaxMetricLabels(name string) {
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, requestSourceRUTypeRRU)
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, requestSourceRUTypeWRU)
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, ruTypeTotal)
}
