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

	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

// ruMaxPerSecSampler owns both the sampling state and all peak GaugeVec
// operations. No peak metric lookup or deletion runs on the request or token
// loop paths, where a concurrent Prometheus collection could block it.
// Like the other controller metrics, the labels assume one active controller
// per process.
type ruMaxPerSecSampler map[string]*ruMaxPerSecGroup

type ruMaxPerSecGroup struct {
	gc      *groupCostController
	tracker ruMaxPerSecTracker
	seen    bool
}

func (c *ResourceGroupsController) runRUMaxPerSecMetrics(ctx context.Context) {
	defer c.wg.Done()
	sampler := make(ruMaxPerSecSampler)
	defer sampler.clear()
	ticker := time.NewTicker(defaultGroupStateUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sampler.sample(c)
		}
	}
}

func (s ruMaxPerSecSampler) sample(c *ResourceGroupsController) {
	c.groupsController.Range(func(key, value any) bool {
		// Use the cache key: a tombstone uses default's config but still belongs
		// to the original resource group.
		name, gc := key.(string), value.(*groupCostController)
		state := s[name]
		if state == nil {
			state = &ruMaxPerSecGroup{gc: gc, tracker: newRUMaxPerSecTracker(name, gc.createdAt)}
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
			deleteRUMaxPerSecMetricLabels(name)
			delete(s, name)
		} else {
			state.seen = false
		}
	}
}

func (s ruMaxPerSecSampler) clear() {
	for name := range s {
		deleteRUMaxPerSecMetricLabels(name)
		delete(s, name)
	}
}

const ruMaxPerSecWindow = 60 * time.Second

type ruRateSample struct {
	at       time.Duration
	rru, wru float64
}

// ruMaxPerSecTracker publishes the maximum sampled rate whose sampling interval
// ended within the last 60 seconds. All times are relative to the controller
// creation time, preserving the monotonic clock while keeping samples compact.
type ruMaxPerSecTracker struct {
	rruGauge, wruGauge, ruGauge prometheus.Gauge
	base                        time.Time
	last                        time.Duration
	prevRRU, prevWRU            float64
	samples                     []ruRateSample
}

func newRUMaxPerSecTracker(name string, now time.Time) ruMaxPerSecTracker {
	return ruMaxPerSecTracker{
		rruGauge: metrics.RUMaxPerSecGauge.WithLabelValues(name, requestSourceRUTypeRRU),
		wruGauge: metrics.RUMaxPerSecGauge.WithLabelValues(name, requestSourceRUTypeWRU),
		ruGauge:  metrics.RUMaxPerSecGauge.WithLabelValues(name, ruTypeTotal),
		base:     now,
	}
}

func (t *ruMaxPerSecTracker) observe(curRRU, curWRU float64, now time.Time) {
	elapsed := now.Sub(t.base)
	duration := elapsed - t.last
	if duration <= 0 {
		return
	}
	// Failed requests can roll back consumption. Clamp each type before summing
	// so a negative net increment cannot cancel the other type's consumption.
	rru := math.Max(0, curRRU-t.prevRRU) / duration.Seconds()
	wru := math.Max(0, curWRU-t.prevWRU) / duration.Seconds()
	t.last, t.prevRRU, t.prevWRU = elapsed, curRRU, curWRU

	maxRRU, maxWRU, maxRU := rru, wru, rru+wru
	kept := t.samples[:0]
	for _, sample := range t.samples {
		if elapsed-sample.at >= ruMaxPerSecWindow {
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

func deleteRUMaxPerSecMetricLabels(name string) {
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, requestSourceRUTypeRRU)
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, requestSourceRUTypeWRU)
	metrics.RUMaxPerSecGauge.DeleteLabelValues(name, ruTypeTotal)
}
