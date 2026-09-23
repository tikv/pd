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

package server

import (
	"maps"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// ruTimelineSeconds is the number of seconds each source can replay.
	ruTimelineSeconds = 180
	// ruWindowSeconds is the aligned window summarized from the timeline.
	ruWindowSeconds = 60
	// ruWindowDelay is how long a closed window waits for regular reports.
	ruWindowDelay        = 30
	ruTimelineMaxSources = 10000
	ruTimelineMaxGroups  = 4096
)

// ruWindowSummary describes the busiest second of a closed window.
type ruWindowSummary struct {
	end            int64
	peak, rru, wru float64
	peakAt         int64
	available      bool
}

type publishedRUSummary struct {
	keyspaceName string
	summary      ruWindowSummary
}

// ruSummaryCollector exposes immutable, explicitly timestamped window summaries.
// Scrapes never advance windows, clear timelines, or perform aggregation.
type ruSummaryCollector struct {
	mu                                sync.RWMutex
	results                           map[trackerKey]publishedRUSummary
	peak, available, peakAt, rru, wru *prometheus.Desc
}

func newRUSummaryCollector() *ruSummaryCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("resource_manager_resource_unit_"+name, help, []string{keyspaceNameLabel, newResourceGroupNameLabel}, nil)
	}
	return &ruSummaryCollector{
		results:   make(map[trackerKey]publishedRUSummary),
		peak:      desc("peak_per_second", "Maximum cluster net RRU+WRU in a natural second of the completed minute, in RU/s."),
		available: desc("peak_available", "Whether the completed minute passed observed source coverage and known data quality checks."),
		peakAt:    desc("peak_second_timestamp_seconds", "Unix timestamp of the earliest second attaining the minute peak."),
		rru:       desc("peak_rru_per_second", "RRU contribution in the second attaining the total minute peak."),
		wru:       desc("peak_wru_per_second", "WRU contribution in the second attaining the total minute peak."),
	}
}

// Describe implements prometheus.Collector.
func (c *ruSummaryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.peak, c.available, c.peakAt, c.rru, c.wru} {
		ch <- desc
	}
}

// Collect implements prometheus.Collector.
func (c *ruSummaryCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for key, p := range c.results {
		emit := func(desc *prometheus.Desc, value float64) {
			ch <- prometheus.NewMetricWithTimestamp(time.Unix(p.summary.end, 0), prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, p.keyspaceName, key.groupName))
		}
		if !p.summary.available {
			emit(c.available, 0)
			continue
		}
		emit(c.available, 1)
		emit(c.peak, p.summary.peak)
		emit(c.peakAt, float64(p.summary.peakAt))
		emit(c.rru, p.summary.rru)
		emit(c.wru, p.summary.wru)
	}
}

var (
	ruSummaryMetrics        = newRUSummaryCollector()
	ruTimelineQualityEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "resource_manager", Subsystem: "resource_unit", Name: "peak_quality_events_total",
		Help: "RU timeline data quality events, including violations discovered after publication.",
	}, []string{"reason"})
	ruTimelineInvalid  = ruTimelineQualityEvents.WithLabelValues("invalid_payload")
	ruTimelineConflict = ruTimelineQualityEvents.WithLabelValues("conflict")
	ruTimelineLate     = ruTimelineQualityEvents.WithLabelValues("late")
	ruTimelineCapacity = ruTimelineQualityEvents.WithLabelValues("capacity")
	ruTimelineClock    = ruTimelineQualityEvents.WithLabelValues("clock")
)

func init() { prometheus.MustRegister(ruSummaryMetrics, ruTimelineQualityEvents) }

type ruSourceKey struct {
	client                  uint64
	isBackground, isTiFlash bool
}

type ruSecondBucket struct {
	second   int64
	rru, wru float64
}

type ruSource struct {
	buckets         [ruTimelineSeconds + 1]ruSecondBucket
	first, lastSeen int64
}

type ruTimelineGroup struct {
	keyspaceName string
	sources      map[ruSourceKey]*ruSource
	nextWindow   int64
	invalid      map[int64]bool
}

// ruTimeline merges the RU timelines replayed by clients into
// per-group timelines and publishes a summary for each closed window.
// All state belongs to backgroundMetricsFlush. Only immutable summaries cross
// into the scrape goroutines under the collector lock.
type ruTimeline struct {
	collector   *ruSummaryCollector
	groups      map[trackerKey]*ruTimelineGroup
	sourceCount int
	last        time.Time
}

func newRUTimeline(c *ruSummaryCollector, now time.Time) *ruTimeline {
	t := &ruTimeline{collector: c}
	t.reset(now)
	return t
}

func windowStart(second int64) int64 {
	return second / ruWindowSeconds * ruWindowSeconds
}

func (t *ruTimeline) reset(now time.Time) {
	t.collector.mu.Lock()
	clear(t.collector.results)
	t.collector.mu.Unlock()
	t.groups = make(map[trackerKey]*ruTimelineGroup)
	t.sourceCount = 0
	t.last = now
}

func (t *ruTimeline) remove(key trackerKey) {
	if g := t.groups[key]; g != nil {
		t.sourceCount -= len(g.sources)
		delete(t.groups, key)
	}
	t.collector.mu.Lock()
	delete(t.collector.results, key)
	t.collector.mu.Unlock()
}

func (g *ruTimelineGroup) invalidate(start, end int64) {
	for w := max(windowStart(start), g.nextWindow); w <= windowStart(end); w += ruWindowSeconds {
		g.invalid[w] = true
	}
}

// advance resets the timeline after a server clock discontinuity.
func (t *ruTimeline) advance(now time.Time) {
	if !t.last.IsZero() {
		wall := float64(now.UnixNano()-t.last.UnixNano()) / 1e9
		if now.Unix() < t.last.Unix() || math.Abs(wall-now.Sub(t.last).Seconds()) >= 1 {
			ruTimelineClock.Inc()
			t.reset(now)
		}
	}
	t.last = now
}

func (t *ruTimeline) record(item *consumptionItem, now time.Time) {
	t.advance(now)
	sec := now.Unix()
	key := trackerKey{item.keyspaceID, item.resourceGroupName}
	g := t.groups[key]
	if g == nil {
		if len(t.groups) >= ruTimelineMaxGroups {
			ruTimelineCapacity.Inc()
			return
		}
		g = &ruTimelineGroup{keyspaceName: item.keyspaceName, sources: make(map[ruSourceKey]*ruSource), nextWindow: windowStart(sec), invalid: make(map[int64]bool)}
		t.groups[key] = g
		// A newly observed group cannot establish coverage before its first report.
		g.invalid[g.nextWindow] = true
	}
	sourceKey := ruSourceKey{item.clientUniqueID, item.isBackground, item.isTiFlash}
	source := g.sources[sourceKey]
	if source == nil {
		if t.sourceCount >= ruTimelineMaxSources {
			// The rejected source replays up to a full retained window.
			ruTimelineCapacity.Inc()
			g.invalidate(sec-ruTimelineSeconds, sec)
			return
		}
		source = &ruSource{first: sec}
		g.sources[sourceKey] = source
		t.sourceCount++
	}
	source.lastSeen = sec
	payload := item.GetRuBySecond()
	// Validate atomically: a malformed suffix must not leave a usable prefix.
	// Buckets must be closed seconds; those older than retention are skipped below.
	valid := payload != nil && item.clientUniqueID != 0 && len(payload.Buckets) <= ruTimelineSeconds &&
		int64(len(payload.Buckets)) <= sec-payload.StartUnixSec
	if valid {
		for _, b := range payload.Buckets {
			if b == nil || math.IsNaN(b.Rru) || math.IsInf(b.Rru, 0) || math.IsNaN(b.Wru) || math.IsInf(b.Wru, 0) {
				valid = false
				break
			}
		}
	}
	if !valid {
		ruTimelineInvalid.Inc()
		g.invalidate(sec-ruTimelineSeconds, sec)
		return
	}
	for i, b := range payload.Buckets {
		second := payload.StartUnixSec + int64(i)
		if second < sec-ruTimelineSeconds {
			ruTimelineLate.Inc()
			continue
		}
		source.first = min(source.first, second)
		old := &source.buckets[second%int64(len(source.buckets))]
		if old.second == second {
			if old.rru != b.Rru || old.wru != b.Wru {
				ruTimelineConflict.Inc()
				g.invalidate(second, second)
			}
			continue
		}
		if second < g.nextWindow {
			ruTimelineLate.Inc()
			continue
		}
		*old = ruSecondBucket{second: second, rru: b.Rru, wru: b.Wru}
	}
}

// summarize sums every source by second and finds the busiest second of the
// window beginning at start. Every observed source must cover the entire
// window: new membership, legacy reports, crashes, and gaps all withhold it.
func (g *ruTimelineGroup) summarize(start int64) ruWindowSummary {
	end := start + ruWindowSeconds
	s := ruWindowSummary{end: end, peak: math.Inf(-1), peakAt: start, available: !g.invalid[start] && len(g.sources) > 0}
	var totals [ruWindowSeconds]ruSecondBucket
	for _, source := range g.sources {
		if source.first >= end {
			continue
		}
		for second := start; second < end; second++ {
			b := source.buckets[second%int64(len(source.buckets))]
			if b.second != second {
				s.available = false
				continue
			}
			totals[second-start].rru += b.rru
			totals[second-start].wru += b.wru
		}
	}
	for i, b := range totals {
		total := b.rru + b.wru
		if math.IsNaN(total) || math.IsInf(total, 0) {
			s.available = false
		}
		if total > s.peak {
			s.peak, s.peakAt, s.rru, s.wru = total, start+int64(i), b.rru, b.wru
		}
	}
	return s
}

func (t *ruTimeline) flush(now time.Time) {
	t.advance(now)
	sec := now.Unix()
	for key, g := range t.groups {
		// Windows beyond retention cannot be verified; skip them after a stall.
		if oldest := windowStart(sec - ruTimelineSeconds); g.nextWindow < oldest {
			g.nextWindow = oldest
			maps.DeleteFunc(g.invalid, func(w int64, _ bool) bool { return w < oldest })
		}
		for ; g.nextWindow+ruWindowSeconds+ruWindowDelay <= sec; g.nextWindow += ruWindowSeconds {
			s := g.summarize(g.nextWindow)
			t.collector.mu.Lock()
			t.collector.results[key] = publishedRUSummary{keyspaceName: g.keyspaceName, summary: s}
			t.collector.mu.Unlock()
			delete(g.invalid, g.nextWindow)
		}
		for k, source := range g.sources {
			if sec-source.lastSeen > ruTimelineSeconds {
				// The source can no longer replay its unreported tail. Later
				// windows are judged by the remaining sources, as for a source
				// that never reported.
				g.invalidate(source.lastSeen, sec)
				delete(g.sources, k)
				t.sourceCount--
			}
		}
		if len(g.sources) == 0 {
			t.remove(key)
		}
	}
}
