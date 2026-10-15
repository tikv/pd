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
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	// ruWindowSeconds is the aligned window summarized from the timeline.
	ruWindowSeconds = 60
	// ruWindowDelay is how long a closed window waits for regular reports. It
	// covers both the report interval and any lag of the client clock.
	ruWindowDelay = 30
	// ruTimelineSeconds is how long a second stays useful: its window is
	// published at most a window plus the delay after the second closes, and
	// a source replays at most this many seconds.
	ruTimelineSeconds = ruWindowSeconds + ruWindowDelay
	// ruTimelineClockSkew is how far, in seconds, a client clock may run ahead
	// of the resource manager before its reported seconds are dropped.
	ruTimelineClockSkew = 5
	// ruSourceSeconds spans every second a flush may still summarize, from the
	// start of the window holding the replay horizon to the tolerated clock
	// skew.
	ruSourceSeconds = ruTimelineSeconds + ruWindowSeconds + ruTimelineClockSkew
	// ruTimelineMaxSources bounds the retained sources, which hold nearly all
	// of the timeline's memory.
	ruTimelineMaxSources = 10000
)

// ruWindowSummary describes the busiest second of a closed window.
type ruWindowSummary struct {
	end            int64
	peak, rru, wru float64
	peakAt         int64
}

type publishedRUSummary struct {
	keyspaceName string
	summary      ruWindowSummary
}

// ruSummaryCollector exposes the latest window of each group as immutable,
// explicitly timestamped samples. Scrapes never advance windows, clear
// timelines, or perform aggregation.
type ruSummaryCollector struct {
	mu                     sync.RWMutex
	results                map[trackerKey]publishedRUSummary
	peak, peakAt, rru, wru *prometheus.Desc
}

func newRUSummaryCollector() *ruSummaryCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("resource_manager_resource_unit_"+name, help, []string{keyspaceNameLabel, newResourceGroupNameLabel}, nil)
	}
	return &ruSummaryCollector{
		results: make(map[trackerKey]publishedRUSummary),
		peak:    desc("peak_per_second", "Maximum net foreground RRU+WRU of a natural second in the completed minute, summed over the clients that reported it, in RU/s."),
		peakAt:  desc("peak_second_timestamp_seconds", "Unix timestamp of the earliest second attaining the minute peak."),
		rru:     desc("peak_rru_per_second", "RRU contribution in the second attaining the total minute peak."),
		wru:     desc("peak_wru_per_second", "WRU contribution in the second attaining the total minute peak."),
	}
}

// Describe implements prometheus.Collector.
func (c *ruSummaryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.peak, c.peakAt, c.rru, c.wru} {
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
		emit(c.peak, p.summary.peak)
		emit(c.peakAt, float64(p.summary.peakAt))
		emit(c.rru, p.summary.rru)
		emit(c.wru, p.summary.wru)
	}
}

var ruSummaryMetrics = newRUSummaryCollector()

func init() { prometheus.MustRegister(ruSummaryMetrics) }

type ruSecondBucket struct {
	second   int64
	rru, wru float64
}

// ruSource holds the seconds one client reports for a group. Seconds are
// accepted only within the span of its buckets, which ends at the tolerated
// clock skew ahead of now, so an accepted second never overwrites one that is
// still to be summarized.
type ruSource struct {
	buckets [ruSourceSeconds]ruSecondBucket
	// newest is the latest second the source has reported.
	newest int64
}

type ruTimelineGroup struct {
	keyspaceName string
	sources      map[uint64]*ruSource
}

// ruTimeline merges the RU timelines that clients report into per-group
// timelines and publishes the busiest second of each closed window. It is best
// effort: a window sums whatever its clients reported in time, a second that
// no client reported counts as zero, and a window without any reported second
// has no sample.
// All state belongs to backgroundMetricsFlush. Only immutable summaries cross
// into the scrape goroutines under the collector lock.
// The resource manager clock only schedules windows, while values come from
// client seconds.
type ruTimeline struct {
	collector   *ruSummaryCollector
	groups      map[trackerKey]*ruTimelineGroup
	sourceCount int
	// since is the first second of this term.
	since int64
	// nextWindow is the start of the next window to publish. Every group
	// follows this one schedule.
	nextWindow int64
	// rejected counts the reports dropped at the source limit since the last
	// published window.
	rejected int
}

func newRUTimeline(c *ruSummaryCollector) *ruTimeline {
	t := &ruTimeline{collector: c}
	t.reset()
	return t
}

func windowStart(second int64) int64 {
	return second / ruWindowSeconds * ruWindowSeconds
}

func (t *ruTimeline) reset() {
	t.collector.mu.Lock()
	clear(t.collector.results)
	t.collector.mu.Unlock()
	t.groups = make(map[trackerKey]*ruTimelineGroup)
	t.sourceCount = 0
	t.since, t.nextWindow = 0, 0
	t.rejected = 0
}

func (t *ruTimeline) record(item *consumptionItem, now time.Time) {
	payload := item.GetRuBySecond()
	// TiKV background tasks and TiFlash report RU without seconds; the peak
	// covers the foreground RU that SQL-layer clients confirm by second.
	if item.isBackground || item.isTiFlash || payload == nil {
		return
	}
	sec := now.Unix()
	end := sec + ruTimelineClockSkew
	first := end - ruSourceSeconds
	start, buckets := payload.GetStartUnixSec(), payload.GetBuckets()
	if start >= end || start+int64(len(buckets)) <= first {
		return
	}
	key := trackerKey{item.keyspaceID, item.resourceGroupName}
	g := t.groups[key]
	var source *ruSource
	if g != nil {
		source = g.sources[item.clientUniqueID]
	}
	for i := max(first-start, 0); i < min(end-start, int64(len(buckets))); i++ {
		b := buckets[i]
		if math.IsNaN(b.GetRru()) || math.IsInf(b.GetRru(), 0) || math.IsNaN(b.GetWru()) || math.IsInf(b.GetWru(), 0) {
			continue
		}
		if source == nil {
			if t.sourceCount >= ruTimelineMaxSources {
				t.rejected++
				return
			}
			if g == nil {
				g = &ruTimelineGroup{keyspaceName: item.keyspaceName, sources: make(map[uint64]*ruSource)}
				t.groups[key] = g
			}
			source = &ruSource{}
			g.sources[item.clientUniqueID] = source
			t.sourceCount++
		}
		// A replayed second overwrites the same second from the same client,
		// so a resent report is never counted twice.
		second := start + i
		source.buckets[second%int64(len(source.buckets))] = ruSecondBucket{second: second, rru: b.GetRru(), wru: b.GetWru()}
		source.newest = max(source.newest, second)
	}
}

// summarize sums every source by second and finds the busiest second of the
// window beginning at start, counting a second that no source reported as
// zero. It reports false when no source reported any second of the window.
func (g *ruTimelineGroup) summarize(start int64) (ruWindowSummary, bool) {
	var (
		totals   [ruWindowSeconds]ruSecondBucket
		reported bool
	)
	for _, source := range g.sources {
		for i := range totals {
			second := start + int64(i)
			if b := source.buckets[second%int64(len(source.buckets))]; b.second == second {
				totals[i].rru += b.rru
				totals[i].wru += b.wru
				reported = true
			}
		}
	}
	if !reported {
		return ruWindowSummary{}, false
	}
	s := ruWindowSummary{end: start + ruWindowSeconds, peak: math.Inf(-1)}
	for i, b := range totals {
		if total := b.rru + b.wru; total > s.peak && !math.IsInf(total, 0) {
			s.peak, s.peakAt, s.rru, s.wru = total, start+int64(i), b.rru, b.wru
		}
	}
	return s, !math.IsInf(s.peak, 0)
}

func (t *ruTimeline) flush(now time.Time) {
	sec := now.Unix()
	// Like a closed window, a new term waits for regular reports before it
	// publishes, so that its first windows include the unacknowledged seconds
	// of every reconnected client. Seconds the previous term acknowledged are
	// not replayed, so those windows may be lower.
	if t.since == 0 {
		t.since = sec
	}
	if sec < t.since+ruWindowDelay {
		return
	}
	// Publish from the oldest window within the replay horizon, which the
	// previous term may have left unpublished. After a stall or a forward
	// clock step, older windows are skipped: they may have lost their seconds,
	// and only the latest window of a group is exposed anyway.
	t.nextWindow = max(t.nextWindow, windowStart(sec-ruTimelineSeconds))
	for ; t.nextWindow+ruWindowSeconds+ruWindowDelay <= sec; t.nextWindow += ruWindowSeconds {
		t.collector.mu.Lock()
		for key, g := range t.groups {
			// A window without any reported second keeps the previous sample
			// exposed until the silent sources are dropped.
			if s, ok := g.summarize(t.nextWindow); ok {
				t.collector.results[key] = publishedRUSummary{keyspaceName: g.keyspaceName, summary: s}
			}
		}
		t.collector.mu.Unlock()
		if t.rejected > 0 {
			log.Warn("RU timeline source limit reached, seconds from new sources are not counted",
				zap.Int("limit", ruTimelineMaxSources), zap.Int("rejected-reports", t.rejected))
			t.rejected = 0
		}
	}
	// A source is dropped one window after its last second was published, so
	// that the last sample of a group stays exposed for a window.
	for key, g := range t.groups {
		for id, source := range g.sources {
			if source.newest < t.nextWindow-ruWindowSeconds {
				delete(g.sources, id)
				t.sourceCount--
			}
		}
		if len(g.sources) == 0 {
			delete(t.groups, key)
			t.collector.mu.Lock()
			delete(t.collector.results, key)
			t.collector.mu.Unlock()
		}
	}
}
