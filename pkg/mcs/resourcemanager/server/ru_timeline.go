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
	// ruWindowSeconds is the window summarized from the timeline.
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
	// start of the current window to the tolerated clock skew. Windows only
	// move forward, so an older second is never summarized again.
	ruSourceSeconds = ruTimelineSeconds + ruTimelineClockSkew
	// ruTimelineMaxSources bounds the retained sources, which hold nearly all
	// of the timeline's memory.
	ruTimelineMaxSources = 10000
)

type publishedRUSummary struct {
	keyspaceName string
	// peak is the busiest second of the window.
	peak ruSecondBucket
}

// ruSummaryCollector exposes each group's busiest second in the trailing
// 60-second window as a plain gauge. The value is updated by the flush loop;
// scrapes never advance windows, clear timelines, or perform aggregation.
type ruSummaryCollector struct {
	mu             sync.RWMutex
	results        map[trackerKey]publishedRUSummary
	peak, rru, wru *prometheus.Desc
}

func newRUSummaryCollector() *ruSummaryCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("resource_manager_resource_unit_"+name, help, []string{keyspaceNameLabel, newResourceGroupNameLabel}, nil)
	}
	return &ruSummaryCollector{
		results: make(map[trackerKey]publishedRUSummary),
		peak:    desc("peak_per_second_rolling_60s", "Maximum net foreground RRU+WRU of a natural second in the 60-second window ending about 30 seconds ago, summed over the clients that reported it, in RU/s. A second is visible for about a minute only when its complete contribution arrives before the window is exposed; late data can shorten that. RU is attributed to the second the client settles it, so a long request lands in one second; TiFlash, background task, SQL CPU RU and RUv2 are excluded."),
		rru:     desc("peak_rru_per_second_rolling_60s", "RRU contribution in the second attaining the peak of the 60-second window ending about 30 seconds ago."),
		wru:     desc("peak_wru_per_second_rolling_60s", "WRU contribution in the second attaining the peak of the 60-second window ending about 30 seconds ago."),
	}
}

// Describe implements prometheus.Collector.
func (c *ruSummaryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.peak, c.rru, c.wru} {
		ch <- desc
	}
}

// Collect implements prometheus.Collector.
func (c *ruSummaryCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for key, p := range c.results {
		emit := func(desc *prometheus.Desc, value float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, p.keyspaceName, key.groupName)
		}
		emit(c.peak, p.peak.rru+p.peak.wru)
		emit(c.rru, p.peak.rru)
		emit(c.wru, p.peak.wru)
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
	// since is the first second of this term. The first 30 seconds wait for
	// clients to replay their unacknowledged seconds before publishing.
	since int64
	// rejected counts the reports dropped at the source limit since the last
	// warning.
	rejected int
	// lastRejectLog is the last second a source-limit warning was logged.
	lastRejectLog int64
}

func newRUTimeline(c *ruSummaryCollector) *ruTimeline {
	return &ruTimeline{collector: c, groups: make(map[trackerKey]*ruTimelineGroup)}
}

func (t *ruTimeline) reset() {
	t.collector.mu.Lock()
	clear(t.collector.results)
	t.collector.mu.Unlock()
	*t = *newRUTimeline(t.collector)
}

func (t *ruTimeline) record(item *consumptionItem, now time.Time) {
	payload := item.GetRuBySecond()
	// TiKV background tasks and TiFlash report RU without seconds; the peak
	// covers the foreground RU that SQL-layer clients confirm by second.
	if item.isBackground || item.isTiFlash || payload == nil {
		return
	}
	nowSec := now.Unix()
	limit := nowSec + ruTimelineClockSkew
	first := limit - ruSourceSeconds
	start, end := payload.GetStartUnixSec(), payload.GetEndUnixSec()
	if start >= end {
		// An empty or reversed interval cannot be attributed to a second. A
		// report without a watermark (end 0) is empty or entirely too late.
		return
	}
	if end <= first || start >= limit {
		// Too late to be summarized, or too far ahead of the resource manager
		// clock. The receive window is [now-90, now+5): a second before now-90
		// never enters a published window again. These drops are silent for
		// now; the TODO is to expose them by reason.
		return
	}
	// Only the intersection with the receive window is useful; everything
	// outside it is either about to be summarized or too old.
	coveredFrom, coveredTo := max(start, first), min(end, limit)
	buckets := payload.GetBuckets()
	// Offsets must be strictly increasing and inside the reported interval;
	// otherwise the report is malformed and is dropped as a whole instead of
	// silently stacking or skipping seconds.
	lastOffset := int64(-1)
	for _, b := range buckets {
		off := int64(b.GetOffset())
		if off <= lastOffset || off >= end-start {
			return
		}
		lastOffset = off
	}
	key := trackerKey{item.keyspaceID, item.resourceGroupName}
	g := t.groups[key]
	var source *ruSource
	if g != nil {
		source = g.sources[item.clientUniqueID]
	}
	if source == nil {
		if t.sourceCount >= ruTimelineMaxSources {
			// New sources are rejected once the cap is reached; the report is
			// lost and only a per-flush warning is emitted. Prefer a
			// configurable/LRU policy if the cap becomes a production limit.
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
	// Fill the covered interval with explicit zeros first: seconds that the
	// client did not list are zeros, and a fully idle report keeps its series
	// visible instead of disappearing. Only a slot that no longer holds this
	// second is initialized, so a replayed or concurrent value is kept.
	for second := coveredFrom; second < coveredTo; second++ {
		slot := &source.buckets[second%int64(len(source.buckets))]
		if slot.second != second {
			*slot = ruSecondBucket{second: second}
		}
	}
	source.newest = max(source.newest, coveredTo-1)
	for _, b := range buckets {
		second := start + int64(b.GetOffset())
		if second < coveredFrom || second >= coveredTo {
			continue
		}
		rru, wru := b.GetRru(), b.GetWru()
		if math.IsNaN(rru) || math.IsInf(rru, 0) || math.IsNaN(wru) || math.IsInf(wru, 0) {
			continue
		}
		source.buckets[second%int64(len(source.buckets))] = ruSecondBucket{second: second, rru: rru, wru: wru}
	}
}

// summarize sums every source by second and finds the busiest second of the
// window beginning at start, counting a second that no source reported as
// zero. It reports false when no source reported any second of the window.
func (g *ruTimelineGroup) summarize(start int64) (ruSecondBucket, bool) {
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
		return ruSecondBucket{}, false
	}
	peak, best := ruSecondBucket{}, math.Inf(-1)
	for i, b := range totals {
		if total := b.rru + b.wru; total > best && !math.IsInf(total, 0) {
			b.second = start + int64(i)
			peak, best = b, total
		}
	}
	return peak, !math.IsInf(best, 0)
}

func (t *ruTimeline) flush(now time.Time) {
	sec := now.Unix()
	if t.since == 0 {
		t.since = sec
	}
	// The exposed value is the busiest second in [sec-90, sec-30). A second s
	// is visible while the window end lies in (s+30, s+90], so the full minute
	// only holds when its complete contribution arrives before the window is
	// first exposed; late, retried or leader-changed data can shorten or
	// remove the visible time.
	if sec >= t.since+ruWindowDelay {
		start := sec - ruWindowSeconds - ruWindowDelay
		// Summarize outside the collector lock, so scrapes only wait for the
		// results to be swapped in. A group without any reported second in the
		// window (not even a zero watermark) is left out, which withdraws its
		// series instead of exposing a stale peak until the source is recycled.
		results := make(map[trackerKey]publishedRUSummary, len(t.groups))
		for key, g := range t.groups {
			if peak, ok := g.summarize(start); ok {
				results[key] = publishedRUSummary{keyspaceName: g.keyspaceName, peak: peak}
			}
		}
		t.collector.mu.Lock()
		t.collector.results = results
		t.collector.mu.Unlock()
	}
	if t.rejected > 0 && sec-t.lastRejectLog >= 60 {
		log.Warn("RU timeline source limit reached, seconds from new sources are not counted",
			zap.Int("limit", ruTimelineMaxSources), zap.Int("rejected-reports", t.rejected))
		t.rejected = 0
		t.lastRejectLog = sec
	}
	// Drop sources whose newest second can no longer be summarized, then
	// groups without sources.
	for key, g := range t.groups {
		for id, source := range g.sources {
			if source.newest < sec-ruTimelineSeconds {
				delete(g.sources, id)
				t.sourceCount--
			}
		}
		if len(g.sources) == 0 {
			delete(t.groups, key)
		}
	}
}
