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
	"time"

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

type ruSecondBucket struct {
	second int64
	// ru is the RRU+WRU the client reported for the second.
	ru float64
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
// All state belongs to backgroundMetricsFlush. The resource manager clock only
// schedules windows, while values come from client seconds.
type ruTimeline struct {
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

func newRUTimeline() *ruTimeline {
	return &ruTimeline{groups: make(map[trackerKey]*ruTimelineGroup)}
}

// reset ends the term. A published group is still retained, because a group
// is dropped only after its window emptied and withdrew its series.
func (t *ruTimeline) reset() {
	for key, g := range t.groups {
		ruPeakPerSec.DeleteLabelValues(key.groupName, g.keyspaceName)
	}
	*t = *newRUTimeline()
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
	// Only the intersection with the receive window [now-90, now+5) is useful:
	// a second before it never enters a published window again, and one after
	// it is too far ahead of the resource manager clock. An empty or reversed
	// interval, including a report without a watermark (end 0), has none.
	coveredFrom, coveredTo := max(start, first), min(end, limit)
	if coveredFrom >= coveredTo {
		return
	}
	buckets := payload.GetBuckets()
	// A report with offsets that are not strictly increasing and inside the
	// interval, or with a value that is not finite, is dropped as a whole.
	lastOffset := int64(-1)
	for _, b := range buckets {
		off := int64(b.GetOffset())
		ru := b.GetRru() + b.GetWru()
		if off <= lastOffset || off >= end-start || math.IsNaN(ru) || math.IsInf(ru, 0) {
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
		source.buckets[second%int64(len(source.buckets))] = ruSecondBucket{second: second, ru: b.GetRru() + b.GetWru()}
	}
}

// summarize sums every source by second and returns the busiest second of the
// window beginning at start, counting a second that no source reported as zero.
// It reports false when no source reported any second of the window.
func (g *ruTimelineGroup) summarize(start int64) (float64, bool) {
	var (
		totals   [ruWindowSeconds]float64
		reported bool
	)
	for _, source := range g.sources {
		for i := range totals {
			second := start + int64(i)
			if b := source.buckets[second%int64(len(source.buckets))]; b.second == second {
				totals[i] += b.ru
				reported = true
			}
		}
	}
	if !reported {
		return 0, false
	}
	peak := math.Inf(-1)
	for _, total := range totals {
		// Finite values can still overflow when summed over sources.
		if total > peak && !math.IsInf(total, 0) {
			peak = total
		}
	}
	return peak, !math.IsInf(peak, 0)
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
		for key, g := range t.groups {
			peak, ok := g.summarize(start)
			if !ok {
				// Not even a zero watermark in the window: withdraw the series
				// instead of exposing a stale peak.
				ruPeakPerSec.DeleteLabelValues(key.groupName, g.keyspaceName)
				continue
			}
			ruPeakPerSec.WithLabelValues(key.groupName, g.keyspaceName).Set(peak)
		}
	}
	if t.rejected > 0 && sec-t.lastRejectLog >= 60 {
		log.Warn("RU timeline source limit reached, seconds from new sources are not counted",
			zap.Int("limit", ruTimelineMaxSources), zap.Int("rejected-reports", t.rejected))
		t.rejected = 0
		t.lastRejectLog = sec
	}
	// Drop sources whose newest second can no longer be summarized, then
	// groups without sources. Such a group summarized to nothing above.
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
