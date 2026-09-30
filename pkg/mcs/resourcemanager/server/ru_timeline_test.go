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
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const testTimelineStart int64 = 1800000000

var testTimelineKey = trackerKey{0, "test"}

func timelineReport(client uint64, start int64, values [][2]float64) *consumptionItem {
	buckets := make([]*rmpb.RUConsumptionBucket, len(values))
	for i, v := range values {
		buckets[i] = &rmpb.RUConsumptionBucket{Rru: v[0], Wru: v[1]}
	}
	return &consumptionItem{clientUniqueID: client, resourceGroupName: "test", Consumption: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, Buckets: buckets}}}
}

// newWarmTimeline returns a timeline whose clients were already observed
// before the window starting at testTimelineStart.
func newWarmTimeline(clients ...uint64) (*ruSummaryCollector, *ruTimeline) {
	const start = testTimelineStart
	c := newRUSummaryCollector()
	t := newRUTimeline(c)
	// The term began early enough that no unseen source can still replay.
	t.since = start - ruWindowSeconds - ruTimelineSeconds
	for _, client := range clients {
		t.record(timelineReport(client, start-60, make([][2]float64, 1)), time.Unix(start-59, 0))
	}
	return c, t
}

func TestRUTimelineMergesSources(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	for _, shuffled := range []bool{false, true} {
		c, timeline := newWarmTimeline(1, 2)
		one, two := make([][2]float64, 60), make([][2]float64, 60)
		one[10] = [2]float64{100, 0}
		two[11] = [2]float64{0, 120}
		one[20] = [2]float64{80, 0}
		two[20] = [2]float64{0, 70}
		one[21] = [2]float64{-500, 0}
		two[21] = [2]float64{0, 200}
		var reports []*consumptionItem
		for i := 0; i < 60; i += 5 {
			reports = append(reports, timelineReport(1, start+int64(i), one[i:i+5]), timelineReport(2, start+int64(i), two[i:i+5]))
		}
		if shuffled {
			rand.Shuffle(len(reports), func(i, j int) { reports[i], reports[j] = reports[j], reports[i] })
		}
		for _, r := range reports {
			timeline.record(r, time.Unix(start+65, 0))
			timeline.record(r, time.Unix(start+65, 0))
		}
		timeline.flush(time.Unix(start+89, 0))
		re.NotEqual(start+60, c.results[testTimelineKey].summary.end)
		timeline.flush(time.Unix(start+90, 0))
		got := c.results[testTimelineKey].summary
		re.Equal(ruWindowSummary{end: start + 60, peak: 150, peakAt: start + 20, rru: 80, wru: 70}, got)

		registry := prometheus.NewRegistry()
		registry.MustRegister(c)
		first, err := registry.Gather()
		re.NoError(err)
		second, err := registry.Gather()
		re.NoError(err)
		re.Equal(first, second)
		for _, family := range first {
			re.Equal((start+60)*1000, family.GetMetric()[0].GetTimestampMs())
		}
		// A late conflict is observable but never rewrites the published summary.
		one[20][0] = 999
		timeline.record(timelineReport(1, start, one), time.Unix(start+95, 0))
		re.Equal(got, c.results[testTimelineKey].summary)
		timeline.remove(testTimelineKey)
		empty, err := registry.Gather()
		re.NoError(err)
		re.Empty(empty)
	}
}

func TestRUTimelineUnavailable(t *testing.T) {
	const start = testTimelineStart
	for _, scenario := range []string{"gap", "overflow", "conflict", "legacy", "nan", "future-second", "oversized", "new-source", "late-joiner", "capacity", "capacity-tail"} {
		t.Run(scenario, func(t *testing.T) {
			re := require.New(t)
			c, timeline := newWarmTimeline(1)
			values := make([][2]float64, 60)
			values[10] = [2]float64{10, 20}
			report := timelineReport(1, start, values)
			switch scenario {
			case "gap":
				report.RuBySecond.Buckets = report.RuBySecond.Buckets[:59]
			case "overflow":
				for _, b := range report.RuBySecond.Buckets {
					b.Rru, b.Wru = math.MaxFloat64, math.MaxFloat64
				}
			case "capacity-tail":
				// A source rejected within the grace before the window may still
				// be consuming in it.
				timeline.sourceCount = ruTimelineMaxSources
				timeline.record(timelineReport(2, start-ruWindowDelay-1, nil), time.Unix(start-ruWindowDelay, 0))
			}
			timeline.record(report, time.Unix(start+65, 0))
			// Each follow-up would let the window be published if it were ignored.
			var next *consumptionItem
			switch scenario {
			case "conflict":
				next = timelineReport(1, start+10, [][2]float64{{11, 20}})
			case "legacy":
				next = timelineReport(1, start, values)
				next.RuBySecond = nil
			case "nan":
				next = timelineReport(1, start+60, [][2]float64{{math.NaN(), 0}})
			case "future-second":
				// Beyond the tolerated clock skew of the client.
				next = timelineReport(1, start, slices.Concat(values, make([][2]float64, 6+ruTimelineClockSkew+1)))
			case "oversized":
				next = timelineReport(1, start-121, slices.Concat(make([][2]float64, 121), values))
			case "new-source":
				next = timelineReport(2, start+15, values[15:])
			case "late-joiner":
				// A first report trimmed to the next minute hides this one.
				next = timelineReport(2, start+60, make([][2]float64, 6))
			case "capacity":
				timeline.sourceCount = ruTimelineMaxSources
				next = timelineReport(2, start, values)
			}
			missing, invalid := testutil.ToFloat64(ruTimelineMissing), testutil.ToFloat64(ruTimelineInvalid)
			if next != nil {
				timeline.record(next, time.Unix(start+66, 0))
			}
			if scenario == "legacy" {
				re.Equal(missing+1, testutil.ToFloat64(ruTimelineMissing))
				re.Equal(invalid, testutil.ToFloat64(ruTimelineInvalid))
			}
			timeline.flush(time.Unix(start+90, 0))

			registry := prometheus.NewRegistry()
			registry.MustRegister(c)
			// An incomplete window exposes no sample at all.
			families, err := registry.Gather()
			re.NoError(err)
			re.Empty(families)
		})
	}
}

// TestRUTimelineCapacityGrace pins the other side of the capacity-tail
// scenario: a source rejected more than the grace before a window cannot be
// consuming in it, so the window still publishes.
func TestRUTimelineCapacityGrace(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	timeline.sourceCount = ruTimelineMaxSources
	timeline.record(timelineReport(2, start-ruWindowDelay-2, nil), time.Unix(start-ruWindowDelay-1, 0))
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(start+60, c.results[testTimelineKey].summary.end)
}

func TestRUTimelineIgnoresUntimedProducers(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	// TiKV background tasks and TiFlash report aggregates without seconds;
	// they are outside the peak and neither join nor withhold the group.
	missing := testutil.ToFloat64(ruTimelineMissing)
	for _, item := range []*consumptionItem{
		{clientUniqueID: 7, resourceGroupName: "test", isBackground: true, Consumption: &rmpb.Consumption{RRU: 100}},
		{clientUniqueID: 8, resourceGroupName: "test", isTiFlash: true, Consumption: &rmpb.Consumption{RRU: 100}},
	} {
		timeline.record(item, time.Unix(start+66, 0))
	}
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(missing, testutil.ToFloat64(ruTimelineMissing))
	re.Len(timeline.groups[testTimelineKey].sources, 1)
	re.Equal(start+60, c.results[testTimelineKey].summary.end)
}

func TestRUTimelineFlushLag(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	// The flush lags until the oldest window it may still summarize, while
	// the client clock runs four seconds ahead: its newest accepted second
	// must not overwrite a second of that window.
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	timeline.record(timelineReport(1, start+60, make([][2]float64, 60)), time.Unix(start+125, 0))
	timeline.record(timelineReport(1, start+120, make([][2]float64, 34)), time.Unix(start+149, 0))
	timeline.flush(time.Unix(start+149, 0))
	re.Equal(start+60, c.results[testTimelineKey].summary.end)
}

func TestRUTimelineToleratesClockSkew(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	// The client clock runs one second ahead: its last closed second is the
	// resource manager's current second.
	timeline.record(timelineReport(1, start, make([][2]float64, 66)), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(start+60, c.results[testTimelineKey].summary.end)
}

func TestRUTimelineServerClockStep(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	values := make([][2]float64, 60)
	values[10] = [2]float64{10, 20}
	timeline.record(timelineReport(1, start, values), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	published := c.results[testTimelineKey].summary
	re.Equal(start+60, published.end)

	// After a backward step, the published window stays put, and seconds from
	// the client's future are rejected rather than merged early.
	timeline.record(timelineReport(1, start+60, make([][2]float64, 60)), time.Unix(start+30, 0))
	timeline.flush(time.Unix(start+30, 0))
	re.Equal(published, c.results[testTimelineKey].summary)
	// Once the clock catches up, later windows are published without a reset.
	timeline.record(timelineReport(1, start+60, make([][2]float64, 60)), time.Unix(start+125, 0))
	timeline.flush(time.Unix(start+150, 0))
	re.Equal(ruWindowSummary{end: start + 120, peakAt: start + 60}, c.results[testTimelineKey].summary)

	// After a forward step, a new source only withholds the windows it could
	// replay, however far the clock jumped.
	future := start + 365*86400
	timeline.record(timelineReport(2, future-5, make([][2]float64, 5)), time.Unix(future, 0))
	re.LessOrEqual(len(timeline.groups[testTimelineKey].invalid), ruTimelineSeconds/ruWindowSeconds+1)
	// After a backward step, a new group withholds no more windows than early
	// in a term.
	past := start - 365*86400
	report := timelineReport(3, past-5, make([][2]float64, 5))
	report.resourceGroupName = "other"
	timeline.record(report, time.Unix(past, 0))
	re.LessOrEqual(len(timeline.groups[trackerKey{0, "other"}].invalid), ruTimelineSeconds/ruWindowSeconds+1)
}

func TestRUTimelineResetWarmup(t *testing.T) {
	const start = testTimelineStart
	for _, tc := range []struct {
		name     string
		offset   int64 // where the first post-reset report lands within its window
		withheld int64 // windows withheld after the first one
	}{
		{"grace-within-window", 0, 0},
		{"grace-crosses-window", ruWindowSeconds - ruWindowDelay, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			c, timeline := newWarmTimeline(1)
			timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
			timeline.flush(time.Unix(start+90, 0))
			re.NotEmpty(c.results)

			timeline.reset()
			re.Empty(c.results)
			// A new term does not know the sources that stayed silent across
			// the reset, so even a full replay withholds the window of the
			// first report, and the next one when the 30-second grace for
			// such sources crosses into it.
			firstNow := start + 120 + tc.offset
			base := windowStart(firstNow)
			timeline.record(timelineReport(1, firstNow-ruTimelineSeconds, make([][2]float64, ruTimelineSeconds)), time.Unix(firstNow, 0))
			for w := base; w <= base+360; w += ruWindowSeconds {
				timeline.record(timelineReport(1, w, make([][2]float64, 60)), time.Unix(w+65, 0))
				timeline.flush(time.Unix(w+90, 0))
				if w <= base+tc.withheld*ruWindowSeconds {
					re.Empty(c.results)
				} else {
					re.Equal(ruWindowSummary{end: w + 60, peakAt: w}, c.results[testTimelineKey].summary)
				}
			}
		})
	}
}

// TestRUTimelineIdleCadenceNoExpiry verifies that a client reporting at the
// minimum required cadence, once every 20 seconds, never trips the
// 30-second silence grace: a healthy idle client produces no gaps. It flushes
// every second as production does; otherwise the check never sees the silence
// between reports.
func TestRUTimelineIdleCadenceNoExpiry(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	published := make(map[int64]bool)
	for offset := range int64(301) {
		now := start + offset
		if offset%20 == 0 {
			replay := max(start-60, now-ruTimelineSeconds)
			timeline.record(timelineReport(1, replay, make([][2]float64, now-replay)), time.Unix(now, 0))
		}
		timeline.flush(time.Unix(now, 0))
		re.Equal(1, timeline.sourceCount)
		if s, ok := c.results[testTimelineKey]; ok {
			published[s.summary.end-start] = true
		}
	}
	// Every window after the initial warm-up one publishes; the 20-second
	// cadence never lets the source miss the 30-second grace and open a gap.
	re.Equal(map[int64]bool{60: true, 120: true, 180: true, 240: true}, published)
}

func TestRUTimelineIdleAndRetention(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1)
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(ruWindowSummary{end: start + 60, peakAt: start}, c.results[testTimelineKey].summary)
	timeline.flush(time.Unix(start+600, 0))
	re.Empty(timeline.groups)
	re.Zero(timeline.sourceCount)
	re.Empty(c.results)
}

func TestRUTimelineSourceExpiry(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newWarmTimeline(1, 2)
	published := make(map[int64]bool)
	for offset := int64(5); offset <= 405; offset += 20 {
		now := start + offset
		replay := max(start-60, now-ruTimelineSeconds)
		timeline.record(timelineReport(1, replay, make([][2]float64, now-replay)), time.Unix(now, 0))
		// Client 2 stops reporting; its seconds from start+65 are never known.
		if offset <= 65 {
			timeline.record(timelineReport(2, replay, make([][2]float64, now-replay)), time.Unix(now, 0))
		}
		timeline.flush(time.Unix(now, 0))
		if s, ok := c.results[testTimelineKey]; ok {
			// Only the latest window is exposed, never an older one.
			re.Equal(timeline.groups[testTimelineKey].nextWindow, s.summary.end)
			published[s.summary.end-start] = true
		}
	}
	re.Len(timeline.groups[testTimelineKey].sources, 1)
	// Windows overlapping the unknown tail are withheld until expiry; later
	// windows are judged by the remaining source alone.
	// Client 2 last reported at start+65 and expires 30 seconds later, which
	// withholds only the window ending at start+120.
	re.Equal(map[int64]bool{60: true, 180: true, 240: true, 300: true, 360: true}, published)
}
