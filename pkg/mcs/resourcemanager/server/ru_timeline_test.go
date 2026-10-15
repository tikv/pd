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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

// newTestTimeline returns a timeline whose term began well before the window
// starting at testTimelineStart.
func newTestTimeline() (*ruSummaryCollector, *ruTimeline) {
	c := newRUSummaryCollector()
	t := newRUTimeline(c)
	t.since = testTimelineStart - 3600
	return c, t
}

func TestRUTimelineMergesSources(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	for _, shuffled := range []bool{false, true} {
		c, timeline := newTestTimeline()
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
		// Every report is resent, as after a failed RPC; it must not count twice.
		for _, r := range reports {
			timeline.record(r, time.Unix(start+65, 0))
			timeline.record(r, time.Unix(start+65, 0))
		}
		timeline.flush(time.Unix(start+89, 0))
		re.Empty(c.results)
		timeline.flush(time.Unix(start+90, 0))
		got := c.results[testTimelineKey].summary
		re.Equal(ruWindowSummary{end: start + 60, peak: 150, rru: 80, wru: 70}, got)

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
		// A late second never rewrites the published summary.
		one[20][0] = 999
		timeline.record(timelineReport(1, start, one), time.Unix(start+95, 0))
		re.Equal(got, c.results[testTimelineKey].summary)
	}
}

// TestRUTimelineBestEffort pins that a window sums whatever was reported in
// time and counts an unreported second as zero.
func TestRUTimelineBestEffort(t *testing.T) {
	const start = testTimelineStart
	full := make([][2]float64, 60)
	full[10] = [2]float64{10, 20}
	full[40] = [2]float64{5, 0}
	for _, tc := range []struct {
		name string
		// reports from client 2, recorded after client 1 reported full
		reports []*consumptionItem
		want    ruWindowSummary
	}{
		{
			name:    "partial-source",
			reports: []*consumptionItem{timelineReport(2, start+30, [][2]float64{{40, 0}})},
			want:    ruWindowSummary{end: start + 60, peak: 40, rru: 40},
		},
		{
			name:    "gap-in-source",
			reports: []*consumptionItem{timelineReport(2, start, [][2]float64{{1, 0}}), timelineReport(2, start+40, [][2]float64{{0, 100}})},
			want:    ruWindowSummary{end: start + 60, peak: 105, rru: 5, wru: 100},
		},
		{
			name: "non-finite-second",
			reports: []*consumptionItem{
				timelineReport(2, start+10, [][2]float64{{math.NaN(), 0}, {math.Inf(1), 0}, {7, 0}}),
			},
			want: ruWindowSummary{end: start + 60, peak: 30, rru: 10, wru: 20},
		},
		{
			name:    "legacy-client",
			reports: []*consumptionItem{{clientUniqueID: 2, resourceGroupName: "test", Consumption: &rmpb.Consumption{RRU: 1000}}},
			want:    ruWindowSummary{end: start + 60, peak: 30, rru: 10, wru: 20},
		},
		{
			// A resent second overwrites the earlier value of the same client.
			name:    "overwrite",
			reports: []*consumptionItem{timelineReport(1, start+10, [][2]float64{{50, 0}})},
			want:    ruWindowSummary{end: start + 60, peak: 50, rru: 50},
		},
		{
			// The newer report arrives first, as with concurrent reports.
			name:    "out-of-order",
			reports: []*consumptionItem{timelineReport(2, start+50, [][2]float64{{0, 1}}), timelineReport(2, start+40, [][2]float64{{90, 0}})},
			want:    ruWindowSummary{end: start + 60, peak: 95, rru: 95},
		},
		{
			// A second beyond the tolerated clock skew is dropped instead of
			// taking the place of a second that is still to be summarized.
			name:    "future-second",
			reports: []*consumptionItem{timelineReport(1, start+10+ruSourceSeconds, [][2]float64{{0, 0}})},
			want:    ruWindowSummary{end: start + 60, peak: 30, rru: 10, wru: 20},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			c, timeline := newTestTimeline()
			timeline.record(timelineReport(1, start, full), time.Unix(start+60, 0))
			for _, r := range tc.reports {
				timeline.record(r, time.Unix(start+65, 0))
			}
			timeline.flush(time.Unix(start+90, 0))
			re.Equal(tc.want, c.results[testTimelineKey].summary)
		})
	}
}

// TestRUTimelinePublishesTimelyReports models clients that report at random
// intervals, trim, lose and delay their reports. Every published window must
// equal the sum of the seconds that arrived before it was published, which
// never exceeds the true total when no second is negative.
func TestRUTimelinePublishesTimelyReports(t *testing.T) {
	re := require.New(t)
	const start, clients = testTimelineStart, 4
	type delivery struct {
		client uint64
		report *consumptionItem
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	c, timeline := newTestTimeline()
	values := map[uint64]map[int64]float64{}
	arrived := map[uint64]map[int64]int64{}
	acked, nextReport := map[uint64]int64{}, map[uint64]int64{}
	deliveries := map[int64][]delivery{}
	for id := uint64(1); id <= clients; id++ {
		values[id], arrived[id], acked[id], nextReport[id] = map[int64]float64{}, map[int64]int64{}, start, start
	}
	var lastEnd int64
	published := 0
	for now := start + 1; now < start+600; now++ {
		for id := uint64(1); id <= clients; id++ {
			values[id][now-1] = float64(rnd.IntN(100))
			if now < nextReport[id] {
				continue
			}
			nextReport[id] = now + 1 + int64(rnd.IntN(20))
			from := max(acked[id], now-ruTimelineSeconds)
			if rnd.IntN(5) == 0 {
				from += int64(rnd.IntN(int(now - from)))
			}
			report := make([][2]float64, now-from)
			for i := range report {
				report[i][0] = values[id][from+int64(i)]
			}
			if rnd.IntN(10) > 0 {
				at := now + int64(rnd.IntN(45))
				deliveries[at] = append(deliveries[at], delivery{id, timelineReport(id, from, report)})
			}
		}
		rnd.Shuffle(len(deliveries[now]), func(i, j int) { deliveries[now][i], deliveries[now][j] = deliveries[now][j], deliveries[now][i] })
		for _, d := range deliveries[now] {
			timeline.record(d.report, time.Unix(now, 0))
			seconds := d.report.RuBySecond
			end := seconds.StartUnixSec + int64(len(seconds.Buckets))
			for s := seconds.StartUnixSec; s < end; s++ {
				if _, ok := arrived[d.client][s]; !ok && s >= now+ruTimelineClockSkew-ruSourceSeconds {
					arrived[d.client][s] = now
				}
			}
			acked[d.client] = max(acked[d.client], end)
		}
		timeline.flush(time.Unix(now, 0))
		got := c.results[testTimelineKey].summary
		if got.end == lastEnd {
			continue
		}
		re.Greater(got.end, lastEnd, "a window is published once, in order")
		lastEnd = got.end
		published++
		want, truePeak := ruWindowSummary{end: got.end, peak: math.Inf(-1)}, 0.0
		for s := got.end - ruWindowSeconds; s < got.end; s++ {
			var total, all float64
			for id := uint64(1); id <= clients; id++ {
				if _, ok := arrived[id][s]; ok {
					total += values[id][s]
				}
				all += values[id][s]
			}
			if total > want.peak {
				want.peak, want.rru = total, total
			}
			truePeak = max(truePeak, all)
		}
		re.Equal(want, got)
		re.LessOrEqual(got.peak, truePeak)
	}
	re.Greater(published, 6)
}

func TestRUTimelineSourceLimit(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	full := make([][2]float64, 60)
	full[10] = [2]float64{10, 20}
	timeline.record(timelineReport(1, start, full), time.Unix(start+60, 0))
	timeline.sourceCount = ruTimelineMaxSources
	// A source beyond the limit is not counted; the others still publish.
	timeline.record(timelineReport(2, start, [][2]float64{{1000, 0}}), time.Unix(start+60, 0))
	re.Equal(1, timeline.rejected)
	re.Len(timeline.groups[testTimelineKey].sources, 1)
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(ruWindowSummary{end: start + 60, peak: 30, rru: 10, wru: 20}, c.results[testTimelineKey].summary)
	re.Zero(timeline.rejected)
}

func TestRUTimelineCountsForegroundOnly(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	// The peak covers foreground RU; background and TiFlash requests stay out
	// of it even if they carried seconds.
	background, tiflash := timelineReport(7, start, [][2]float64{{100, 0}}), timelineReport(8, start, [][2]float64{{100, 0}})
	background.isBackground, tiflash.isTiFlash = true, true
	timeline.record(background, time.Unix(start+66, 0))
	timeline.record(tiflash, time.Unix(start+66, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.Len(timeline.groups[testTimelineKey].sources, 1)
	re.Equal(ruWindowSummary{end: start + 60}, c.results[testTimelineKey].summary)
}

func TestRUTimelineFlushLag(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	// The flush lags until the oldest window it may still summarize, while
	// the client clock runs four seconds ahead and replays from a minute
	// earlier: neither its newest nor its oldest seconds may overwrite a
	// second that is still to be summarized.
	values := make([][2]float64, 120)
	values[0] = [2]float64{100, 0}
	values[100] = [2]float64{200, 0}
	timeline.record(timelineReport(1, start, values[:60]), time.Unix(start+65, 0))
	timeline.record(timelineReport(1, start+60, values[60:]), time.Unix(start+125, 0))
	timeline.record(timelineReport(1, start+120, make([][2]float64, 34)), time.Unix(start+149, 0))
	timeline.record(timelineReport(1, start-60, make([][2]float64, 60)), time.Unix(start+149, 0))
	timeline.flush(time.Unix(start+149, 0))
	re.Equal(ruWindowSummary{end: start + 60, peak: 100, rru: 100}, c.results[testTimelineKey].summary)
	timeline.flush(time.Unix(start+150, 0))
	re.Equal(ruWindowSummary{end: start + 120, peak: 200, rru: 200}, c.results[testTimelineKey].summary)
}

func TestRUTimelineToleratesClockSkew(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	// The client clock runs one second ahead: its last closed second is the
	// resource manager's current second.
	values := make([][2]float64, 66)
	values[59] = [2]float64{100, 0}
	timeline.record(timelineReport(1, start, values), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.Equal(ruWindowSummary{end: start + 60, peak: 100, rru: 100}, c.results[testTimelineKey].summary)
}

func TestRUTimelineServerClockStep(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	values := make([][2]float64, 60)
	values[10] = [2]float64{10, 20}
	timeline.record(timelineReport(1, start, values), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	published := c.results[testTimelineKey].summary
	re.Equal(start+60, published.end)

	// After a backward step, the published window stays put, and seconds from
	// the client's future are dropped rather than merged early.
	timeline.record(timelineReport(1, start+60, make([][2]float64, 60)), time.Unix(start+30, 0))
	timeline.flush(time.Unix(start+30, 0))
	re.Equal(published, c.results[testTimelineKey].summary)
	// Once the clock catches up, later windows are published without a reset.
	timeline.record(timelineReport(1, start+60, make([][2]float64, 60)), time.Unix(start+125, 0))
	timeline.flush(time.Unix(start+150, 0))
	re.Equal(ruWindowSummary{end: start + 120}, c.results[testTimelineKey].summary)

	// After a forward step, the timeline resumes at the latest windows.
	future := start + 365*86400
	timeline.record(timelineReport(1, future, make([][2]float64, 5)), time.Unix(future+10, 0))
	timeline.flush(time.Unix(future+ruWindowSeconds+ruWindowDelay, 0))
	re.Equal(ruWindowSummary{end: future + ruWindowSeconds}, c.results[testTimelineKey].summary)
}

// TestRUTimelineTermStart pins that a new term waits for every client to
// reconnect, then publishes from the oldest window that replayed seconds can
// reach, which the previous term may have left unpublished.
func TestRUTimelineTermStart(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	timeline.record(timelineReport(1, start, make([][2]float64, 60)), time.Unix(start+65, 0))
	timeline.flush(time.Unix(start+90, 0))
	re.NotEmpty(c.results)

	timeline.reset()
	re.Empty(c.results)
	now := start + 150
	replay := make([][2]float64, ruTimelineSeconds)
	replay[10] = [2]float64{40, 0}
	timeline.record(timelineReport(1, now-ruTimelineSeconds, replay), time.Unix(now, 0))
	timeline.flush(time.Unix(now, 0))
	// Another client reconnects later and replays the same window.
	replay = make([][2]float64, ruTimelineSeconds+10)
	replay[10] = [2]float64{25, 0}
	timeline.record(timelineReport(2, now-ruTimelineSeconds, replay), time.Unix(now+10, 0))
	timeline.flush(time.Unix(now+ruWindowDelay-1, 0))
	re.Empty(c.results)
	timeline.flush(time.Unix(now+ruWindowDelay, 0))
	re.Equal(ruWindowSummary{end: start + 120, peak: 65, rru: 65}, c.results[testTimelineKey].summary)
}

// TestRUTimelineSourceLifecycle pins that windows keep publishing while any
// client reports, that a silent client's seconds still count in their windows,
// and that a group goes away only after its last sample was exposed.
func TestRUTimelineSourceLifecycle(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	published := make(map[int64]float64)
	for offset := range int64(421) {
		now := start + offset
		// Client 1 reports every 20 seconds, idle after the first minute.
		if offset%20 == 0 && offset < 300 {
			replay := max(start, now-20)
			values := make([][2]float64, now-replay)
			for i := range values {
				if second := replay + int64(i); second < start+60 {
					values[i] = [2]float64{10, 0}
				}
			}
			timeline.record(timelineReport(1, replay, values), time.Unix(now, 0))
		}
		// Client 2 reports a burst in its last second and then stops.
		if offset == 70 {
			values := make([][2]float64, 70)
			values[69] = [2]float64{500, 0}
			timeline.record(timelineReport(2, start, values), time.Unix(now, 0))
		}
		timeline.flush(time.Unix(now, 0))
		if s, ok := c.results[testTimelineKey]; ok {
			published[s.summary.end-start] = s.summary.peak
		}
	}
	re.Equal(map[int64]float64{60: 10, 120: 500, 180: 0, 240: 0, 300: 0}, published)
	re.Empty(timeline.groups)
	re.Zero(timeline.sourceCount)
	re.Empty(c.results)
}
