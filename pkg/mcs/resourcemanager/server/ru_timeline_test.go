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
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const testTimelineStart int64 = 1800000000

func timelineReport(client uint64, start int64, values [][2]float64) *consumptionItem {
	buckets := make([]*rmpb.RUConsumptionBucket, len(values))
	for i, v := range values {
		buckets[i] = &rmpb.RUConsumptionBucket{Rru: v[0], Wru: v[1], Offset: uint32(i)}
	}
	return &consumptionItem{clientUniqueID: client, resourceGroupName: "test", Consumption: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + int64(len(values)), Buckets: buckets}}}
}

// newTestTimeline returns a timeline whose term began well before the window
// starting at testTimelineStart.
func newTestTimeline(t *testing.T) *ruTimeline {
	timeline := newRUTimeline()
	timeline.since = testTimelineStart - 3600
	t.Cleanup(timeline.reset)
	return timeline
}

// summaryAt returns the published peak of the only group of a test.
func summaryAt() (float64, bool) {
	if promtestutil.CollectAndCount(ruPeakPerSec) == 0 {
		return 0, false
	}
	return promtestutil.ToFloat64(ruPeakPerSec.WithLabelValues("test", "")), true
}

// A burst stays visible in rolling windows for 60 seconds and then leaves.
func TestRUTimelineRollingPeak(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	values[30] = [2]float64{100, 200}
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0)) // window [start, start+60)
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(300), s)

	// The same second is still inside the next windows, then leaves.
	timeline.flush(time.Unix(start+120, 0)) // window [start+30, start+90)
	s, ok = summaryAt()
	re.True(ok)
	re.Equal(float64(300), s)
	g := timeline.groups[trackerKey{0, "test"}]
	_, ok = g.summarize(start + 61)
	re.False(ok, "a window without reported seconds has no sample")
}

func TestRUTimelineMergesSources(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.record(timelineReport(2, start, values), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(20), s)
}

// A retried or overlapping report replays seconds the resource manager already
// holds for the client; they replace the stored value instead of adding to it.
func TestRUTimelineReplayIsIdempotent(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	values := make([][2]float64, 60)
	values[7] = [2]float64{100, 0}
	first := timelineReport(1, start, values[:10])
	timeline.record(first, time.Unix(start+60, 0))
	// The retry resends the first ten seconds together with the later ones,
	// and the original report arrives once more afterwards.
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.record(first, time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(100), s)
}

// After a client clock rollback restarts its timeline, a report it sent from
// before the rollback can still arrive later and carry the same labels. Only
// one value per second is kept: the later report replaces the listed seconds.
func TestRUTimelineDelayedReportFromBeforeRollback(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	current := make([][2]float64, 10)
	current[1] = [2]float64{5, 0}
	timeline.record(timelineReport(1, start, current), time.Unix(start+10, 0))
	// The old report covers [start+1, start+91) and is processed at start+60,
	// when [start+1, start+65) is inside the receive window.
	old := make([][2]float64, 90)
	old[0] = [2]float64{100, 0}
	timeline.record(timelineReport(1, start+1, old), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(100), s)
}

// A second older than the current window is never summarized again, so it is
// not accepted and cannot overwrite a second that shares its slot.
func TestRUTimelineRejectsSecondsBeforeWindow(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	now := time.Unix(start+150, 0)
	timeline.record(timelineReport(1, start+95, [][2]float64{{7, 0}}), now)
	// start and start+95 share a slot; start is before the window [now-90, now-30).
	timeline.record(timelineReport(1, start, [][2]float64{{100, 0}}), now)
	timeline.flush(now)
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(7), s)
}

func TestRUTimelineIdleWatermark(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	item := &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 60},
	}}
	timeline.record(item, time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt()
	re.True(ok, "an idle but covered window publishes zero")
	re.Zero(s)
}

func TestRUTimelineNewTermDelay(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	timeline.since = start
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	// The seconds fill the windows of both flushes below.
	timeline.record(timelineReport(1, start-61, values), time.Unix(start+29, 0))
	timeline.flush(time.Unix(start+29, 0))
	_, ok := summaryAt()
	re.False(ok, "the first 30 seconds of a term do not publish")
	timeline.flush(time.Unix(start+30, 0))
	_, ok = summaryAt()
	re.True(ok)
}

func TestRUTimelineSparseGaps(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	item := &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 60,
			Buckets: []*rmpb.RUConsumptionBucket{{Rru: 1, Offset: 0}, {Rru: 9, Offset: 5}}},
	}}
	timeline.record(item, time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(9), s)
}

func TestRUTimelineRejectsMalformedReports(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	report := func(start, end int64, offsets ...uint32) *consumptionItem {
		buckets := make([]*rmpb.RUConsumptionBucket, len(offsets))
		for i, off := range offsets {
			buckets[i] = &rmpb.RUConsumptionBucket{Rru: 1, Offset: off}
		}
		return &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: end, Buckets: buckets},
		}}
	}
	emptyNoBuckets := &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start + 10, EndUnixSec: start + 10},
	}}
	for name, item := range map[string]*consumptionItem{
		"no-watermark":    report(start, 0, 0),
		"reversed":        report(start+10, start+5, 0),
		"empty":           report(start+10, start+10, 0),
		"empty-no-bucket": emptyNoBuckets,
		"offset-order":    report(start, start+10, 3, 2),
		"offset-range":    report(start, start+10, 10),
		"nan":             {clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 10, Buckets: []*rmpb.RUConsumptionBucket{{Rru: math.NaN()}}}}},
		"inf":             {clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 10, Buckets: []*rmpb.RUConsumptionBucket{{Wru: math.Inf(1)}}}}},
		"future":          report(start+1000, start+1060, 0),
		"late":            report(start-1000, start-940, 0),
	} {
		timeline := newTestTimeline(t)
		timeline.record(item, time.Unix(start+20, 0))
		// The window [start, start+60) would hold every second of an accepted
		// report, and the group outlives this flush.
		timeline.flush(time.Unix(start+90, 0))
		_, ok := summaryAt()
		re.False(ok, name)
		re.Empty(timeline.groups, name)
	}
}

func TestRUTimelineSourceLimit(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	timeline.sourceCount = ruTimelineMaxSources
	timeline.record(timelineReport(1, start, [][2]float64{{1, 0}}), time.Unix(start+10, 0))
	re.Zero(timeline.sourceCount - ruTimelineMaxSources)
	re.Equal(1, timeline.rejected)
}

func TestRUTimelineCountsForegroundOnly(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	item := timelineReport(1, start, [][2]float64{{1, 0}})
	item.isBackground = true
	timeline.record(item, time.Unix(start+10, 0))
	item = timelineReport(1, start, [][2]float64{{1, 0}})
	item.isTiFlash = true
	timeline.record(item, time.Unix(start+10, 0))
	timeline.flush(time.Unix(start+90, 0))
	_, ok := summaryAt()
	re.False(ok)
}

func TestRUTimelineTrimmedGapIsNotCovered(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	report := func(from, to int64) *consumptionItem {
		return &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: from, EndUnixSec: to},
		}}
	}
	timeline.record(report(start, start+60), time.Unix(start+65, 0))
	timeline.record(report(start+120, start+180), time.Unix(start+185, 0))
	g := timeline.groups[trackerKey{0, "test"}]
	_, ok := g.summarize(start + 60)
	re.False(ok, "the trimmed gap must not be treated as covered zeros")
}

func TestRUTimelineWithdrawsStaleSample(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	timeline.since = start
	timeline.record(timelineReport(1, start+10, [][2]float64{{10, 0}}), time.Unix(start+11, 0))
	for _, step := range []struct {
		offset  int64
		visible bool
		sources int
	}{
		{100, true, 1},  // window [start+10, start+70)
		{20, true, 1},   // rollback into the term's initial waiting period
		{100, true, 1},  // restore the clock
		{101, false, 0}, // the second leaves the window; source and group expire without leaving a stale sample
	} {
		timeline.flush(time.Unix(start+step.offset, 0))
		_, ok := summaryAt()
		re.Equal(step.visible, ok, "flush offset %d", step.offset)
		re.Equal(step.sources, timeline.sourceCount)
		re.Len(timeline.groups, step.sources)
	}
}

func TestRUTimelineLateArrivalVisibility(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	timeline := newTestTimeline(t)
	// The second arrives at s+89, just before the receive window closes.
	timeline.record(timelineReport(1, start, [][2]float64{{100, 0}}), time.Unix(start+89, 0))
	timeline.flush(time.Unix(start+89, 0))
	s, ok := summaryAt()
	re.True(ok)
	re.Equal(float64(100), s)
	timeline.flush(time.Unix(start+90, 0))
	s, ok = summaryAt()
	re.True(ok)
	re.Equal(float64(100), s)
	// Only two flush instants expose it; by the next window the second is out.
	timeline.flush(time.Unix(start+91, 0))
	_, ok = summaryAt()
	re.False(ok)
}
