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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const testTimelineStart int64 = 1800000000

var testTimelineKey = trackerKey{0, "test"}

func timelineReport(client uint64, start int64, values [][2]float64) *consumptionItem {
	buckets := make([]*rmpb.RUConsumptionBucket, len(values))
	for i, v := range values {
		buckets[i] = &rmpb.RUConsumptionBucket{Rru: v[0], Wru: v[1], Offset: uint32(i)}
	}
	return &consumptionItem{clientUniqueID: client, resourceGroupName: "test", Consumption: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + int64(len(values)), Buckets: buckets}}}
}

// newTestTimeline returns a timeline whose term began well before the window
// starting at testTimelineStart.
func newTestTimeline() (*ruSummaryCollector, *ruTimeline) {
	c := newRUSummaryCollector()
	t := newRUTimeline(c)
	t.since = testTimelineStart - 3600
	return c, t
}

func summaryAt(t *testing.T, c *ruSummaryCollector) (ruWindowSummary, bool) {
	t.Helper()
	p, ok := c.results[testTimelineKey]
	return p.summary, ok
}

// A burst stays visible in rolling windows for 60 seconds and then leaves.
func TestRUTimelineRollingPeak(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	values[30] = [2]float64{100, 200}
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0)) // window [start, start+60)
	s, ok := summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(300), s.peak)
	re.Equal(float64(100), s.rru)
	re.Equal(float64(200), s.wru)

	// The same second is still inside the next windows, then leaves.
	timeline.flush(time.Unix(start+120, 0)) // window [start+30, start+90)
	s, ok = summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(300), s.peak)
	g := timeline.groups[testTimelineKey]
	_, ok = g.summarize(start + 61)
	re.False(ok, "a window without reported seconds has no sample")
}

func TestRUTimelineMergesSources(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.record(timelineReport(2, start, values), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(20), s.peak)
}

func TestRUTimelineIdleWatermark(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	item := &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 60},
	}}
	timeline.record(item, time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt(t, c)
	re.True(ok, "an idle but covered window publishes zero")
	re.Zero(s.peak)
}

func TestRUTimelineNewTermDelay(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c := newRUSummaryCollector()
	timeline := newRUTimeline(c)
	timeline.since = start
	values := make([][2]float64, 60)
	for i := range values {
		values[i] = [2]float64{10, 0}
	}
	timeline.record(timelineReport(1, start, values), time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+29, 0))
	_, ok := summaryAt(t, c)
	re.False(ok, "the first 30 seconds of a term do not publish")
	timeline.flush(time.Unix(start+90, 0))
	_, ok = summaryAt(t, c)
	re.True(ok)
}

func TestRUTimelineSparseGaps(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	item := &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 60,
			Buckets: []*rmpb.RUConsumptionBucket{{Rru: 1, Offset: 0}, {Rru: 9, Offset: 5}}},
	}}
	timeline.record(item, time.Unix(start+60, 0))
	timeline.flush(time.Unix(start+90, 0))
	s, ok := summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(9), s.peak)
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
		"future":          report(start+1000, start+1060, 0),
		"late":            report(start-1000, start-940, 0),
	} {
		c, timeline := newTestTimeline()
		timeline.record(item, time.Unix(start+20, 0))
		timeline.flush(time.Unix(start+100, 0))
		re.Empty(c.results, name)
		re.Empty(timeline.groups, name)
	}
}

func TestRUTimelineSourceLimit(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	timeline.sourceCount = ruTimelineMaxSources
	timeline.record(timelineReport(1, start, [][2]float64{{1, 0}}), time.Unix(start+10, 0))
	re.Zero(timeline.sourceCount - ruTimelineMaxSources)
	re.Equal(1, timeline.rejected)
	re.Empty(c.results)
}

func TestRUTimelineCountsForegroundOnly(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	item := timelineReport(1, start, [][2]float64{{1, 0}})
	item.isBackground = true
	timeline.record(item, time.Unix(start+10, 0))
	item = timelineReport(1, start, [][2]float64{{1, 0}})
	item.isTiFlash = true
	timeline.record(item, time.Unix(start+10, 0))
	timeline.flush(time.Unix(start+100, 0))
	re.Empty(c.results)
}

func TestRUTimelineNoTimestamp(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	timeline.record(timelineReport(1, start, [][2]float64{{1, 2}}), time.Unix(start+10, 0))
	timeline.flush(time.Unix(start+90, 0))
	ch := make(chan prometheus.Metric, 10)
	c.Collect(ch)
	close(ch)
	count := 0
	for metric := range ch {
		var m dto.Metric
		re.NoError(metric.Write(&m))
		re.Nil(m.TimestampMs, "a rolling gauge must not carry a timestamp")
		count++
	}
	re.Equal(3, count)
}

func TestRUTimelineCleansUpSources(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	timeline.record(timelineReport(1, start, [][2]float64{{1, 0}}), time.Unix(start+10, 0))
	timeline.flush(time.Unix(start+90, 0))
	_, ok := summaryAt(t, c)
	re.True(ok)
	timeline.flush(time.Unix(start+ruSourceSeconds+100, 0))
	re.Empty(timeline.groups)
	re.Zero(timeline.sourceCount)
	re.Empty(c.results)
}

func TestRUTimelineTrimmedGapIsNotCovered(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	_, timeline := newTestTimeline()
	report := func(from, to int64) *consumptionItem {
		return &consumptionItem{clientUniqueID: 1, resourceGroupName: "test", Consumption: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: from, EndUnixSec: to},
		}}
	}
	timeline.record(report(start, start+60), time.Unix(start+65, 0))
	timeline.record(report(start+120, start+180), time.Unix(start+185, 0))
	g := timeline.groups[testTimelineKey]
	_, ok := g.summarize(start + 60)
	re.False(ok, "the trimmed gap must not be treated as covered zeros")
}

func TestRUTimelineWithdrawsStaleSample(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
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
		{160, false, 1}, // no reported seconds in the window, source still retained
		{161, false, 0}, // source and group expire without leaving a stale sample
	} {
		timeline.flush(time.Unix(start+step.offset, 0))
		_, ok := summaryAt(t, c)
		re.Equal(step.visible, ok, "flush offset %d", step.offset)
		re.Equal(step.sources, timeline.sourceCount)
		re.Len(timeline.groups, step.sources)
	}
}

func TestRUTimelineLateArrivalVisibility(t *testing.T) {
	re := require.New(t)
	const start = testTimelineStart
	c, timeline := newTestTimeline()
	// The second arrives at s+89, just before the receive window closes.
	timeline.record(timelineReport(1, start, [][2]float64{{100, 0}}), time.Unix(start+89, 0))
	timeline.flush(time.Unix(start+89, 0))
	s, ok := summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(100), s.peak)
	timeline.flush(time.Unix(start+90, 0))
	s, ok = summaryAt(t, c)
	re.True(ok)
	re.Equal(float64(100), s.peak)
	// Only two flush instants expose it; by the next window the second is out.
	timeline.flush(time.Unix(start+91, 0))
	_, ok = summaryAt(t, c)
	re.False(ok)
}
