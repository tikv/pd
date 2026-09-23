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

package controller

import (
	"math"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	// ruTimelineSeconds is the number of closed seconds retained and replayed.
	ruTimelineSeconds = 180
	// ruTimelineBucketsPerRPC bounds the RU timeline payload of one token RPC
	// to about 2 MiB, so that seconds accumulated during an outage never push
	// token requests past the server's message size limit.
	ruTimelineBucketsPerRPC = 100000
)

type ruSecondBucket struct {
	second   int64
	rru, wru float64
}

// ruTimeline records RRU and WRU by natural second. It is guarded by the mu
// of the controller returned by timelineController.
// Each report carries the closed seconds that the resource manager has not
// acknowledged, bounded by the retained window. A failed report is resent
// from the same second and the receiver deduplicates the overlap.
// Both recording and snapshotting advance the clock, so a clock rollback
// quarantines the timeline before a previously closed second can be changed.
// A full retained window is quarantined after a clock discontinuity or an
// untimed external aggregate: neither can be attributed to natural seconds.
type ruTimeline struct {
	buckets [ruTimelineSeconds + 1]ruSecondBucket
	// start is the first second the timeline can attest to. It moves into the
	// future while the timeline is quarantined.
	start int64
	// acked is the first second the resource manager has not acknowledged.
	acked int64
	last  time.Time
}

func (t *ruTimeline) advance(now time.Time) {
	second := now.Unix()
	if t.start == 0 {
		t.start = second
	}
	if !t.last.IsZero() {
		wall := float64(now.UnixNano()-t.last.UnixNano()) / 1e9
		if second < t.last.Unix() || math.Abs(wall-now.Sub(t.last).Seconds()) >= 1 {
			t.invalidate(now)
		}
	}
	t.last = now
}

func (t *ruTimeline) invalidate(now time.Time) {
	t.start = max(t.start, now.Unix()+ruTimelineSeconds)
	t.buckets = [ruTimelineSeconds + 1]ruSecondBucket{}
}

func (t *ruTimeline) record(now time.Time, rru, wru float64) {
	t.advance(now)
	second := now.Unix()
	if second < t.start {
		return
	}
	bucket := &t.buckets[second%int64(len(t.buckets))]
	if bucket.second != second {
		*bucket = ruSecondBucket{second: second}
	}
	bucket.rru += rru
	bucket.wru += wru
}

// ack records that the resource manager has received every second before end.
func (t *ruTimeline) ack(end int64) {
	t.acked = max(t.acked, end)
}

// snapshot copies the unacknowledged closed seconds, filling idle seconds
// with zero. It returns nil while the timeline is quarantined, and after a
// clock rollback until the clock passes the acknowledged seconds again.
func (t *ruTimeline) snapshot(now time.Time) *rmpb.RUConsumptionBySecond {
	t.advance(now)
	end := now.Unix()
	start := max(t.start, end-ruTimelineSeconds, t.acked)
	if end < start {
		return nil
	}
	values := make([]rmpb.RUConsumptionBucket, end-start)
	buckets := make([]*rmpb.RUConsumptionBucket, len(values))
	for i := range values {
		second := start + int64(i)
		if b := t.buckets[second%int64(len(t.buckets))]; b.second == second {
			values[i].Rru, values[i].Wru = b.rru, b.wru
		}
		buckets[i] = &values[i]
	}
	return &rmpb.RUConsumptionBySecond{StartUnixSec: start, Buckets: buckets}
}

// trimRUTimelines keeps at most the newest budget/len(requests) seconds of each
// request's RU timeline. Dropped seconds become gaps, and the resource manager
// withholds the minutes they belong to.
func trimRUTimelines(requests []*rmpb.TokenBucketRequest, budget int) {
	total := 0
	for _, req := range requests {
		total += len(req.GetConsumptionSinceLastRequest().GetRuBySecond().GetBuckets())
	}
	if total <= budget {
		return
	}
	limit := budget / len(requests)
	for _, req := range requests {
		seconds := req.GetConsumptionSinceLastRequest().GetRuBySecond()
		if drop := len(seconds.GetBuckets()) - limit; drop > 0 {
			seconds.StartUnixSec += int64(drop)
			seconds.Buckets = seconds.Buckets[drop:]
		}
	}
}
