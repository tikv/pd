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
	"sync"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	// ruTimelineSeconds is the number of closed seconds retained and replayed.
	// The resource manager publishes a minute 30 seconds after it ends, so a
	// second is useless once it is 90 seconds old.
	ruTimelineSeconds = 90
	// ruTimelineBucketsPerRPC bounds the RU timeline payload of one token RPC
	// to about 2 MiB, half of the 4 MiB that a gRPC server receives by default,
	// so that seconds accumulated during an outage do not fail token requests.
	// A bucket takes at most 20 bytes on the wire. The other half holds the
	// rest of a batch of up to about 16,000 resource groups.
	ruTimelineBucketsPerRPC = 100000
)

type ruSecondBucket struct {
	second   int64
	rru, wru float64
}

// ruTimeline records RRU and WRU by natural second. A client reports one
// timeline per resource group, so controllers reporting as the same group
// share one timeline.
// Each report carries the closed seconds that the resource manager has not
// acknowledged, bounded by the retained window. A failed report is resent
// from the same second and the receiver deduplicates the overlap.
// Both recording and snapshotting advance the clock. After a clock rollback,
// consumption lands in seconds that may already hold earlier consumption, so
// nothing is reported until the clock passes the latest second observed
// before it; a reported second never overstates its consumption, and the
// skipped seconds are never reported. A forward step only relabels later
// seconds, like clock skew between clients.
// Methods read the clock while holding mu, so concurrent callers cannot
// present seconds out of order and be mistaken for a clock rollback.
type ruTimeline struct {
	mu sync.Mutex
	// buckets hold the retained closed seconds and the open one.
	buckets [ruTimelineSeconds + 1]ruSecondBucket
	// from is the first second the next snapshot may report. It moves past the
	// seconds of an acknowledged report, and past the latest observed second
	// after a clock rollback.
	from int64
	// last is the latest second observed.
	last int64
}

func newRUTimeline(now time.Time) *ruTimeline {
	return &ruTimeline{from: now.Unix(), last: now.Unix()}
}

func (t *ruTimeline) advanceLocked(now time.Time) {
	second := now.Unix()
	if second < t.last {
		t.from = max(t.from, t.last+1)
	}
	t.last = second
}

func (t *ruTimeline) record(rru, wru float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordLocked(time.Now(), rru, wru)
}

func (t *ruTimeline) recordLocked(now time.Time, rru, wru float64) {
	t.advanceLocked(now)
	second := now.Unix()
	bucket := &t.buckets[second%int64(len(t.buckets))]
	if bucket.second != second {
		*bucket = ruSecondBucket{second: second}
	}
	bucket.rru += rru
	bucket.wru += wru
}

// ack stops reporting the seconds before end, which a successful report
// carried or trimmed.
func (t *ruTimeline) ack(end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.from = max(t.from, end)
}

// snapshot copies the unacknowledged closed seconds, filling idle seconds
// with zero. After a clock rollback it reports no seconds until the clock
// passes the seconds it can no longer attest to.
func (t *ruTimeline) snapshot() *rmpb.RUConsumptionBySecond {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(time.Now())
}

func (t *ruTimeline) snapshotLocked(now time.Time) *rmpb.RUConsumptionBySecond {
	t.advanceLocked(now)
	end := now.Unix()
	start := max(t.from, end-ruTimelineSeconds)
	if end < start {
		return &rmpb.RUConsumptionBySecond{StartUnixSec: end}
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
// request's RU timeline. Dropped seconds are never reported, so they count as
// zero in their minutes.
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
