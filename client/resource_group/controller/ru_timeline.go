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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
)

const (
	// ruTimelineSeconds is the number of closed seconds retained and replayed.
	// The resource manager's 60-second window ends 30 seconds ago, so a second
	// is useless once it is 90 seconds old.
	ruTimelineSeconds = 90
	// ruTimelineRequestBudget caps the serialized size of one token RPC: the
	// 4 MiB gRPC receive limit minus 128 KiB of transport headroom. The
	// per-group request fields are part of the same payload.
	ruTimelineRequestBudget = 4<<20 - 128<<10
)

var lastTrimWarn atomic.Int64

// ruTimelineReport is the timeline a token request reports, with the
// generation to acknowledge it with.
type ruTimelineReport struct {
	timeline *ruTimeline
	gen      uint64
}

type ruSecondBucket struct {
	second   int64
	rru, wru float64
}

// ruTimeline records RRU and WRU by natural second. A client reports one
// timeline per resource group.
// Each report carries the closed seconds that the resource manager has not
// acknowledged, bounded by the retained window. A failed report is resent
// from the same second and the receiver deduplicates the overlap.
// Both recording and snapshotting advance the clock. After a clock rollback,
// consumption lands in seconds that may already hold earlier consumption, so
// nothing is reported until the clock passes the latest second observed
// before it; a reported second never overstates its consumption, and the
// skipped seconds are never reported. A rollback past every retained second,
// such as the correction of a large forward jump, starts the timeline over
// instead of waiting for the clock to catch up. A forward step only relabels
// later seconds, like clock skew between clients.
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
	// gen changes whenever the timeline starts over, so that a report taken
	// before cannot acknowledge seconds of the new one.
	gen uint64
}

func newRUTimeline(now time.Time) *ruTimeline {
	return &ruTimeline{from: now.Unix(), last: now.Unix()}
}

func (t *ruTimeline) advanceLocked(now time.Time) {
	second := now.Unix()
	switch {
	case second < t.last-ruTimelineSeconds:
		// No retained second can be reused: start over. The resource manager
		// keeps one value per second, so for a second it already holds from
		// before the rollback, or from a report still in flight, one of the
		// values is kept and the other lost; they are never added up.
		t.buckets = [len(t.buckets)]ruSecondBucket{}
		t.from = second
		t.gen++
	case second < t.last:
		// Clock rollback: the seconds recorded before the clock catches up
		// cannot be attributed safely and are skipped (never reported), which
		// loses up to the rollback span of RU, plus recorded seconds that were
		// not acknowledged yet.
		t.from = max(t.from, t.last+1)
	}
	t.last = second
}

func (t *ruTimeline) record(rru, wru float64) {
	if t == nil {
		return
	}
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

// ack stops reporting the seconds before end, which a successful report of
// generation gen carried or trimmed. A report taken before the timeline
// started over acknowledges nothing.
func (t *ruTimeline) ack(end int64, gen uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if gen == t.gen {
		t.from = max(t.from, end)
	}
}

// snapshot copies the unacknowledged closed seconds as a sparse timeline:
// only non-zero seconds are listed, and end_unix_sec is the watermark that
// tells the resource manager which seconds are covered by zeros. After a
// clock rollback it reports no seconds until the clock passes the seconds it
// can no longer attest to. It also returns the generation to acknowledge it
// with.
func (t *ruTimeline) snapshot() (*rmpb.RUConsumptionBySecond, uint64) {
	if t == nil {
		return nil, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Read gen after snapshotLocked, which may start the timeline over.
	seconds := t.snapshotLocked(time.Now())
	return seconds, t.gen
}

func (t *ruTimeline) snapshotLocked(now time.Time) *rmpb.RUConsumptionBySecond {
	t.advanceLocked(now)
	end := now.Unix()
	// from passes end after a clock rollback, which leaves the report empty.
	start := min(max(t.from, end-ruTimelineSeconds), end)
	buckets := make([]*rmpb.RUConsumptionBucket, 0, end-start)
	for second := start; second < end; second++ {
		b := t.buckets[second%int64(len(t.buckets))]
		if b.second != second || (b.rru == 0 && b.wru == 0) {
			continue
		}
		buckets = append(buckets, &rmpb.RUConsumptionBucket{
			Rru: b.rru, Wru: b.wru, Offset: uint32(second - start),
		})
	}
	return &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: end, Buckets: buckets}
}

// trimRUTimelines keeps the newest non-zero seconds of each request so that
// the serialized TokenBucketsRequest fits budget. It sizes the whole batch,
// because the per-group request fields, not only the buckets, can exhaust the
// 4 MiB gRPC limit at a large group count. Trimmed seconds fall outside the
// report's coverage and are unreported, not zeros. If even the empty coverage
// metadata does not fit, the whole RuBySecond field is dropped (nil): those
// seconds are not acknowledged and the next request resends them. If the
// requests without any timeline already exceed the limit, the original
// request was over the limit; this path cannot make it fit.
func trimRUTimelines(requests []*rmpb.TokenBucketRequest, budget int) {
	batch := &rmpb.TokenBucketsRequest{Requests: requests}
	// Fast path: normal reports carry few seconds and fit without resizing.
	if batch.Size() <= budget {
		return
	}
	type savedTimeline struct {
		req     *rmpb.TokenBucketRequest
		start   int64
		buckets []*rmpb.RUConsumptionBucket
	}
	saved := make([]savedTimeline, 0, len(requests))
	for _, req := range requests {
		seconds := req.GetConsumptionSinceLastRequest().GetRuBySecond()
		if seconds == nil {
			continue
		}
		saved = append(saved, savedTimeline{req, seconds.StartUnixSec, seconds.Buckets})
	}
	// Without any timeline there is nothing to trim.
	if len(saved) == 0 {
		return
	}
	setLimit := func(limit int) {
		for _, s := range saved {
			seconds := s.req.GetConsumptionSinceLastRequest().GetRuBySecond()
			seconds.StartUnixSec = s.start
			seconds.Buckets = s.buckets
			drop := len(s.buckets) - limit
			if drop <= 0 {
				continue
			}
			if limit <= 0 {
				// The report becomes an empty interval, which the resource
				// manager rejects; the seconds are unreported, not zeros.
				seconds.StartUnixSec = seconds.GetEndUnixSec()
				seconds.Buckets = nil
				continue
			}
			kept := make([]*rmpb.RUConsumptionBucket, limit)
			shift := int64(s.buckets[drop].GetOffset())
			for i, b := range s.buckets[drop:] {
				kept[i] = &rmpb.RUConsumptionBucket{Rru: b.GetRru(), Wru: b.GetWru(), Offset: b.GetOffset() - uint32(shift)}
			}
			seconds.StartUnixSec += shift
			seconds.Buckets = kept
		}
	}
	setLimit(0)
	if coverageSize := batch.Size(); coverageSize > budget {
		// The coverage metadata alone does not fit. Drop the whole field so the
		// per-group wrapper headers disappear too. These seconds are not
		// acknowledged and will be resent; if the remaining requests still
		// exceed the limit, the original request was already over it.
		for _, s := range saved {
			s.req.GetConsumptionSinceLastRequest().RuBySecond = nil
		}
		// Throttle the warning: this function runs on every token request.
		// CompareAndSwap keeps concurrent controllers from all logging.
		now := time.Now().Unix()
		if last := lastTrimWarn.Load(); now-last >= 60 && lastTrimWarn.CompareAndSwap(last, now) {
			log.Warn("resource group timeline omitted: coverage metadata exceeds the token RPC budget",
				zap.Int("coverage_size", coverageSize), zap.Int("size_without_timeline", batch.Size()), zap.Int("budget", budget))
		}
		return
	}
	lo, hi := 0, ruTimelineSeconds
	for lo < hi {
		mid := (lo + hi + 1) / 2
		setLimit(mid)
		if batch.Size() <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	setLimit(lo)
}
