// Copyright 2019 TiKV Project Authors.
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

package election

import (
	"context"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	revokeLeaseTimeout = time.Second
	requestTimeout     = etcdutil.DefaultRequestTimeout
	slowRequestTime    = etcdutil.DefaultSlowRequestTime
)

// Lease is used as the low-level mechanism for campaigning and renewing elected leadership.
// The way to gain and maintain leadership is to update and keep the lease alive continuously.
type Lease struct {
	// purpose is used to show what this election for
	Purpose string
	// etcd client and lease
	client *clientv3.Client
	lease  clientv3.Lease
	ID     atomic.Value // store as clientv3.LeaseID
	// closed marks that Close() has halted the underlying etcd lessor.
	// A halted lessor returns ErrKeepAliveHalted on any native KeepAlive
	// call, so Grant() consults this flag and rebuilds l.lease on reuse.
	closed atomic.Bool
	// leaseTimeout and expireTime are used to control the lease's lifetime
	leaseTimeout time.Duration
	expireTime   atomic.Value
	metrics      leaseMetrics
}

// NewLease creates a new Lease instance.
func NewLease(client *clientv3.Client, purpose string) *Lease {
	return &Lease{
		Purpose: purpose,
		client:  client,
		lease:   clientv3.NewLease(client),
		metrics: newLeaseMetrics(purpose),
	}
}

// Grant uses `lease.Grant` to initialize the lease and expireTime.
func (l *Lease) Grant(leaseTimeout int64) error {
	if l == nil {
		return errs.ErrEtcdGrantLease.GenWithStackByCause("lease is nil")
	}
	// Rebuild the lessor if a previous Close() halted it; see closed field doc.
	if l.closed.Swap(false) {
		l.lease = clientv3.NewLease(l.client)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(l.client.Ctx(), requestTimeout)
	leaseResp, err := l.lease.Grant(ctx, leaseTimeout)
	cancel()
	if err != nil {
		return errs.ErrEtcdGrantLease.Wrap(err).GenWithStackByCause()
	}
	if cost := time.Since(start); cost > slowRequestTime {
		log.Warn("lease grants too slow", zap.Duration("cost", cost), zap.String("purpose", l.Purpose))
	}
	log.Info("lease granted", zap.Int64("lease-id", int64(leaseResp.ID)), zap.Int64("lease-timeout", leaseTimeout), zap.String("purpose", l.Purpose))
	l.ID.Store(leaseResp.ID)
	l.leaseTimeout = time.Duration(leaseTimeout) * time.Second
	l.expireTime.Store(start.Add(time.Duration(leaseResp.TTL) * time.Second))
	return nil
}

// Close releases the lease.
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	// Reset expire time.
	l.expireTime.Store(typeutil.ZeroTime)
	l.metrics.ttlRemaining.Set(0)
	// Try to revoke lease to make subsequent elections faster.
	ctx, cancel := context.WithTimeout(l.client.Ctx(), revokeLeaseTimeout)
	defer cancel()
	if _, err := l.lease.Revoke(ctx, l.loadLeaseID()); err != nil {
		log.Error("revoke lease failed", zap.String("purpose", l.Purpose), errs.ZapError(err))
	}
	err := l.lease.Close()
	l.closed.Store(true)
	return err
}

func (l *Lease) getExpireTime() time.Time {
	if l == nil {
		return typeutil.ZeroTime
	}
	expireTime := l.expireTime.Load()
	if expireTime == nil {
		return typeutil.ZeroTime
	}
	return expireTime.(time.Time)
}

// IsExpired checks if the lease is expired. If it returns true,
// the current leader/primary should step down and try to re-elect again.
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.getExpireTime())
}

// KeepAlive runs the lease renewal loop until the caller cancels ctx, the
// etcd keepalive stream is closed, or the locally tracked lease estimate is
// exhausted. It is safe to call at most once per Grant; callers typically
// invoke it in a dedicated goroutine.
//
// Control flow:
//
//  1. Subscribe to the shared etcd keepalive stream. All keepalive sends
//     happen inside the etcd client's global sendKeepAliveLoop, so no
//     per-call sender goroutine is needed.
//  2. In a single select loop, multiplex three sources:
//     - ctx cancellation: stop immediately.
//     - keepalive response: extend expireTime and reset the watchdog.
//     - watchdog timer: if no valid response arrived within leaseTimeout,
//     step down so the election owner can re-campaign.
func (l *Lease) KeepAlive(ctx context.Context) {
	if l == nil {
		return
	}
	defer logutil.LogPanic()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	leaseID := l.loadLeaseID()
	// expectedInterval mirrors etcd's internal send cadence (TTL/3). It is only
	// used to classify a late response for diagnostics; the real watchdog is
	// leaseTimeout below.
	expectedInterval := l.leaseTimeout / 3

	// Scoped logger bakes in purpose + lease-id so every structured log in
	// this loop automatically carries them; call sites only add per-event
	// fields. Existing field names are preserved for operator alerts.
	logger := log.L().With(
		zap.String("purpose", l.Purpose),
		zap.Int64("lease-id", int64(leaseID)),
	)

	defer logger.Info("lease keep alive stopped")

	keepAliveCh, err := l.lease.KeepAlive(ctx, leaseID)
	if err != nil {
		logger.Error("lease keep alive stream failed",
			zap.Duration("lease-timeout", l.leaseTimeout),
			zap.Duration("expected-interval", expectedInterval),
			errs.ZapError(err))
		return
	}

	watchdog := time.NewTimer(l.leaseTimeout)
	defer watchdog.Stop()

	// maxExpire keeps expireTime monotonic: a stale/late response with a
	// smaller absolute expire must never pull the local estimate backwards.
	var (
		maxExpire        time.Time
		lastResponseTime time.Time
	)
	for {
		select {
		case <-ctx.Done():
			l.metrics.contextCanceled.Inc()
			return
		case resp, ok := <-keepAliveCh:
			if !ok {
				// A closed keepalive channel means the etcd client can no
				// longer maintain this lease stream. Stop immediately rather
				// than waiting for the watchdog deadline.
				if ctx.Err() == nil {
					logger.Warn("lease keep alive channel closed",
						zap.Time("last-response-time", lastResponseTime))
				}
				return
			}
			now := time.Now()
			expire, valid := processKeepAliveResponse(logger, resp, now, lastResponseTime, expectedInterval)
			if !valid {
				l.metrics.invalidTTL.Inc()
				return
			}
			if !lastResponseTime.IsZero() {
				l.metrics.responseInterval.Observe(now.Sub(lastResponseTime).Seconds())
			}
			lastResponseTime = now
			if expire.After(maxExpire) {
				maxExpire = expire
				// The response may land while ctx is already cancelled (for
				// example, Close racing with a late renewal). Re-check before
				// publishing, otherwise we would revive a lease the caller
				// just abandoned.
				select {
				case <-ctx.Done():
					l.metrics.contextCanceled.Inc()
					return
				default:
					l.expireTime.Store(expire)
				}
			}
			l.metrics.observeRemainingTTL(maxExpire.Sub(now))
			resetTimer(watchdog, l.leaseTimeout)
		case <-watchdog.C:
			// Timer delivery can race with a keepalive response that already
			// extended expireTime. Only step down when the local lease
			// estimate is actually exhausted.
			actualExpire := l.getExpireTime()
			if remaining := time.Until(actualExpire); remaining > 0 {
				watchdog.Reset(remaining)
				continue
			}
			l.metrics.observeRemainingTTL(time.Until(actualExpire))
			l.metrics.leaseExpired.Inc()
			logger.Info("keep alive lease too slow",
				zap.Duration("timeout-duration", l.leaseTimeout),
				zap.Time("actual-expire", actualExpire))
			return
		}
	}
}

// processKeepAliveResponse validates an incoming keepalive event and returns
// the absolute expire time derived from the arrival wall clock.
//
// valid=false signals that the response reports the lease as lost (nil
// payload or non-positive TTL). The caller should stop the loop so the
// election owner can step down instead of masking a missing lease.
//
// When valid=true, the helper may also emit a slow-consumer warning if the
// gap between successive responses exceeds twice the expected cadence.
//
// The caller-supplied logger is expected to carry purpose and lease-id so
// those fields appear on every log line this helper emits.
func processKeepAliveResponse(
	logger *zap.Logger,
	resp *clientv3.LeaseKeepAliveResponse,
	now, lastResponseTime time.Time,
	expectedInterval time.Duration,
) (expire time.Time, valid bool) {
	var ttl int64
	if resp != nil {
		ttl = resp.TTL
	}
	if ttl <= 0 {
		logger.Warn("lease keep alive got invalid ttl",
			zap.Int64("ttl", ttl),
			zap.Time("last-response-time", lastResponseTime))
		return time.Time{}, false
	}
	if !lastResponseTime.IsZero() && expectedInterval > 0 {
		if interval := now.Sub(lastResponseTime); interval > expectedInterval*2 {
			logger.Warn("the interval between lease keep alive responses is too long",
				zap.Duration("interval", interval),
				zap.Duration("expected-interval", expectedInterval),
				zap.Int64("ttl", ttl),
				zap.Time("last-response-time", lastResponseTime))
		}
	}
	// KeepAlive responses do not carry a per-request start time in this
	// streaming model. Using arrival time makes the local estimate reflect
	// when PD actually observed the renewal, not when etcd generated it.
	return now.Add(time.Duration(ttl) * time.Second), true
}

// loadLeaseID returns the current lease ID or 0 when none has been granted.
func (l *Lease) loadLeaseID() clientv3.LeaseID {
	if v := l.ID.Load(); v != nil {
		return v.(clientv3.LeaseID)
	}
	return 0
}

// resetTimer drains t's channel (if a tick is already pending) and rearms it
// to fire after d. It is safe to call whether t has fired, is pending, or
// has been stopped.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
