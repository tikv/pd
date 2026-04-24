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
	// Closing an etcd lessor permanently halts its keepalive stream, while
	// existing callers still reuse Lease after Close.
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
	var leaseID clientv3.LeaseID
	if l.ID.Load() != nil {
		leaseID = l.ID.Load().(clientv3.LeaseID)
	}
	if _, err := l.lease.Revoke(ctx, leaseID); err != nil {
		log.Error("revoke lease failed", zap.String("purpose", l.Purpose), errs.ZapError(err))
	}
	err := l.lease.Close()
	l.closed.Store(true)
	return err
}

// IsExpired checks if the lease is expired. If it returns true,
// the current leader/primary should step down and try to re-elect again.
func (l *Lease) IsExpired() bool {
	if l == nil || l.expireTime.Load() == nil {
		return true
	}
	return time.Now().After(l.expireTime.Load().(time.Time))
}

// KeepAlive auto renews the lease and update expireTime.
func (l *Lease) KeepAlive(ctx context.Context) {
	defer logutil.LogPanic()

	if l == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeCh := l.keepAliveWorker(ctx, l.leaseTimeout/3)
	defer log.Info("lease keep alive stopped", zap.String("purpose", l.Purpose))

	var (
		maxExpire        time.Time
		lastResponseTime time.Time
	)
	timer := time.NewTimer(l.leaseTimeout)
	defer timer.Stop()
	for {
		select {
		case t, ok := <-timeCh:
			if !ok {
				log.Info("lease keep alive worker stopped",
					zap.String("purpose", l.Purpose),
					zap.Time("actual-expire", l.expireTime.Load().(time.Time)))
				return
			}
			now := time.Now()
			if !lastResponseTime.IsZero() {
				l.metrics.responseInterval.Observe(now.Sub(lastResponseTime).Seconds())
			}
			lastResponseTime = now
			if t.After(maxExpire) {
				maxExpire = t
				// Check again to make sure the `expireTime` still needs to be updated.
				select {
				case <-ctx.Done():
					l.metrics.contextCanceled.Inc()
					return
				default:
					l.expireTime.Store(t)
				}
			}
			l.metrics.observeRemainingTTL(maxExpire.Sub(now))
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(l.leaseTimeout)
		case <-timer.C:
			actualExpire := l.expireTime.Load().(time.Time)
			// Timer delivery can race with a keepalive response that already
			// extended expireTime. Only step down when the local lease estimate is
			// actually exhausted.
			if remaining := time.Until(actualExpire); remaining > 0 {
				log.Info("keep alive lease timer fired before local lease expired",
					zap.Duration("timeout-duration", l.leaseTimeout),
					zap.Duration("remaining", remaining),
					zap.Time("actual-expire", actualExpire),
					zap.String("purpose", l.Purpose))
				timer.Reset(remaining)
				continue
			}
			l.metrics.observeRemainingTTL(time.Until(actualExpire))
			l.metrics.leaseExpired.Inc()
			log.Info("keep alive lease too slow", zap.Duration("timeout-duration", l.leaseTimeout), zap.Time("actual-expire", actualExpire), zap.String("purpose", l.Purpose))
			return
		case <-ctx.Done():
			l.metrics.contextCanceled.Inc()
			return
		}
	}
}

// keepAliveWorker keeps the lease alive through the etcd lease keepalive stream.
func (l *Lease) keepAliveWorker(ctx context.Context, expectedInterval time.Duration) <-chan time.Time {
	// Keep the stream reader independent from the outer loop. If the outer loop
	// is delayed, the latest successful response is enough to refresh the local
	// lease estimate; older expire times would only add stale work.
	ch := make(chan time.Time, 1)

	go func() {
		defer logutil.LogPanic()
		defer close(ch)

		var leaseID clientv3.LeaseID
		if l.ID.Load() != nil {
			leaseID = l.ID.Load().(clientv3.LeaseID)
		}
		stopReason := "context_canceled"
		log.Info("start lease keep alive worker",
			zap.String("purpose", l.Purpose),
			zap.Int64("lease-id", int64(leaseID)),
			zap.Duration("lease-timeout", l.leaseTimeout),
			zap.Duration("expected-interval", expectedInterval))
		defer func() {
			log.Info("stop lease keep alive worker",
				zap.String("purpose", l.Purpose),
				zap.Int64("lease-id", int64(leaseID)),
				zap.String("reason", stopReason))
		}()

		keepAliveCh, err := l.lease.KeepAlive(ctx, leaseID)
		if err != nil {
			stopReason = "keepalive_error"
			log.Warn("lease keep alive stream failed",
				zap.String("purpose", l.Purpose),
				zap.Int64("lease-id", int64(leaseID)),
				zap.Duration("lease-timeout", l.leaseTimeout),
				zap.Duration("expected-interval", expectedInterval),
				errs.ZapError(err))
			return
		}
		var lastResponseTime time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-keepAliveCh:
				if !ok {
					// A closed keepalive channel means the etcd client can no
					// longer maintain this lease stream. Propagate that as an
					// immediate worker stop instead of waiting for the watchdog.
					if ctx.Err() == nil {
						stopReason = "channel_closed"
						log.Warn("lease keep alive channel closed",
							zap.String("purpose", l.Purpose),
							zap.Int64("lease-id", int64(leaseID)),
							zap.Time("last-response-time", lastResponseTime))
					}
					return
				}
				var ttl int64
				if res != nil {
					ttl = res.TTL
				}
				if ttl <= 0 {
					l.metrics.invalidTTL.Inc()
					// etcd reports non-positive TTL when the lease is no longer
					// valid. Continuing the local loop would hide a lost lease from
					// the election owner.
					stopReason = "invalid_ttl"
					log.Warn("lease keep alive got invalid ttl",
						zap.String("purpose", l.Purpose),
						zap.Int64("lease-id", int64(leaseID)),
						zap.Int64("ttl", ttl),
						zap.Time("last-response-time", lastResponseTime))
					return
				}
				// KeepAlive responses do not have a per-request start time in this
				// streaming model. Use arrival time so the local estimate reflects
				// when PD actually observed the renewal.
				now := time.Now()
				if lastResponseTime.IsZero() {
					log.Info("lease keep alive stream received first response",
						zap.String("purpose", l.Purpose),
						zap.Int64("lease-id", int64(leaseID)),
						zap.Int64("ttl", ttl),
						zap.Duration("expected-interval", expectedInterval))
				} else if expectedInterval > 0 {
					responseInterval := now.Sub(lastResponseTime)
					if responseInterval > expectedInterval*2 {
						log.Warn("the interval between lease keep alive responses is too long",
							zap.String("purpose", l.Purpose),
							zap.Int64("lease-id", int64(leaseID)),
							zap.Duration("interval", responseInterval),
							zap.Duration("expected-interval", expectedInterval),
							zap.Int64("ttl", ttl),
							zap.Time("last-response-time", lastResponseTime))
					}
				}
				lastResponseTime = now
				expire := now.Add(time.Duration(ttl) * time.Second)
				select {
				case oldExpire := <-ch:
					log.Warn("drop stale lease keep alive expire time because outer loop is slow",
						zap.String("purpose", l.Purpose),
						zap.Int64("lease-id", int64(leaseID)),
						zap.Time("old-expire", oldExpire),
						zap.Time("new-expire", expire),
						zap.Int64("ttl", ttl))
				default:
				}
				select {
				case ch <- expire:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch
}
