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
	// maxLeaseKeepAliveInterval caps the keepalive cadence at 500ms, decoupling
	// renewal frequency from the configured lease timeout. etcd's clientv3
	// keepalive uses TTL/3 with no upper bound; we deviate so that operators who
	// raise the lease TTL (e.g. to tolerate longer GC pauses) do not also slow
	// down PD's leader-failover reaction time. PD has only a handful of leases
	// per cluster, so the extra RPCs at large TTLs are negligible.
	maxLeaseKeepAliveInterval = 500 * time.Millisecond
)

// Lease is used as the low-level mechanism for campaigning and renewing elected leadership.
// The way to gain and maintain leadership is to update and keep the lease alive continuously.
type Lease struct {
	// purpose is used to show what this election for
	purpose string
	// etcd client and lease
	client *clientv3.Client
	lease  clientv3.Lease
	id     atomic.Value // store as clientv3.LeaseID
	// leaseTimeout and expireTime are used to control the lease's lifetime
	leaseTimeout time.Duration
	expireTime   atomic.Value
	// closed guards Close against being run more than once. A leadership can be
	// resigned concurrently from more than one path (e.g. a transfer API call and the
	// election loop stepping down), and a lease is single-use, so the second Close
	// must be a no-op instead of revoking an already-revoked lease and logging a
	// spurious error.
	closed  atomic.Bool
	metrics leaseMetrics
}

// NewLease creates a new Lease instance.
func NewLease(client *clientv3.Client, purpose string) *Lease {
	return &Lease{
		purpose: purpose,
		client:  client,
		lease:   clientv3.NewLease(client),
		metrics: newLeaseMetrics(purpose),
	}
}

func (l *Lease) setID(id clientv3.LeaseID) {
	if l == nil {
		return
	}
	l.id.Store(id)
}

// GetID returns the underlying etcd lease ID. It returns 0 if the lease has not
// been granted yet.
func (l *Lease) GetID() clientv3.LeaseID {
	if l == nil {
		return 0
	}
	loaded := l.id.Load()
	if loaded == nil {
		return 0
	}
	return loaded.(clientv3.LeaseID)
}

// Grant uses `lease.Grant` to initialize the lease and expireTime.
func (l *Lease) Grant(leaseTimeout int64) error {
	if l == nil {
		return errs.ErrEtcdGrantLease.GenWithStackByCause("lease is nil")
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(l.client.Ctx(), requestTimeout)
	leaseResp, err := l.lease.Grant(ctx, leaseTimeout)
	cancel()
	if err != nil {
		return errs.ErrEtcdGrantLease.Wrap(err).GenWithStackByCause()
	}
	if cost := time.Since(start); cost > slowRequestTime {
		log.Warn("lease grants too slow",
			zap.Duration("cost", cost),
			zap.String("purpose", l.purpose))
	}
	log.Info("lease granted",
		zap.Int64("lease-id", int64(leaseResp.ID)),
		zap.Int64("lease-timeout", leaseTimeout),
		zap.String("purpose", l.purpose))
	l.setID(leaseResp.ID)
	l.leaseTimeout = time.Duration(leaseTimeout) * time.Second
	l.expireTime.Store(start.Add(time.Duration(leaseResp.TTL) * time.Second))
	localTTLRemaining.register(l)
	// A lease may be re-granted after being closed; clear the close guard so the
	// next Close revokes this new grant.
	l.closed.Store(false)
	return nil
}

// Close releases the lease. It is idempotent: only the first call revokes the lease
// and tears down the client, subsequent calls are no-ops.
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	localTTLRemaining.unregister(l)
	// Reset expire time.
	l.expireTime.Store(typeutil.ZeroTime)
	// Try to revoke lease to make subsequent elections faster.
	ctx, cancel := context.WithTimeout(l.client.Ctx(), revokeLeaseTimeout)
	defer cancel()
	leaseID := l.GetID()
	if _, err := l.lease.Revoke(ctx, leaseID); err != nil {
		log.Error("revoke lease failed",
			zap.String("purpose", l.purpose),
			zap.Int64("lease-id", int64(leaseID)),
			errs.ZapError(err))
	}
	return l.lease.Close()
}

// GetTimeoutSeconds returns the configured lease timeout in seconds, i.e. the
// `leaseTimeout` the lease was granted with. It returns 0 when the lease is nil or
// not granted yet; the caller is responsible for supplying a sensible default so a
// non-positive timeout is never fed into an etcd lease grant.
func (l *Lease) GetTimeoutSeconds() int64 {
	if l == nil {
		return 0
	}
	return int64(l.leaseTimeout / time.Second)
}

// IsExpired checks if the lease is expired. If it returns true,
// the current leader/primary should step down and try to re-elect again.
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.loadExpireTime())
}

func (l *Lease) loadExpireTime() time.Time {
	if l == nil {
		return typeutil.ZeroTime
	}
	expireTime, ok := l.expireTime.Load().(time.Time)
	if !ok {
		return typeutil.ZeroTime
	}
	return expireTime
}

// getKeepAliveInterval returns the interval used to drive the keepalive ticker.
// It takes the minimum of `leaseTimeout/3` and `maxLeaseKeepAliveInterval` so the
// renewal cadence never gets slower as the lease timeout grows.
func (l *Lease) getKeepAliveInterval() time.Duration {
	interval := l.leaseTimeout / 3
	if interval > maxLeaseKeepAliveInterval {
		return maxLeaseKeepAliveInterval
	}
	return interval
}

// KeepAlive auto renews the lease and update expireTime.
func (l *Lease) KeepAlive(ctx context.Context) {
	defer logutil.LogPanic()

	if l == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeCh := l.keepAliveWorker(ctx)
	defer log.Info("lease keep alive stopped", zap.String("purpose", l.purpose))

	var (
		maxExpire        time.Time
		lastResponseTime time.Time
	)
	timer := time.NewTimer(l.leaseTimeout)
	defer timer.Stop()
	for {
		select {
		case t := <-timeCh:
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
					log.Info("lease keep alive canceled while applying keepalive response",
						zap.String("purpose", l.purpose),
						zap.Time("last-response-time", lastResponseTime),
						zap.Time("max-expire", maxExpire),
						zap.Error(ctx.Err()))
					l.metrics.contextCanceled.Inc()
					return
				default:
					l.expireTime.Store(t)
				}
			}
			timer.Reset(l.leaseTimeout)
		case <-timer.C:
			actualExpire := l.loadExpireTime()
			// The keepalive timeout can fire concurrently with a caller-initiated
			// cancellation. Treat that race as a clean shutdown so we don't
			// over-count `lease_expired`.
			if ctx.Err() != nil {
				log.Info("lease keep alive timed out during caller cancellation",
					zap.Duration("timeout-duration", l.leaseTimeout),
					zap.Time("actual-expire", actualExpire),
					zap.String("purpose", l.purpose),
					zap.Error(ctx.Err()))
				l.metrics.contextCanceled.Inc()
				return
			}
			l.metrics.leaseExpired.Inc()
			log.Info("keep alive lease too slow",
				zap.Duration("timeout-duration", l.leaseTimeout),
				zap.Time("actual-expire", actualExpire),
				zap.String("purpose", l.purpose))
			return
		case <-ctx.Done():
			log.Info("lease keep alive canceled by caller",
				zap.String("purpose", l.purpose),
				zap.Error(ctx.Err()))
			l.metrics.contextCanceled.Inc()
			return
		}
	}
}

// Periodically call `lease.KeepAliveOnce` and post back latest received expire time into the channel.
func (l *Lease) keepAliveWorker(ctx context.Context) <-chan time.Time {
	ch := make(chan time.Time)

	interval := l.getKeepAliveInterval()
	go func() {
		defer logutil.LogPanic()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		logger := log.With(zap.String("purpose", l.purpose),
			zap.Int64("lease-id", int64(l.GetID())),
			zap.Duration("interval", interval))
		logger.Info("start lease keep alive worker")
		defer logger.Info("stop lease keep alive worker")

		// Although it's very unlikely, under extreme conditions (such as multiple `KeepAliveOnce`
		// goroutines triggered in a short period of time) there may be race condition updates,
		// so atomic.Value is used here.
		var lastTime atomic.Value
		lastTime.Store(time.Now())
		for {
			// Record the start time outside the later `KeepAliveOnce` goroutine
			// to avoid being affected by the potential runtime schedule delay.
			start := time.Now()
			go func() {
				defer logutil.LogPanic()

				ctx1, cancel := context.WithTimeout(ctx, l.leaseTimeout)
				defer cancel()
				// Record the start time of the `KeepAliveOnce` request to track the request duration
				// and calculate the tick interval between consecutive `KeepAliveOnce` requests later.
				requestStart := time.Now()
				lastRequestStart, _ := lastTime.Swap(requestStart).(time.Time)
				res, err := l.lease.KeepAliveOnce(ctx1, l.GetID())

				// Record the duration of the `KeepAliveOnce` request.
				l.metrics.observeKeepAliveRequestDurationMetrics(time.Since(requestStart), err)
				// Record the interval between the consecutive `KeepAliveOnce` requests.
				tickInterval := requestStart.Sub(lastRequestStart)
				l.metrics.tickInterval.Observe(tickInterval.Seconds())
				// If the interval is too long, log a warning to indicate the potential runtime schedule delay.
				if tickInterval > interval*2 {
					logger.Warn("the interval between keeping alive lease is too long",
						zap.Time("start", start),
						zap.Time("current-time", requestStart),
						zap.Time("last-time", lastRequestStart),
						zap.Duration("tick-interval", tickInterval))
				}

				if err != nil {
					logger.Warn("lease keep alive failed",
						zap.Time("start", start),
						zap.Time("current-time", requestStart),
						errs.ZapError(err))
					return
				}
				// KeepAliveOnce currently returns ErrLeaseNotFound for a non-positive
				// TTL. Keep this as a defensive guard for mocked or custom lease
				// implementations that may return a successful response with an invalid TTL.
				if res.TTL <= 0 {
					logger.Error("lease keep alive failed with an invalid ttl value received",
						zap.Time("start", start),
						zap.Time("current-time", requestStart),
						zap.Int64("ttl", res.TTL))
					l.metrics.invalidTTL.Inc()
					return
				}
				expire := start.Add(time.Duration(res.TTL) * time.Second)
				select {
				case ch <- expire:
				// Here we don't use `ctx1.Done()` because we want to make sure if the keep alive success, we can update the expire time.
				case <-ctx.Done():
					logger.Info("lease keep alive once exit",
						zap.Time("start", start),
						zap.Time("current-time", requestStart),
						zap.Time("expire", expire))
				}
			}()

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return ch
}
