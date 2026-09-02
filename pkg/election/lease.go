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
	"strings"
	"sync/atomic"
	"time"

<<<<<<< HEAD
=======
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
>>>>>>> 703f4bb25d (server, member, election: document and test the election client pinning (#11110))
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
<<<<<<< HEAD
	Purpose string
=======
	purpose string
	// name scopes test failpoints to one member.
	name string
>>>>>>> 703f4bb25d (server, member, election: document and test the election client pinning (#11110))
	// etcd client and lease
	client *clientv3.Client
	lease  clientv3.Lease
	ID     atomic.Value // store as clientv3.LeaseID
	// leaseTimeout and expireTime are used to control the lease's lifetime
	leaseTimeout time.Duration
	expireTime   atomic.Value
}

// NewLease creates a new Lease instance.
func NewLease(client *clientv3.Client, purpose, name string) *Lease {
	return &Lease{
<<<<<<< HEAD
		Purpose: purpose,
=======
		purpose: purpose,
		name:    name,
>>>>>>> 703f4bb25d (server, member, election: document and test the election client pinning (#11110))
		client:  client,
		lease:   clientv3.NewLease(client),
	}
}

<<<<<<< HEAD
=======
// matchesFailpointTarget matches "<purpose>@<name>" for member-scoped failpoints.
func (l *Lease) matchesFailpointTarget(val failpoint.Value) bool {
	target, ok := val.(string)
	if !ok {
		return false
	}
	purpose, name, found := strings.Cut(target, "@")
	if !found {
		return false
	}
	return purpose == l.purpose && name == l.name
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

>>>>>>> 703f4bb25d (server, member, election: document and test the election client pinning (#11110))
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
	return l.lease.Close()
}

// IsExpired checks if the lease is expired. If it returns true,
// current leader should step down and try to re-elect again.
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

	var maxExpire time.Time
	timer := time.NewTimer(l.leaseTimeout)
	defer timer.Stop()
	for {
		select {
		case t := <-timeCh:
			if t.After(maxExpire) {
				maxExpire = t
				// Check again to make sure the `expireTime` still needs to be updated.
				select {
				case <-ctx.Done():
					return
				default:
					l.expireTime.Store(t)
				}
			}
			timer.Reset(l.leaseTimeout)
		case <-timer.C:
			log.Info("keep alive lease too slow", zap.Duration("timeout-duration", l.leaseTimeout), zap.Time("actual-expire", l.expireTime.Load().(time.Time)), zap.String("purpose", l.Purpose))
			return
		case <-ctx.Done():
			return
		}
	}
}

// Periodically call `lease.KeepAliveOnce` and post back latest received expire time into the channel.
func (l *Lease) keepAliveWorker(ctx context.Context, interval time.Duration) <-chan time.Time {
	ch := make(chan time.Time)

	go func() {
		defer logutil.LogPanic()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Info("start lease keep alive worker", zap.Duration("interval", interval), zap.String("purpose", l.Purpose))
		defer log.Info("stop lease keep alive worker", zap.String("purpose", l.Purpose))
		lastTime := time.Now()
		for {
			start := time.Now()
			if start.Sub(lastTime) > interval*2 {
				log.Warn("the interval between keeping alive lease is too long", zap.Time("last-time", lastTime))
			}
			go func(start time.Time) {
				defer logutil.LogPanic()
				ctx1, cancel := context.WithTimeout(ctx, l.leaseTimeout)
				defer cancel()
<<<<<<< HEAD
				var leaseID clientv3.LeaseID
				if l.ID.Load() != nil {
					leaseID = l.ID.Load().(clientv3.LeaseID)
=======
				// Record the start time of the `KeepAliveOnce` request to track the request duration
				// and calculate the tick interval between consecutive `KeepAliveOnce` requests later.
				requestStart := time.Now()
				lastRequestStart, _ := lastTime.Swap(requestStart).(time.Time)
				res, err := l.lease.KeepAliveOnce(ctx1, l.GetID())
				failpoint.Inject("keepAliveFailed", func(val failpoint.Value) {
					// Inject after the request so etcd keeps the lease alive while
					// the caller observes renewal failure.
					if l.matchesFailpointTarget(val) {
						res, err = nil, errors.New("keep alive failed")
					}
				})

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
>>>>>>> 703f4bb25d (server, member, election: document and test the election client pinning (#11110))
				}
				res, err := l.lease.KeepAliveOnce(ctx1, leaseID)
				if err != nil {
					log.Warn("lease keep alive failed", zap.String("purpose", l.Purpose), zap.Time("start", start), errs.ZapError(err))
					return
				}
				if res.TTL > 0 {
					expire := start.Add(time.Duration(res.TTL) * time.Second)
					select {
					case ch <- expire:
					// Here we don't use `ctx1.Done()` because we want to make sure if the keep alive success, we can update the expire time.
					case <-ctx.Done():
					}
				} else {
					log.Error("keep alive response ttl is zero", zap.String("purpose", l.Purpose))
				}
			}(start)

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lastTime = start
			}
		}
	}()

	return ch
}
