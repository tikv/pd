// Copyright 2023 TiKV Project Authors.
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

// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package controller

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has one main methods Reserve.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it,
// or its associated context.Context is canceled.
//
// Some changes about burst(b):
//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
//   - If b < 0, that means the limiter is unlimited capacity and r is ignored, can be seen as r == Inf (burst within an unlimited capacity).
//   - If b > 0, that means the limiter is limited capacity.
type Limiter struct {
	mu     sync.Mutex
	limit  Limit
	tokens float64
	burst  int64
	// last is the last time the limiter's tokens field was updated
	last                time.Time
	notifyThreshold     float64
	lowTokensNotifyChan chan<- notifyMsg
	// To prevent too many chan sent, the notifyThreshold is set to 0 after notify.
	// So the notifyThreshold cannot show whether the limiter is in the low token state,
	// isLowProcess is used to check it.
	isLowProcess bool
	// remainingNotifyTimes is used to limit notify when the speed limit is already set.
	remainingNotifyTimes int
	name                 string

	// metrics
	metrics *limiterMetricsCollection
}

// notifyMsg is a message to notify the low token state.
type notifyMsg struct {
	startTime time.Time
}

// limiterMetricsCollection is a collection of metrics for a limiter.
type limiterMetricsCollection struct {
	lowTokenNotifyCounter prometheus.Counter
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(now time.Time, r Limit, b int64, tokens float64, lowTokensNotifyChan chan<- notifyMsg) *Limiter {
	lim := &Limiter{
		limit:               r,
		last:                now,
		tokens:              tokens,
		burst:               b,
		lowTokensNotifyChan: lowTokensNotifyChan,
	}
	log.Debug("new limiter", zap.String("limiter", fmt.Sprintf("%+v", lim)))
	return lim
}

// NewLimiterWithCfg returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiterWithCfg(name string, now time.Time, cfg tokenBucketReconfigureArgs, lowTokensNotifyChan chan<- notifyMsg) *Limiter {
	lim := &Limiter{
		name:                name,
		limit:               Limit(cfg.NewRate),
		last:                now,
		tokens:              cfg.NewTokens,
		burst:               cfg.NewBurst,
		notifyThreshold:     cfg.NotifyThreshold,
		lowTokensNotifyChan: lowTokensNotifyChan,
	}
	lim.metrics = &limiterMetricsCollection{
		lowTokenNotifyCounter: lowTokenRequestNotifyCounter.WithLabelValues(lim.name),
	}
	log.Debug("new limiter", zap.String("limiter", fmt.Sprintf("%+v", lim)))
	return lim
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok               bool
	lim              *Limiter
	tokens           float64
	timeToAct        time.Time
	needWaitDuration time.Duration
	// This is the Limit at reservation time, it can change later.
	limit           Limit
	remainingTokens float64
	err             error
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(1<<63 - 1)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the limiter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses tokens which be refilled into limiter.
func (r *Reservation) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	if r.tokens == 0 || r.lim.burst < 0 || r.lim.limit == Inf {
		return
	}
	// advance time to now
	now, _, tokens := r.lim.advance(now)
	// calculate new number of tokens
	tokens += r.tokens

	// update state
	r.lim.last = now
	r.lim.tokens = tokens
}

// Reserve returns a Reservation that indicates how long the caller must wait before n events happen.
// The Limiter takes this Reservation into account when allowing future events.
// The returned Reservation's OK() method returns false if wait duration exceeds deadline.
// Usage example:
//
//	r := lim.Reserve(time.Now(), 1)
//	if !r.OK() {
//	  // Not allowed to act! Did you remember to set lim.burst to be > 0 ?
//	  return
//	}
//	time.Sleep(r.Delay())
//	Act()
//
// Use this method if you wish to wait and slow down in accordance with the rate limit without dropping events.
func (lim *Limiter) Reserve(ctx context.Context, waitDuration time.Duration, now time.Time, n float64) *Reservation {
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return &Reservation{
			ok:  false,
			err: ctx.Err(),
		}
	default:
	}
	// Determine wait limit
	waitLimit := waitDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}
	r := lim.reserveN(now, n, waitLimit)
	return &r
}

// SetupNotificationThreshold enables the notification at the given threshold.
func (lim *Limiter) SetupNotificationThreshold(threshold float64) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.notifyThreshold = threshold
}

// SetName sets the name of the limiter.
func (lim *Limiter) SetName(name string) *Limiter {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.name = name
	return lim
}

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure or StartNotification).
func (lim *Limiter) notify() {
	if lim.isLowProcess {
		return
	}
	lim.notifyThreshold = 0
	lim.isLowProcess = true
	select {
	case lim.lowTokensNotifyChan <- notifyMsg{startTime: time.Now()}:
		if lim.metrics != nil {
			lim.metrics.lowTokenNotifyCounter.Inc()
		}
	default:
	}
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.
func (lim *Limiter) maybeNotify() {
	if lim.isLowTokensLocked() {
		lim.notify()
	}
}

func (lim *Limiter) isLowTokensLocked() bool {
	if lim.burst >= 0 && (lim.isLowProcess || (lim.notifyThreshold > 0 && lim.tokens < lim.notifyThreshold)) {
		return true
	}
	return false
}

// IsLowTokens returns whether the limiter is in low tokens
func (lim *Limiter) IsLowTokens() bool {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.isLowTokensLocked()
}

// GetBurst returns the burst size of the limiter
func (lim *Limiter) GetBurst() int64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.burst
}

// RemoveTokens decreases the amount of tokens currently available.
func (lim *Limiter) RemoveTokens(now time.Time, amount float64) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if lim.burst < 0 || lim.limit == Inf {
		return
	}
	now, _, tokens := lim.advance(now)
	lim.last = now
	lim.tokens = tokens - amount
	lim.maybeNotify()
}

type tokenBucketReconfigureArgs struct {
	NewTokens       float64
	NewRate         float64
	NewBurst        int64
	NotifyThreshold float64
}

// LimiterOption configures Limiter.
type LimiterOption func(*Limiter)

func resetLowProcess() func(*Limiter) {
	return func(limiter *Limiter) {
		limiter.isLowProcess = false
	}
}

// Reconfigure modifies all setting for limiter
func (lim *Limiter) Reconfigure(now time.Time,
	args tokenBucketReconfigureArgs,
	opts ...LimiterOption,
) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	logControllerTrace("[resource group controller] before reconfigure", zap.String("name", lim.name), zap.Float64("old-tokens", lim.tokens), zap.Float64("old-rate", float64(lim.limit)), zap.Float64("old-notify-threshold", lim.notifyThreshold), zap.Int64("old-burst", lim.burst))
	if args.NewBurst < 0 {
		lim.last = now
		lim.tokens = args.NewTokens
	} else {
		now, _, tokens := lim.advance(now)
		lim.last = now
		lim.tokens = tokens + args.NewTokens
	}
	lim.limit = Limit(args.NewRate)
	lim.burst = args.NewBurst
	lim.notifyThreshold = args.NotifyThreshold
	for _, opt := range opts {
		opt(lim)
	}
	lim.maybeNotify()
	logControllerTrace("[resource group controller] after reconfigure", zap.String("name", lim.name), zap.Float64("tokens", lim.tokens), zap.Float64("rate", float64(lim.limit)), zap.Float64("notify-threshold", args.NotifyThreshold), zap.Int64("burst", lim.burst))
}

// AvailableTokens decreases the amount of tokens currently available.
func (lim *Limiter) AvailableTokens(now time.Time) float64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	_, _, tokens := lim.advance(now)
	return tokens
}

func (lim *Limiter) updateLast(t time.Time) {
	// make sure lim.last is monotonic
	// see issue: https://github.com/tikv/pd/issues/8435.
	if lim.last.Before(t) {
		lim.last = t
	}
}

const reserveWarnLogInterval = 10 * time.Millisecond

// reserveN is a helper method for Reserve.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation.
func (lim *Limiter) reserveN(now time.Time, n float64, maxFutureReserve time.Duration) Reservation {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	if lim.burst < 0 || lim.limit == Inf {
		return Reservation{
			ok:        true,
			lim:       lim,
			tokens:    n,
			timeToAct: now,
		}
	}
	now, last, tokens := lim.advance(now)

	// Calculate the remaining number of tokens resulting from the request.
	tokens -= n
	// Calculate the wait duration
	var waitDuration time.Duration
	if tokens < 0 {
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}

	// Decide result
	ok := waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:               ok,
		lim:              lim,
		limit:            lim.limit,
		needWaitDuration: waitDuration,
		remainingTokens:  tokens,
	}
	if ok {
		r.tokens = n
		r.timeToAct = now.Add(waitDuration)
	}
	// Update state
	if ok {
		lim.updateLast(now)
		lim.tokens = tokens
		lim.maybeNotify()
	} else {
		// print log if the limiter cannot reserve for a while.
		if time.Since(lim.last) > reserveWarnLogInterval {
			log.Warn("[resource group controller] cannot reserve enough tokens",
				zap.Duration("need-wait-duration", waitDuration),
				zap.Duration("max-wait-duration", maxFutureReserve),
				zap.Float64("current-ltb-tokens", lim.tokens),
				zap.Float64("current-ltb-rate", float64(lim.limit)),
				zap.Float64("request-tokens", n),
				zap.Float64("notify-threshold", lim.notifyThreshold),
				zap.Bool("is-low-process", lim.isLowProcess),
				zap.Int64("burst", lim.burst),
				zap.Int("remaining-notify-times", lim.remainingNotifyTimes),
				zap.String("name", lim.name))
		}
		lim.updateLast(last)
		if lim.limit == 0 {
			lim.notify()
		} else if lim.remainingNotifyTimes > 0 {
			// When fillrate is greater than 0, the speed limit is already set.
			// If limiter are in limit state, the server has allocated tokens as much as possible. Don't need to request tokens.
			// But there is a special case, see issue https://github.com/tikv/pd/issues/6300.
			lim.remainingNotifyTimes--
			lim.notify()
		}
	}
	return r
}

// ResetRemainingNotifyTimes resets the remaining notify times to 3.
func (lim *Limiter) ResetRemainingNotifyTimes() {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.remainingNotifyTimes = 3
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func (lim *Limiter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}
	if lim.burst < 0 {
		return now, last, lim.tokens
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := now.Sub(last)
	delta := lim.limit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if lim.burst > 0 {
		if burst := float64(lim.burst); tokens > burst {
			tokens = burst
		}
	}
	return now, last, tokens
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	if limit <= 0 {
		return InfDuration
	}
	seconds := tokens / float64(limit)
	return time.Duration(float64(time.Second) * seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	if limit <= 0 {
		return 0
	}
	return d.Seconds() * float64(limit)
}

// WaitReservations is used to process a series of reservations
// so that all limiter tokens are returned if one reservation fails
func WaitReservations(ctx context.Context, now time.Time, reservations []*Reservation) (time.Duration, error) {
	if len(reservations) == 0 {
		return 0, nil
	}
	cancel := func() {
		for _, res := range reservations {
			res.CancelAt(now)
		}
	}
	longestDelayDuration := time.Duration(0)
	for _, res := range reservations {
		if !res.ok {
			cancel()
			if res.err != nil {
				return res.needWaitDuration, res.err
			}
			return res.needWaitDuration, errs.ErrClientResourceGroupThrottled.FastGenByArgs(res.needWaitDuration, res.limit, res.remainingTokens)
		}
		delay := res.DelayFrom(now)
		if delay > longestDelayDuration {
			longestDelayDuration = delay
		}
	}
	if longestDelayDuration <= 0 {
		return 0, nil
	}
	t := time.NewTimer(longestDelayDuration)
	defer t.Stop()

	select {
	case <-t.C:
		// We can proceed.
		return longestDelayDuration, nil
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		cancel()
		return 0, ctx.Err()
	}
}
