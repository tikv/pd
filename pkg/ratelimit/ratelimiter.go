// Copyright 2022 TiKV Project Authors.
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

package ratelimit

import (
	"context"
	"time"

	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

// RateLimiter is a rate limiter based on `golang.org/x/time/rate`.
// It implements `Available` function which is not included in `golang.org/x/time/rate`.
// Note: AvailableN will increase the wait time of WaitN.
type RateLimiter struct {
	mu      syncutil.Mutex
	limiter *rate.Limiter
}

// NewRateLimiter returns a new Limiter that allows events up to rate r (it means limiter refill r token per second)
// and permits bursts of at most b tokens.
func NewRateLimiter(r float64, b int) *RateLimiter {
	return &RateLimiter{limiter: rate.NewLimiter(rate.Limit(r), b)}
}

// Available returns whether limiter has enough tokens.
// Note: Available will increase the wait time of WaitN.
func (l *RateLimiter) Available(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	r := l.limiter.ReserveN(now, n)
	delay := r.DelayFrom(now)
	r.CancelAt(now)
	return delay == 0
}

// Allow is same as `rate.Limiter.Allow`.
func (l *RateLimiter) Allow() bool {
	return l.AllowN(1)
}

// AllowN is same as `rate.Limiter.AllowN`.
func (l *RateLimiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	return l.limiter.AllowN(now, n)
}

// SetBurst is shorthand for SetBurstAt(time.Now(), newBurst).
func (l *RateLimiter) SetBurst(burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limiter.SetBurst(burst)
}

// SetLimit is shorthand for SetLimitAt(time.Now(), newLimit).
func (l *RateLimiter) SetLimit(limit rate.Limit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limiter.SetLimit(limit)
}

// Limit returns the maximum overall event rate.
func (l *RateLimiter) Limit() rate.Limit {
	return l.limiter.Limit()
}

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless limit == Inf.
func (l *RateLimiter) Burst() int {
	return l.limiter.Burst()
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the Limiter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst limit is ignored if the rate limit is Inf.
func (l *RateLimiter) WaitN(ctx context.Context, n int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limiter.WaitN(ctx, n)
}
