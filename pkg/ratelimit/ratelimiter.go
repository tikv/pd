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
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter is a rate limiter based on `golang.org/x/time/rate`.
// It implements `Available` function which is not included in `golang.org/x/time/rate`.
// Note: AvailableN will increase the wait time of WaitN.
type RateLimiter struct {
	mu sync.Mutex
	*rate.Limiter
}

// NewRateLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewRateLimiter(r float64, b int) *RateLimiter {
	return &RateLimiter{Limiter: rate.NewLimiter(rate.Limit(r), b)}
}

// Available is shorthand for AvailableN(1).
func (l *RateLimiter) Available() bool {
	return l.AvailableN(1)
}

// AvailableN returns whether limiter has enough tokens.
// Note: AvailableN will increase the wait time of WaitN.
func (l *RateLimiter) AvailableN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	r := l.Limiter.ReserveN(now, n)
	delay := r.Delay()
	r.CancelAt(now)
	return delay <= 0
}

// Allow is same as `rate.Limiter.Allow`.
func (l *RateLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Limiter.Allow()
}

// AllowN is same as `rate.Limiter.AllowN`.
func (l *RateLimiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	return l.Limiter.AllowN(now, n)
}
