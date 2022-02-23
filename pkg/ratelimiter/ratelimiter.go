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

package ratelimiter

import (
	"sync"

	"golang.org/x/time/rate"
)

// RateLimiter is a controller for the request rate.
type RateLimiter struct {
	qpsLimiter         sync.Map
	concurrencyLimiter sync.Map
}

// NewLimiter returns a global limiter which can be updated in the later.
func NewLimiter() *RateLimiter {
	return &RateLimiter{}
}

// Allow is used to check whether it has enough token.
func (l *RateLimiter) Allow(label string) bool {
	var cl *concurrencyLimiter
	var ok bool
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		if cl, ok = limiter.(*concurrencyLimiter); ok && !cl.allow() {
			return false
		}
	}

	if limiter, exist := l.qpsLimiter.Load(label); exist {
		if ql, ok := limiter.(*rate.Limiter); ok && !ql.Allow() {
			if cl != nil {
				cl.release()
			}
			return false
		}
	}

	return true
}

// Release is used to refill token. It may be not uesful for some limiters because they will refill automatically
func (l *RateLimiter) Release(label string) {
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		if cl, ok := limiter.(*concurrencyLimiter); ok {
			cl.release()
		}
	}
}

// Update is used to update Ratelimiter with Options
func (l *RateLimiter) Update(label string, opts ...Option) {
	for _, opt := range opts {
		opt(label, l)
	}
}

// GetQPSLimiterStatus returns the status of a given label's QPS limiter.
func (l *RateLimiter) GetQPSLimiterStatus(label string) (limit rate.Limit, burst int) {
	if limiter, exist := l.qpsLimiter.Load(label); exist {
		return limiter.(*rate.Limiter).Limit(), limiter.(*rate.Limiter).Burst()
	}

	return 0, 0
}

// GetConcurrencyLimiterStatus returns the status of a given label's concurrency limiter.
func (l *RateLimiter) GetConcurrencyLimiterStatus(label string) (limit uint64, current uint64) {
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		return limiter.(*concurrencyLimiter).getLimit(), limiter.(*concurrencyLimiter).getCurrent()
	}

	return 0, 0
}
