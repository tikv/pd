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

	"golang.org/x/time/rate"
)

// LimiterConfig is the config of Limiter
type LimiterConfig map[string]DimensionConfig

// NewLimiterConfig returns a new LimiterConfig
func NewLimiterConfig() LimiterConfig {
	return make(map[string]DimensionConfig)
}

// DimensionConfig is the limit dimension config of one label
type DimensionConfig struct {
	// qps conifg
	QPS      float64
	QPSBurst int
	// concurrency config
	ConcurrencyLimit uint64
}

// Limiter is a controller for the request rate.
type Limiter struct {
	configMux          sync.Mutex
	labelConfig        LimiterConfig
	qpsLimiter         sync.Map
	concurrencyLimiter sync.Map
	// the label which is in labelAllowList won't be limited
	labelAllowList map[string]struct{}
}

// NewLimiter returns a global limiter which can be updated in the later.
func NewLimiter() *Limiter {
	return &Limiter{
		labelAllowList: make(map[string]struct{}),
		labelConfig:    NewLimiterConfig(),
	}
}

// Allow is used to check whether it has enough token.
func (l *Limiter) Allow(label string) bool {
	var cl *concurrencyLimiter
	var ok bool
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		if cl, ok = limiter.(*concurrencyLimiter); ok && !cl.allow() {
			return false
		}
	}

	if limiter, exist := l.qpsLimiter.Load(label); exist {
		if ql, ok := limiter.(*RateLimiter); ok && !ql.Allow() {
			if cl != nil {
				cl.release()
			}
			return false
		}
	}

	return true
}

// Release is used to refill token. It may be not uesful for some limiters because they will refill automatically
func (l *Limiter) Release(label string) {
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		if cl, ok := limiter.(*concurrencyLimiter); ok {
			cl.release()
		}
	}
}

// Update is used to update Ratelimiter with Options
func (l *Limiter) Update(label string, opts ...Option) UpdateStatus {
	var status UpdateStatus
	for _, opt := range opts {
		status |= opt(label, l)
	}
	return status
}

// GetQPSLimiterStatus returns the status of a given label's QPS limiter.
func (l *Limiter) GetQPSLimiterStatus(label string) (limit rate.Limit, burst int) {
	if limiter, exist := l.qpsLimiter.Load(label); exist {
		return limiter.(*RateLimiter).Limit(), limiter.(*RateLimiter).Burst()
	}

	return 0, 0
}

// deleteQPSLimiter deletes QPS limiter of a given label
func (l *Limiter) deleteQPSLimiter(label string) {
	l.qpsLimiter.Delete(label)
}

// GetConcurrencyLimiterStatus returns the status of a given label's concurrency limiter.
func (l *Limiter) GetConcurrencyLimiterStatus(label string) (limit uint64, current uint64) {
	if limiter, exist := l.concurrencyLimiter.Load(label); exist {
		return limiter.(*concurrencyLimiter).getLimit(), limiter.(*concurrencyLimiter).getCurrent()
	}

	return 0, 0
}

// deleteConcurrencyLimiter deletes concurrency limiter of a given label
func (l *Limiter) deleteConcurrencyLimiter(label string) {
	l.concurrencyLimiter.Delete(label)
}

// IsInAllowList returns whether this label is in allow list.
// If returns true, the given label won't be limited
func (l *Limiter) IsInAllowList(label string) bool {
	_, allow := l.labelAllowList[label]
	return allow
}
