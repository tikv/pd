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

package ratelimit

import (
	"math"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

// DoneFunc is done function.
type DoneFunc func()

// DimensionConfig is the limit dimension config of one label
type DimensionConfig struct {
	// qps conifg
	QPS      float64
	QPSBurst int
	// concurrency config
	ConcurrencyLimit uint64
}

type limiter struct {
	mu          syncutil.RWMutex
	concurrency *concurrencyLimiter
	rate        *RateLimiter
}

func newLimiter() *limiter {
	lim := &limiter{}
	return lim
}

func (l *limiter) getConcurrencyLimiter() *concurrencyLimiter {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.concurrency
}

func (l *limiter) getRateLimiter() *RateLimiter {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.rate
}

func (l *limiter) deleteRateLimiter() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rate = nil
	return l.isEmpty()
}

func (l *limiter) deleteConcurrency() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.concurrency = nil
	return l.isEmpty()
}

func (l *limiter) isEmpty() bool {
	return l.concurrency == nil && l.rate == nil
}

func (l *limiter) getQPSLimiterStatus() (limit rate.Limit, burst int) {
	baseLimiter := l.getRateLimiter()
	if baseLimiter != nil {
		return baseLimiter.Limit(), baseLimiter.Burst()
	}
	return 0, 0
}

func (l *limiter) getConcurrencyLimiterStatus() (limit uint64, current uint64) {
	baseLimiter := l.getConcurrencyLimiter()
	if baseLimiter != nil {
		return baseLimiter.getLimit(), baseLimiter.getCurrent()
	}
	return 0, 0
}

func (l *limiter) updateConcurrencyConfig(limit uint64) UpdateStatus {
	oldConcurrencyLimit, _ := l.getConcurrencyLimiterStatus()
	if oldConcurrencyLimit == limit {
		return ConcurrencyNoChange
	}
	if limit < 1 {
		l.deleteConcurrency()
		return ConcurrencyDeleted
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.concurrency != nil {
		l.concurrency.setLimit(limit)
	} else {
		l.concurrency = newConcurrencyLimiter(limit)
	}
	return ConcurrencyChanged
}

func (l *limiter) updateQPSConfig(limit float64, burst int) UpdateStatus {
	oldQPSLimit, oldBurst := l.getQPSLimiterStatus()
	if math.Abs(float64(oldQPSLimit)-limit) < eps && oldBurst == burst {
		return QPSNoChange
	}
	if limit <= eps || burst < 1 {
		l.deleteRateLimiter()
		return QPSDeleted
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate != nil {
		l.rate.SetLimit(rate.Limit(limit))
		l.rate.SetBurst(burst)
	} else {
		l.rate = NewRateLimiter(limit, burst)
	}
	return QPSChanged
}

func (l *limiter) updateDimensionConfig(cfg *DimensionConfig) UpdateStatus {
	status := l.updateQPSConfig(cfg.QPS, cfg.QPSBurst)
	status |= l.updateConcurrencyConfig(cfg.ConcurrencyLimit)
	return status
}

func (l *limiter) allow() (DoneFunc, error) {
	concurrency := l.getConcurrencyLimiter()
	if concurrency != nil && !concurrency.allow() {
		return nil, errs.ErrRateLimitExceeded
	}

	rate := l.getRateLimiter()
	if rate != nil && !rate.Allow() {
		if concurrency != nil {
			concurrency.release()
		}
		return nil, errs.ErrRateLimitExceeded
	}
	return func() {
		if concurrency != nil {
			concurrency.release()
		}
	}, nil
}
