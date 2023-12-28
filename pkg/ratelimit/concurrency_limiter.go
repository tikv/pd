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

import "github.com/tikv/pd/pkg/utils/syncutil"

type concurrencyLimiter struct {
	mu      syncutil.RWMutex
	current uint64
	limit   uint64

	// statistic
	maxLimit uint64
}

func newConcurrencyLimiter(limit uint64) *concurrencyLimiter {
	return &concurrencyLimiter{limit: limit}
}

const unlimit = uint64(0)

func (l *concurrencyLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.limit == unlimit || l.current+1 <= l.limit {
		l.current++
		if l.current > l.maxLimit {
			l.maxLimit = l.current
		}
		return true
	}
	return false
}

func (l *concurrencyLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.current > 0 {
		l.current--
	}
}

func (l *concurrencyLimiter) getLimit() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.limit
}

func (l *concurrencyLimiter) setLimit(limit uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.limit = limit
}

func (l *concurrencyLimiter) getCurrent() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.current
}

func (l *concurrencyLimiter) getMaxConcurrency() uint64 {
	l.mu.Lock()
	defer func() {
		l.maxLimit = l.current
		l.mu.Unlock()
	}()

	return l.maxLimit
}
