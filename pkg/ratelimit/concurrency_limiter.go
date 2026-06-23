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

	"github.com/tikv/pd/pkg/utils/syncutil"
)

// ConcurrencyLimiter is a limiter that limits the number of concurrent tasks.
type ConcurrencyLimiter struct {
	mu      syncutil.Mutex
	current uint64
	waiting uint64
	limit   uint64

	// statistic
	maxLimit uint64
	queue    chan struct{} // wake-up signals for waiting tasks
}

// NewConcurrencyLimiter creates a new ConcurrencyLimiter.
func NewConcurrencyLimiter(limit uint64) *ConcurrencyLimiter {
	return &ConcurrencyLimiter{limit: limit, queue: make(chan struct{}, limit)}
}

const unlimit = uint64(0)

// old interface. only used in the ratelimiter package.
func (l *ConcurrencyLimiter) allow() bool {
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

// old interface. only used in the ratelimiter package.
func (l *ConcurrencyLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.current > 0 {
		l.current--
	}
}

// old interface. only used in the ratelimiter package.
func (l *ConcurrencyLimiter) getLimit() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.limit
}

// old interface. only used in the ratelimiter package.
func (l *ConcurrencyLimiter) setLimit(limit uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.limit = limit
}

// GetRunningTasksNum returns the number of running tasks.
func (l *ConcurrencyLimiter) GetRunningTasksNum() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.current
}

// old interface. only used in the ratelimiter package.
func (l *ConcurrencyLimiter) getMaxConcurrency() uint64 {
	l.mu.Lock()
	defer func() {
		l.maxLimit = l.current
		l.mu.Unlock()
	}()

	return l.maxLimit
}

// GetWaitingTasksNum returns the number of waiting tasks.
func (l *ConcurrencyLimiter) GetWaitingTasksNum() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waiting
}

// AcquireToken acquires a token from the limiter. which will block until a token is available or ctx is done, like Timeout.
func (l *ConcurrencyLimiter) AcquireToken(ctx context.Context) (*TaskToken, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if l.limit == unlimit || l.current < l.limit {
		l.current++
		l.mu.Unlock()
		return &TaskToken{}, nil
	}
	l.waiting++
	l.mu.Unlock()

	// Block the waiting task on the caller goroutine.
	for {
		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.waiting--
			l.mu.Unlock()
			return nil, ctx.Err()
		case <-l.queue:
			l.mu.Lock()
			if err := ctx.Err(); err != nil {
				l.waiting--
				shouldWakeUp := l.waiting > 0 && (l.limit == unlimit || l.current < l.limit)
				l.mu.Unlock()
				if shouldWakeUp {
					select {
					case l.queue <- struct{}{}:
					default:
					}
				}
				return nil, err
			}
			if l.limit == unlimit || l.current < l.limit {
				l.waiting--
				l.current++
				l.mu.Unlock()
				return &TaskToken{}, nil
			}
			l.mu.Unlock()
		}
	}
}

// ReleaseToken releases the token.
func (l *ConcurrencyLimiter) ReleaseToken(token *TaskToken) {
	l.mu.Lock()
	if token.released {
		l.mu.Unlock()
		return
	}
	token.released = true
	l.current--
	shouldWakeUp := l.waiting > 0 && (l.limit == unlimit || l.current < l.limit)
	l.mu.Unlock()
	if shouldWakeUp {
		select {
		case l.queue <- struct{}{}:
		default:
		}
	}
}

// TaskToken is a token that must be released after the task is done.
type TaskToken struct {
	released bool
}
