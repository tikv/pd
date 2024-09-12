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
	queue    chan *TaskToken
}

// NewConcurrencyLimiter creates a new ConcurrencyLimiter.
func NewConcurrencyLimiter(limit uint64) *ConcurrencyLimiter {
	return &ConcurrencyLimiter{limit: limit, queue: make(chan *TaskToken, limit)}
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
	l.mu.Lock()
	if l.current >= l.limit {
		l.waiting++
		l.mu.Unlock()
		// block the waiting task on the caller goroutine
		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.waiting--
			l.mu.Unlock()
			return nil, ctx.Err()
		case token := <-l.queue:
			l.mu.Lock()
			token.released = false
			l.current++
			l.waiting--
			l.mu.Unlock()
			return token, nil
		}
	}
	l.current++
	token := &TaskToken{}
	l.mu.Unlock()
	return token, nil
}

// ReleaseToken releases the token.
func (l *ConcurrencyLimiter) ReleaseToken(token *TaskToken) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if token.released {
		return
	}
	token.released = true
	l.current--
	if len(l.queue) < int(l.limit) {
		l.queue <- token
	}
}

// TaskToken is a token that must be released after the task is done.
type TaskToken struct {
	released bool
}
