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

	. "github.com/pingcap/check"
	"golang.org/x/time/rate"
)

var _ = Suite(&testRatelimiterSuite{})

type testRatelimiterSuite struct {
}

func (s *testRatelimiterSuite) TestUpdateConcurrencyLimiter(c *C) {
	c.Parallel()

	opts := []Option{UpdateConcurrencyLimiter(10)}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}
	var lock sync.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
		}()
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 5)
	c.Assert(successCount, Equals, 10)
	for i := 0; i < 10; i++ {
		limiter.Release(label)
	}

	limit, current := limiter.GetConcurrencyLimiterStatus(label)
	c.Assert(limit, Equals, uint64(10))
	c.Assert(current, Equals, uint64(0))

	limiter.Update(label, UpdateConcurrencyLimiter(5))
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 10)
	c.Assert(successCount, Equals, 5)
	for i := 0; i < 5; i++ {
		limiter.Release(label)
	}

	limiter.DeleteConcurrencyLimiter(label)
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 0)
	c.Assert(successCount, Equals, 15)

	limit, current = limiter.GetConcurrencyLimiterStatus(label)
	c.Assert(limit, Equals, uint64(0))
	c.Assert(current, Equals, uint64(0))
}

func (s *testRatelimiterSuite) TestBlockList(c *C) {
	c.Parallel()
	opts := []Option{AddLabelAllowList()}
	limiter := NewLimiter()
	label := "test"

	c.Assert(limiter.IsInAllowList(label), Equals, false)
	for _, opt := range opts {
		opt(label, limiter)
	}
	c.Assert(limiter.IsInAllowList(label), Equals, true)

	UpdateQPSLimiter(rate.Every(time.Second), 1)(label, limiter)
	for i := 0; i < 10; i++ {
		c.Assert(limiter.Allow(label), Equals, true)
	}
}

func (s *testRatelimiterSuite) TestUpdateQPSLimiter(c *C) {
	c.Parallel()
	opts := []Option{UpdateQPSLimiter(rate.Every(time.Second), 1)}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}

	var lock sync.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 2)
	c.Assert(successCount, Equals, 1)

	limit, burst := limiter.GetQPSLimiterStatus(label)
	c.Assert(limit, Equals, rate.Limit(1))
	c.Assert(burst, Equals, 1)

	limiter.Update(label, UpdateQPSLimiter(5, 5))
	limit, burst = limiter.GetQPSLimiterStatus(label)
	c.Assert(limit, Equals, rate.Limit(5))
	c.Assert(burst, Equals, 5)
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		if i < 5 {
			c.Assert(limiter.Allow(label), Equals, true)
		} else {
			c.Assert(limiter.Allow(label), Equals, false)
		}
	}
	time.Sleep(time.Second)
	limiter.DeleteQPSLimiter(label)
	for i := 0; i < 10; i++ {
		c.Assert(limiter.Allow(label), Equals, true)
	}
	qLimit, qCurrent := limiter.GetQPSLimiterStatus(label)
	c.Assert(qLimit, Equals, rate.Limit(0))
	c.Assert(qCurrent, Equals, 0)
}

func (s *testRatelimiterSuite) TestQPSLimiter(c *C) {
	c.Parallel()
	opts := []Option{UpdateQPSLimiter(rate.Every(3*time.Second), 100)}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}

	var lock sync.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount+successCount, Equals, 200)
	c.Assert(failedCount, Equals, 100)
	c.Assert(successCount, Equals, 100)

	time.Sleep(4 * time.Second) // 3+1
	wg.Add(1)
	CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	wg.Wait()
	c.Assert(successCount, Equals, 101)
}

func (s *testRatelimiterSuite) TestTwoLimiters(c *C) {
	c.Parallel()
	opts := []Option{UpdateQPSLimiter(100, 100),
		UpdateConcurrencyLimiter(100),
	}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}

	var lock sync.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 100)
	c.Assert(successCount, Equals, 100)
	time.Sleep(1 * time.Second)

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(failedCount, Equals, 200)
	c.Assert(successCount, Equals, 100)

	for i := 0; i < 100; i++ {
		limiter.Release(label)
	}
	limiter.Update(label, UpdateQPSLimiter(rate.Every(10*time.Second), 1))
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go CountRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	c.Assert(successCount, Equals, 101)
	c.Assert(failedCount, Equals, 299)
	limit, current := limiter.GetConcurrencyLimiterStatus(label)
	c.Assert(limit, Equals, uint64(100))
	c.Assert(current, Equals, uint64(1))
}

func CountRateLimiterHandleResult(limiter *Limiter, label string, successCount *int,
	failedCount *int, lock *sync.Mutex, wg *sync.WaitGroup) {
	result := limiter.Allow(label)
	lock.Lock()
	defer lock.Unlock()
	if result {
		*successCount++
	} else {
		*failedCount++
	}
	wg.Done()
}
