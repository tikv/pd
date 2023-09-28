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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

func TestUpdateConcurrencyLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	opts := []Option{UpdateConcurrencyLimiter(10)}
	limiter := NewLimiter()

	label := "test"
	status := limiter.Update(label, opts...)
	re.True(status&ConcurrencyChanged != 0)
	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
		}()
	}
	wg.Wait()
	re.Equal(5, failedCount)
	re.Equal(10, successCount)
	for i := 0; i < 10; i++ {
		limiter.Release(label)
	}

	limit, current := limiter.GetConcurrencyLimiterStatus(label)
	re.Equal(uint64(10), limit)
	re.Equal(uint64(0), current)

	status = limiter.Update(label, UpdateConcurrencyLimiter(10))
	re.True(status&ConcurrencyNoChange != 0)

	status = limiter.Update(label, UpdateConcurrencyLimiter(5))
	re.True(status&ConcurrencyChanged != 0)
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(10, failedCount)
	re.Equal(5, successCount)
	for i := 0; i < 5; i++ {
		limiter.Release(label)
	}

	status = limiter.Update(label, UpdateConcurrencyLimiter(0))
	re.True(status&ConcurrencyDeleted != 0)
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(0, failedCount)
	re.Equal(15, successCount)

	limit, current = limiter.GetConcurrencyLimiterStatus(label)
	re.Equal(uint64(0), limit)
	re.Equal(uint64(0), current)
}

func TestBlockList(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	opts := []Option{AddLabelAllowList()}
	limiter := NewLimiter()
	label := "test"

	re.False(limiter.IsInAllowList(label))
	for _, opt := range opts {
		opt(label, limiter)
	}
	re.True(limiter.IsInAllowList(label))

	status := UpdateQPSLimiter(float64(rate.Every(time.Second)), 1)(label, limiter)
	re.True(status&InAllowList != 0)
	for i := 0; i < 10; i++ {
		re.True(limiter.Allow(label))
	}
}

func TestUpdateQPSLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	opts := []Option{UpdateQPSLimiter(float64(rate.Every(time.Second)), 1)}
	limiter := NewLimiter()

	label := "test"
	status := limiter.Update(label, opts...)
	re.True(status&QPSChanged != 0)

	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(2, failedCount)
	re.Equal(1, successCount)

	limit, burst := limiter.GetQPSLimiterStatus(label)
	re.Equal(rate.Limit(1), limit)
	re.Equal(1, burst)

	status = limiter.Update(label, UpdateQPSLimiter(float64(rate.Every(time.Second)), 1))
	re.True(status&QPSNoChange != 0)

	status = limiter.Update(label, UpdateQPSLimiter(5, 5))
	re.True(status&QPSChanged != 0)
	limit, burst = limiter.GetQPSLimiterStatus(label)
	re.Equal(rate.Limit(5), limit)
	re.Equal(5, burst)
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		if i < 5 {
			re.True(limiter.Allow(label))
		} else {
			re.False(limiter.Allow(label))
		}
	}
	time.Sleep(time.Second)

	status = limiter.Update(label, UpdateQPSLimiter(0, 0))
	re.True(status&QPSDeleted != 0)
	for i := 0; i < 10; i++ {
		re.True(limiter.Allow(label))
	}
	qLimit, qCurrent := limiter.GetQPSLimiterStatus(label)
	re.Equal(rate.Limit(0), qLimit)
	re.Equal(0, qCurrent)
}

func TestQPSLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	opts := []Option{UpdateQPSLimiter(float64(rate.Every(3*time.Second)), 100)}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}

	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(200, failedCount+successCount)
	re.Equal(100, failedCount)
	re.Equal(100, successCount)

	time.Sleep(4 * time.Second) // 3+1
	wg.Add(1)
	countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	wg.Wait()
	re.Equal(101, successCount)
}

func TestTwoLimiters(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg := &DimensionConfig{
		QPS:              100,
		QPSBurst:         100,
		ConcurrencyLimit: 100,
	}
	opts := []Option{UpdateDimensionConfig(cfg)}
	limiter := NewLimiter()

	label := "test"
	for _, opt := range opts {
		opt(label, limiter)
	}

	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(100, failedCount)
	re.Equal(100, successCount)
	time.Sleep(time.Second)

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(200, failedCount)
	re.Equal(100, successCount)

	for i := 0; i < 100; i++ {
		limiter.Release(label)
	}
	limiter.Update(label, UpdateQPSLimiter(float64(rate.Every(10*time.Second)), 1))
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go countRateLimiterHandleResult(limiter, label, &successCount, &failedCount, &lock, &wg)
	}
	wg.Wait()
	re.Equal(101, successCount)
	re.Equal(299, failedCount)
	limit, current := limiter.GetConcurrencyLimiterStatus(label)
	re.Equal(uint64(100), limit)
	re.Equal(uint64(1), current)
}

func countRateLimiterHandleResult(limiter *Limiter, label string, successCount *int,
	failedCount *int, lock *syncutil.Mutex, wg *sync.WaitGroup) {
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
