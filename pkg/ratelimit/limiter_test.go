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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

type releaseUtil struct {
	dones []DoneFunc
}

func (r *releaseUtil) release() {
	if len(r.dones) > 0 {
		r.dones[0]()
		r.dones = r.dones[1:]
	}
}

func (r *releaseUtil) append(d DoneFunc) {
	r.dones = append(r.dones, d)
}

func TestWithConcurrencyLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	limiter := newLimiter()
	status := limiter.updateConcurrencyConfig(10)
	re.NotZero(status & ConcurrencyChanged)
	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	r := &releaseUtil{}
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
		}()
	}
	wg.Wait()
	re.Equal(5, failedCount)
	re.Equal(10, successCount)
	for i := 0; i < 10; i++ {
		r.release()
	}

	limit, current := limiter.getConcurrencyLimiterStatus()
	re.Equal(uint64(10), limit)
	re.Equal(uint64(0), current)

	status = limiter.updateConcurrencyConfig(10)
	re.NotZero(status & ConcurrencyNoChange)

	status = limiter.updateConcurrencyConfig(5)
	re.NotZero(status & ConcurrencyChanged)
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(10, failedCount)
	re.Equal(5, successCount)
	for i := 0; i < 5; i++ {
		r.release()
	}

	status = limiter.updateConcurrencyConfig(0)
	re.NotZero(status & ConcurrencyDeleted)
	failedCount = 0
	successCount = 0
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(0, failedCount)
	re.Equal(15, successCount)

	limit, current = limiter.getConcurrencyLimiterStatus()
	re.Equal(uint64(0), limit)
	re.Equal(uint64(15), current)
}

func TestWithQPSLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	limiter := newLimiter()
	status := limiter.updateQPSConfig(float64(rate.Every(time.Second)), 1)
	re.NotZero(status & QPSChanged)

	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	r := &releaseUtil{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(2, failedCount)
	re.Equal(1, successCount)

	limit, burst := limiter.getQPSLimiterStatus()
	re.Equal(rate.Limit(1), limit)
	re.Equal(1, burst)

	status = limiter.updateQPSConfig(float64(rate.Every(time.Second)), 1)
	re.NotZero(status & QPSNoChange)

	status = limiter.updateQPSConfig(5, 5)
	re.NotZero(status & QPSChanged)
	limit, burst = limiter.getQPSLimiterStatus()
	re.Equal(rate.Limit(5), limit)
	re.Equal(5, burst)
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		if i < 5 {
			_, err := limiter.allow()
			re.NoError(err)
		} else {
			_, err := limiter.allow()
			re.Error(err)
		}
	}
	time.Sleep(time.Second)

	status = limiter.updateQPSConfig(0, 0)
	re.NotZero(status & QPSDeleted)
	for i := 0; i < 10; i++ {
		_, err := limiter.allow()
		re.NoError(err)
	}
	qLimit, qCurrent := limiter.getQPSLimiterStatus()
	re.Equal(rate.Limit(0), qLimit)
	re.Zero(qCurrent)

	successCount = 0
	failedCount = 0
	status = limiter.updateQPSConfig(float64(rate.Every(3*time.Second)), 100)
	re.NotZero(status & QPSChanged)
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(200, failedCount+successCount)
	re.Equal(100, failedCount)
	re.Equal(100, successCount)

	time.Sleep(4 * time.Second) // 3+1
	wg.Add(1)
	countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	wg.Wait()
	re.Equal(101, successCount)
}

func TestWithTwoLimiters(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg := &DimensionConfig{
		QPS:              100,
		QPSBurst:         100,
		ConcurrencyLimit: 100,
	}
	limiter := newLimiter()
	status := limiter.updateDimensionConfig(cfg)
	re.NotZero(status & QPSChanged)
	re.NotZero(status & ConcurrencyChanged)

	var lock syncutil.Mutex
	successCount, failedCount := 0, 0
	var wg sync.WaitGroup
	r := &releaseUtil{}
	wg.Add(200)
	for i := 0; i < 200; i++ {
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(100, failedCount)
	re.Equal(100, successCount)
	time.Sleep(time.Second)

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(200, failedCount)
	re.Equal(100, successCount)

	for i := 0; i < 100; i++ {
		r.release()
	}
	status = limiter.updateQPSConfig(float64(rate.Every(10*time.Second)), 1)
	re.NotZero(status & QPSChanged)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
	}
	wg.Wait()
	re.Equal(101, successCount)
	re.Equal(299, failedCount)
	limit, current := limiter.getConcurrencyLimiterStatus()
	re.Equal(uint64(100), limit)
	re.Equal(uint64(1), current)

	cfg = &DimensionConfig{}
	status = limiter.updateDimensionConfig(cfg)
	re.NotZero(status & ConcurrencyDeleted)
	re.NotZero(status & QPSDeleted)
}

func countSingleLimiterHandleResult(limiter *limiter, successCount *int,
	failedCount *int, lock *syncutil.Mutex, wg *sync.WaitGroup, r *releaseUtil) {
	doneFucn, err := limiter.allow()
	lock.Lock()
	defer lock.Unlock()
	if err == nil {
		*successCount++
		r.append(doneFucn)
	} else {
		*failedCount++
	}
	wg.Done()
}
