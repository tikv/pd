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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

type releaseUtil struct {
	mu    sync.Mutex
	dones []DoneFunc
}

func (r *releaseUtil) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.dones) > 0 {
		r.dones[0]()
		r.dones = r.dones[1:]
	}
}

func (r *releaseUtil) append(d DoneFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

func TestWithBBR(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg := &DimensionConfig{
		EnableBBR: true,
	}
	limiter := newLimiter()
	status := limiter.updateDimensionConfig(cfg, optsForTest...)
	re.NotEqual(0, status&BBRChanged)
	re.NotEqual(0, status&QPSNoChange)
	re.NotEqual(0, status&ConcurrencyNoChange)

	re.NotNil(limiter.getBBR())
	re.NotNil(limiter.getConcurrencyLimiter())

	// make BBR full.
	for i := 0; i < 14; i++ {
		for j := 0; j < i+2; j++ {
			go func() {
				done, err := limiter.allow()
				time.Sleep(bucketDuration * 2)
				if err == nil {
					done()
				}
			}()
		}
		time.Sleep(bucketDuration)
	}
	enable, maxConcurrency := limiter.getBBRStatus()
	re.True(enable)
	limit, _ := limiter.getConcurrencyLimiterStatus()
	re.Equal(limit, uint64(maxConcurrency))

	// go check()
	tricker := time.NewTicker(time.Millisecond)
	pctx := context.Background()
	ctx, cancel := context.WithCancel(pctx)
	go func(ctx context.Context) {
		select {
		case <-tricker.C:
			_, current := limiter.getConcurrencyLimiterStatus()
			re.LessOrEqual(current, uint64(maxConcurrency))
		case <-ctx.Done():
			return
		}
	}(ctx)
	var wg sync.WaitGroup
	var exceeded atomic.Bool
	for i := 0; i < 14; i++ {
		for j := 0; j < 14; j++ {
			wg.Add(1)
			go func() {
				done, err := limiter.allow()
				time.Sleep(bucketDuration * 2)
				if err == nil {
					done()
				} else {
					exceeded.Store(true)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	cancel()
	tricker.Stop()
	re.True(exceeded.Load())
	_, current := limiter.getConcurrencyLimiterStatus()
	re.Equal(uint64(0), current)

	// test short RT
	exceeded.Store(false)
	ctx, cancel = context.WithCancel(pctx)
	go func(ctx context.Context) {
		select {
		case <-tricker.C:
			_, current := limiter.getConcurrencyLimiterStatus()
			re.LessOrEqual(current, uint64(maxConcurrency))
		case <-ctx.Done():
			return
		}
	}(ctx)
	for i := 0; i < 14; i++ {
		for j := 0; j < 100; j++ {
			done, err := limiter.allow()
			if err == nil {
				done()
			} else {
				exceeded.Store(true)
			}
		}
	}
	cancel()
	re.False(exceeded.Load())

	// disable
	cfg = &DimensionConfig{
		EnableBBR: false,
	}
	status = limiter.updateDimensionConfig(cfg)
	re.NotEqual(0, status&BBRDeleted)

	re.Nil(limiter.getBBR())
	re.Nil(limiter.getConcurrencyLimiter())

	// make BBR full.
	for i := 0; i < 14; i++ {
		for j := 0; j < i+2; j++ {
			go func() {
				done, err := limiter.allow()
				time.Sleep(bucketDuration * 2)
				if err == nil {
					done()
				}
			}()
		}
		time.Sleep(bucketDuration)
	}
	enable, maxConcurrency = limiter.getBBRStatus()
	re.False(enable)
	limit, _ = limiter.getConcurrencyLimiterStatus()
	re.Equal(uint64(0), limit)

	exceeded.Store(false)
	for i := 0; i < 14; i++ {
		for j := 0; j < i+2; j++ {
			go func() {
				done, err := limiter.allow()
				time.Sleep(bucketDuration * 2)
				if err == nil {
					done()
				}
			}()
		}
		time.Sleep(bucketDuration)
	}
	wg.Wait()
	re.False(exceeded.Load())
}

func TestWithTwoLimitersAndBBRConfig(t *testing.T) {
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

	cfg.EnableBBR = true
	limiter.updateDimensionConfig(cfg, optsForTest...)
	failedCount = 0
	successCount = 0
	go func() {
		for i := 0; i < 20; i++ {
			wg.Add(1)
			countSingleLimiterHandleResult(limiter, &successCount, &failedCount, &lock, &wg, r)
			time.Sleep(bucketDuration)
		}
	}()
	for i := 0; i < 20; i++ {
		time.Sleep(bucketDuration * 2)
		r.release()
	}
	wg.Wait()
	re.GreaterOrEqual(failedCount, 1)
	enable, maxConcurrency := limiter.getBBRStatus()
	re.True(enable)
	limit, _ = limiter.getConcurrencyLimiterStatus()
	re.Equal(limit, uint64(maxConcurrency))
	re.LessOrEqual(limit, cfg.ConcurrencyLimit)
	re.NotNil(limiter.getConcurrencyLimiter())

	re.Equal(uint64(100), limiter.cfg.ConcurrencyLimit)
	cfg.EnableBBR = false

	status = limiter.updateDimensionConfig(cfg)
	re.NotEqual(0, status&BBRDeleted)
	re.NotNil(limiter.getConcurrencyLimiter())
	limit, _ = limiter.getConcurrencyLimiterStatus()
	re.Equal(limit, cfg.ConcurrencyLimit)

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
