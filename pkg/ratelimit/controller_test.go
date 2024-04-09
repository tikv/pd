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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"golang.org/x/time/rate"
)

type changeAndResult struct {
	opt               Option
	checkOptionStatus func(string, Option)
	totalRequest      int
	success           int
	fail              int
	release           int
	waitDuration      time.Duration
	checkStatusFunc   func(string)
	requestInterval   time.Duration
}

type labelCase struct {
	label string
	round []changeAndResult
}

func runMulitLabelLimiter(t *testing.T, limiter *Controller, testCase []labelCase) {
	re := require.New(t)
	var caseWG sync.WaitGroup
	for _, tempCas := range testCase {
		caseWG.Add(1)
		cas := tempCas
		go func() {
			var lock syncutil.Mutex
			successCount, failedCount := 0, 0
			var wg sync.WaitGroup
			r := &releaseUtil{}
			for _, rd := range cas.round {
				rd.checkOptionStatus(cas.label, rd.opt)
				time.Sleep(rd.waitDuration)
				for i := 0; i < rd.totalRequest; i++ {
					wg.Add(1)
					if rd.requestInterval > 0 {
						time.Sleep(rd.requestInterval)
					}
					go func() {
						countRateLimiterHandleResult(limiter, cas.label, &successCount, &failedCount, &lock, &wg, r)
					}()
				}
				wg.Wait()
				if rd.fail >= 0 {
					re.Equal(rd.fail, failedCount)
				}
				if rd.success >= 0 {
					re.Equal(rd.success, successCount)
				}
				for i := 0; i < rd.release; i++ {
					r.release()
				}
				rd.checkStatusFunc(cas.label)
				failedCount = 0
				successCount = 0
			}
			caseWG.Done()
		}()
	}
	caseWG.Wait()
}

func TestControllerWithConcurrencyLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	limiter := NewController(context.Background(), "grpc", nil)
	defer limiter.Close()
	testCase := []labelCase{
		{
			label: "test1",
			round: []changeAndResult{
				{
					opt: UpdateConcurrencyLimiter(10),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyChanged)
					},
					totalRequest: 15,
					fail:         5,
					success:      10,
					release:      10,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(10), limit)
						re.Equal(uint64(0), current)
					},
				},
				{
					opt: UpdateConcurrencyLimiter(10),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyNoChange)
					},
					checkStatusFunc: func(label string) {},
				},
				{
					opt: UpdateConcurrencyLimiter(5),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyChanged)
					},
					totalRequest: 15,
					fail:         10,
					success:      5,
					release:      5,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(5), limit)
						re.Equal(uint64(0), current)
					},
				},
				{
					opt: UpdateConcurrencyLimiter(0),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyDeleted)
					},
					totalRequest: 15,
					fail:         0,
					success:      15,
					release:      5,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(0), limit)
						re.Equal(uint64(10), current)
					},
				},
			},
		},
		{
			label: "test2",
			round: []changeAndResult{
				{
					opt: UpdateConcurrencyLimiter(15),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyChanged)
					},
					totalRequest: 10,
					fail:         0,
					success:      10,
					release:      0,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(15), limit)
						re.Equal(uint64(10), current)
					},
				},
				{
					opt: UpdateConcurrencyLimiter(10),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & ConcurrencyChanged)
					},
					totalRequest: 10,
					fail:         10,
					success:      0,
					release:      10,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(10), limit)
						re.Equal(uint64(0), current)
					},
				},
			},
		},
	}
	runMulitLabelLimiter(t, limiter, testCase)
}

func TestBlockList(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	opts := []Option{AddLabelAllowList()}
	limiter := NewController(context.Background(), "grpc", nil)
	defer limiter.Close()
	label := "test"

	re.False(limiter.IsInAllowList(label))
	for _, opt := range opts {
		opt(label, limiter)
	}
	re.True(limiter.IsInAllowList(label))

	status := UpdateQPSLimiter(float64(rate.Every(time.Second)), 1)(label, limiter)
	re.NotZero(status & InAllowList)
	for i := 0; i < 10; i++ {
		_, err := limiter.Allow(label)
		re.NoError(err)
	}
}

func TestControllerWithQPSLimiter(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	limiter := NewController(context.Background(), "grpc", nil)
	defer limiter.Close()
	testCase := []labelCase{
		{
			label: "test1",
			round: []changeAndResult{
				{
					opt: UpdateQPSLimiter(float64(rate.Every(time.Second)), 1),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 3,
					fail:         2,
					success:      1,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(1), limit)
						re.Equal(1, burst)
					},
				},
				{
					opt: UpdateQPSLimiter(float64(rate.Every(time.Second)), 1),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSNoChange)
					},
					checkStatusFunc: func(label string) {},
				},
				{
					opt: UpdateQPSLimiter(5, 5),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 10,
					fail:         5,
					success:      5,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(5), limit)
						re.Equal(5, burst)
					},
				},
				{
					opt: UpdateQPSLimiter(0, 0),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSDeleted)
					},
					totalRequest: 10,
					fail:         0,
					success:      10,
					release:      0,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(0), limit)
						re.Zero(burst)
					},
				},
			},
		},
		{
			label: "test2",
			round: []changeAndResult{
				{
					opt: UpdateQPSLimiter(50, 5),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 10,
					fail:         5,
					success:      5,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(50), limit)
						re.Equal(5, burst)
					},
				},
				{
					opt: UpdateQPSLimiter(0, 0),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSDeleted)
					},
					totalRequest: 10,
					fail:         0,
					success:      10,
					release:      0,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(0), limit)
						re.Zero(burst)
					},
				},
			},
		},
	}
	runMulitLabelLimiter(t, limiter, testCase)
}

func TestControllerWithTwoLimiters(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	limiter := NewController(context.Background(), "grpc", nil)
	defer limiter.Close()
	testCase := []labelCase{
		{
			label: "test1",
			round: []changeAndResult{
				{
					opt: UpdateDimensionConfig(&DimensionConfig{
						QPS:              100,
						QPSBurst:         100,
						ConcurrencyLimit: 100,
					}),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 200,
					fail:         100,
					success:      100,
					release:      100,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(100), limit)
						re.Equal(100, burst)
						climit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(100), climit)
						re.Equal(uint64(0), current)
					},
				},
				{
					opt: UpdateQPSLimiter(float64(rate.Every(time.Second)), 1),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 200,
					fail:         199,
					success:      1,
					release:      0,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						limit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(100), limit)
						re.Equal(uint64(1), current)
					},
				},
			},
		},
		{
			label: "test2",
			round: []changeAndResult{
				{
					opt: UpdateQPSLimiter(50, 5),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSChanged)
					},
					totalRequest: 10,
					fail:         5,
					success:      5,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(50), limit)
						re.Equal(5, burst)
					},
				},
				{
					opt: UpdateQPSLimiter(0, 0),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotZero(status & QPSDeleted)
					},
					totalRequest: 10,
					fail:         0,
					success:      10,
					release:      0,
					waitDuration: 0,
					checkStatusFunc: func(label string) {
						limit, burst := limiter.GetQPSLimiterStatus(label)
						re.Equal(rate.Limit(0), limit)
						re.Equal(0, burst)
					},
				},
			},
		},
	}
	runMulitLabelLimiter(t, limiter, testCase)
}

func TestControllerWithEnableBBR(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	limiter := NewController(context.Background(), "grpc", nil)
	testCase := []labelCase{
		{
			label: "test1",
			round: []changeAndResult{
				{
					opt: UpdateDimensionConfigForTest(&DimensionConfig{
						EnableBBR: true,
					}, optsForTest...),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotEqual(0, status&BBRChanged)
					},
					totalRequest:    200,
					fail:            -1,
					success:         -1,
					release:         0,
					waitDuration:    time.Second,
					requestInterval: 10 * time.Millisecond,
					checkStatusFunc: func(label string) {
						time.Sleep(bucketDuration)
						bbr, limit := limiter.GetBBRStatus(label)
						re.Less(int64(1), limit)
						re.Less(limit, inf)
						re.True(bbr)
						climit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Less(current, uint64(200))
						re.Less(uint64(1), current)
						re.Equal(climit, current)
					},
				},
				{
					opt: UpdateDimensionConfigForTest(&DimensionConfig{
						ConcurrencyLimit: 200,
						EnableBBR:        true,
					}, optsForTest...),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotEqual(0, status&BBRNoChange)
						re.NotEqual(0, status&ConcurrencyNoChange)
					},
					totalRequest: 0,
					fail:         0,
					success:      0,
					release:      20,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						climit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Greater(climit, current)
					},
				},
				{
					opt: UpdateDimensionConfigForTest(&DimensionConfig{
						ConcurrencyLimit: 200,
						EnableBBR:        true,
					}, optsForTest...),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotEqual(0, status&BBRNoChange)
						re.NotEqual(0, status&ConcurrencyNoChange)
					},
					totalRequest: 100,
					fail:         80,
					success:      20,
					release:      0,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						climit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(climit, current)
					},
				},
				{
					opt: UpdateDimensionConfigForTest(&DimensionConfig{
						ConcurrencyLimit: 200,
						EnableBBR:        false,
					}, optsForTest...),
					checkOptionStatus: func(label string, o Option) {
						status := limiter.Update(label, o)
						re.NotEqual(0, status&BBRDeleted)
						re.NotEqual(0, status&ConcurrencyNoChange)
					},
					totalRequest: 300,
					fail:         -1,
					success:      -1,
					release:      0,
					waitDuration: time.Second,
					checkStatusFunc: func(label string) {
						climit, current := limiter.GetConcurrencyLimiterStatus(label)
						re.Equal(uint64(200), current)
						re.Equal(uint64(200), climit)
					},
				},
			},
		},
	}
	runMulitLabelLimiter(t, limiter, testCase)
}

func countRateLimiterHandleResult(limiter *Controller, label string, successCount *int,
	failedCount *int, lock *syncutil.Mutex, wg *sync.WaitGroup, r *releaseUtil) {
	doneFucn, err := limiter.Allow(label)
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
