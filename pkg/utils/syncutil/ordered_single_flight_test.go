// Copyright 2025 TiKV Project Authors.
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

package syncutil

import (
	"context"
	"math/rand/v2"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
)

func mustBlocking[T any](re *require.Assertions, ch chan T) {
	select {
	case <-ch:
		re.FailNow("received result when the execution is expected still running")
	case <-time.After(100 * time.Millisecond):
	}
}

func mustGetResult[T any](re *require.Assertions, ch chan T) T {
	var res T
	select {
	case res = <-ch:
	case <-time.After(time.Second):
		re.FailNow("result not received")
	}
	return res
}

func TestOrderedSingleFlight(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	s := NewOrderedSingleFlight[int]()

	res, err := s.Do(ctx, func() (int, error) {
		return 1, nil
	})
	re.NoError(err)
	re.Equal(1, res)

	res, err = s.Do(ctx, func() (int, error) {
		return 2, errors.New("err")
	})
	re.Error(err)
	re.Equal(2, res)

	const concurrency = 5

	inCh := make(chan int, 100)
	outCh := make(chan int, 100)

	//nolint:unparam
	f := func() (int, error) {
		return <-inCh, nil
	}

	// Start the first call
	go func() {
		res, err := s.Do(ctx, f)
		re.NoError(err)
		outCh <- res
	}()

	mustBlocking(re, outCh)

	// Start some concurrent invocations. As there's already an existing execution, the following invocations will enter
	// the next batch and return the second result.
	for i := range concurrency {
		i2 := i
		go func() {
			labels := pprof.Labels("worker", strconv.FormatInt(int64(i2), 10))
			pprof.Do(context.Background(), labels, func(context.Context) {
				res, err := s.Do(ctx, func() (int, error) {
					return <-inCh, nil
				})
				re.NoError(err)
				outCh <- res
			})
		}()
	}

	mustBlocking(re, outCh)

	inCh <- 101
	// The first call finishes.
	re.Equal(101, mustGetResult(re, outCh))
	// But the other calls are still blocked.
	mustBlocking(re, outCh)

	inCh <- 102
	// The remaining calls finishes.
	for range concurrency {
		re.Equal(102, mustGetResult(re, outCh))
	}

	// No corrupted state is left.
	re.Nil(s.pendingTask)
	res, err = s.Do(ctx, func() (int, error) {
		return 200, nil
	})
	re.NoError(err)
	re.Equal(200, res)
}

func TestOrderedSingleFlightCancellation(t *testing.T) {
	re := require.New(t)

	type result struct {
		v     int
		err   error
		index int
	}

	inCh := make(chan int, 100)
	outCh := make(chan result, 100)
	//nolint:unparam
	f := func() (int, error) {
		res := <-inCh
		return res, nil
	}

	s := NewOrderedSingleFlight[int]()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		res, err := s.Do(ctx, f)
		outCh <- result{v: res, err: err}
	}()
	select {
	case <-outCh:
		re.Fail("received result when the execution is expected still running")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case r := <-outCh:
		re.Error(r.err)
		re.ErrorIs(r.err, context.Canceled)
	case <-time.After(time.Second):
		re.Fail("result not received")
	}

	// The function is still executing. Next call will be blocked.
	// No caller is still waiting for the result here, but it doesn't support canceling the execution now.
	go func() {
		res, err := s.Do(context.Background(), f)
		outCh <- result{v: res, err: err}
	}()
	mustBlocking(re, outCh)
	inCh <- 1
	// The first execution exits, the next is being blocked.
	mustBlocking(re, outCh)
	inCh <- 2
	// The next execution exits.
	r := mustGetResult(re, outCh)
	re.NoError(r.err)
	re.Equal(2, r.v)

	// The state is clear
	re.Nil(s.pendingTask)
	res, err := s.Do(context.Background(), func() (int, error) {
		return 100, nil
	})
	re.NoError(err)
	re.Equal(100, res)

	// Test multiple concurrent invocations.
	const concurrency = 5
	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() {
		res, err := s.Do(ctx1, f)
		outCh <- result{v: res, err: err}
	}()
	mustBlocking(re, outCh)
	ctx2, cancel2 := context.WithCancel(context.Background())
	for i := range concurrency {
		i2 := i
		go func() {
			ctx3 := context.Background()
			if i2 == 0 || i2 == 2 {
				ctx3 = ctx2
			}
			res, err := s.Do(ctx3, f)
			outCh <- result{v: res, err: err, index: i2}
		}()
	}
	mustBlocking(re, outCh)
	// Cancel (only) the first invocation, and receive its result.
	cancel1()
	r = mustGetResult(re, outCh)
	re.Error(r.err)
	re.ErrorIs(r.err, context.Canceled)
	// The remaining requests are still being blocked
	mustBlocking(re, outCh)
	// Cancel the second.
	cancel2()
	// Two of the invocations are canceled.
	r1 := mustGetResult(re, outCh)
	re.Error(r1.err)
	re.ErrorIs(r1.err, context.Canceled)
	r2 := mustGetResult(re, outCh)
	re.Error(r2.err)
	re.ErrorIs(r2.err, context.Canceled)
	re.ElementsMatch([]int{r1.index, r2.index}, []int{0, 2})
	// The 3 remaining invocations are still blocked
	mustBlocking(re, outCh)

	// Finish the first execution whose invocations are already canceled.
	inCh <- 3
	mustBlocking(re, outCh)
	// Finish the second execution that gives the result of the next several concurrent invocations.
	inCh <- 4

	index := make([]int, 0, 3)
	for range 3 {
		r = mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(4, r.v)
		index = append(index, r.index)
	}
	re.ElementsMatch([]int{1, 3, 4}, index)

	// The state is clear now, no remaining invocations or executions are left.
	re.Nil(s.pendingTask)
	res, err = s.Do(context.Background(), func() (int, error) {
		return 101, nil
	})
	re.NoError(err)
	re.Equal(101, res)
}

func TestOrderedSingleFightRandom(t *testing.T) {
	re := require.New(t)

	const concurrency = 10
	const testTime = time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), testTime)
	defer cancel()
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	currentValue := &atomic.Int64{}
	s := NewOrderedSingleFlight[int64]()
	//nolint:unparam
	f := func() (int64, error) {
		res := currentValue.Load()
		time.Sleep(time.Duration(rand.Int64N(20)) * time.Millisecond)
		return res, nil
	}

	// One goroutine to monotonically increase the current value.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			currentValue.Add(1)
		}
	}()

	type result struct {
		v   int64
		err error
	}

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				innerCtx, innerCancel := context.WithCancel(context.Background())
				ch := make(chan result, 1)

				beforeValue := currentValue.Load()
				go func() {
					res, err := s.Do(innerCtx, f)
					ch <- result{v: res, err: err}
				}()
				// Randomly cancel the call at rate 10%.
				shouldCancel := rand.Int64N(10) == 0
				if shouldCancel {
					innerCancel()
				}

				select {
				case <-ctx.Done():
					innerCancel()
					return

				case r := <-ch:
					afterValue := currentValue.Load()
					innerCancel()

					if shouldCancel {
						// It might finish before the cancel takes effect.
						if r.err != nil {
							re.ErrorIs(r.err, context.Canceled)
						}
					} else {
						re.NoError(r.err)
					}

					// Check the strong order if the invocation is successful
					if r.err == nil {
						re.GreaterOrEqual(r.v, beforeValue)
						re.LessOrEqual(r.v, afterValue)
					}

				case <-time.After(testTime * 2):
					innerCancel()
					re.FailNow("result blocked for too long")
				}
			}
		}()
	}
}
