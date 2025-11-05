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
	case <-time.After(time.Hour):
		re.FailNow("result not received")
	}
	return res
}

func TestOrderedSingleFlight(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	s := NewOrderedSingleFlight[int]()

	res, err := s.Do(ctx, func(context.Context) (int, error) {
		return 1, nil
	})
	re.NoError(err)
	re.Equal(1, res)

	res, err = s.Do(ctx, func(context.Context) (int, error) {
		return 2, errors.New("err")
	})
	re.Error(err)
	re.Equal(2, res)

	const concurrency = 5

	inCh := make(chan int, 100)
	outCh := make(chan int, 100)

	//nolint:unparam
	f := func(context.Context) (int, error) {
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
				res, err := s.Do(ctx, func(context.Context) (int, error) {
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
	res, err = s.Do(ctx, func(context.Context) (int, error) {
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
	f := func(context.Context) (int, error) {
		res := <-inCh
		return res, nil
	}

	s := NewOrderedSingleFlight[int]()

	// Test cancelling the first call
	{
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			res, err := s.Do(ctx, f)
			outCh <- result{v: res, err: err}
		}()

		mustBlocking(re, outCh)
		cancel()
		r := mustGetResult(re, outCh)
		re.Error(r.err)
		re.ErrorIs(r.err, context.Canceled)

		// The function is still executing. Next call will be blocked.
		// No caller is still waiting for the result here, but the function `f` is unable to be canceled by ctx.
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
		r = mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(2, r.v)

		// The state is clear
		re.Nil(s.pendingTask)
		res, err := s.Do(context.Background(), func(context.Context) (int, error) {
			return 100, nil
		})
		re.NoError(err)
		re.Equal(100, res)
	}

	// Test cancelling the second (waiting) call
	{
		go func() {
			res, err := s.Do(context.Background(), f)
			outCh <- result{v: res, err: err}
		}()
		mustBlocking(re, outCh)
		outCh2 := make(chan result, 10)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			res, err := s.Do(ctx, f)
			outCh2 <- result{v: res, err: err}
		}()
		mustBlocking(re, outCh2)

		cancel()
		r := mustGetResult(re, outCh2)
		re.Error(r.err)
		re.ErrorIs(r.err, context.Canceled)

		// Finish the first call.
		inCh <- 11
		r = mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(11, r.v)

		// In this case, the current implementation doesn't fully clear the state, and there's still a non-nil task.
		// But it's ok to continue using it, and the next execution can start immediately.
		re.NotNil(s.pendingTask)
		go func() {
			res, err := s.Do(context.Background(), f)
			outCh <- result{v: res, err: err}
		}()
		mustBlocking(re, outCh)
		inCh <- 12
		r = mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(12, r.v)

		// Now the state is clear.
		re.Nil(s.pendingTask)
		res, err := s.Do(context.Background(), func(context.Context) (int, error) {
			return 100, nil
		})
		re.NoError(err)
		re.Equal(100, res)
	}

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
	r := mustGetResult(re, outCh)
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
	inCh <- 21
	mustBlocking(re, outCh)
	// Finish the second execution that gives the result of the next several concurrent invocations.
	inCh <- 22

	index := make([]int, 0, 3)
	for range 3 {
		r = mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(22, r.v)
		index = append(index, r.index)
	}
	re.ElementsMatch([]int{1, 3, 4}, index)

	// The state is clear now, no remaining invocations or executions are left.
	re.Nil(s.pendingTask)
	res, err := s.Do(context.Background(), func(context.Context) (int, error) {
		return 101, nil
	})
	re.NoError(err)
	re.Equal(101, res)
}

func TestOrderedSingleFlightCancellationWithCancellableFunction(t *testing.T) {
	re := require.New(t)

	type result struct {
		v   int
		err error
	}

	inCh := make(chan int, 100)
	// Receives each result of a call to `Do` returns.
	outCh := make(chan result, 100)
	// Receives each actual result of the execution of the function `f`. No message here if invocations to `Do` are
	// canceled before execution of `f`.
	execResCh := make(chan result, 100)
	//nolint:unparam
	f := func(ctx context.Context) (int, error) {
		var res int
		var err error
		select {
		case res = <-inCh:
		case <-ctx.Done():
			err = ctx.Err()
		}
		execResCh <- result{v: res, err: err}
		return res, err
	}
	s := NewOrderedSingleFlight[int]()
	startCall := func(ctx context.Context) {
		go func() {
			res, err := s.Do(ctx, f)
			outCh <- result{v: res, err: err}
		}()
	}

	checkStateIsClear := func() {
		re.Nil(s.pendingTask)
		startCall(context.Background())
		mustBlocking(re, outCh)
		inCh <- 100
		r := mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(100, r.v)
		r = mustGetResult(re, execResCh)
		re.NoError(r.err)
		re.Equal(100, r.v)
	}

	// Cancel single call
	{
		ctx, cancel := context.WithCancel(context.Background())
		startCall(ctx)
		mustBlocking(re, outCh)
		cancel()
		r := mustGetResult(re, outCh)
		re.Error(r.err)
		re.ErrorIs(r.err, context.Canceled)
		r = mustGetResult(re, execResCh)
		re.Error(r.err)
		re.ErrorIs(r.err, context.Canceled)

		checkStateIsClear()
	}

	// Construct the case that two invocations shares the same execution, test cancel partially and cancel completely
	// for multi-caller case.
	for caseIndex := 1; caseIndex <= 3; caseIndex++ {
		shouldCancel1, shouldCancel2 := (caseIndex&1) != 0, (caseIndex&2) != 0

		// Start first call that blocks following calls
		startCall(context.Background())
		mustBlocking(re, outCh)
		// Start another two calls that shares execution, one of which will be canceled
		// Call 1:
		ctx1, cancel1 := context.WithCancel(context.Background())
		startCall(ctx1)
		// Call 2:
		ctx2, cancel2 := context.WithCancel(context.Background())
		startCall(ctx2)

		mustBlocking(re, outCh)

		// Finish the first execution and start the next.
		inCh <- 11
		r := mustGetResult(re, outCh)
		re.NoError(r.err)
		re.Equal(11, r.v)
		r = mustGetResult(re, execResCh)
		re.NoError(r.err)
		re.Equal(11, r.v)

		mustBlocking(re, outCh)
		mustBlocking(re, execResCh)

		re.True(shouldCancel1 || shouldCancel2)
		if shouldCancel1 {
			cancel1()
		}
		if shouldCancel2 {
			cancel2()
		}
		cancelBoth := shouldCancel1 && shouldCancel2
		if !cancelBoth {
			// One of the call is canceled, but the other is still waiting for the result, and therefore the execution
			// should not be canceled.
			r = mustGetResult(re, outCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
			mustBlocking(re, outCh)
			mustBlocking(re, execResCh)

			inCh <- 12
			r = mustGetResult(re, outCh)
			re.NoError(r.err)
			re.Equal(12, r.v)
			r = mustGetResult(re, execResCh)
			re.NoError(r.err)
			re.Equal(12, r.v)
		} else {
			for range 2 {
				r = mustGetResult(re, outCh)
				re.Error(r.err)
				re.ErrorIs(r.err, context.Canceled)
			}
			r = mustGetResult(re, execResCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
		}

		cancel1()
		cancel2()

		checkStateIsClear()
	}

	// When all caller are cancelled when waiting for the previous execution to finish, the context is not canceled
	// and further calls can still be accepted.
	for _, subsequentCallAfterFirstFinish := range []bool{false, true} {
		startCall(context.Background())
		mustBlocking(re, outCh)
		ctx1, cancel1 := context.WithCancel(context.Background())
		startCall(ctx1)
		ctx2, cancel2 := context.WithCancel(context.Background())
		startCall(ctx2)
		mustBlocking(re, outCh)

		cancel1()
		cancel2()
		for range 2 {
			r := mustGetResult(re, outCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
		}
		mustBlocking(re, execResCh)

		if subsequentCallAfterFirstFinish {
			// First finish the first call, and then add another call.
			inCh <- 31
			r := mustGetResult(re, outCh)
			re.NoError(r.err)
			re.Equal(31, r.v)
			r = mustGetResult(re, execResCh)
			re.NoError(r.err)
			re.Equal(31, r.v)

			ctx3, cancel3 := context.WithCancel(context.Background())
			startCall(ctx3) // Call 3
			mustBlocking(re, outCh)
			// The above call (call 3) starts execution immediately, and then the following call 4 won't share the
			// execution and will be blocked.
			startCall(context.Background()) // Call 4
			mustBlocking(re, outCh)

			cancel3()
			r = mustGetResult(re, outCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
			r = mustGetResult(re, execResCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
			mustBlocking(re, outCh)
			inCh <- 32
			r = mustGetResult(re, outCh)
			re.NoError(r.err)
			re.Equal(32, r.v)
			r = mustGetResult(re, execResCh)
			re.NoError(r.err)
			re.Equal(32, r.v)
		} else {
			// Add another call before finishing the first call.
			ctx3, cancel3 := context.WithCancel(context.Background())
			startCall(ctx3)
			ctx4, cancel4 := context.WithCancel(context.Background())
			startCall(ctx4)
			mustBlocking(re, outCh)

			// Finish the first call
			inCh <- 31
			r := mustGetResult(re, outCh)
			re.NoError(r.err)
			re.Equal(31, r.v)
			r = mustGetResult(re, execResCh)
			re.NoError(r.err)
			re.Equal(31, r.v)

			mustBlocking(re, outCh)

			cancel3()
			r = mustGetResult(re, outCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
			mustBlocking(re, outCh)
			mustBlocking(re, execResCh)
			cancel4()
			r = mustGetResult(re, outCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
			r = mustGetResult(re, execResCh)
			re.Error(r.err)
			re.ErrorIs(r.err, context.Canceled)
		}

		checkStateIsClear()
	}
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
	f := func(ctx context.Context) (int64, error) {
		res := currentValue.Load()
		time.Sleep(time.Duration(rand.Int64N(20)) * time.Millisecond)

		select {
		case <-ctx.Done():
			// Randomly cancel. When it doesn't cancel, it simulates the case that the function has run into
			// a state where it has no more chance to check the ctx and cancel.
			if rand.IntN(2) == 1 {
				return 0, ctx.Err()
			}
		default:
		}

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

func BenchmarkOrderedSingleFlightSingleThread(b *testing.B) {
	s := NewOrderedSingleFlight[int]()
	noop := func(context.Context) (int, error) { return 0, nil }
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		_, _ = s.Do(ctx, noop)
	}
}

func benchmarkOrderedSingleFlightParallel(b *testing.B, parallelism int) {
	s := NewOrderedSingleFlight[int]()
	f := func(context.Context) (int, error) {
		return 0, nil
	}
	ctx := context.Background()

	b.ResetTimer()
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Do(ctx, f)
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(s.ExecCount()), "exec/op")
	b.ReportMetric(1-float64(s.ExecCount())/float64(b.N), "reusing_rate")
}

func BenchmarkOrderedSingleFlightParallel1(b *testing.B) {
	benchmarkOrderedSingleFlightParallel(b, 1)
}

func BenchmarkOrderedSingleFlightParallel8(b *testing.B) {
	benchmarkOrderedSingleFlightParallel(b, 8)
}

func BenchmarkOrderedSingleFlightParallel128(b *testing.B) {
	benchmarkOrderedSingleFlightParallel(b, 128)
}
