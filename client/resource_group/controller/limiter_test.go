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

// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package controller

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	d = 1 * time.Second
)

var (
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t6 = t0.Add(time.Duration(6) * d)
	t7 = t0.Add(time.Duration(7) * d)
	t8 = t0.Add(time.Duration(8) * d)
)

func resetTime() {
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t6 = t0.Add(time.Duration(6) * d)
	t7 = t0.Add(time.Duration(7) * d)
	t8 = t0.Add(time.Duration(8) * d)
}

type request struct {
	t   time.Time
	n   float64
	act time.Time
	ok  bool
}

// dFromDuration converts a duration to the nearest multiple of the global constant d.
func dFromDuration(dur time.Duration) int {
	// Add d/2 to dur so that integer division will round to
	// the nearest multiple instead of truncating.
	// (We don't care about small inaccuracies.)
	return int((dur + (d / 2)) / d)
}

// dSince returns multiples of d since t0
func dSince(t time.Time) int {
	return dFromDuration(t.Sub(t0))
}

func runReserveMax(t *testing.T, lim *Limiter, req request) *Reservation {
	return runReserve(t, lim, req, InfDuration)
}

func runReserve(t *testing.T, lim *Limiter, req request, maxReserve time.Duration) *Reservation {
	t.Helper()
	r := lim.reserveN(req.t, req.n, maxReserve)
	if r.ok && (dSince(r.timeToAct) != dSince(req.act)) || r.ok != req.ok {
		t.Errorf("lim.reserveN(t%d, %v, %v) = (t%d, %v) want (t%d, %v)",
			dSince(req.t), req.n, maxReserve, dSince(r.timeToAct), r.ok, dSince(req.act), req.ok)
	}
	return &r
}

func checkTokens(re *require.Assertions, lim *Limiter, t time.Time, expected float64) {
	re.LessOrEqual(math.Abs(expected-lim.AvailableTokens(t)), 1e-7)
}

func TestSimpleReserve(t *testing.T) {
	lim := NewLimiter(t0, 1, 0, 2, make(chan notifyMsg, 1))

	runReserveMax(t, lim, request{t0, 3, t1, true})
	runReserveMax(t, lim, request{t0, 3, t4, true})
	runReserveMax(t, lim, request{t3, 2, t6, true})

	runReserve(t, lim, request{t3, 2, t7, false}, time.Second*4)
	runReserve(t, lim, request{t5, 2000, t6, false}, time.Second*100)

	runReserve(t, lim, request{t3, 2, t8, true}, time.Second*8)
	// unlimited
	args := tokenBucketReconfigureArgs{
		NewBurst: -1,
	}
	lim.Reconfigure(t1, args)
	runReserveMax(t, lim, request{t5, 2000, t5, true})
}

func TestReconfig(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 1, 0, 2, make(chan notifyMsg, 1))

	runReserveMax(t, lim, request{t0, 4, t2, true})
	args := tokenBucketReconfigureArgs{
		NewTokens: 6.,
		NewRate:   2,
	}
	lim.Reconfigure(t1, args)
	checkTokens(re, lim, t1, 5)
	checkTokens(re, lim, t2, 7)

	args = tokenBucketReconfigureArgs{
		NewTokens: 6.,
		NewRate:   2,
		NewBurst:  -1,
	}
	lim.Reconfigure(t1, args)
	checkTokens(re, lim, t1, 6)
	checkTokens(re, lim, t2, 6)
	re.Equal(int64(-1), lim.GetBurst())
}

func TestNotify(t *testing.T) {
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 1, 0, 0, nc)

	args := tokenBucketReconfigureArgs{
		NewTokens:       1000.,
		NewRate:         2,
		NotifyThreshold: 400,
	}
	lim.Reconfigure(t1, args)
	runReserveMax(t, lim, request{t2, 1000, t2, true})
	select {
	case <-nc:
	default:
		t.Errorf("no notify")
	}
}

func TestCancel(t *testing.T) {
	resetTime()
	ctx := context.Background()
	ctx1, cancel1 := context.WithDeadline(ctx, t2)
	re := require.New(t)
	nc := make(chan notifyMsg, 1)
	lim1 := NewLimiter(t0, 1, 0, 10, nc)
	lim2 := NewLimiter(t0, 1, 0, 0, nc)

	r1 := runReserveMax(t, lim1, request{t0, 5, t0, true})
	checkTokens(re, lim1, t0, 5)
	r1.CancelAt(t1)
	checkTokens(re, lim1, t1, 11)

	r1 = lim1.Reserve(ctx, InfDuration, t1, 5)
	r2 := lim2.Reserve(ctx1, InfDuration, t1, 5)
	checkTokens(re, lim1, t2, 7)
	checkTokens(re, lim2, t2, 2)
	d, err := WaitReservations(ctx, t2, []*Reservation{r1, r2})
	re.Error(err)
	re.Equal(4*time.Second, d)
	re.Contains(err.Error(), "estimated wait time 4s, ltb state is 1.00:-4.00")
	checkTokens(re, lim1, t3, 13)
	checkTokens(re, lim2, t3, 3)
	cancel1()

	ctx2, cancel2 := context.WithCancel(ctx)
	r1 = lim1.Reserve(ctx, InfDuration, t3, 5)
	r2 = lim2.Reserve(ctx2, InfDuration, t3, 5)
	checkTokens(re, lim1, t3, 8)
	checkTokens(re, lim2, t3, -2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := WaitReservations(ctx2, t3, []*Reservation{r1, r2})
		re.Error(err)
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	cancel2()
	wg.Wait()
	checkTokens(re, lim1, t5, 15)
	checkTokens(re, lim2, t5, 5)
}

func TestCancelErrorOfReservation(t *testing.T) {
	re := require.New(t)
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 10, 0, 10, nc)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := lim.Reserve(ctx, InfDuration, t0, 5)
	d, err := WaitReservations(context.Background(), t0, []*Reservation{r})
	re.Equal(0*time.Second, d)
	re.Error(err)
	re.Contains(err.Error(), "context canceled")
}

func TestQPS(t *testing.T) {
	re := require.New(t)
	cases := []struct {
		concurrency int
		reserveN    int64
		ruPerSec    int64
	}{
		{1000, 10, 400000},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("concurrency=%d,reserveN=%d,limit=%d", tc.concurrency, tc.reserveN, tc.ruPerSec), func(t *testing.T) {
			qps, ruSec, waitTime := testQPSCase(tc.concurrency, tc.reserveN, tc.ruPerSec)
			t.Log(fmt.Printf("QPS: %.2f, RU: %.2f, new request need wait  %s\n", qps, ruSec, waitTime))
			re.LessOrEqual(math.Abs(float64(tc.ruPerSec)-ruSec), float64(100)*float64(tc.reserveN))
			re.LessOrEqual(math.Abs(float64(tc.ruPerSec)/float64(tc.reserveN)-qps), float64(100))
		})
	}
}

const testCaseRunTime = 4 * time.Second

func testQPSCase(concurrency int, reserveN int64, limit int64) (qps float64, ru float64, needWait time.Duration) {
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(time.Now(), Limit(limit), limit, float64(limit), nc)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var totalRequests int64
	start := time.Now()

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
				r := lim.Reserve(context.Background(), 30*time.Second, time.Now(), float64(reserveN))
				if r.OK() {
					delay := r.DelayFrom(time.Now())
					<-time.After(delay)
				} else {
					panic("r not ok")
				}
				atomic.AddInt64(&totalRequests, 1)
			}
		}()
	}
	var vQPS atomic.Value
	var wait time.Duration
	ch := make(chan struct{})
	go func() {
		var windowRequests int64
		for {
			elapsed := time.Since(start)
			if elapsed >= testCaseRunTime {
				close(ch)
				break
			}
			windowRequests = atomic.SwapInt64(&totalRequests, 0)
			vQPS.Store(float64(windowRequests))
			r := lim.Reserve(ctx, 30*time.Second, time.Now(), float64(reserveN))
			wait = r.Delay()
			time.Sleep(1 * time.Second)
		}
	}()
	<-ch
	cancel()
	wg.Wait()
	qps = vQPS.Load().(float64)
	return qps, qps * float64(reserveN), wait
}
