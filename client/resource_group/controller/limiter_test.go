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

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/client/errs"
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
	if r.reserved && (dSince(r.timeToAct) != dSince(req.act)) || r.reserved != req.ok {
		t.Errorf("lim.reserveN(t%d, %v, %v) = (t%d, %v) want (t%d, %v)",
			dSince(req.t), req.n, maxReserve, dSince(r.timeToAct), r.reserved, dSince(req.act), req.ok)
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
		newBurst: -1,
	}
	lim.Reconfigure(t1, args)
	runReserveMax(t, lim, request{t5, 2000, t5, true})
}

func TestReconfig(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 1, 0, 2, make(chan notifyMsg, 1))

	runReserveMax(t, lim, request{t0, 4, t2, true})
	args := tokenBucketReconfigureArgs{
		newTokens:   6.,
		newFillRate: 2,
	}
	lim.Reconfigure(t1, args)
	checkTokens(re, lim, t1, 5)
	checkTokens(re, lim, t2, 7)

	args = tokenBucketReconfigureArgs{
		newTokens:   6.,
		newFillRate: 2,
		newBurst:    -1,
	}
	lim.Reconfigure(t1, args)
	checkTokens(re, lim, t1, 6)
	checkTokens(re, lim, t2, 6)
	re.Equal(int64(-1), lim.GetBurst())
}

// TestLastStaysMonotonicOnStaleNow guards the assumption used by
// groupCostController.acquireTokens: a mutator's now is captured before it
// takes lim.mu, so under concurrency a sibling request on the same per-group
// limiter can advance lim.last past that now. The stale (backward) now must
// not rewind the limiter's logical clock.
//
// In every sub-case the shared Reserve below plays that concurrent sibling: it
// advances lim.last to advancedAt, after which each mutator runs with the
// earlier staleNow.
func TestLastStaysMonotonicOnStaleNow(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(lim *Limiter, r *Reservation, staleNow time.Time)
		expectedPool float64
		checkPool    bool
	}{
		{
			name: "remove tokens",
			mutate: func(lim *Limiter, _ *Reservation, staleNow time.Time) {
				lim.RemoveTokens(staleNow, 0)
			},
			expectedPool: 1090,
			checkPool:    true,
		},
		{
			name: "refund tokens",
			mutate: func(lim *Limiter, _ *Reservation, staleNow time.Time) {
				lim.RefundTokens(staleNow, 0)
			},
			expectedPool: 1090,
			checkPool:    true,
		},
		// CancelAt is covered by TestAcquireTokensCancelKeepsLastMonotonic.
		{
			name: "reconfigure",
			mutate: func(lim *Limiter, _ *Reservation, staleNow time.Time) {
				lim.Reconfigure(staleNow, tokenBucketReconfigureArgs{
					newTokens:   10,
					newFillRate: 100,
				})
			},
			expectedPool: 1100,
			checkPool:    true,
		},
		{
			name: "reconfigure unlimited",
			mutate: func(lim *Limiter, _ *Reservation, staleNow time.Time) {
				lim.Reconfigure(staleNow, tokenBucketReconfigureArgs{
					newTokens:   10,
					newFillRate: 100,
					newBurst:    -1,
				})
			},
			checkPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			resetTime()
			nc := make(chan notifyMsg, 1)
			lim := NewLimiter(t0, 100, 0, 0, nc) // fillRate 100/s, burst 0 (no clamp)

			advancedAt := t0.Add(10 * time.Second)
			staleNow := t0.Add(9 * time.Second)
			checkAt := t0.Add(11 * time.Second)

			// Advance last through the public API to t0+10s and leave pool =
			// 100*10 - 10 = 990.
			r := lim.Reserve(context.Background(), InfDuration, advancedAt, 10)
			re.True(r.reserved)
			_, pool := lim.getTokens(advancedAt)
			re.InDelta(990, pool, 1e-6)

			tt.mutate(lim, r, staleNow)
			re.Equal(advancedAt, lim.last)

			if tt.checkPool {
				// Accrual over t0+10s..t0+11s is 1s*100 = 100. A rewound
				// last (to t0+9s) would over-accrue by another 100 tokens.
				_, pool = lim.getTokens(checkAt)
				re.InDelta(tt.expectedPool, pool, 1e-6)
			}
		})
	}
}

func TestRefundTokens(t *testing.T) {
	re := require.New(t)
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 0, 0, 100, nc)

	// Consume some tokens.
	lim.RemoveTokens(t0, 30)
	checkTokens(re, lim, t0, 70)

	// Refund part of them.
	lim.RefundTokens(t0, 20)
	checkTokens(re, lim, t0, 90)

	// Refund beyond the initial amount - allowed (no burst cap when burst==0).
	lim.RefundTokens(t0, 50)
	checkTokens(re, lim, t0, 140)

	// With burst > 0, refund up to burst stays.
	limBurst := NewLimiter(t0, 0, 100, 50, nc)
	limBurst.RemoveTokens(t0, 30)
	checkTokens(re, limBurst, t0, 20)
	limBurst.RefundTokens(t0, 80) // tokens = 20 + 80 = 100, burst = 100
	checkTokens(re, limBurst, t0, 100)

	// Refund beyond burst - capped by getTokens.
	limBurst.RefundTokens(t0, 50) // tokens = 100 + 50 = 150, capped to 100
	checkTokens(re, limBurst, t0, 100)

	// Burstable (burst < 0): RefundTokens is a no-op.
	limUnlimited := NewLimiter(t0, 0, -1, 100, nc)
	limUnlimited.RefundTokens(t0, 50)
	checkTokens(re, limUnlimited, t0, 100)
}

func TestRefundTokensWakesAcquireRetry(t *testing.T) {
	lim := NewLimiter(t0, 0, 0, 100, make(chan notifyMsg, 1))
	reconfiguredCh := lim.GetReconfiguredCh()

	lim.RefundTokens(t0, 50)

	select {
	case <-reconfiguredCh:
	default:
		t.Fatal("RefundTokens should wake acquireTokens retry loops after reflowing accepted reservations")
	}
}

func TestNotify(t *testing.T) {
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 1, 0, 0, nc)

	args := tokenBucketReconfigureArgs{
		newTokens:          1000.,
		newFillRate:        2,
		newNotifyThreshold: 400,
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

func TestReconfiguredCh(t *testing.T) {
	re := require.New(t)
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 1, 0, 0, nc)

	// The channel should block initially.
	ch := lim.GetReconfiguredCh()
	select {
	case <-ch:
		t.Fatal("reconfiguredCh should not be closed before Reconfigure")
	default:
	}

	// After Reconfigure the old channel must be closed.
	args := tokenBucketReconfigureArgs{newTokens: 10, newFillRate: 1}
	lim.Reconfigure(t1, args)
	select {
	case <-ch:
	default:
		t.Fatal("reconfiguredCh should be closed after Reconfigure")
	}

	// A new channel is created; it should block again.
	ch2 := lim.GetReconfiguredCh()
	re.NotEqual(fmt.Sprintf("%p", ch), fmt.Sprintf("%p", ch2))
	select {
	case <-ch2:
		t.Fatal("new reconfiguredCh should not be closed yet")
	default:
	}

	// Successive Reconfigure calls each close the current channel.
	lim.Reconfigure(t2, args)
	select {
	case <-ch2:
	default:
		t.Fatal("second reconfiguredCh should be closed after second Reconfigure")
	}
}

func TestReconfiguredChWakesMultipleWaiters(t *testing.T) {
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(t0, 1, 0, 0, nc)

	const numWaiters = 5
	ch := lim.GetReconfiguredCh()
	wokenUp := make(chan struct{}, numWaiters)

	var wg sync.WaitGroup
	for range numWaiters {
		wg.Go(func() {
			select {
			case <-ch:
				wokenUp <- struct{}{}
			case <-time.After(2 * time.Second):
			}
		})
	}

	// Close by reconfiguring.
	lim.Reconfigure(t1, tokenBucketReconfigureArgs{newTokens: 10, newFillRate: 1})
	wg.Wait()

	require.Len(t, wokenUp, numWaiters)
}

func TestReservationDelayRemainsFixedAfterLimiterMutations(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.Reserve(context.Background(), InfDuration, t0, 500)

	re.True(reservation.Reserved())
	re.Equal(5*time.Second, reservation.DelayFrom(t0))

	lim.RemoveTokens(t1, 500)
	re.Equal(4*time.Second, reservation.DelayFrom(t1))

	lim.RefundTokens(t1, 1000)
	re.Equal(4*time.Second, reservation.DelayFrom(t1))

	lim.Reconfigure(t1, tokenBucketReconfigureArgs{
		newTokens:   1000,
		newFillRate: 1000,
	})
	re.Equal(4*time.Second, reservation.DelayFrom(t1))
}

func TestWaitNRefundPullsForwardAcceptedReservation(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))

	const fp = "github.com/tikv/pd/client/resource_group/controller/waitReservationsBeforeSelect"
	reached := make(chan struct{}, 1)
	re.NoError(failpoint.EnableCall(fp, func() {
		select {
		case reached <- struct{}{}:
		default:
		}
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := lim.waitN(ctx, time.Second, now, 1000)
		resultCh <- err
	}()

	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("waitN did not start waiting for the accepted reservation")
	}

	lim.RefundTokens(time.Now(), 1000)
	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("refund did not pull forward and wake the accepted reservation")
	}
}

func TestQueuedReservationsWithFixedFillRateAreSequenced(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 5000, 0, 0, make(chan notifyMsg, 1))

	const (
		requestRU  = 1000
		requestCnt = 10
	)
	expectedInterval := time.Duration(float64(time.Second) * requestRU / 5000)
	var previous time.Time
	for i := range requestCnt {
		reservation := lim.reserveQueued(context.Background(), InfDuration, t0, requestRU)
		re.Equal(queuedReservationWaiting, reservation.state)
		expected := t0.Add(time.Duration(i+1) * expectedInterval)
		re.Equal(expected, reservation.timeToAct)
		if i > 0 {
			re.False(reservation.timeToAct.Before(previous))
			re.Equal(expectedInterval, reservation.timeToAct.Sub(previous))
		}
		previous = reservation.timeToAct
	}
}

func TestReconfigureReflowsQueuedReservations(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	ctx := context.Background()

	oldReservations := make([]*queuedReservation, 3)
	for i := range oldReservations {
		oldReservations[i] = lim.reserveQueued(ctx, InfDuration, t0, 1000)
		re.Equal(queuedReservationWaiting, oldReservations[i].state)
	}
	re.Equal(t0.Add(10*time.Second), oldReservations[0].timeToAct)
	re.Equal(t0.Add(20*time.Second), oldReservations[1].timeToAct)
	re.Equal(t0.Add(30*time.Second), oldReservations[2].timeToAct)

	lim.Reconfigure(t1, tokenBucketReconfigureArgs{
		newTokens:   5000,
		newFillRate: 1000,
	})

	for i := range oldReservations {
		re.Zero(queuedDelayFrom(oldReservations[i], t1),
			"existing sleeping reservations should be reflowed after Reconfigure")
	}

	newReservation := lim.reserveQueued(ctx, InfDuration, t1, 1000)
	re.Equal(queuedReservationReady, newReservation.state)
	re.Equal(t1, newReservation.timeToAct)
	re.False(newReservation.timeToAct.Before(oldReservations[0].timeToAct),
		"new reservations must not slip ahead of old sleepers after Reconfigure")
}

func TestReconfigureWakesQueuedReservationWaiter(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reservation := lim.reserveQueued(ctx, time.Second, now, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)
	resultCh := make(chan error, 1)
	go func() {
		_, err := reservation.wait(ctx)
		resultCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	lim.Reconfigure(now.Add(20*time.Millisecond), tokenBucketReconfigureArgs{
		newTokens:   1000,
		newFillRate: 1000,
	})

	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("reconfigure should reflow and wake the existing future reservation")
	}
}

func TestReconfigureToUnlimitedWakesQueuedReservationWaiter(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reservation := lim.reserveQueued(ctx, 2*time.Second, now, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)
	resultCh := make(chan error, 1)
	go func() {
		_, err := reservation.wait(ctx)
		resultCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	lim.Reconfigure(now.Add(20*time.Millisecond), tokenBucketReconfigureArgs{
		newTokens:   0,
		newFillRate: 0,
		newBurst:    -1,
	})

	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("unlimited reconfigure should wake the existing future reservation")
	}
}

func TestReconfigureToUnlimitedResetsLedgerAfterDueReservation(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), InfDuration, t0, 100)

	lim.Reconfigure(t2, tokenBucketReconfigureArgs{
		newTokens: 50,
		newBurst:  -1,
	})

	re.Equal(queuedReservationReady, reservation.state)
	re.InDelta(50, lim.tokens, 1e-9)
}

func TestQueuedReservationWaitWakesWhenCanceled(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reservation := lim.reserveQueued(ctx, 2*time.Second, now, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)
	resultCh := make(chan error, 1)
	go func() {
		_, err := reservation.wait(ctx)
		resultCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	reservation.cancelAt(now.Add(20 * time.Millisecond))

	select {
	case err := <-resultCh:
		re.ErrorIs(err, errQueuedReservationCanceled)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceling the queued reservation should wake its waiter")
	}
}

func TestReconfigureKeepsQueuedReservationsWithinNewFillRateEnvelope(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 5000, 0, 0, make(chan notifyMsg, 1))

	const (
		pageRU           = 256
		reservationCount = 20
	)
	ctx := context.Background()
	oldReservations := make([]*queuedReservation, reservationCount)
	for i := range oldReservations {
		oldReservations[i] = lim.reserveQueued(ctx, InfDuration, t0, pageRU)
		re.Equal(queuedReservationWaiting, oldReservations[i].state)
	}

	reconfigureAt := t0.Add(50 * time.Millisecond)
	lim.Reconfigure(reconfigureAt, tokenBucketReconfigureArgs{
		newTokens:   0,
		newFillRate: 1000,
		newBurst:    0,
	})

	reflowedRU := reservedRUInWindow(oldReservations, reconfigureAt, reconfigureAt.Add(time.Second))

	lim.mu.Lock()
	fillRateAfterReconfigure := lim.fillRate
	lim.mu.Unlock()
	re.LessOrEqual(reflowedRU, float64(fillRateAfterReconfigure)+pageRU,
		"reflow keeps old reservations close to the post-reconfigure envelope")
}

func TestRemoveTokensReflowsQueuedReservations(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	ctx := context.Background()

	a := lim.reserveQueued(ctx, InfDuration, t0, 500)
	b := lim.reserveQueued(ctx, InfDuration, t0, 500)
	re.Equal(5*time.Second, queuedDelayFrom(a, t0))
	re.Equal(10*time.Second, queuedDelayFrom(b, t0))

	lim.RemoveTokens(t1, 500)
	re.Equal(9*time.Second, queuedDelayFrom(a, t1))
	re.Equal(14*time.Second, queuedDelayFrom(b, t1))
}

func TestAcceptedQueuedReservationCanMoveBeyondInitialWaitLimit(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), 5*time.Second, t0, 500)

	re.Equal(queuedReservationWaiting, reservation.state)
	re.Equal(t5, reservation.timeToAct)

	lim.RemoveTokens(t1, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)
	re.Equal(t0.Add(15*time.Second), reservation.timeToAct)
}

func TestMutationDoesNotRevokeDueQueuedReservation(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 100, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), InfDuration, t0, 1000)

	re.Equal(queuedReservationWaiting, reservation.state)
	re.Equal(t0.Add(10*time.Second), reservation.timeToAct)

	lim.RemoveTokens(t0.Add(20*time.Second), 100)
	re.Equal(queuedReservationReady, reservation.state)
	re.Equal(t0.Add(10*time.Second), reservation.timeToAct)
}

func TestRefundTokensReflowsQueuedReservations(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	ctx := context.Background()

	a := lim.reserveQueued(ctx, InfDuration, t0, 500)
	b := lim.reserveQueued(ctx, InfDuration, t0, 500)
	c := lim.reserveQueued(ctx, InfDuration, t0, 500)
	re.Equal(5*time.Second, queuedDelayFrom(a, t0))
	re.Equal(10*time.Second, queuedDelayFrom(b, t0))
	re.Equal(15*time.Second, queuedDelayFrom(c, t0))

	lim.RefundTokens(t1, 600)
	re.Zero(queuedDelayFrom(a, t1))
	re.Equal(3*time.Second, queuedDelayFrom(b, t1))
	re.Equal(8*time.Second, queuedDelayFrom(c, t1))
	re.False(b.timeToAct.After(c.timeToAct), "refund reflow must preserve FIFO order")

	lim.RefundTokens(t1, 300)
	re.Zero(queuedDelayFrom(b, t1))
	re.Equal(5*time.Second, queuedDelayFrom(c, t1))

	lim.RefundTokens(t1, 1000)
	re.Zero(queuedDelayFrom(c, t1))
}

func TestRefundReflowPreservesCommittedCreditAboveBurst(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 100, 0, make(chan notifyMsg, 1))
	ctx := context.Background()
	a := lim.reserveQueued(ctx, InfDuration, t0, 100)
	b := lim.reserveQueued(ctx, InfDuration, t0, 100)

	lim.RefundTokens(t0, 1000)
	re.Zero(queuedDelayFrom(a, t0))
	re.Zero(queuedDelayFrom(b, t0))
	re.InDelta(800, lim.tokens, 1e-9)

	c := lim.reserveQueued(ctx, InfDuration, t0, 100)
	re.Equal(queuedReservationReady, a.state)
	re.Equal(queuedReservationReady, b.state)
	re.Equal(t0, c.timeToAct)
	re.InDelta(0, lim.tokens, 1e-9,
		"new admission clamps unused credit to burst after committed reservations become ready")
}

func TestRefundDoesNotDiscardAccruedCreditForLargeQueuedReservation(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 100, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), InfDuration, t0, 500)
	re.Equal(t5, reservation.timeToAct)

	lim.RefundTokens(t2, 50)
	re.Equal(t0.Add(4500*time.Millisecond), reservation.timeToAct)
	re.InDelta(-250, lim.tokens, 1e-9)
}

func TestStaleNowReflowUsesLimiterLogicalClock(t *testing.T) {
	resetTime()
	tests := []struct {
		name      string
		mutate    func(lim *Limiter, first *queuedReservation, staleNow time.Time)
		wantFirst time.Time
		wantLast  time.Time
	}{
		{
			name: "remove tokens",
			mutate: func(lim *Limiter, _ *queuedReservation, staleNow time.Time) {
				lim.RemoveTokens(staleNow, 100)
			},
			wantFirst: t6,
			wantLast:  t0.Add(11 * time.Second),
		},
		{
			name: "refund tokens",
			mutate: func(lim *Limiter, _ *queuedReservation, staleNow time.Time) {
				lim.RefundTokens(staleNow, 100)
			},
			wantFirst: t4,
			wantLast:  t0.Add(9 * time.Second),
		},
		{
			name: "reconfigure",
			mutate: func(lim *Limiter, _ *queuedReservation, staleNow time.Time) {
				lim.Reconfigure(staleNow, tokenBucketReconfigureArgs{
					newTokens:   100,
					newFillRate: 100,
				})
			},
			wantFirst: t4,
			wantLast:  t0.Add(9 * time.Second),
		},
		{
			name: "cancel",
			mutate: func(_ *Limiter, first *queuedReservation, staleNow time.Time) {
				first.cancelAt(staleNow)
			},
			wantLast: t5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
			first := lim.reserveQueued(context.Background(), InfDuration, t0, 500)
			last := lim.reserveQueued(context.Background(), InfDuration, t0, 500)

			lim.RemoveTokens(t2, 0)
			tt.mutate(lim, first, t1)

			re.Equal(t2, lim.last)
			if !tt.wantFirst.IsZero() {
				re.Equal(tt.wantFirst, first.timeToAct)
			}
			re.Equal(tt.wantLast, last.timeToAct)
		})
	}
}

func TestQueuedReservationUsesLimiterLogicalClockForStaleNow(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	lim.RemoveTokens(t2, 200)

	reservation := lim.reserveQueued(context.Background(), InfDuration, t1, 100)
	re.Equal(t2, lim.last)
	re.Equal(t0.Add(3*time.Second), reservation.timeToAct)
}

func TestRejectedQueuedReservationCleanupAdvancesLogicalClock(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	first := lim.reserveQueued(context.Background(), InfDuration, t0, 500)
	last := lim.reserveQueued(context.Background(), InfDuration, t0, 1500)

	rejected := lim.reserveQueued(context.Background(), 0, t0.Add(10*time.Second), 2000)
	re.Equal(queuedReservationRejected, rejected.state)
	re.Equal(queuedReservationReady, first.state)
	re.Equal(t0.Add(10*time.Second), lim.last)

	lim.Reconfigure(t1, tokenBucketReconfigureArgs{
		newFillRate: 200,
		newBurst:    0,
	})
	re.Equal(t0.Add(10*time.Second), lim.last)
	re.Equal(t0.Add(15*time.Second), last.timeToAct)
}

func TestQueuedReservationAdmissionUsesShorterContextDeadline(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(100*time.Millisecond))
	defer cancel()

	reservation := lim.reserveQueued(ctx, time.Second, now, 500)
	re.Equal(queuedReservationRejected, reservation.state)
	delay, err := reservation.wait(ctx)
	re.Equal(500*time.Millisecond, delay)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
}

func TestCancelAtReflowsRemainingQueuedReservations(t *testing.T) {
	re := require.New(t)
	resetTime()
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	ctx := context.Background()

	a := lim.reserveQueued(ctx, InfDuration, t0, 500)
	b := lim.reserveQueued(ctx, InfDuration, t0, 500)
	c := lim.reserveQueued(ctx, InfDuration, t0, 500)
	re.Equal(5*time.Second, queuedDelayFrom(a, t0))
	re.Equal(10*time.Second, queuedDelayFrom(b, t0))
	re.Equal(15*time.Second, queuedDelayFrom(c, t0))

	a.cancelAt(t1)
	re.Equal(4*time.Second, queuedDelayFrom(b, t1))
	re.Equal(9*time.Second, queuedDelayFrom(c, t1))
	tokensAfterFirstCancel := lim.tokens
	a.cancelAt(t1)
	re.Equal(tokensAfterFirstCancel, lim.tokens, "cancelAt must not refund the same reservation twice")
}

func TestCancelAtDoesNotCancelOverdueQueuedReservation(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), InfDuration, t0, 100)
	re.Equal(t1, reservation.timeToAct)

	canceled := reservation.cancelAt(t2)
	re.False(canceled)
	re.Equal(queuedReservationReady, reservation.state)
	re.Equal(t2, lim.last)
	re.InDelta(100, lim.tokens, 1e-9)
}

func TestCancelAtUnlinksMiddleAndTailQueuedReservations(t *testing.T) {
	re := require.New(t)
	lim := NewLimiter(t0, 100, 0, 0, make(chan notifyMsg, 1))
	ctx := context.Background()
	a := lim.reserveQueued(ctx, InfDuration, t0, 500)
	b := lim.reserveQueued(ctx, InfDuration, t0, 500)
	c := lim.reserveQueued(ctx, InfDuration, t0, 500)

	b.cancelAt(t1)
	re.Equal(4*time.Second, queuedDelayFrom(a, t1))
	re.Equal(9*time.Second, queuedDelayFrom(c, t1))

	c.cancelAt(t1)
	d := lim.reserveQueued(ctx, InfDuration, t1, 500)
	re.Equal(9*time.Second, queuedDelayFrom(d, t1))
}

func TestWaitReservationsCancelHandlesNilReservation(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lim := NewLimiter(t0, 1, 0, 0, make(chan notifyMsg, 1))
	failed := lim.Reserve(ctx, InfDuration, t0, 1)

	delay, err := WaitReservations(context.Background(), t0, []*Reservation{nil, failed})
	re.Zero(delay)
	re.ErrorIs(err, context.Canceled)
}

func TestQueuedReservationWaitFailpointRunsOnceAcrossReflow(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), 2*time.Second, now, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)

	const fp = "github.com/tikv/pd/client/resource_group/controller/waitReservationsBeforeSelect"
	reached := make(chan struct{}, 1)
	var calls atomic.Int32
	re.NoError(failpoint.EnableCall(fp, func() {
		if calls.Add(1) == 1 {
			reached <- struct{}{}
		}
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := reservation.wait(ctx)
		resultCh <- err
	}()

	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("queued reservation wait did not reach the pre-select hook")
	}

	lim.RefundTokens(now.Add(10*time.Millisecond), 100)
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-resultCh:
		re.ErrorIs(err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued reservation wait did not exit after cancellation")
	}
	re.Equal(int32(1), calls.Load(), "the synchronization failpoint must remain a one-shot hook")
}

func TestQueuedReservationWaitReturnsSuccessWhenReadyBeatsContextCancel(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	lim := NewLimiter(now, 1000, 0, 0, make(chan notifyMsg, 1))
	reservation := lim.reserveQueued(context.Background(), 2*time.Second, now, 1000)
	re.Equal(queuedReservationWaiting, reservation.state)

	const fp = "github.com/tikv/pd/client/resource_group/controller/waitReservationsBeforeSelect"
	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall(fp, func() {
		close(reached)
		<-release
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := reservation.wait(ctx)
		resultCh <- err
	}()

	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("queued reservation wait did not reach the pre-select hook")
	}

	lim.mu.Lock()
	lim.removeQueuedReservationLocked(reservation)
	reservation.state = queuedReservationReady
	lim.mu.Unlock()
	cancel()
	close(release)

	select {
	case err := <-resultCh:
		re.NoError(err)
	case <-time.After(time.Second):
		t.Fatal("queued reservation wait did not resolve the ready/cancel race")
	}
}

func queuedDelayFrom(reservation *queuedReservation, now time.Time) time.Duration {
	delay := reservation.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

func reservedRUInWindow(reservations []*queuedReservation, start, end time.Time) float64 {
	var total float64
	for _, reservation := range reservations {
		if !reservation.timeToAct.Before(start) && reservation.timeToAct.Before(end) {
			total += reservation.tokens
		}
	}
	return total
}

const testCaseRunTime = 4 * time.Second

func testQPSCase(concurrency int, reserveN int64, limit int64) (qps float64, ru float64, needWait time.Duration) {
	nc := make(chan notifyMsg, 1)
	lim := NewLimiter(time.Now(), fillRate(limit), limit, float64(limit), nc)
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
				if r.Reserved() {
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
