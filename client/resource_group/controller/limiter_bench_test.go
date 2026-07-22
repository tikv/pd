// Copyright 2026 TiKV Project Authors.
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

package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

var queuedReservationBenchmarkDepths = []int{1, 10, 100, 1000, 10000}

func BenchmarkQueuedReservationRefundRemoveCycle(b *testing.B) {
	for _, depth := range queuedReservationBenchmarkDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			lim, now := newBenchmarkLimiterWithQueue(b, depth)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lim.RefundTokens(now, 1)
				lim.RemoveTokens(now, 1)
			}
		})
	}
}

func BenchmarkQueuedReservationReconfigureCycle(b *testing.B) {
	for _, depth := range queuedReservationBenchmarkDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			lim, now := newBenchmarkLimiterWithQueue(b, depth)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lim.Reconfigure(now, tokenBucketReconfigureArgs{newFillRate: 1_000_001})
				lim.Reconfigure(now, tokenBucketReconfigureArgs{newFillRate: 1_000_000})
			}
		})
	}
}

func BenchmarkQueuedReservationCancelAndReplace(b *testing.B) {
	for _, depth := range queuedReservationBenchmarkDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			lim, now := newBenchmarkLimiterWithQueue(b, depth)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lim.mu.Lock()
				reservation := lim.queuedHead
				lim.mu.Unlock()
				if !reservation.cancelAt(now) {
					b.Fatal("queued reservation was not canceled")
				}
				if replacement := lim.reserveQueued(context.Background(), InfDuration, now, 1_000_000); replacement.state != queuedReservationWaiting {
					b.Fatalf("replacement reservation state is %d", replacement.state)
				}
			}
		})
	}
}

// BenchmarkQueuedReservationBroadcastAndTimerRearm measures the scheduler,
// mutex, and timer-allocation amplification caused by closing the shared queue
// generation channel. The workers mirror the relevant part of
// queuedReservation.wait without requiring a production-only benchmark hook.
func BenchmarkQueuedReservationBroadcastAndTimerRearm(b *testing.B) {
	for _, depth := range queuedReservationBenchmarkDepths {
		b.Run(fmt.Sprintf("waiters=%d", depth), func(b *testing.B) {
			lim := NewLimiter(time.Now(), 1, 0, 0, make(chan notifyMsg, 1))
			ackCh := make(chan struct{}, depth)
			stopCh := make(chan struct{})
			var ready sync.WaitGroup
			var stopped sync.WaitGroup
			ready.Add(depth)
			stopped.Add(depth)
			for range depth {
				go func() {
					defer stopped.Done()
					lim.mu.Lock()
					updateCh := lim.queuedUpdateChannelLocked()
					lim.mu.Unlock()
					timer := time.NewTimer(time.Hour)
					ready.Done()
					for {
						select {
						case <-updateCh:
							stopAndDrainTimer(timer)
							lim.mu.Lock()
							updateCh = lim.queuedUpdateChannelLocked()
							lim.mu.Unlock()
							timer = time.NewTimer(time.Hour)
							ackCh <- struct{}{}
						case <-stopCh:
							stopAndDrainTimer(timer)
							return
						}
					}
				}()
			}
			ready.Wait()
			b.Cleanup(func() {
				close(stopCh)
				stopped.Wait()
			})
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				lim.mu.Lock()
				lim.signalQueuedUpdateLocked()
				lim.mu.Unlock()
				for range depth {
					<-ackCh
				}
			}
		})
	}
}

func newBenchmarkLimiterWithQueue(b *testing.B, depth int) (*Limiter, time.Time) {
	b.Helper()
	now := time.Unix(1_700_000_000, 0)
	lim := NewLimiter(now, 1_000_000, 0, 0, make(chan notifyMsg, 1))
	for range depth {
		reservation := lim.reserveQueued(context.Background(), InfDuration, now, 1_000_000)
		if reservation.state != queuedReservationWaiting {
			b.Fatalf("reservation state is %d", reservation.state)
		}
	}
	return lim, now
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
