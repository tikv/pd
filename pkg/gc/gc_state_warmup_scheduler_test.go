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

package gc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/config"
)

// Scheduler tests use cached loads only, so no storage or metadata goroutines
// can interfere with the fake clock's deterministic worker scheduling.
func newGCWarmupSchedulerTest(t *testing.T) *gcStateWarmup {
	t.Helper()
	m := NewGCStateManager(endpoint.GCStateProvider{}, config.PDServerConfig{GCStateCacheWarmupConcurrency: 4}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	generation := &gcStateGeneration{ctx: ctx, cancel: cancel, done: make(chan struct{}), flights: make(map[uint32]*gcStateLoadFlight)}
	m.activeGeneration.Store(generation)
	w := newGCStateWarmup(m, generation, nil)
	w.nullPending = false
	w.recordScopeResult(constant.NullKeyspaceID, w.scopes[constant.NullKeyspaceID], nil, time.Now())
	return w
}

func failGCWarmupScope(t *testing.T, w *gcStateWarmup, id uint32, now time.Time, previousDelay time.Duration) {
	t.Helper()
	w.add(id)
	next, ok := w.next()
	require.True(t, ok)
	require.Equal(t, id, next)
	w.scopes[id].retryDelay = previousDelay
	w.recordScopeResult(id, w.scopes[id], errors.New("unavailable scope"), now)
}

func TestGCStateWarmupBoundsDueRetries(t *testing.T) {
	w := newGCWarmupSchedulerTest(t)
	now := time.Now()
	for id := range uint32(20000) {
		failGCWarmupScope(t, w, id, now, 0)
	}
	w.refresh(now.Add(time.Second))
	require.Len(t, w.candidates, endpoint.MaxGCSafePointBatchSize, "one scheduling turn must not promote every due scope while holding stateMu")
	for range endpoint.MaxGCSafePointBatchSize {
		_, ok := w.next()
		require.True(t, ok)
	}
	w.refresh(now.Add(time.Second))
	require.Len(t, w.candidates, endpoint.MaxGCSafePointBatchSize, "another turn must keep making bounded progress")
}

func TestGCStateWarmupGloballyGatesJoinedChecks(t *testing.T) {
	w := newGCWarmupSchedulerTest(t)
	now := time.Now()
	success := &gcStateLoadFlight{done: make(chan struct{})}
	failed := &gcStateLoadFlight{done: make(chan struct{})}
	for _, id := range []uint32{100, 101} {
		w.add(id)
		_, ok := w.next()
		require.True(t, ok)
	}
	w.recordBatchResult(gcStateLoadResult{joined: map[uint32]*gcStateLoadFlight{100: success, 101: failed}}, now)
	w.refresh(now.Add(100 * time.Millisecond))
	close(success.done)
	failed.err = errors.New("joined flight failed")
	close(failed.done)
	for range w.concurrency {
		w.refresh(now.Add(100 * time.Millisecond))
	}
	require.False(t, w.scopes[100].completed, "competing workers must not repeat the joined scan within its poll interval")
	w.refresh(now.Add(200 * time.Millisecond))
	require.True(t, w.scopes[100].completed, "flight success completes a scope even without a cache entry")
	require.False(t, w.scopes[101].completed)
	require.Equal(t, time.Second, w.scopes[101].retryDelay)
	w.refresh(now.Add(1199 * time.Millisecond))
	require.Empty(t, w.candidates)
	w.refresh(now.Add(1200 * time.Millisecond))
	require.Equal(t, []uint32{101}, w.candidates)
}

func TestGCStateWarmupSleepsUntilRetryDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newGCWarmupSchedulerTest(t)
		now := time.Now()
		for id := range uint32(20000) {
			failGCWarmupScope(t, w, id, now, 15*time.Second)
		}
		var turns atomic.Int32
		const schedule = "github.com/tikv/pd/pkg/gc/beforeGCStateWarmupSchedule"
		require.NoError(t, failpoint.EnableCall(schedule, func() { turns.Add(1) }))
		defer func() { require.NoError(t, failpoint.Disable(schedule)) }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.run(ctx)
		synctest.Wait()
		initial := turns.Load()
		require.EqualValues(t, w.concurrency, initial)
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, initial, turns.Load(), "long-backoff scopes must not wake workers on a periodic ticker")
		cancel()
		synctest.Wait()
		gcWarmupWait(t, w.done)
	})
}

func TestGCStateWarmupRetryOrderAndBackoff(t *testing.T) {
	w := newGCWarmupSchedulerTest(t)
	now := time.Now()
	failGCWarmupScope(t, w, 100, now, 15*time.Second)
	failGCWarmupScope(t, w, 101, now, time.Second)
	failGCWarmupScope(t, w, 102, now, 0)
	for _, tc := range []struct {
		delay time.Duration
		id    uint32
	}{{time.Second, 102}, {2 * time.Second, 101}, {30 * time.Second, 100}} {
		require.Equal(t, now.Add(tc.delay), w.nextDeadline())
		w.refresh(now.Add(tc.delay - time.Nanosecond))
		require.Empty(t, w.candidates)
		w.refresh(now.Add(tc.delay))
		id, ok := w.next()
		require.True(t, ok)
		require.Equal(t, tc.id, id)
		w.recordScopeResult(id, w.scopes[id], nil, now.Add(tc.delay))
	}
	require.True(t, w.nextDeadline().IsZero())

	w.add(103)
	_, ok := w.next()
	require.True(t, ok)
	for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		w.recordScopeResult(103, w.scopes[103], errors.New("retry still fails"), now)
		require.Len(t, w.retries, 1, "a scope must have only one pending retry")
		require.Equal(t, now.Add(delay), w.nextDeadline())
		now = now.Add(delay)
		w.refresh(now)
		id, ok := w.next()
		require.True(t, ok)
		require.Equal(t, uint32(103), id)
		require.Empty(t, w.retries)
	}
	w.recordScopeResult(103, w.scopes[103], nil, now)
	require.Zero(t, w.remaining)
	require.True(t, w.nextDeadline().IsZero())
}

func TestGCStateWarmupBoundsObsoleteRetries(t *testing.T) {
	w := newGCWarmupSchedulerTest(t)
	now := time.Now()
	for id := range uint32(20000) {
		failGCWarmupScope(t, w, id, now, 0)
	}
	w.index = newEnabledKeyspaceIndex(nil, "")
	w.index.publish(context.Background(), map[uint32]enabledKeyspace{}, 1)
	w.initialSnapshotReconciled = true
	w.refresh(now.Add(time.Second))
	require.Empty(t, w.candidates)
	require.Equal(t, 20000-endpoint.MaxGCSafePointBatchSize, w.remaining, "obsolete targets must consume the scheduling budget too")
	require.Len(t, w.retries, w.remaining)

	// Null GC still needs a retry even when the live keyspace index is empty.
	w = newGCWarmupSchedulerTest(t)
	w.index = newEnabledKeyspaceIndex(nil, "")
	w.index.publish(context.Background(), map[uint32]enabledKeyspace{}, 1)
	w.initialSnapshotReconciled = true
	scope := w.scopes[constant.NullKeyspaceID]
	scope.completed = false
	w.remaining++
	w.recordScopeResult(constant.NullKeyspaceID, scope, errors.New("null scope failed"), now)
	w.refresh(now.Add(time.Second))
	require.Equal(t, []uint32{constant.NullKeyspaceID}, w.candidates)
	require.False(t, scope.completed)
}

func TestGCStateWarmupReconcilesSchedulingContainers(t *testing.T) {
	w := newGCWarmupSchedulerTest(t)
	now := time.Now()
	for _, id := range []uint32{100, 101, 104} {
		failGCWarmupScope(t, w, id, now, 15*time.Second)
	}
	failGCWarmupScope(t, w, 107, now, time.Second)
	w.add(102)
	_, ok := w.next()
	require.True(t, ok)
	flight := &gcStateLoadFlight{done: make(chan struct{})}
	w.recordBatchResult(gcStateLoadResult{joined: map[uint32]*gcStateLoadFlight{102: flight}}, now)
	w.add(106)
	_, ok = w.next()
	require.True(t, ok)
	w.add(103)
	entries := append(gcWarmupEntries(104, 2), gcWarmupEntries(107, 1)...)
	w.onInitialSnapshot(entries)
	w.reconcileInitialSnapshot()
	require.Len(t, w.scopes, 4)
	require.Equal(t, []uint32{105}, w.candidates)
	require.Empty(t, w.joined)
	require.Len(t, w.retries, 2, "discarded page hints must not remain in the retry heap")
	require.Equal(t, now.Add(2*time.Second), w.nextDeadline(), "removing the last joined flight must also remove its poll deadline")
	// A stale in-flight result arriving after reconciliation cannot recreate work.
	w.recordBatchResult(gcStateLoadResult{failed: map[uint32]error{106: errors.New("stale load failed")}}, now)
	require.Len(t, w.retries, 2)
	close(flight.done)
	w.refresh(now.Add(30 * time.Second))
	require.ElementsMatch(t, []uint32{104, 105, 107}, w.candidates)
	for range 3 {
		id, ok := w.next()
		require.True(t, ok)
		w.recordScopeResult(id, w.scopes[id], nil, now)
	}
	require.True(t, w.tryFinish())
	require.True(t, w.nextDeadline().IsZero())
}

func TestGCStateWarmupEarlierDeadlineWakesWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newGCWarmupSchedulerTest(t)
		start := time.Now()
		failGCWarmupScope(t, w, 100, start, 15*time.Second)
		w.add(101)
		_, ok := w.next()
		require.True(t, ok)
		w.manager.gcStateCache.store(101, gcStateCacheEntry{})
		w.manager.gcStateCache.store(102, gcStateCacheEntry{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.run(ctx)
		synctest.Wait()
		time.Sleep(time.Second)
		// A different batch finishes while every worker is waiting for the
		// existing 30-second retry. Its earlier deadline must replace that wait.
		w.stateMu.Lock()
		w.recordBatchResult(gcStateLoadResult{failed: map[uint32]error{101: errors.New("new failure")}}, time.Now())
		w.stateMu.Unlock()
		w.notify()
		synctest.Wait()
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		require.False(t, w.scopes[101].completed)
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.True(t, w.scopes[101].completed)
		require.False(t, w.scopes[100].completed)
		// New metadata must also interrupt the remaining long retry deadline.
		w.onPage(gcWarmupEntries(102, 1))
		synctest.Wait()
		require.True(t, w.scopes[102].completed)
		w.onInitialSnapshot(gcWarmupEntries(101, 2))
		synctest.Wait()
		gcWarmupWait(t, w.done)
		require.Equal(t, start.Add(2*time.Second), time.Now())
	})
}

func TestGCStateWarmupInputBeforeWorkerWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newGCWarmupSchedulerTest(t)
		paused := make(chan struct{}, w.concurrency)
		resume := make(chan struct{})
		var resumeOnce sync.Once
		unblock := func() { resumeOnce.Do(func() { close(resume) }) }
		defer unblock()
		const beforeWait = "github.com/tikv/pd/pkg/gc/beforeGCStateWarmupWait"
		require.NoError(t, failpoint.EnableCall(beforeWait, func() {
			select {
			case <-resume:
				return
			default:
			}
			paused <- struct{}{}
			<-resume
		}))
		defer func() { require.NoError(t, failpoint.Disable(beforeWait)) }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.run(ctx)
		for range w.concurrency {
			gcWarmupWait(t, paused)
		}
		start := time.Now()
		w.manager.gcStateCache.store(100, gcStateCacheEntry{})
		w.onPage(gcWarmupEntries(100, 1))
		w.onInitialSnapshot(gcWarmupEntries(100, 1))
		// Notifications precede select, after each worker observed no deadline.
		unblock()
		synctest.Wait()
		gcWarmupWait(t, w.done)
		require.Equal(t, start, time.Now(), "incoming work must not wait for a timer to repair a lost wakeup")
	})
}

func TestGCStateWarmupIdleCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		retire   bool
	}{
		{"cancel-without-deadline", false, false},
		{"cancel-with-deadline", true, false},
		{"retire-without-deadline", false, true},
		{"retire-with-deadline", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newGCWarmupSchedulerTest(t)
				start := time.Now()
				if tc.deadline {
					failGCWarmupScope(t, w, 100, start, 15*time.Second)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go w.run(ctx)
				synctest.Wait()
				if tc.retire {
					w.generation.retire()
				} else {
					cancel()
				}
				synctest.Wait()
				gcWarmupWait(t, w.done)
				require.Equal(t, start, time.Now(), "idle workers must exit without waiting for a deadline")
				require.Nil(t, w.retries)
				require.Nil(t, w.joined)
			})
		})
	}
}
