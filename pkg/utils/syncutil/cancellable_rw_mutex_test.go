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

package syncutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lockAttempt struct {
	errCh     chan error
	releaseCh chan struct{}
	doneCh    chan struct{}
	cancel    context.CancelFunc
	once      sync.Once
}

func startLockAttempt(t *testing.T, parent context.Context, lock func(context.Context) error, unlock func()) *lockAttempt {
	t.Helper()

	ctx, cancel := context.WithCancel(parent)
	attempt := &lockAttempt{
		errCh:     make(chan error, 1),
		releaseCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
		cancel:    cancel,
	}

	go func() {
		defer close(attempt.doneCh)

		err := lock(ctx)
		attempt.errCh <- err
		if err != nil {
			return
		}

		<-attempt.releaseCh
		unlock()
	}()

	t.Cleanup(attempt.cleanup)
	return attempt
}

func startWriterAttempt(t *testing.T, parent context.Context, mu *CancellableRWMutex) *lockAttempt {
	t.Helper()
	return startLockAttempt(t, parent, mu.Lock, mu.Unlock)
}

func startReaderAttempt(t *testing.T, parent context.Context, mu *CancellableRWMutex) *lockAttempt {
	t.Helper()
	return startLockAttempt(t, parent, mu.RLock, mu.RUnlock)
}

func (a *lockAttempt) release() {
	a.once.Do(func() {
		close(a.releaseCh)
	})
}

func (a *lockAttempt) cleanup() {
	a.cancel()
	a.release()
	<-a.doneCh
}

func mustBlock[T any](t *testing.T, ch <-chan T) {
	t.Helper()

	select {
	case <-ch:
		require.FailNow(t, "received result while still expected to block")
	case <-time.After(time.Millisecond * 100):
	}
}

func mustReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case res := <-ch:
		return res
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for result")
		var zero T
		return zero
	}
}

func assertMutexUsable(t *testing.T, mu *CancellableRWMutex) {
	t.Helper()

	re := require.New(t)
	re.NoError(mu.RLock(context.Background()))
	mu.RUnlock()
	re.NoError(mu.Lock(context.Background()))
	mu.Unlock()
}

func TestCancellableRWMutexBasicLockingAndWakeup(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var mu CancellableRWMutex

	// w-w blocks, and the waiter wakes after the holder unlocks.
	re.NoError(mu.Lock(ctx))
	writerWhileWrite := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, writerWhileWrite.errCh)
	mu.Unlock()
	re.NoError(mustReceive(t, writerWhileWrite.errCh))
	writerWhileWrite.release()

	// w-r blocks, and the waiter wakes after the writer unlocks.
	re.NoError(mu.Lock(ctx))
	readerWhileWrite := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, readerWhileWrite.errCh)
	mu.Unlock()
	re.NoError(mustReceive(t, readerWhileWrite.errCh))
	readerWhileWrite.release()

	// r-r does not block.
	re.NoError(mu.RLock(ctx))
	readerWhileRead := startReaderAttempt(t, ctx, &mu)
	re.NoError(mustReceive(t, readerWhileRead.errCh))

	// r-w blocks until all readers release, then wakes.
	writerWhileRead := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, writerWhileRead.errCh)
	mu.RUnlock()
	mustBlock(t, writerWhileRead.errCh)
	readerWhileRead.release()
	re.NoError(mustReceive(t, writerWhileRead.errCh))
	writerWhileRead.release()
}

func TestCancellableRWMutexCancellation(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var mu CancellableRWMutex

	re.NoError(mu.Lock(ctx))
	readerCtx, readerCancel := context.WithCancel(ctx)
	blockedReader := startReaderAttempt(t, readerCtx, &mu)
	mustBlock(t, blockedReader.errCh)
	readerCancel()
	re.ErrorIs(mustReceive(t, blockedReader.errCh), context.Canceled)
	mu.Unlock()
	assertMutexUsable(t, &mu)

	re.NoError(mu.RLock(ctx))
	writerCtx, writerCancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer writerCancel()
	blockedWriter := startWriterAttempt(t, writerCtx, &mu)
	re.ErrorIs(mustReceive(t, blockedWriter.errCh), context.DeadlineExceeded)
	mu.RUnlock()
	assertMutexUsable(t, &mu)
}

func TestCancellableRWMutexWakeupOrdering(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var mu CancellableRWMutex

	// Releasing a writer wakes one queued writer at a time.
	re.NoError(mu.Lock(ctx))
	writer1 := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, writer1.errCh)
	writer2 := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, writer2.errCh)
	mu.Unlock()
	re.NoError(mustReceive(t, writer1.errCh))
	mustBlock(t, writer2.errCh)
	writer1.release()
	re.NoError(mustReceive(t, writer2.errCh))
	writer2.release()

	// Releasing a writer wakes all queued readers.
	re.NoError(mu.Lock(ctx))
	reader1 := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, reader1.errCh)
	reader2 := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, reader2.errCh)
	reader3 := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, reader3.errCh)
	mu.Unlock()
	re.NoError(mustReceive(t, reader1.errCh))
	re.NoError(mustReceive(t, reader2.errCh))
	re.NoError(mustReceive(t, reader3.errCh))
	reader1.release()
	reader2.release()
	reader3.release()

	// With an active reader and a queued writer, a later reader stays queued behind
	// the writer and only enters after the writer acquires and releases.
	re.NoError(mu.RLock(ctx))
	queuedWriter := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, queuedWriter.errCh)
	queuedReader := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, queuedReader.errCh)
	mu.RUnlock()
	re.NoError(mustReceive(t, queuedWriter.errCh))
	mustBlock(t, queuedReader.errCh)
	queuedWriter.release()
	re.NoError(mustReceive(t, queuedReader.errCh))
	queuedReader.release()

	// If the queued writer is canceled before the original reader releases, the
	// later reader can join immediately.
	re.NoError(mu.RLock(ctx))
	cancelWriterCtx, cancelWriter := context.WithCancel(ctx)
	queuedCanceledWriter := startWriterAttempt(t, cancelWriterCtx, &mu)
	mustBlock(t, queuedCanceledWriter.errCh)
	readerAfterCanceledWriter := startReaderAttempt(t, ctx, &mu)
	mustBlock(t, readerAfterCanceledWriter.errCh)
	cancelWriter()
	re.ErrorIs(mustReceive(t, queuedCanceledWriter.errCh), context.Canceled)
	re.NoError(mustReceive(t, readerAfterCanceledWriter.errCh))
	readerAfterCanceledWriter.release()
	mu.RUnlock()
}

func TestCancellableRWMutexPanicsAndRecoversToUsableState(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var mu CancellableRWMutex

	re.PanicsWithValue(cancellableRWMutexUnlockPanic, func() {
		mu.Unlock()
	})
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	re.PanicsWithValue(cancellableRWMutexRUnlockPanic, func() {
		mu.RUnlock()
	})
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	re.NoError(mu.Lock(ctx))
	re.PanicsWithValue(cancellableRWMutexRUnlockPanic, func() {
		mu.RUnlock()
	})
	mu.Unlock()
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	re.NoError(mu.RLock(ctx))
	re.PanicsWithValue(cancellableRWMutexUnlockPanic, func() {
		mu.Unlock()
	})
	mu.RUnlock()
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	re.NoError(mu.Lock(ctx))
	mu.Unlock()
	re.PanicsWithValue(cancellableRWMutexUnlockPanic, func() {
		mu.Unlock()
	})
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	re.NoError(mu.RLock(ctx))
	mu.RUnlock()
	re.PanicsWithValue(cancellableRWMutexRUnlockPanic, func() {
		mu.RUnlock()
	})
	assertMutexUsable(t, &mu)
}

func TestCancellableRWMutexPanicsOnInconsistentState(t *testing.T) {
	re := require.New(t)
	var mu CancellableRWMutex

	mu.state.Store(-1)
	re.PanicsWithValue(cancellableRWMutexInconsistentStatePanic, func() {
		re.NoError(mu.RLock(context.Background()))
	})
	mu.state.Store(0)
	assertMutexUsable(t, &mu)

	mu = CancellableRWMutex{}
	mu.state.Store(1)
	re.PanicsWithValue(cancellableRWMutexInconsistentStatePanic, func() {
		re.NoError(mu.Lock(context.Background()))
	})
	mu.state.Store(0)
	assertMutexUsable(t, &mu)
}

func TestCancellableRWMutexTryLockAndTryRLock(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var mu CancellableRWMutex

	// Basic success paths.
	re.True(mu.TryLock())
	re.False(mu.TryLock())
	re.False(mu.TryRLock())
	mu.Unlock()

	re.True(mu.TryRLock())
	re.True(mu.TryRLock())
	re.False(mu.TryLock())
	mu.RUnlock()
	mu.RUnlock()

	// TryRLock fails while a writer holds the lock, then succeeds after release.
	re.NoError(mu.Lock(ctx))
	re.False(mu.TryRLock())
	mu.Unlock()
	re.True(mu.TryRLock())
	mu.RUnlock()

	// TryLock fails while readers hold the lock, then succeeds after all release.
	re.NoError(mu.RLock(ctx))
	re.False(mu.TryLock())
	mu.RUnlock()
	re.True(mu.TryLock())
	mu.Unlock()

	// A waiting writer also blocks subsequent TryRLock attempts.
	re.NoError(mu.RLock(ctx))
	queuedWriter := startWriterAttempt(t, ctx, &mu)
	mustBlock(t, queuedWriter.errCh)
	re.False(mu.TryRLock())
	mu.RUnlock()
	re.NoError(mustReceive(t, queuedWriter.errCh))
	queuedWriter.release()

	// If the waiting writer is canceled, TryRLock can join the existing readers again.
	re.NoError(mu.RLock(ctx))
	queuedWriterCtx, cancelQueuedWriter := context.WithCancel(ctx)
	queuedCanceledWriter := startWriterAttempt(t, queuedWriterCtx, &mu)
	mustBlock(t, queuedCanceledWriter.errCh)
	re.False(mu.TryRLock())
	cancelQueuedWriter()
	re.ErrorIs(mustReceive(t, queuedCanceledWriter.errCh), context.Canceled)
	re.True(mu.TryRLock())
	mu.RUnlock()
	mu.RUnlock()

	assertMutexUsable(t, &mu)
}

