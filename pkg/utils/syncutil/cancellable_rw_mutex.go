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
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

const maxReaderCount = 1 << 30

const (
	cancellableRWMutexInconsistentStatePanic = "syncutil.CancellableRWMutex: inconsistent state"
	cancellableRWMutexRUnlockPanic           = "syncutil.CancellableRWMutex: unpaired RUnlock call"
	cancellableRWMutexUnlockPanic            = "syncutil.CancellableRWMutex: unpaired Unlock call"
)

// CancellableRWMutex is a context-aware reader/writer lock.
//
// It is implemented on top of a weighted semaphore, so its scheduling behavior
// differs from sync.RWMutex. Like sync.RWMutex, it is not associated with a
// particular goroutine, but mismatched Unlock/RUnlock calls panic.
//
// The zero value of CancellableRWMutex is ready to use.
type CancellableRWMutex struct {
	initOnce sync.Once
	inner    *semaphore.Weighted
	// Counter of readers or writer.
	// -1 when a writer holds the lock, 0 when unlocked, and >0 for the
	// number of active readers.
	// This is not part of the implementation of the locking mechanism, but only for detecting misuses like
	// unpaired locking and unlocking calls.
	state atomic.Int64
}

func (m *CancellableRWMutex) init() {
	m.initOnce.Do(func() {
		m.inner = semaphore.NewWeighted(maxReaderCount)
	})
}

// RLock acquires a read lock or returns an error if ctx is canceled before the
// lock is acquired.
func (m *CancellableRWMutex) RLock(ctx context.Context) error {
	m.init()

	if err := m.inner.Acquire(ctx, 1); err != nil {
		return err
	}

	if state := m.state.Add(1); state <= 0 {
		m.state.Add(-1)
		m.inner.Release(1)
		panic(cancellableRWMutexInconsistentStatePanic)
	}
	return nil
}

// TryRLock tries to acquire a read lock without blocking.
// It reports whether the read lock was acquired.
func (m *CancellableRWMutex) TryRLock() bool {
	m.init()

	if !m.inner.TryAcquire(1) {
		return false
	}

	if state := m.state.Add(1); state <= 0 {
		m.state.Add(-1)
		m.inner.Release(1)
		panic(cancellableRWMutexInconsistentStatePanic)
	}
	return true
}

// RUnlock releases a read lock.
func (m *CancellableRWMutex) RUnlock() {
	m.init()

	for {
		state := m.state.Load()
		if state <= 0 {
			panic(cancellableRWMutexRUnlockPanic)
		}
		if m.state.CompareAndSwap(state, state-1) {
			break
		}
	}

	m.inner.Release(1)
}

// Lock acquires a write lock or returns an error if ctx is canceled before the
// lock is acquired.
func (m *CancellableRWMutex) Lock(ctx context.Context) error {
	m.init()

	if err := m.inner.Acquire(ctx, maxReaderCount); err != nil {
		return err
	}

	if !m.state.CompareAndSwap(0, -1) {
		m.inner.Release(maxReaderCount)
		panic(cancellableRWMutexInconsistentStatePanic)
	}
	return nil
}

// TryLock tries to acquire a write lock without blocking.
// It reports whether the write lock was acquired.
func (m *CancellableRWMutex) TryLock() bool {
	m.init()

	if !m.inner.TryAcquire(maxReaderCount) {
		return false
	}

	if !m.state.CompareAndSwap(0, -1) {
		m.inner.Release(maxReaderCount)
		panic(cancellableRWMutexInconsistentStatePanic)
	}
	return true
}

// Unlock releases a write lock.
func (m *CancellableRWMutex) Unlock() {
	m.init()

	if !m.state.CompareAndSwap(-1, 0) {
		panic(cancellableRWMutexUnlockPanic)
	}

	m.inner.Release(maxReaderCount)
}
