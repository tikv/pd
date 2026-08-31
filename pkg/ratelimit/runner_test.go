// Copyright 2024 TiKV Project Authors.
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

	"github.com/tikv/pd/pkg/errs"
)

func TestConcurrentRunner(t *testing.T) {
	t.Run("RunTask", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner.Start(ctx)
		defer runner.Stop()

		var wg sync.WaitGroup
		for i := range 10 {
			time.Sleep(50 * time.Millisecond)
			wg.Add(1)
			err := runner.RunTask(
				uint64(i),
				"test1",
				func(context.Context) {
					defer wg.Done()
					time.Sleep(100 * time.Millisecond)
				},
			)
			require.NoError(t, err)
		}
		wg.Wait()
	})

	t.Run("MaxPendingDuration", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), 2*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner.Start(ctx)
		defer runner.Stop()
		var wg sync.WaitGroup
		for i := range 10 {
			wg.Add(1)
			err := runner.RunTask(
				uint64(i),
				"test2",
				func(context.Context) {
					defer wg.Done()
					time.Sleep(100 * time.Millisecond)
				},
			)
			if err != nil {
				wg.Done()
				// task 0 running
				// task 1 after recv by runner, blocked by task 1, wait on Acquire.
				// task 2 enqueue pendingTasks
				// task 3 enqueue pendingTasks
				// task 4 enqueue pendingTasks, check pendingTasks[0] timeout, report error
				require.GreaterOrEqual(t, i, 4)
			}
			time.Sleep(1 * time.Millisecond)
		}
		wg.Wait()
	})

	t.Run("DuplicatedTask", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner.Start(ctx)
		defer runner.Stop()

		for i := 1; i < 11; i++ {
			regionID := uint64(i)
			if i == 10 {
				regionID = 6
			}
			err := runner.RunTask(
				regionID,
				"test3",
				func(ctx context.Context) {
					select {
					case <-time.After(time.Second):
						// Normal completion
					case <-ctx.Done():
						// Context cancelled, return immediately
						return
					}
				},
			)
			require.NoError(t, err)
			time.Sleep(1 * time.Millisecond)
		}

		originalSubmitted, lastSubmitted := func() (time.Time, time.Time) {
			runner.pendingMu.Lock()
			defer runner.pendingMu.Unlock()
			var originalSubmitted time.Time
			for _, task := range runner.pendingTasks[runner.pendingHead:] {
				if task.id == 6 {
					originalSubmitted = task.submittedAt
				}
			}
			lastSubmitted := runner.pendingTasks[len(runner.pendingTasks)-1].submittedAt
			return originalSubmitted, lastSubmitted
		}()
		require.Less(t, originalSubmitted, lastSubmitted)
	})

	t.Run("DuplicatedTaskBeforeDispatch", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
		require.NoError(t, runner.RunTask(0, "test4", func(context.Context) {}))

		oldCalls := 0
		newCalls := 0
		require.NoError(t, runner.RunTask(1, "test4", func(context.Context) { oldCalls++ }))

		// Reproduce the state where the channel task has been consumed while
		// another task with the duplicated ID is still pending.
		<-runner.taskChan
		require.NoError(t, runner.RunTask(1, "test4", func(context.Context) { newCalls++ }))

		task := <-runner.taskChan
		task.f(context.Background())
		require.Zero(t, oldCalls)
		require.Equal(t, 1, newCalls)
		require.Zero(t, runner.pendingTaskNum())
	})

	t.Run("DuplicatedTaskKeepsQueueAge", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Second)
		require.NoError(t, runner.RunTask(1, "test4", func(context.Context) {}))
		require.NoError(t, runner.RunTask(2, "test4", func(context.Context) {}))

		originalSubmitted := time.Now().Add(-2 * time.Second)
		runner.pendingTasks[runner.pendingHead].submittedAt = originalSubmitted
		require.NoError(t, runner.RunTask(2, "test4", func(context.Context) {}))
		require.Equal(t, originalSubmitted, runner.pendingTasks[runner.pendingHead].submittedAt)
		require.ErrorIs(
			t,
			runner.RunTask(3, "test4", func(context.Context) {}),
			errs.ErrMaxWaitingTasksExceeded,
		)
	})

	t.Run("MaxPendingTasks", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
		runner.maxPendingTaskNum = 3
		require.NoError(t, runner.RunTask(0, "test5", func(context.Context) {}))
		for i := 1; i <= runner.maxPendingTaskNum; i++ {
			require.NoError(t, runner.RunTask(uint64(i), "test5", func(context.Context) {}))
		}

		require.Equal(t, runner.maxPendingTaskNum, runner.pendingTaskNum())
		require.NoError(t, runner.RunTask(1, "test5", func(context.Context) {}))
		require.ErrorIs(
			t,
			runner.RunTask(4, "test5", func(context.Context) {}, WithRetained(true)),
			errs.ErrMaxWaitingTasksExceeded,
		)
		require.Equal(t, runner.maxPendingTaskNum, runner.pendingTaskNum())
	})

	t.Run("CompactPendingTasks", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
		require.NoError(t, runner.RunTask(0, "test6", func(context.Context) {}))
		for i := 1; i <= initialCapacity*2; i++ {
			require.NoError(t, runner.RunTask(uint64(i), "test6", func(context.Context) {}))
		}
		require.Greater(t, cap(runner.pendingTasks), initialCapacity)

		for runner.pendingTaskNum() > 0 {
			<-runner.taskChan
			runner.processPendingTasks()
		}
		<-runner.taskChan
		require.Empty(t, runner.pendingTasks)
		require.Equal(t, initialCapacity, cap(runner.pendingTasks))
		require.Empty(t, runner.existTasks)
	})
}
