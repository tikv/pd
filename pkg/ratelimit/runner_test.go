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
)

func TestConcurrentRunner(t *testing.T) {
	t.Run("RunTask", func(t *testing.T) {
		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Second)
		runner.Start(context.TODO())
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
		runner.Start(context.TODO())
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
		runner.Start(context.TODO())
		defer runner.Stop()
		for i := 1; i < 11; i++ {
			regionID := uint64(i)
			if i == 10 {
				regionID = 4
			}
			err := runner.RunTask(
				regionID,
				"test3",
				func(context.Context) {
					time.Sleep(time.Second)
				},
			)
			require.NoError(t, err)
			time.Sleep(1 * time.Millisecond)
		}

		updatedSubmitted := runner.pendingTasks[1].submittedAt
		lastSubmitted := runner.pendingTasks[len(runner.pendingTasks)-1].submittedAt
		require.Greater(t, updatedSubmitted, lastSubmitted)
	})
}
