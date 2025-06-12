// Copyright 2022 TiKV Project Authors.
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimiter(t *testing.T) {
	re := require.New(t)
	cl := NewConcurrencyLimiter(10)
	for range 10 {
		re.True(cl.allow())
	}
	re.False(cl.allow())
	cl.release()
	re.True(cl.allow())
	re.Equal(uint64(10), cl.getLimit())
	re.Equal(uint64(10), cl.getMaxConcurrency())
	re.Equal(uint64(10), cl.getMaxConcurrency())
	cl.setLimit(5)
	re.Equal(uint64(5), cl.getLimit())
	re.Equal(uint64(10), cl.GetRunningTasksNum())
	cl.release()
	re.Equal(uint64(9), cl.GetRunningTasksNum())
	for range 9 {
		cl.release()
	}
	re.Equal(uint64(10), cl.getMaxConcurrency())
	for range 5 {
		re.True(cl.allow())
	}
	re.Equal(uint64(5), cl.GetRunningTasksNum())
	for range 5 {
		cl.release()
	}
	re.Equal(uint64(5), cl.getMaxConcurrency())
	re.Equal(uint64(0), cl.getMaxConcurrency())
}

func TestConcurrencyLimiter2(t *testing.T) {
	limit := uint64(2)
	limiter := NewConcurrencyLimiter(limit)

	require.Equal(t, uint64(0), limiter.GetRunningTasksNum(), "Expected running tasks to be 0")
	require.Equal(t, uint64(0), limiter.GetWaitingTasksNum(), "Expected waiting tasks to be 0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Acquire two tokens
	token1, err := limiter.AcquireToken(ctx)
	require.NoError(t, err, "Failed to acquire token")

	token2, err := limiter.AcquireToken(ctx)
	require.NoError(t, err, "Failed to acquire token")

	require.Equal(t, limit, limiter.GetRunningTasksNum(), "Expected running tasks to be 2")

	// Try to acquire third token, it should not be able to acquire immediately due to limit
	go func() {
		_, err := limiter.AcquireToken(ctx)
		require.NoError(t, err, "Failed to acquire token")
	}()

	time.Sleep(100 * time.Millisecond) // Give some time for the goroutine to run
	require.Equal(t, uint64(1), limiter.GetWaitingTasksNum(), "Expected waiting tasks to be 1")

	// Release a token
	limiter.ReleaseToken(token1)
	time.Sleep(100 * time.Millisecond) // Give some time for the goroutine to run
	require.Equal(t, uint64(2), limiter.GetRunningTasksNum(), "Expected running tasks to be 2")
	require.Equal(t, uint64(0), limiter.GetWaitingTasksNum(), "Expected waiting tasks to be 0")

	// Release the second token
	limiter.ReleaseToken(token2)
	time.Sleep(100 * time.Millisecond) // Give some time for the goroutine to run
	require.Equal(t, uint64(1), limiter.GetRunningTasksNum(), "Expected running tasks to be 1")
}

func TestConcurrencyLimiterAcquire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	limiter := NewConcurrencyLimiter(20)
	sum := int64(0)
	start := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(100)
	for i := range 100 {
		go func(i int) {
			defer wg.Done()
			token, err := limiter.AcquireToken(ctx)
			if err != nil {
				fmt.Printf("Task %d failed to acquire: %v\n", i, err)
				return
			}
			defer limiter.ReleaseToken(token)
			// simulate takes some time
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&sum, 1)
		}(i)
	}
	wg.Wait()
	// We should have 20 tasks running concurrently, so it should take at least 50ms to complete
	require.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(50))
	require.Equal(t, int64(100), sum)
}
