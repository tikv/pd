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

package deadline

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWatcher(t *testing.T) {
	synctest.Test(t, runWatcherTest)
}

// Bubble-owned timers keep repeated runs independent of the global timer pool.
type testTimerPool struct{}

func (testTimerPool) Get(d time.Duration) *time.Timer { return time.NewTimer(d) }
func (testTimerPool) Put(timer *time.Timer)           { timer.Stop() }

func runWatcherTest(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := newWatcherWithTimerPool(ctx, 10, "test", testTimerPool{})
	var deadlineReached atomic.Bool
	deadlineDone := make(chan struct{})
	done := watcher.Start(ctx, time.Millisecond, func() {
		deadlineReached.Store(true)
		close(deadlineDone)
	})
	re.NotNil(done)
	select {
	case <-deadlineDone:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline callback did not run")
	}
	re.True(deadlineReached.Load())

	deadlineReached.Store(false)
	done = watcher.Start(ctx, 500*time.Millisecond, func() {
		deadlineReached.Store(true)
	})
	re.NotNil(done)
	done <- struct{}{}
	time.Sleep(time.Second)
	re.False(deadlineReached.Load())

	deadCtx, deadCancel := context.WithCancel(ctx)
	deadCancel()
	deadlineReached.Store(false)
	done = watcher.Start(deadCtx, time.Millisecond, func() {
		deadlineReached.Store(true)
	})
	re.Nil(done)
	re.False(deadlineReached.Load())
	cancel()
	synctest.Wait()
}
