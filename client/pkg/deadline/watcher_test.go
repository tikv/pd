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
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher := NewWatcher(ctx, 10, "test")
	var deadlineReached atomic.Bool
	done := watcher.Start(ctx, time.Millisecond, func() {
		deadlineReached.Store(true)
	})
	re.NotNil(done)
	time.Sleep(5 * time.Millisecond)
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
	time.Sleep(5 * time.Millisecond)
	re.False(deadlineReached.Load())
}
