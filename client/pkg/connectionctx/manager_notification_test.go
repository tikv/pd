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

package connectionctx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreWakesAllWaiters(t *testing.T) {
	manager := NewManager[int]()
	t.Cleanup(manager.ReleaseAll)
	updated := manager.Updated()
	results := make(chan int, 32)
	for range cap(results) {
		go func() {
			select {
			case <-updated:
				if cc := manager.GetConnectionCtx("leader"); cc != nil {
					results <- cc.Stream
				}
			case <-t.Context().Done():
			}
		}()
	}
	ctx, cancel := context.WithCancel(t.Context())
	require.True(t, manager.Store(ctx, cancel, "leader", 42))
	for range cap(results) {
		select {
		case value := <-results:
			require.Equal(t, 42, value)
		case <-time.After(time.Second):
			t.Fatal("a waiter did not observe the stored connection")
		}
	}
}

func TestStoreNotificationGenerations(t *testing.T) {
	manager := NewManager[int]()
	t.Cleanup(manager.ReleaseAll)
	for _, exclusive := range []bool{false, true} {
		updated := manager.Updated()
		ctx, cancel := context.WithCancel(t.Context())
		var stored bool
		if exclusive {
			stored = manager.CleanAllAndStore(ctx, cancel, "new-leader", 2)
		} else {
			stored = manager.Store(ctx, cancel, "old-leader", 1)
		}
		require.True(t, stored)
		select {
		case <-updated:
		default:
			t.Fatal("a successful store did not wake waiters")
		}
	}
	updated := manager.Updated()
	ctx, cancel := context.WithCancel(t.Context())
	require.False(t, manager.Store(ctx, cancel, "new-leader", 3))
	require.False(t, manager.CleanAllAndStore(ctx, cancel, "new-leader", 3))
	cancel()
	select {
	case <-updated:
		t.Fatal("a rejected store woke waiters")
	default:
	}
	ctx, cancel = context.WithCancel(t.Context())
	require.True(t, manager.Store(ctx, cancel, "new-leader", 3, true))
	select {
	case <-updated:
		require.Equal(t, 3, manager.GetConnectionCtx("new-leader").Stream)
	default:
		t.Fatal("a replacement did not wake waiters")
	}
}

func TestConcurrentStoreNotifications(t *testing.T) {
	manager := NewManager[int]()
	t.Cleanup(manager.ReleaseAll)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				updated := manager.Updated()
				ctx, cancel := context.WithCancel(t.Context())
				manager.Store(ctx, cancel, "leader", 1, true)
				select {
				case <-updated:
				case <-t.Context().Done():
					return
				}
			}
		})
	}
	wg.Wait()
}
