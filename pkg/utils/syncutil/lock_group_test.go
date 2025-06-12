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

package syncutil

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLockGroup(t *testing.T) {
	re := require.New(t)
	group := NewLockGroup(WithHash(func(id uint32) uint32 { return id & 0xF }))
	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func(spaceID uint32) {
			defer wg.Done()
			mustSequentialUpdateSingle(re, spaceID, group, concurrency)
		}(rand.Uint32())
	}
	wg.Wait()
	// Check that size of the lock group is limited.
	re.LessOrEqual(len(group.entries), 16)
}

func TestLockGroupWithRemoveEntryOnUnlock(t *testing.T) {
	re := require.New(t)
	group := NewLockGroup(WithRemoveEntryOnUnlock(true))
	maxID := 1024

	// Test Concurrent lock/unlock.
	var wg sync.WaitGroup
	wg.Add(maxID)
	for i := range maxID {
		go func(spaceID uint32) {
			defer wg.Done()
			mustSequentialUpdateSingle(re, spaceID, group, 10)
		}(uint32(i))
	}

	// Test range lock in a scenario with non-consecutive large key space. One of example is
	// keyspace group split loads non-consecutive keyspace meta in batches and lock all loaded
	// keyspace meta within a batch at the same time.
	for i := range maxID {
		group.Lock(uint32(i))
	}
	re.Len(group.entries, maxID)
	for i := range maxID {
		group.Unlock(uint32(i))
	}

	wg.Wait()
	// Check that size of the lock group is limited.
	re.Empty(group.entries)
}

// mustSequentialUpdateSingle checks that for any given update, update is sequential.
func mustSequentialUpdateSingle(re *require.Assertions, spaceID uint32, group *LockGroup, concurrency int) {
	total := 0
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			group.Lock(spaceID)
			defer group.Unlock(spaceID)
			total++
		}()
	}
	wg.Wait()
	re.Equal(concurrency, total)
}
