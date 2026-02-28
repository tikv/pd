// Copyright 2023 TiKV Project Authors.
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

package gctuner

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/memory"
)

type mockAllocator struct {
	m [][]byte
}

func (a *mockAllocator) alloc(bytes int) (handle int) {
	sli := make([]byte, bytes)
	a.m = append(a.m, sli)
	return len(a.m) - 1
}

func (a *mockAllocator) free(handle int) {
	a.m[handle] = nil
}

func (a *mockAllocator) freeAll() {
	a.m = nil
}

func TestGlobalMemoryTuner(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	const mb = 1 << 20

	// Restore global state after the test. This test intentionally relies on GC behavior,
	// so we should not manually call runtime.GC here.
	oldServerMemoryLimit := memory.ServerMemoryLimit.Load()
	oldGoMemoryLimit := debug.SetMemoryLimit(-1)
	defer func() {
		// The reset goroutine may still be running. Wait it to finish to avoid racing
		// on the global memory limit.
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return !GlobalMemoryLimitTuner.waitingReset.Load()
		}, 10*time.Second, 100*time.Millisecond)

		memory.ServerMemoryLimit.Store(oldServerMemoryLimit)
		GlobalMemoryLimitTuner.UpdateMemoryLimit()
		GlobalMemoryLimitTuner.nextGCTriggeredByMemoryLimit.Store(false)
		debug.SetMemoryLimit(oldGoMemoryLimit)
	}()

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.True(t, GlobalMemoryLimitTuner.isTuning.Load())

	allocator := &mockAllocator{}
	defer allocator.freeAll()
	r := &runtime.MemStats{}
	getNowGCNum := func() uint32 {
		runtime.ReadMemStats(r)
		return r.NumGC
	}
	checkNextGCEqualMemoryLimit := func() {
		runtime.ReadMemStats(r)
		nextGC := r.NextGC
		memoryLimit := GlobalMemoryLimitTuner.calcMemoryLimit(GlobalMemoryLimitTuner.GetPercentage())
		// In golang source, nextGC = memoryLimit - three parts memory.
		require.Less(t, nextGC, uint64(memoryLimit))
	}

	var sink any
	allocGarbage := func(bytes int) {
		garbage := make([]byte, bytes)
		// Ensure the allocation is not optimized away and escapes to heap.
		garbage[0] = 1
		garbage[len(garbage)-1] = 1
		sink = garbage
		sink = nil
	}

	memory600mb := allocator.alloc(600 * mb)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 * mb)
	require.Eventually(t, func() bool {
		allocGarbage(1 * mb)
		return GlobalMemoryLimitTuner.waitingReset.Load() && gcNum < getNowGCNum()
	}, 10*time.Second, 100*time.Millisecond)
	// Test waiting for reset
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(fallbackPercentage) == debug.SetMemoryLimit(-1)
	}, 10*time.Second, 100*time.Millisecond)
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 * mb)
	require.Eventually(t, func() bool {
		return gcNum == getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	// Trigger GC in 80% again
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(GlobalMemoryLimitTuner.GetPercentage()) == debug.SetMemoryLimit(-1)
	}, 10*time.Second, 100*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	gcNum = getNowGCNum()
	checkNextGCEqualMemoryLimit()
	memory210mb = allocator.alloc(210 * mb)
	require.Eventually(t, func() bool {
		allocGarbage(1 * mb)
		return gcNum < getNowGCNum()
	}, 10*time.Second, 100*time.Millisecond)
	allocator.free(memory210mb)
	allocator.free(memory600mb)
	require.Nil(t, sink)
}
