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
	runtime.GC()
}

func TestMemoryLimitTuner(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Swap(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	oldMemoryLimit := memory.ServerMemoryLimit.Swap(1 << 30) // 1GB
	oldRuntimeLimit := debug.SetMemoryLimit(-1)
	defer func() {
		memory.ServerMemoryLimit.Store(oldMemoryLimit)
		debug.SetMemoryLimit(oldRuntimeLimit)
	}()

	// Own the finalizer so later tests cannot reactivate this test's tuner.
	tuner := &memoryLimitTuner{}
	tuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	tuner.Start()
	defer tuner.Stop()
	tuner.UpdateMemoryLimit()
	require.True(t, tuner.isTuning.Load())

	allocator := &mockAllocator{}
	defer func() {
		allocator.freeAll()
		// Drain the reset while the short test interval is still enabled.
		require.Eventually(t, func() bool {
			return !tuner.waitingReset.Load() && !tuner.nextGCTriggeredByMemoryLimit.Load()
		}, 5*time.Second, 100*time.Millisecond)
	}()
	r := &runtime.MemStats{}
	getNowGCNum := func() uint32 {
		runtime.ReadMemStats(r)
		return r.NumGC
	}
	checkNextGCEqualMemoryLimit := func() {
		runtime.ReadMemStats(r)
		nextGC := r.NextGC
		memoryLimit := tuner.calcMemoryLimit(tuner.GetPercentage())
		// In golang source, nextGC = memoryLimit - three parts memory.
		require.Less(t, nextGC, uint64(memoryLimit))
	}

	memory600mb := allocator.alloc(600 << 20)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 << 20)
	require.Eventually(t, func() bool {
		return tuner.waitingReset.Load() && gcNum < getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond)
	// Test waiting for reset
	require.Eventually(t, func() bool {
		return tuner.calcMemoryLimit(fallbackPercentage) == debug.SetMemoryLimit(-1)
	}, 5*time.Second, 100*time.Millisecond)
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 << 20)
	require.Eventually(t, func() bool {
		return gcNum == getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	runtime.GC()
	// Trigger GC in 80% again
	require.Eventually(t, func() bool {
		return tuner.calcMemoryLimit(tuner.GetPercentage()) == debug.SetMemoryLimit(-1)
	}, 5*time.Second, 100*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	gcNum = getNowGCNum()
	checkNextGCEqualMemoryLimit()
	memory210mb = allocator.alloc(210 << 20)
	require.Eventually(t, func() bool {
		return gcNum < getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond)
	allocator.free(memory210mb)
	allocator.free(memory600mb)
}
