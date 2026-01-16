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
	"github.com/tikv/pd/pkg/utils/testutil"
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

func TestGlobalMemoryTuner(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	re.True(GlobalMemoryLimitTuner.isTuning.Load())
	defer func() {
		// If test.count > 1, wait tuning finished.
		testutil.Eventually(re, GlobalMemoryLimitTuner.isTuning.Load)
		testutil.Eventually(re, func() bool {
			return !GlobalMemoryLimitTuner.waitingReset.Load()
		})
		testutil.Eventually(re, func() bool {
			return !GlobalMemoryLimitTuner.nextGCTriggeredByMemoryLimit.Load()
		})
	}()

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
		re.Less(nextGC, uint64(memoryLimit))
	}

	memory600mb := allocator.alloc(600 << 20)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 << 20)
	time.Sleep(100 * time.Millisecond)
	testutil.Eventually(re, func() bool {
		return GlobalMemoryLimitTuner.waitingReset.Load() && gcNum < getNowGCNum()
	})
	// Test waiting for reset
	testutil.Eventually(re, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(fallbackPercentage) == debug.SetMemoryLimit(-1)
	})
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 << 20)
	testutil.Eventually(re, func() bool {
		return gcNum == getNowGCNum()
	}) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	runtime.GC()
	// Trigger GC in 80% again
	testutil.Eventually(re, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(GlobalMemoryLimitTuner.GetPercentage()) == debug.SetMemoryLimit(-1)
	})
	time.Sleep(100 * time.Millisecond)
	gcNum = getNowGCNum()
	checkNextGCEqualMemoryLimit()
	memory210mb = allocator.alloc(210 << 20)
	testutil.Eventually(re, func() bool {
		return gcNum < getNowGCNum()
	})
	allocator.free(memory210mb)
	allocator.free(memory600mb)
}
