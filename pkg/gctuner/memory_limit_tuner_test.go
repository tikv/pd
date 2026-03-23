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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/memory"
)

const memoryLimitEventuallyWait = 15 * time.Second

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
	if version := runtime.Version(); strings.HasPrefix(version, "go1.24") || strings.HasPrefix(version, "go1.25") {
		t.Skipf("skip unstable memory tuner test on %s", version)
	}
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.True(t, GlobalMemoryLimitTuner.isTuning.Load())
	defer func() {
		GlobalMemoryLimitTuner.waitingReset.Store(false)
		GlobalMemoryLimitTuner.nextGCTriggeredByMemoryLimit.Store(false)
	}()

	allocator := &mockAllocator{}
	defer allocator.freeAll()
	r := &runtime.MemStats{}
	getNowGCNum := func() uint32 {
		runtime.ReadMemStats(r)
		return r.NumGC
	}

	memory600mb := allocator.alloc(600 << 20)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 << 20)
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.waitingReset.Load() && gcNum < getNowGCNum()
	}, memoryLimitEventuallyWait, 100*time.Millisecond)
	// Test waiting for reset
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(fallbackPercentage) == debug.SetMemoryLimit(-1)
	}, memoryLimitEventuallyWait, 100*time.Millisecond)
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 << 20)
	require.Eventually(t, func() bool {
		return gcNum == getNowGCNum()
	}, memoryLimitEventuallyWait, 100*time.Millisecond) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	runtime.GC()
	allocator.free(memory210mb)
	allocator.free(memory600mb)
}
