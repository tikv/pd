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

package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestUseCgroupMemoryHookReplacesPhysicalMemoryCache(t *testing.T) {
	originalMemTotal := MemTotal
	originalMemUsed := MemUsed
	originalTotal, originalTotalTime := memLimit.get()
	originalUsage, originalUsageTime := memUsage.get()
	t.Cleanup(func() {
		MemTotal = originalMemTotal
		MemUsed = originalMemUsed
		memLimit.set(originalTotal, originalTotalTime)
		memUsage.set(originalUsage, originalUsageTime)
	})

	const (
		physicalMemory = 32 << 30
		cgroupMemory   = 2 << 30
	)
	memLimit.set(physicalMemory, time.Now())
	memUsage.set(physicalMemory/2, time.Now())

	useCgroupMemoryHook(cgroupMemory)

	require.Equal(t, uint64(cgroupMemory), GetMemTotalIgnoreErr())
	_, usageTime := memUsage.get()
	require.True(t, usageTime.IsZero())
}
