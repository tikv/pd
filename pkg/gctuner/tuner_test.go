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
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/memory"
)

var testHeap []byte

func TestTuner(t *testing.T) {
	re := require.New(t)
	EnableGOGCTuner.Store(true)
	memLimit := uint64(1000 * units.MiB) // 1000 MB
	threshold := memLimit / 2
	tn := newTuner(threshold)
	re.Equal(threshold, tn.threshold.Load())
	re.Equal(defaultGCPercent, tn.getGCPercent())

	// no heap
	testHeap = make([]byte, 1)
	runtime.GC()
	runtime.GC()
	for range 100 {
		runtime.GC()
		re.Eventually(func() bool { return maxGCPercent.Load() == tn.getGCPercent() },
			1*time.Second, 50*time.Microsecond)
	}

	// 1/4 threshold
	testHeap = make([]byte, threshold/4)
	for range 100 {
		runtime.GC()
		re.GreaterOrEqual(tn.getGCPercent(), maxGCPercent.Load()/2)
		re.LessOrEqual(tn.getGCPercent(), maxGCPercent.Load())
	}

	// 1/2 threshold
	testHeap = make([]byte, threshold/2)
	runtime.GC()
	for range 100 {
		runtime.GC()
		re.Eventually(func() bool { return tn.getGCPercent() >= minGCPercent.Load() },
			1*time.Second, 50*time.Microsecond)
		re.Eventually(func() bool { return tn.getGCPercent() <= maxGCPercent.Load()/2 },
			1*time.Second, 50*time.Microsecond)
	}

	// 3/4 threshold
	testHeap = make([]byte, threshold/4*3)
	runtime.GC()
	for range 100 {
		runtime.GC()
		re.Eventually(func() bool { return minGCPercent.Load() == tn.getGCPercent() },
			1*time.Second, 50*time.Microsecond)
	}

	// out of threshold
	testHeap = make([]byte, threshold+1024)
	runtime.GC()
	for range 100 {
		runtime.GC()
		re.Eventually(func() bool { return minGCPercent.Load() == tn.getGCPercent() },
			1*time.Second, 50*time.Microsecond)
	}
	testGetGOGC(re)
}

func TestCalcGCPercent(t *testing.T) {
	const gb = units.GiB
	// use default value when invalid params
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 0))
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 1))
	require.Equal(t, defaultGCPercent, calcGCPercent(1, 0))

	require.Equal(t, maxGCPercent.Load(), calcGCPercent(1, 3*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/10, 4*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/2, 4*gb))
	require.Equal(t, uint32(300), calcGCPercent(1*gb, 4*gb))
	require.Equal(t, uint32(166), calcGCPercent(1.5*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(2*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(3*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(4*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(5*gb, 4*gb))
}

func testGetGOGC(re *require.Assertions) {
	defer func() {
		Tuning(0)
		gt := globalTuner.Load()
		re.Nil(gt)
	}()
	re.Equal(defaultGCPercent, GetGOGCPercent())
	// init tuner
	threshold := uint64(units.GiB)
	Tuning(threshold)
	re.Equal(defaultGCPercent, GetGOGCPercent())
	gt := globalTuner.Load()
	re.Equal(threshold, (*gt).getThreshold())

	// update threshold
	threshold = 50
	Tuning(threshold)
	re.Equal(threshold, (*gt).getThreshold())

	// update gc percent
	percent := uint32(30)
	(*gt).setGCPercent(percent)
	re.Equal(percent, GetGOGCPercent())
}

func TestInitGCTuner(t *testing.T) {
	re := require.New(t)
	prevEnable := EnableGOGCTuner.Load()
	prevMemLimit := memory.ServerMemoryLimit.Load()
	t.Cleanup(func() {
		EnableGOGCTuner.Store(prevEnable)
		memory.ServerMemoryLimit.Store(prevMemLimit)
		Tuning(0)
	})
	// Test initialization
	totalMem := uint64(1000000000) // 1GB
	cfg := &Config{
		EnableGOGCTuner:            true,
		GCTunerThreshold:           0.6,
		ServerMemoryLimit:          0.8,
		ServerMemoryLimitGCTrigger: 0.7,
	}
	state := InitGCTuner(totalMem, cfg)
	re.NotNil(state)
	re.True(state.EnableGCTuner)
	re.Equal(uint64(800000000), state.MemoryLimitBytes) // 1GB * 0.8
	re.Equal(uint64(480000000), state.GCThresholdBytes) // 800MB * 0.6
	re.Equal(0.7, state.MemoryLimitGCTriggerRatio)
	re.Equal(uint64(560000000), state.MemoryLimitGCTriggerBytes) // 800MB * 0.7
	re.True(EnableGOGCTuner.Load())
}

func TestInitGCTunerWithZeroMemoryLimit(t *testing.T) {
	re := require.New(t)

	totalMem := uint64(1000000000) // 1GB
	cfg := &Config{
		EnableGOGCTuner:            true,
		GCTunerThreshold:           0.6,
		ServerMemoryLimit:          0, // zero means use total memory
		ServerMemoryLimitGCTrigger: 0.7,
	}

	state := InitGCTuner(totalMem, cfg)

	re.NotNil(state)
	re.Equal(uint64(0), state.MemoryLimitBytes)
	re.Equal(uint64(600000000), state.GCThresholdBytes) // 1GB * 0.6 (uses totalMem)

	// Cleanup
	Tuning(0)
}

func TestUpdateIfNeeded(t *testing.T) {
	re := require.New(t)

	totalMem := uint64(1000000000) // 1GB
	cfg := &Config{
		EnableGOGCTuner:            true,
		GCTunerThreshold:           0.6,
		ServerMemoryLimit:          0.8,
		ServerMemoryLimitGCTrigger: 0.7,
	}

	state := InitGCTuner(totalMem, cfg)
	originalThreshold := state.GCThresholdBytes

	// Test no change
	updated := state.UpdateIfNeeded(cfg)
	re.False(updated)
	re.Equal(originalThreshold, state.GCThresholdBytes)

	// Test GC threshold change
	cfg.GCTunerThreshold = 0.5
	updated = state.UpdateIfNeeded(cfg)
	re.True(updated)
	re.Equal(uint64(400000000), state.GCThresholdBytes) // 800MB * 0.5

	// Test enable toggle
	cfg.EnableGOGCTuner = false
	updated = state.UpdateIfNeeded(cfg)
	re.True(updated)
	re.False(state.EnableGCTuner)
	re.False(EnableGOGCTuner.Load())

	// Test memory limit change
	cfg.ServerMemoryLimit = 0.9
	cfg.GCTunerThreshold = 0.5
	updated = state.UpdateIfNeeded(cfg)
	re.True(updated)
	re.Equal(uint64(900000000), state.MemoryLimitBytes) // 1GB * 0.9

	// Test ratio-only change (when bytes might be the same due to rounding)
	// Set up a scenario where ratio changes but computed bytes are the same
	cfg.ServerMemoryLimitGCTrigger = 0.8
	updated = state.UpdateIfNeeded(cfg)
	re.True(updated)
	re.Equal(0.8, state.MemoryLimitGCTriggerRatio)

	// Test no change when ratio is the same
	updated = state.UpdateIfNeeded(cfg)
	re.False(updated)

	// Cleanup
	Tuning(0)
}
