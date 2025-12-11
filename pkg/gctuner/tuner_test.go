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
