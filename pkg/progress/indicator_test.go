// Copyright 2025 TiKV Project Authors.
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

package progress

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProgressIndicator_UpdateProgress(t *testing.T) {
	re := require.New(t)
	updateInterval := time.Second
	pi := newProgressIndicator(
		Action("test"),
		0,
		100,
		updateInterval,
	)

	// Test initial state
	re.Zero(pi.ProgressPercent)
	re.Zero(pi.CurrentSpeed)
	re.Equal(math.MaxFloat64, pi.LeftSecond)

	// Push data and test progress update
	pi.push(10)
	re.Equal(0.1, pi.ProgressPercent)
	re.Equal(10.0, pi.CurrentSpeed)
	re.Equal(9.0, pi.LeftSecond)

	// Push more data and test progress update
	pi.push(50)
	re.Equal(0.5, pi.ProgressPercent)
	re.Equal(25.0, pi.CurrentSpeed)
	re.Equal(2.0, pi.LeftSecond)

	// Push data exceeding targetRegionSize
	pi.push(120)
	re.Equal(1.0, pi.ProgressPercent)
	re.Equal(0.0, pi.LeftSecond)
}

func TestWindowCapacity(t *testing.T) {
	re := require.New(t)
	updateInterval := time.Second
	pi := newProgressIndicator(
		Action("test"),
		0,
		1000,
		updateInterval,
	)
	pi.windowCapacity = 100
	pi.windowLength = 10

	// Push data to fill the window
	for i := range 9 {
		pi.push(float64(i + 1))
		re.Equal(i+2, pi.history.Len())
		re.Equal(0.0, pi.front.Value.(float64))
	}

	// Ensure the window length is maintained
	re.Equal(pi.windowLength, pi.history.Len())
	re.Equal(0.0, pi.front.Value.(float64))

	// Push more data to exceed the window length
	pi.push(20)
	re.Equal(pi.windowLength+1, pi.history.Len())
	// Ensure the front element is updated correctly
	re.Equal(1.0, pi.front.Value.(float64))

	for i := 20; i < 200; i++ {
		pi.push(float64(i))
	}
	re.Equal(pi.windowCapacity, pi.history.Len())
}

func TestMoveWindow(t *testing.T) {
	updateInterval := time.Second
	pi := newProgressIndicator(
		Action("test"),
		0,
		100,
		updateInterval,
	)

	// Fill history with known values
	for i := 1; i <= 5; i++ {
		pi.push(float64(i * 10))
	}
	checkProgressWindow(t, pi, 0.0, 600, 6)

	// Decrease the window
	pi.windowLength = 2
	pi.moveWindow()
	checkProgressWindow(t, pi, 40.0, 2, 2)

	// Enlarge the window
	pi.windowLength = 4
	pi.moveWindow()
	checkProgressWindow(t, pi, 20.0, 4, 4)
	// Enlarge the window again, it should not exceed the history length
	pi.windowLength = 10
	pi.moveWindow()
	checkProgressWindow(t, pi, 0.0, 10, 6)
}

func TestAdjustWindowLength(t *testing.T) {
	re := require.New(t)
	updateInterval := time.Second
	pi := newProgressIndicator(
		Action("test"),
		0,
		100,
		updateInterval,
	)

	testCases := []struct {
		duration time.Duration
		expected int
	}{
		{
			duration: 1 * time.Minute,
			expected: int(minSpeedCalculationWindow/updateInterval) + 1,
		},
		{
			duration: 3 * time.Hour,
			expected: int(maxSpeedCalculationWindow/updateInterval) + 1,
		},
		{
			duration: 30 * time.Minute,
			expected: int((30*time.Minute)/updateInterval) + 1,
		},
		{
			duration: minSpeedCalculationWindow,
			expected: int(minSpeedCalculationWindow/updateInterval) + 1,
		},
		{
			duration: maxSpeedCalculationWindow,
			expected: int(maxSpeedCalculationWindow/updateInterval) + 1,
		},
	}

	for _, tc := range testCases {
		pi.adjustWindowLength(tc.duration)
		re.Equal(tc.expected, pi.windowLength)
	}
}

func checkProgressWindow(t *testing.T, pi *progressIndicator,
	expectedFront float64, expectedWindowLength, expectedCurrentWindowLength int) {
	re := require.New(t)
	re.Equal(expectedFront, pi.front.Value.(float64))
	re.Equal(expectedWindowLength, pi.windowLength)
	re.Equal(expectedCurrentWindowLength, pi.currentWindowLength)
}
