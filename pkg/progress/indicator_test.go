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
	re.Zero(pi.Progress.ProgressPercent)
	re.Zero(pi.Progress.CurrentSpeed)
	re.Equal(math.MaxFloat64, pi.Progress.LeftSecond)

	// Push data and test progress update
	pi.push(10)
	re.Equal(0.1, pi.Progress.ProgressPercent)
	re.Equal(10.0, pi.Progress.CurrentSpeed)
	re.Equal(9.0, pi.Progress.LeftSecond)

	// Push more data and test progress update
	pi.push(50)
	re.Equal(0.5, pi.Progress.ProgressPercent)
	re.Equal(25.0, pi.Progress.CurrentSpeed)
	re.Equal(2.0, pi.Progress.LeftSecond)

	// Push data exceeding targetRegionSize
	pi.push(120)
	re.Equal(1.0, pi.Progress.ProgressPercent)
	re.Equal(0.0, pi.Progress.LeftSecond)
}

func TestProgressIndicator_WindowManagement(t *testing.T) {
	re := require.New(t)
	updateInterval := minSpeedCalculationWindow / 10
	pi := newProgressIndicator(
		Action("test"),
		0,
		100,
		updateInterval,
	)

	// Push data to fill the window
	for i := range 9 {
		pi.push(float64(i + 1))
		re.Equal(i+2, pi.history.Len())
		re.Equal(0.0, pi.front.Value.(float64))
	}

	// Ensure the window length is maintained
	re.Equal(pi.windowLength, pi.history.Len())

	// Push more data to exceed the window capacity
	pi.push(20)
	re.Equal(pi.windowLength, pi.history.Len())

	// Ensure the front element is updated correctly
	re.Equal(1.0, pi.front.Value.(float64))
}
