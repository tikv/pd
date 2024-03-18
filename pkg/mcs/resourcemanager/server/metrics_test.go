// Copyright 2024 TiKV Project Authors.
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

package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxPerSecCostTracker(t *testing.T) {
	tracker := newMaxPerSecCostTracker("test", defaultCollectIntervalSec)
	re := require.New(t)

	// Define the expected max values for each flushPeriod
	expectedMaxRRU := []float64{19, 39, 59}
	expectedMaxWRU := []float64{19, 39, 59}

	rSum := 0
	wSum := 0
	for i := 0; i < 60; i++ {
		// Record data
		rSum += i
		wSum += i
		tracker.Observe(float64(rSum), float64(wSum))

		// Check the max values at the end of each flushPeriod
		if (i+1)%20 == 0 {
			period := i / 20
			re.Equal(tracker.maxPerSecRRU, expectedMaxRRU[period], fmt.Sprintf("maxPerSecRRU in period %d is incorrect", period+1))
			re.Equal(tracker.maxPerSecWRU, expectedMaxWRU[period], fmt.Sprintf("maxPerSecWRU in period %d is incorrect", period+1))
		}
	}
}
