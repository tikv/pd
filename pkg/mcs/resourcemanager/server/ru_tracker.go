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
	"math"
	"time"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// minSampledRUPerSec is the minimum RU/s to be sampled by the RU tracker. If it's less than this value,
	// the sampled RU/s will be treated as 0.
	minSampledRUPerSec = 1.0
)

// ruTracker is used to track the RU consumption within a keyspace.
// It uses the algorithm of time-aware exponential moving average (EMA) to
// sample and calculate the real-time RU/s of each resource group. The main
// reason for choosing this EMA algorithm is that conventional EMA algorithms or
// moving average algorithms over a time window cannot handle non-fixed frequency
// data sampling well. Since the reporting interval of RU consumption depends on
// the RU consumption rate of the workload, it is necessary to introduce a time
// dimension to calculate real-time RU/s more accurately.
type ruTracker struct {
	syncutil.RWMutex
	// beta = ln(2) / τ, τ is the time constant which can be thought of as the half-life of the EMA.
	// For example, if τ = 5s, then the decay factor calculated by e^{-β·Δt} will be 0.5 when Δt = 5s,
	// which means the weight of the "old data" is 0.5 when the elapsed time is 5s.
	beta           float64
	lastSampleTime time.Time
	lastEMA        float64
}

func newRUTracker(timeConstant time.Duration) *ruTracker {
	return &ruTracker{
		beta: math.Log(2) / timeConstant.Seconds(),
	}
}

// Sample the RU consumption and calculate the real-time RU/s as `lastEMA`.
// - `now` is the current time point to sample the RU consumption.
// - `totalRU` is the total RU consumption within the `dur`.
// - `dur` is the time cost to run out of the `totalRU`.
func (rt *ruTracker) sample(now time.Time, totalRU float64, dur time.Duration) {
	rt.Lock()
	defer rt.Unlock()
	// If `dur` is not greater than 0, skip this record.
	if dur <= 0 {
		return
	}
	// Calculate the average RU/s within the `dur`.
	ruPerSec := math.Max(0, totalRU) / dur.Seconds()
	// If the last sample time is not set, set the last EMA directly.
	if rt.lastSampleTime.IsZero() {
		rt.lastEMA = ruPerSec
		rt.lastSampleTime = now
		return
	}
	// Calculate the time delta between the last sample time and the current time.
	dt := now.Sub(rt.lastSampleTime).Seconds()
	if dt <= 0 {
		dt = 1e-3 // Avoid division by zero or negative value, use 1 millisecond as the minimum time delta.
	}
	// By using e^{-β·Δt} to calculate the decay factor, we can have the following behavior:
	//   1. The decay factor is always between 0 and 1.
	//   2. The decay factor is time-aware, the larger the time delta, the lower the weight of the "old data".
	decay := math.Exp(-rt.beta * dt)
	rt.lastEMA = decay*rt.lastEMA + (1-decay)*ruPerSec
	// If the `lastEMA` is less than `minSampledRUPerSec`, set it to 0 to avoid converging into a very small value.
	if rt.lastEMA < minSampledRUPerSec {
		rt.lastEMA = 0
	}
	rt.lastSampleTime = now
}

// Get the real-time RU/s calculated by the EMA algorithm.
func (rt *ruTracker) getRUPerSec() float64 {
	rt.RLock()
	defer rt.RUnlock()
	return rt.lastEMA
}
