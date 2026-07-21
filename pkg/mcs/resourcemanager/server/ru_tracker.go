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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	// defaultRUTrackerTimeConstant is the default time-aware EMA time constant for the RU tracker.
	// The following values are reasonable for the RU tracker:
	//   - ~5s, which captures the RU/s spike quickly but may be too sensitive to the short-term fluctuations.
	//   - ~10s, which smooths the RU/s fluctuations but may not track the spike that quickly.
	//   - ~20s, which smooths the RU/s fluctuations but can only be used to observe the long-term trend.
	defaultRUTrackerTimeConstant = 5 * time.Second
	// minSampledRUPerSec is the minimum RU/s to be sampled by the RU tracker. If it's less than this value,
	// the sampled RU/s will be treated as 0.
	minSampledRUPerSec = 1.0
)

// ruTracker is used to track the RU consumption dynamically.
// It uses the algorithm of time-aware exponential moving average (EMA) to sample
// and calculate the real-time RU/s. The main reason for choosing this EMA algorithm
// is that conventional EMA algorithms or moving average algorithms over a time window
// cannot handle non-fixed frequency data sampling well. Since the reporting interval
// of RU consumption depends on the RU consumption rate of the workload, it is necessary
// to introduce a time dimension to calculate real-time RU/s more accurately.
type ruTracker struct {
	syncutil.RWMutex
	initialized bool
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

// sample the RU consumption and calculate the real-time RU/s as `lastEMA`.
// - `clientUniqueID` is the unique ID of the client.
// - `now` is the current time point to sample the RU consumption.
// - `totalRU` is the total RU consumption within the elapsed duration since last sample.
func (rt *ruTracker) sample(clientUniqueID uint64, now time.Time, totalRU float64) {
	rt.Lock()
	defer rt.Unlock()
	// Calculate the elapsed duration since the last sample time.
	prevSampleTime := rt.lastSampleTime
	var dur time.Duration
	if !prevSampleTime.IsZero() {
		dur = now.Sub(prevSampleTime)
	}
	rt.lastSampleTime = now
	// If `dur` is not greater than 0, skip this record.
	if dur <= 0 {
		log.Info("skip ru tracker sample due to non-positive duration",
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Duration("dur", dur),
			zap.Time("prev-sample-time", prevSampleTime),
			zap.Time("now", now),
			zap.Float64("total-ru", totalRU),
		)
		return
	}
	durSeconds := dur.Seconds()
	// Calculate the average RU/s within the `dur`.
	ruPerSec := math.Max(0, totalRU) / durSeconds
	// If the RU tracker is not initialized, set the last EMA directly.
	if !rt.initialized {
		rt.initialized = true
		rt.lastEMA = ruPerSec
		log.Info("init ru tracker ema",
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Float64("ru-per-sec", ruPerSec),
			zap.Float64("total-ru", totalRU),
			zap.Duration("dur", dur),
		)
		return
	}
	// By using e^{-β·Δt} to calculate the decay factor, we can have the following behavior:
	//   1. The decay factor is always between 0 and 1.
	//   2. The decay factor is time-aware, the larger the time delta, the lower the weight of the "old data".
	decay := math.Exp(-rt.beta * durSeconds)
	rt.lastEMA = decay*rt.lastEMA + (1-decay)*ruPerSec
	// If the `lastEMA` is less than `minSampledRUPerSec`, set it to 0 to avoid converging into a very small value.
	if rt.lastEMA < minSampledRUPerSec {
		rt.lastEMA = 0
	}
}

// getRUPerSec returns the real-time RU/s calculated by the EMA algorithm.
func (rt *ruTracker) getRUPerSec() float64 {
	rt.RLock()
	defer rt.RUnlock()
	return rt.lastEMA
}

// isInitialized returns whether the RU tracker has been initialized with at least one valid sample.
func (rt *ruTracker) isInitialized() bool {
	rt.RLock()
	defer rt.RUnlock()
	return rt.initialized
}

// groupRUTracker is used to track the RU consumption of a resource group.
// It maintains a map of RU trackers for each client unique ID.
type groupRUTracker struct {
	syncutil.RWMutex
	ruTrackers map[uint64]*ruTracker
}

func newGroupRUTracker() *groupRUTracker {
	return &groupRUTracker{
		ruTrackers: make(map[uint64]*ruTracker),
	}
}

func (grt *groupRUTracker) getOrCreateRUTracker(clientUniqueID uint64) *ruTracker {
	grt.RLock()
	rt := grt.ruTrackers[clientUniqueID]
	grt.RUnlock()
	if rt == nil {
		grt.Lock()
		// Double check the RU tracker is not created by other goroutine.
		rt = grt.ruTrackers[clientUniqueID]
		if rt == nil {
			rt = newRUTracker(defaultRUTrackerTimeConstant)
			grt.ruTrackers[clientUniqueID] = rt
			log.Info("create ru tracker",
				zap.Uint64("client-unique-id", clientUniqueID),
			)
		}
		grt.Unlock()
	}
	return rt
}

func (grt *groupRUTracker) sample(clientUniqueID uint64, now time.Time, totalRU float64) {
	grt.getOrCreateRUTracker(clientUniqueID).sample(clientUniqueID, now, totalRU)
}
