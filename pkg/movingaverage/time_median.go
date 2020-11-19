// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package movingaverage

import "time"

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * reportInterval/2
// and the min filled period is aotSize * reportInterval, which is not related with mfSize
type TimeMedian struct {
	aotInterval time.Duration
	aot         *AvgOverTime
	mf          *MedianFilter
	aotSize     int
	mfSize      int
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize, reportInterval int) *TimeMedian {
	interval := time.Duration(aotSize*reportInterval) * time.Second
	return &TimeMedian{
		aotInterval: interval,
		aot:         NewAvgOverTime(interval),
		mf:          NewMedianFilter(mfSize),
		aotSize:     aotSize,
		mfSize:      mfSize,
	}
}

// Get returns change rate in the median of the several intervals.
func (t *TimeMedian) Get() float64 {
	return t.mf.Get()
}

// Add adds recent change to TimeMedian.
func (t *TimeMedian) Add(delta float64, interval time.Duration) {
	if interval < time.Second {
		return
	}
	t.aot.Add(delta, interval)
	if t.aot.intervalSum >= t.aotInterval {
		t.mf.Add(t.aot.Get())
		t.aot.Clear()
	}
}

// Set sets the given average.
func (t *TimeMedian) Set(avg float64) {
	t.mf.Set(avg)
}

// GetFilledPeriod returns filled period.
func (t *TimeMedian) GetFilledPeriod() int { // it is unrelated with mfSize
	return t.aotSize
}
