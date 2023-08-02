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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package movingaverage

import "time"

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * reportInterval/4
// and the min filled period is aotSize * reportInterval, which is not related with mfSize
type TimeMedian struct {
	aot *AvgOverTime
	mf  *MedianFilter
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize int, reportInterval time.Duration) *TimeMedian {
	return &TimeMedian{
		aot: NewAvgOverTime(time.Duration(aotSize) * reportInterval),
		mf:  NewMedianFilter(mfSize),
	}
}

// Get returns change rate in the median of the several intervals.
func (t *TimeMedian) Get() float64 {
	return t.mf.Get()
}

// Add adds recent change to TimeMedian.
func (t *TimeMedian) Add(delta float64, interval time.Duration) {
	t.aot.Add(delta, interval)
	if t.aot.IsFull() {
		t.mf.Add(t.aot.Get())
	}
}

// Set sets the given average.
func (t *TimeMedian) Set(avg float64) {
	t.mf.Set(avg)
}

// GetInstantaneous returns instantaneous speed
func (t *TimeMedian) GetInstantaneous() float64 {
	return t.aot.GetInstantaneous()
}

// Clone returns a copy of TimeMedian
func (t *TimeMedian) Clone() *TimeMedian {
	return &TimeMedian{
		aot: t.aot.Clone(),
		mf:  t.mf.Clone(),
	}
}
