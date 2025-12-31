// Copyright 2019 TiKV Project Authors.
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

import (
	"time"

	"github.com/phf/go-queue/queue"
)

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

// AvgOverTime maintains change rate in the last avgInterval.
//
// AvgOverTime takes changes with their own intervals,
// stores recent changes that happened in the last avgInterval,
// then calculates the change rate by (sum of changes) / (sum of intervals).
type AvgOverTime struct {
	que         *queue.Queue      // The element is `deltaWithInterval`, sum of all elements' interval is less than `avgInterval`
	margin      deltaWithInterval // The last element from `PopFront` in `que`, it will be changed if the sum exclude it is greater than `avgInterval`
	deltaSum    float64           // Including `margin` and all elements in `que`
	intervalSum time.Duration     // Including `margin` and all elements in `que`
	avgInterval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que: queue.New(),
		margin: deltaWithInterval{
			delta:    0,
			interval: 0,
		},
		deltaSum:    0,
		intervalSum: 0,
		avgInterval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	if aot.intervalSum < aot.avgInterval {
		return 0
	}

	marginDelta := aot.margin.delta * (aot.intervalSum.Seconds() - aot.avgInterval.Seconds()) / aot.margin.interval.Seconds()
	return (aot.deltaSum - marginDelta) / aot.avgInterval.Seconds()
}

// Clear clears the AvgOverTime.
func (aot *AvgOverTime) Clear() {
	for aot.que.Len() > 0 {
		aot.que.PopFront()
	}
	aot.margin = deltaWithInterval{
		delta:    0,
		interval: 0,
	}
	aot.intervalSum = 0
	aot.deltaSum = 0
}

// Add adds recent change to AvgOverTime.
// It will pop item until the retain item's sum is greater than avgInterval.
// such as:
// que [1,1,1,6], avgInterval is 5.
// It will pop 6 if adding 2, the retaining item's sum is 5(2,1,1,1) >= avgInterval.
// It can't pop 6 if adding 1, the retaining item's sum is 4(1,1,1,1) < avgInterval.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	if interval == 0 {
		return
	}

	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.intervalSum += interval

	for aot.intervalSum-aot.margin.interval >= aot.avgInterval {
		aot.deltaSum -= aot.margin.delta
		aot.intervalSum -= aot.margin.interval
		aot.margin = aot.que.PopFront().(deltaWithInterval)
	}
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	aot.Clear()
	aot.margin.delta = avg * aot.avgInterval.Seconds()
	aot.margin.interval = aot.avgInterval
	aot.deltaSum = aot.margin.delta
	aot.intervalSum = aot.avgInterval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.intervalSum})
}

// IsFull returns whether AvgOverTime is full
func (aot *AvgOverTime) IsFull() bool {
	return aot.intervalSum >= aot.avgInterval
}

// Clone returns a copy of AvgOverTime
func (aot *AvgOverTime) Clone() *AvgOverTime {
	q := queue.New()
	for range aot.que.Len() {
		v := aot.que.PopFront()
		aot.que.PushBack(v)
		q.PushBack(v)
	}
	margin := deltaWithInterval{
		delta:    aot.margin.delta,
		interval: aot.margin.interval,
	}
	return &AvgOverTime{
		que:         q,
		margin:      margin,
		deltaSum:    aot.deltaSum,
		intervalSum: aot.intervalSum,
		avgInterval: aot.avgInterval,
	}
}

// GetIntervalSum returns the sum of interval
func (aot *AvgOverTime) GetIntervalSum() time.Duration {
	return aot.intervalSum
}

// GetInstantaneous returns the value just added.
func (aot *AvgOverTime) GetInstantaneous() float64 {
	if aot.que.Len() == 0 || aot.que.Back() == nil {
		if aot.margin.interval != 0 {
			return aot.margin.delta / aot.margin.interval.Seconds()
		}
		return aot.margin.delta
	}
	data := aot.que.Back().(deltaWithInterval)
	return data.delta / data.interval.Seconds()
}
