// Copyright 2018 PingCAP, Inc.
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

package statistics

import (
	"fmt"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/phf/go-queue/queue"
)

func storeTag(id uint64) string {
	return fmt.Sprintf("store-%d", id)
}

// MovingAvg provides moving average.
// Ref: https://en.wikipedia.org/wiki/Moving_average
type MovingAvg interface {
	// Add adds a data point to the data set.
	Add(data float64)
	// Get returns the moving average.
	Get() float64
	// Reset cleans the data set.
	Reset()
	// Set = Reset + Add
	Set(data float64)
}

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
type MedianFilter struct {
	records []float64
	size    uint64
	count   uint64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	r.records[r.count%r.size] = n
	r.count++
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	if r.count == 0 {
		return 0
	}
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	median, _ := stats.Median(records)
	return median
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.count = 0
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.records[0] = n
	r.count = 1
}

func maxInt(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

// AvgOverTime maintains change rate in the last interval.
type AvgOverTime struct {
	que      *queue.Queue
	deltaSum float64
	durSum   time.Duration
	interval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que:      queue.New(),
		deltaSum: 0,
		durSum:   0,
		interval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	if aot.durSum.Seconds() < 1 {
		return 0
	}
	return aot.deltaSum / aot.durSum.Seconds()
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.durSum += interval
	if aot.durSum <= aot.interval {
		return
	}
	for aot.que.Len() > 0 {
		front := aot.que.Front().(deltaWithInterval)
		if aot.durSum-front.interval >= aot.interval {
			aot.que.PopFront()
			aot.deltaSum -= front.delta
			aot.durSum -= front.interval
		} else {
			break
		}
	}
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	for aot.que.Len() > 0 {
		aot.que.PopFront()
	}
	aot.deltaSum = avg * aot.interval.Seconds()
	aot.durSum = aot.interval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.durSum})
}
