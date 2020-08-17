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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"time"

	"github.com/phf/go-queue/queue"
)

const (
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10
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
	que         *queue.Queue
	deltaSum    float64
	intervalSum time.Duration
	avgInterval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que:         queue.New(),
		deltaSum:    0,
		intervalSum: 0,
		avgInterval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	return aot.deltaSum / aot.intervalSum.Seconds()
}

// Clear clears the AvgOverTime.
func (aot *AvgOverTime) Clear() {
	aot.que = queue.New()
	aot.intervalSum = 0
	aot.deltaSum = 0
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.intervalSum += interval
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	aot.Clear()
	aot.deltaSum = avg * aot.avgInterval.Seconds()
	aot.intervalSum = aot.avgInterval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.intervalSum})
}

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * StoreHeartBeatReportInterval /4
type TimeMedian struct {
	aotInterval time.Duration
	aot         *AvgOverTime
	mf          *MedianFilter
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize int) *TimeMedian {
	interval := time.Duration(aotSize*StoreHeartBeatReportInterval) * time.Second
	return &TimeMedian{
		aotInterval: interval,
		aot:         NewAvgOverTime(interval),
		mf:          NewMedianFilter(mfSize),
	}
}

// Get returns change rate in the median of the several intervals.
func (t *TimeMedian) Get() float64 {
	return t.mf.Get()
}

// Add adds recent change to TimeMedian.
func (t *TimeMedian) Add(delta float64, interval time.Duration) {
	if interval < 1 {
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
