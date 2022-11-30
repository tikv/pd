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

	"github.com/tikv/pd/pkg/syncutil"
)

// AvgOverTime maintains change rate in the last avgInterval.
//
// AvgOverTime takes changes with their own intervals,
// stores recent changes that happened in the last avgInterval,
// then calculates the change rate by (sum of changes) / (sum of intervals).
type AvgOverTime struct {
	syncutil.RWMutex
	records []float64
	size    int
	count   int
	result  float64
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	size := int(interval.Seconds())
	return &AvgOverTime{
		records: make([]float64, size),
		size:    size,
		count:   0,
		result:  0,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	aot.RLock()
	defer aot.RUnlock()
	return aot.result
}

// Clear clears the AvgOverTime.
func (aot *AvgOverTime) Clear() {
	aot.Lock()
	defer aot.Unlock()
	aot.count = 0
	aot.result = 0
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	aot.Lock()
	defer aot.Unlock()
	if interval == 0 {
		return
	}
	for i := 0; i < int(interval.Seconds()); i++ {
		aot.records[aot.count%aot.size] = delta / interval.Seconds()
		aot.count++
	}
	aot.result = 0
	for i := 0; i < aot.size; i++ {
		aot.result += aot.records[i]
	}
	aot.result /= float64(aot.size)
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	aot.Clear()
	for i := 0; i < aot.size; i++ {
		aot.records[i] = avg
	}
}

// IsFull returns whether AvgOverTime is full
func (aot *AvgOverTime) IsFull() bool {
	aot.RLock()
	defer aot.RUnlock()
	return aot.count >= aot.size
}

// Clone returns a copy of AvgOverTime
func (aot *AvgOverTime) Clone() *AvgOverTime {
	aot.RLock()
	defer aot.RUnlock()
	records := make([]float64, aot.size)
	for i := 0; i < aot.size; i++ {
		records[i] = aot.records[i]
	}
	return &AvgOverTime{
		records: records,
		size:    aot.size,
		count:   aot.count,
		result:  aot.result,
	}
}

// // CopyFrom copies the AvgOverTime from another AvgOverTime
// func (aot *AvgOverTime) CopyFrom(other *AvgOverTime) {
// 	aot.que = other.que
// 	aot.margin = deltaWithInterval{
// 		delta:    other.margin.delta,
// 		interval: other.margin.interval,
// 	}
// 	aot.deltaSum = other.deltaSum
// 	aot.intervalSum = other.intervalSum
// 	aot.avgInterval = other.avgInterval
// }

func GCAvgOverTime(*AvgOverTime) {

}
