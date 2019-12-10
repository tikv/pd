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
	"math"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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

const defaultDecay = 0.075

// EMA works as an exponential moving average filter.
// References: https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average.
type EMA struct {
	// EMA will learn from (2-decay)/decay records. It must be less than 1 and greater than 0.
	decay float64
	// Exponential smoothing puts substantial weight on past observations, so the initial value of demand will have an
	// unreasonably large effect on early forecasts. This problem can be overcome by allowing the process to evolve for
	// a reasonable number of periods (say 10 or more) and using the arithmetic average of the demand during
	// those periods as the initial forecast.
	wakeNum uint64
	count   uint64
	value   float64
}

// NewEMA returns an EMA.
func NewEMA(decays ...float64) *EMA {
	decay := defaultDecay
	if len(decays) != 0 && decays[0] < 1 && decays[0] > 0 {
		decay = decays[0]
	}
	wakeNum := uint64((2 - decay) / decay)
	return &EMA{
		decay:   decay,
		wakeNum: wakeNum,
	}
}

// Add adds a data point.
func (e *EMA) Add(num float64) {
	if e.count < e.wakeNum {
		e.count++
		e.value += num
	} else if e.count == e.wakeNum {
		e.count++
		e.value /= float64(e.wakeNum)
		e.value = (num * e.decay) + (e.value * (1 - e.decay))
	} else {
		e.value = (num * e.decay) + (e.value * (1 - e.decay))
	}
}

// Get returns the result of the data set.If return 0.0, it means the filter can not be wake up.
func (e *EMA) Get() float64 {
	if e.count == 0 {
		return 0
	}
	if e.count <= e.wakeNum {
		return e.value / float64(e.count)
	}
	return e.value
}

// Reset cleans the data set.
func (e *EMA) Reset() {
	e.count = 0
	e.value = 0
}

// Set = Reset + Add.
func (e *EMA) Set(n float64) {
	e.value = n
	e.count = 1
}

const defaultSize = 10
const defaultInterval = time.Minute

// HMA works as hull moving average
// There are at most `size` data points for calculating.
// References: https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/hull-moving-average
type HMA struct {
	size uint64
	wma  []*WMA
}

// NewHMA returns a WMA.
func NewHMA(sizes ...float64) *HMA {
	size := defaultSize
	if len(sizes) != 0 && sizes[0] > 1 {
		size = int(sizes[0])
	}
	wma := make([]*WMA, 3)
	wma[0] = NewWMA(size / 2)
	wma[1] = NewWMA(size)
	wma[2] = NewWMA(int(math.Sqrt(float64(size))))
	return &HMA{
		wma:  wma,
		size: uint64(size),
	}
}

// Add adds a data point.
func (h *HMA) Add(n float64) {
	h.wma[0].Add(n)
	h.wma[1].Add(n)
	h.wma[2].Add(2*h.wma[0].Get() - h.wma[1].Get())
}

// Get returns the weight average of the data set.
func (h *HMA) Get() float64 {
	return h.wma[2].Get()
}

// Reset cleans the data set.
func (h *HMA) Reset() {
	h.wma[0] = NewWMA(int(h.size / 2))
	h.wma[1] = NewWMA(int(h.size))
	h.wma[2] = NewWMA(int(math.Sqrt(float64(h.size))))
}

// Set = Reset + Add.
func (h *HMA) Set(n float64) {
	h.Reset()
	h.Add(n)
}

// WMA works as a weight with specified window size.
// There are at most `size` data points for calculating.
// References:https://en.wikipedia.org/wiki/Moving_average#Weighted_moving_average
type WMA struct {
	records []float64
	size    uint64
	count   uint64
}

// NewWMA returns a WMA.
func NewWMA(sizes ...int) *WMA {
	size := defaultSize
	if len(sizes) != 0 && sizes[0] > 1 {
		size = sizes[0]
	}
	return &WMA{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

// Add adds a data point.
func (w *WMA) Add(n float64) {
	w.records[w.count%w.size] = n
	w.count++
}

// Get returns the weight average of the data set.
func (w *WMA) Get() float64 {
	if w.count == 0 {
		return 0
	}

	sum := 0.0

	if w.count < w.size {
		for i := 0; i < int(w.count); i++ {
			sum += w.records[i]
		}
		return sum / float64(w.count)
	}

	for i := uint64(0); i < w.size; i++ {
		sum += w.records[(w.count-i)%w.size] * float64(w.size-i)
	}

	return sum / float64((w.size+1)*w.size/2)
}

// Reset cleans the data set.
func (w *WMA) Reset() {
	w.count = 0
}

// Set = Reset + Add.
func (w *WMA) Set(n float64) {
	w.records[0] = n
	w.count = 1
}

// TickMovingAverages is used to count with fixed interval with hma.
type TickMovingAverages struct {
	size      uint64
	interval  time.Duration
	counters  map[uint64]MovingAvg
	lastTimes map[uint64]time.Time
}

// NewTickMovingAverages returns a TickMovingAverages
func NewTickMovingAverages() *TickMovingAverages {
	return &TickMovingAverages{
		size:      defaultSize,
		interval:  defaultInterval,
		counters:  make(map[uint64]MovingAvg),
		lastTimes: make(map[uint64]time.Time),
	}
}

// Add adds a data point, if interval allows.
func (c *TickMovingAverages) Add(id uint64, num float64) {
	if _, ok := c.counters[id]; !ok {
		c.counters[id] = NewHMA(defaultSize)
		c.lastTimes[id] = time.Time{}
	}

	if lastTime, ok := c.lastTimes[id]; ok {
		if time.Now().After(lastTime.Add(c.interval)) {
			log.Info("updateStoreStatusLocked", zap.Float64("regionSize", num), zap.Uint64("storeID", id))
			c.counters[id].Add(num)
			c.lastTimes[id] = time.Now()
		}
	}
}

// Get returns the moving average of the data set.
func (c *TickMovingAverages) Get(id uint64) float64 {
	if counter, ok := c.counters[id]; ok {
		return counter.Get()
	}
	return 0
}

// SetInterval to set interval.
func (c *TickMovingAverages) SetInterval(interval time.Duration) {
	c.interval = interval
}

// SetSize to set size.
func (c *TickMovingAverages) SetSize(size uint64) {
	c.size = size
}
