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

const defaultWMASize = 10

// WMA works as a weight with specified window size.
// There are at most `size` data points for calculating.
// References:https://en.wikipedia.org/wiki/Moving_average#Weighted_moving_average
type WMA struct {
	records []float64
	size    uint64
	count   uint64
	score   float64
	sum     float64
}

// NewWMA returns a WMA.
func NewWMA(sizes ...int) *WMA {
	size := defaultWMASize
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
	w.score = w.score - w.sum + n*float64(w.size)
	// to avoid reset
	if w.count < w.size {
		w.sum += n
	} else {
		w.sum = w.sum - w.records[w.count%w.size] + n
	}
	w.records[w.count%w.size] = n
	w.count++
}

// Get returns the weight average of the data set.
func (w *WMA) Get() float64 {
	if w.count == 0 {
		return 0
	}
	if w.count < w.size {
		// the weight = (the first element +the last element)*count/2
		return w.score / float64((w.size+(w.size-w.count+1))*w.count/2.0)
	}
	return w.score / float64((w.size+1)*w.size/2)
}

// Reset cleans the data set.
func (w *WMA) Reset() {
	w.count = 0
	w.score = 0
	w.sum = 0
}

// Set = Reset + Add.
func (w *WMA) Set(n float64) {
	w.Reset()
	w.Add(n)
}

// GetInstantaneous returns the value just added.
func (w *WMA) GetInstantaneous() float64 {
	if w.count == 0 {
		return 0
	}
	return w.records[(w.count-1)%w.size]
}
