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

import "github.com/montanaflynn/stats"

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
type MedianFilter struct {
	records       []float64
	size          uint64
	count         uint64
	instantaneous float64
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
	r.instantaneous = n
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
	r.instantaneous = 0
	r.count = 0
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.instantaneous = n
	r.records[0] = n
	r.count = 1
}

// GetInstantaneous returns the value just added.
func (r *MedianFilter) GetInstantaneous() float64 {
	return r.instantaneous
}
