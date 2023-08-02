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

import "github.com/elliotchance/pie/v2"

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
type MedianFilter struct {
	// It is not thread safe to read and write records at the same time.
	// If there are concurrent read and write, the read may get an old value.
	// And we should avoid concurrent write.
	records []float64
	size    uint64
	count   uint64
	result  float64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
		result:  0,
	}
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	r.records[r.count%r.size] = n
	r.count++
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	r.result = pie.Median(records)
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	return r.result
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.count = 0
	r.result = 0
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.records[0] = n
	r.count = 1
	r.result = n
}

// GetInstantaneous returns the value just added.
func (r *MedianFilter) GetInstantaneous() float64 {
	return r.records[(r.count-1)%r.size]
}

// Clone returns a copy of MedianFilter
func (r *MedianFilter) Clone() *MedianFilter {
	records := make([]float64, len(r.records))
	copy(records, r.records)
	return &MedianFilter{
		records: records,
		size:    r.size,
		count:   r.count,
		result:  r.result,
	}
}
