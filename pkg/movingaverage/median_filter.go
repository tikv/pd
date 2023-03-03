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
<<<<<<< HEAD
	records       []float64
	size          uint64
	count         uint64
	instantaneous float64
=======
	// It is not thread safe to read and write records at the same time.
	// If there are concurrent read and write, the read may get an old value.
	// And we should avoid concurrent write.
	records []float64
	size    uint64
	count   uint64
	result  float64
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
<<<<<<< HEAD
=======
		result:  0,
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
	}
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	r.instantaneous = n
	r.records[r.count%r.size] = n
	r.count++
<<<<<<< HEAD
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	if r.count == 0 {
		return 0
	}
=======
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
<<<<<<< HEAD
	return pie.Median(records)
=======
	r.result = pie.Median(records)
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	return r.result
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.instantaneous = 0
	r.count = 0
<<<<<<< HEAD
=======
	r.result = 0
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.instantaneous = n
	r.records[0] = n
	r.count = 1
<<<<<<< HEAD
=======
	r.result = n
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
}

// GetInstantaneous returns the value just added.
func (r *MedianFilter) GetInstantaneous() float64 {
	return r.instantaneous
}

// Clone returns a copy of MedianFilter
func (r *MedianFilter) Clone() *MedianFilter {
	records := make([]float64, len(r.records))
	copy(records, r.records)
	return &MedianFilter{
<<<<<<< HEAD
		records:       records,
		size:          r.size,
		count:         r.count,
		instantaneous: r.instantaneous,
=======
		records: records,
		size:    r.size,
		count:   r.count,
		result:  r.result,
>>>>>>> d85a0e4e3 (moving_filter: fix data race with cache result (#6080))
	}
}
