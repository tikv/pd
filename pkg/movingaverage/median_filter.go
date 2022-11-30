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

import (
	"errors"
	"math"
)

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
// Note: MedianFilter is not Thread-Safety.
type MedianFilter struct {
	records       []float64
	size          uint64
	count         uint64
	instantaneous float64
	// g is used to count the number of values which are greater than median
	g uint64
	// l is used to count the number of values which are less than median
	l      uint64
	median float64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

// err is used to avoid returning MaxFloat64 for concurrency
func (r *MedianFilter) findTwoMinNumber(needSecond bool) (first, second float64, err error) {
	len := r.size
	if r.count < r.size {
		len = r.count
	}
	first = math.MaxFloat64
	second = math.MaxFloat64
	var pos uint64
	for i := uint64(0); i < len; i++ {
		if r.records[i] > r.median && r.records[i] < first {
			first = r.records[i]
			pos = i
		}
	}
	if !(first < math.MaxFloat64) {
		return 0, 0, errors.New("invalid values")
	}
	if needSecond {
		for i := uint64(0); i < len; i++ {
			if i != pos && r.records[i] > r.median && r.records[i] < second {
				second = r.records[i]
			}
		}
		if !(second < math.MaxFloat64) {
			return 0, 0, errors.New("invalid values")
		}
	}
	return
}

func (r *MedianFilter) findTwoMaxNumber(needSecond bool) (first, second float64, err error) {
	len := r.size
	if r.count < r.size {
		len = r.count
	}
	first = -math.MaxFloat64
	second = -math.MaxFloat64
	var pos uint64
	for i := uint64(0); i < len; i++ {
		if r.records[i] < r.median && r.records[i] > first {
			first = r.records[i]
			pos = i
		}
	}
	if !(first > -math.MaxFloat64) {
		return 0, 0, errors.New("invalid values")
	}
	if needSecond {
		for i := uint64(0); i < len; i++ {
			if i != pos && r.records[i] < r.median && r.records[i] > second {
				second = r.records[i]
			}
		}
		if !(second > -math.MaxFloat64) {
			return 0, 0, errors.New("invalid values")
		}
	}
	return
}

func (r *MedianFilter) add(n float64) {
	r.instantaneous = n
	r.records[r.count%r.size] = n
	r.count++
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	len := r.count + 1
	if r.count >= r.size {
		len = r.size
		pos := r.records[r.count%r.size]
		if pos > r.median {
			r.g--
		} else if pos < r.median {
			r.l--
		}
	}
	if n > r.median {
		r.g++
	} else if n < r.median {
		r.l++
	}
	r.add(n)

	updateStatus := func() {
		r.g = 0
		r.l = 0
		for i := uint64(0); i < len; i++ {
			if r.records[i] > r.median {
				r.g++
			} else if r.records[i] < r.median {
				r.l++
			}
		}
	}
	// When the length is even
	if len%2 == 0 {
		if r.g > len/2 { // the example for this case is [1 3 5 6] -> [1 5 6 7]
			g1, g2, err := r.findTwoMinNumber(true)
			if err != nil {
				return
			}
			r.median = (g1 + g2) / 2
			updateStatus()
		} else if r.g == len/2 {
			g1, _, err := r.findTwoMinNumber(false)
			if err != nil {
				return
			}
			if r.l < len/2 { // the example for this case is [1 3 5] -> [1 3 5 6]
				r.median = (r.median + g1) / 2
			} else { // the example for this case is [1 3 5 6] -> [1 3 6 6]
				l1, _, err := r.findTwoMaxNumber(false)
				if err != nil {
					return
				}
				r.median = (l1 + g1) / 2
			}
			updateStatus()
		} else if r.l == len/2 { // the example for this case is [1 3 5 6] -> [1 1 3 5]
			l1, _, err := r.findTwoMaxNumber(false)
			if err != nil {
				return
			}
			if r.g < len/2 { // the example for this case is [1 3 5] -> [1 2 3 5]
				r.median = (r.median + l1) / 2
			} else { // the example for this case is [1 3 5 6] -> [1 2 5 6]
				g1, _, err := r.findTwoMinNumber(false)
				if err != nil {
					return
				}
				r.median = (l1 + g1) / 2
			}
			updateStatus()
		} else if r.l == len/2+1 { // the example for this case is [1 3 5 6] -> [1 2 3 5]
			l1, l2, err := r.findTwoMaxNumber(true)
			if err != nil {
				return
			}
			r.median = (l1 + l2) / 2
			updateStatus()
		} // In the other case, the median didn't change
	} else {
		if r.l == len/2+1 {
			l1, _, err := r.findTwoMaxNumber(false)
			if err != nil {
				return
			}
			r.median = l1
			updateStatus()
		} else if r.g == len/2+1 {
			g1, _, err := r.findTwoMinNumber(false)
			if err != nil {
				return
			}
			r.median = g1
			updateStatus()
		} // In the other case, the median didn't change
	}
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	return r.median
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.instantaneous = 0
	r.count = 0
	r.median = 0
	r.g = 0
	r.l = 0
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.instantaneous = n
	r.records[0] = n
	r.count = 1
	r.median = n
	r.g = 0
	r.l = 0
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
		records:       records,
		size:          r.size,
		count:         r.count,
		instantaneous: r.instantaneous,
	}
}

// GetAll only used in test
func (r *MedianFilter) GetAll() []float64 {
	if r.count == 0 {
		return nil
	}
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	return records
}
