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
	"math"
)

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
type MedianFilter struct {
	records       []float64
	size          uint64
	count         uint64
	instantaneous float64
	g             uint64
	median        float64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

func (r *MedianFilter) findTwoMinNumber() (first, second float64) {
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
	for i := uint64(0); i < len; i++ {
		if i != pos && r.records[i] > r.median && r.records[i] < second {
			second = r.records[i]
		}
	}
	return
}

func (r *MedianFilter) findTwoMaxNumber() (first, second float64) {
	len := r.size
	if r.count < r.size {
		len = r.count
	}
	first = -math.MaxFloat64
	second = -math.MaxFloat64
	var pos uint64
	for i := uint64(0); i < len; i++ {
		if r.records[i] <= r.median && r.records[i] > first {
			first = r.records[i]
			pos = i
		}
	}
	for i := uint64(0); i < len; i++ {
		if i != pos && r.records[i] <= r.median && r.records[i] > second {
			second = r.records[i]
		}
	}
	return
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	r.instantaneous = n
	if r.count >= r.size {
		pop := r.records[r.count%r.size]
		if pop > r.median {
			r.g--
		}
	}
	r.records[r.count%r.size] = n
	r.count++
	len := r.count
	if r.count > r.size {
		len = r.size
	}
	if n > r.median {
		r.g++
	}
	if len%2 == 0 {
		if r.g > len/2 {
			g1, g2 := r.findTwoMinNumber()
			r.median = (g1 + g2) / 2
			r.g = 0
			for i := uint64(0); i < len; i++ {
				if r.records[i] > r.median {
					r.g++
				}
			}
			return
		} else if r.g == len/2 {
			g1, _ := r.findTwoMinNumber()
			l1, _ := r.findTwoMaxNumber()
			r.median = (l1 + g1) / 2
		} else {
			l1, l2 := r.findTwoMaxNumber()
			r.median = (l1 + l2) / 2
			r.g = 0
			for i := uint64(0); i < len; i++ {
				if r.records[i] > r.median {
					r.g++
				}
			}
			return
		}
	} else {
		if r.g > len/2 {
			g1, _ := r.findTwoMinNumber()
			r.median = g1
			r.g = 0
			for i := uint64(0); i < len; i++ {
				if r.records[i] > r.median {
					r.g++
				}
			}
		} else if r.g < len/2 {
			_, l2 := r.findTwoMaxNumber()
			r.median = l2
			r.g = 0
			for i := uint64(0); i < len; i++ {
				if r.records[i] > r.median {
					r.g++
				}
			}
		} else {
			l1, _ := r.findTwoMaxNumber()
			r.median = l1
			r.g = 0
			for i := uint64(0); i < len; i++ {
				if r.records[i] > r.median {
					r.g++
				}
			}
		}
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
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.instantaneous = n
	r.records[0] = n
	r.count = 1
	r.median = n
	r.g = 0
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
