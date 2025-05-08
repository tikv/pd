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
// Note: MedianFilter is not Thread-Safety.
// If the some items are `math.MaxFloat64`, no guarantee that the results are correct.
type MedianFilter struct {
	records []float64
	size    uint64
	count   uint64
	// countGreaterThanMedian is used to count the number of values which are greater than median
	countGreaterThanMedian uint64
	// countLessThanMedian is used to count the number of values which are less than median
	countLessThanMedian uint64
	result              float64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

func (r *MedianFilter) getLength() uint64 {
	len := r.size
	if r.count < r.size {
		len = r.count
	}
	return len
}

// findMinValueGreaterThanMedain returns the minimum item which is larger number than the median, and its index.
func (r *MedianFilter) findMinValueGreaterThanMedain(len uint64) (min float64, pos uint64) {
	min = math.MaxFloat64
	for i := uint64(0); i < len; i++ {
		if r.records[i] > r.result && r.records[i] < min {
			min = r.records[i]
			pos = i
		}
	}
	return
}

// findMinGreaterThanMedian returns the minimum item which is larger number than the median.
func (r *MedianFilter) findMinGreaterThanMedian() float64 {
	len := r.getLength()
	min, _ := r.findMinValueGreaterThanMedain(len)
	return min
}

// findTwoMinValuesGreaterThanMedian returns the two minimum items which is larger than the median.
func (r *MedianFilter) findTwoMinValuesGreaterThanMedian() (first, second float64) {
	len := r.getLength()
	second = math.MaxFloat64
	first, pos := r.findMinValueGreaterThanMedain(len)
	for i := uint64(0); i < len; i++ {
		if i != pos && r.records[i] > r.result && r.records[i] < second {
			second = r.records[i]
		}
	}
	return
}

// findMaxLessThanMedian returns the maximal item which is less than the median, and its index.
func (r *MedianFilter) findMaxLessThanMedian(len uint64) (max float64, pos uint64) {
	max = -math.MaxFloat64
	for i := uint64(0); i < len; i++ {
		if r.records[i] < r.result && r.records[i] > max {
			max = r.records[i]
			pos = i
		}
	}
	return
}

// findMaxValueLessThanMedian returns the maximal item which is less than the median.
func (r *MedianFilter) findMaxValueLessThanMedian() float64 {
	len := r.getLength()
	max, _ := r.findMaxLessThanMedian(len)
	return max
}

// findTwoMaxLessThanMedian returns the two maximal items which is less than the median.
func (r *MedianFilter) findTwoMaxLessThanMedian() (first, second float64) {
	len := r.getLength()
	second = -math.MaxFloat64
	first, pos := r.findMaxLessThanMedian(len)
	for i := uint64(0); i < len; i++ {
		if i != pos && r.records[i] < r.result && r.records[i] > second {
			second = r.records[i]
		}
	}
	return
}

func (r *MedianFilter) updateStatus(len uint64) {
	r.countGreaterThanMedian = 0
	r.countLessThanMedian = 0
	for i := uint64(0); i < len; i++ {
		if r.records[i] > r.result {
			r.countGreaterThanMedian++
		} else if r.records[i] < r.result {
			r.countLessThanMedian++
		}
	}
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	len := r.count + 1
	if r.count >= r.size {
		len = r.size
		posValue := r.records[r.count%r.size]
		if posValue > r.result {
			r.countGreaterThanMedian--
		} else if posValue < r.result {
			r.countLessThanMedian--
		}
	}
	if n > r.result {
		r.countGreaterThanMedian++
	} else if n < r.result {
		r.countLessThanMedian++
	}
	r.records[r.count%r.size] = n
	r.count++

	// When the length is even
	if len%2 == 0 {
		if r.countGreaterThanMedian > len/2 { // the example for this case is [1 3 5 6] -> [1 5 6 7]
			g1, g2 := r.findTwoMinValuesGreaterThanMedian()
			r.result = (g1 + g2) / 2
			r.updateStatus(len)
		} else if r.countGreaterThanMedian == len/2 {
			g1 := r.findMinGreaterThanMedian()
			if r.countLessThanMedian < len/2 { // the example for this case is [1 3 5] -> [1 3 5 6]
				r.result = (r.result + g1) / 2
			} else { // the example for this case is [1 3 5 6] -> [1 3 6 6]
				l1 := r.findMaxValueLessThanMedian()
				r.result = (l1 + g1) / 2
			}
			r.updateStatus(len)
		} else if r.countLessThanMedian == len/2 { // the example for this case is [1 3 5 6] -> [1 1 3 5]
			l1 := r.findMaxValueLessThanMedian()
			if r.countGreaterThanMedian < len/2 { // the example for this case is [1 3 5] -> [1 2 3 5]
				r.result = (r.result + l1) / 2
			} else { // the example for this case is [1 3 5 6] -> [1 2 5 6]
				g1 := r.findMinGreaterThanMedian()
				r.result = (l1 + g1) / 2
			}
			r.updateStatus(len)
		} else if r.countLessThanMedian == len/2+1 { // the example for this case is [1 3 5 6] -> [1 2 3 5]
			l1, l2 := r.findTwoMaxLessThanMedian()
			r.result = (l1 + l2) / 2
			r.updateStatus(len)
		} // In the other case, the median didn't change
	} else {
		if r.countLessThanMedian == len/2+1 { // the example for this case is [1 2 3 4 5] -> [1 1 2 3 4]
			l1 := r.findMaxValueLessThanMedian()
			r.result = l1
			r.updateStatus(len)
		} else if r.countGreaterThanMedian == len/2+1 { // the example for this case is [1 2 3 4 5] -> [2 3 4 5 6]
			g1 := r.findMinGreaterThanMedian()
			r.result = g1
			r.updateStatus(len)
		} // In the other case, the median didn't change
	}
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	return r.result
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.count = 0
	r.result = 0
	r.countGreaterThanMedian = 0
	r.countLessThanMedian = 0
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.records[0] = n
	r.count = 1
	r.result = n
	r.countGreaterThanMedian = 0
	r.countLessThanMedian = 0
}

// GetInstantaneous returns the value just added.
func (r *MedianFilter) GetInstantaneous() float64 {
	return r.records[(r.count-1)%r.size]
}

// Clone returns a copy of MedianFilter.
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

// GetAll only used in test.
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
