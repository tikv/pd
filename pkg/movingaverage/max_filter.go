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

// MaxFilter works as a maximum filter with specified window size.
// There are at most `size` data points for calculating.
type MaxFilter struct {
	records []float64
	size    uint64
	count   uint64
}

// NewMaxFilter returns a MaxFilter.
func NewMaxFilter(size int) *MaxFilter {
	return &MaxFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

// Add adds a data point.
func (r *MaxFilter) Add(n float64) {
	r.records[r.count%r.size] = n
	r.count++
}

// Get returns the maximum of the data set.
func (r *MaxFilter) Get() float64 {
	if r.count == 0 {
		return 0
	}
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	return pie.Max(records)
}

// Reset cleans the data set.
func (r *MaxFilter) Reset() {
	r.count = 0
}

// Set = Reset + Add.
func (r *MaxFilter) Set(n float64) {
	r.records[0] = n
	r.count = 1
}

// GetInstantaneous returns the value just added.
func (r *MaxFilter) GetInstantaneous() float64 {
	if r.count == 0 {
		return 0
	}
	return r.records[(r.count-1)%r.size]
}
