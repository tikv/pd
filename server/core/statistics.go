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

package core

import (
	"github.com/montanaflynn/stats"
)

// RollingStats provides rolling statistics with specified windows size.
// There have windows size records used for calculating. it provides Rolling
// `Median` now, which used to filter noise in some situation.
type RollingStats struct {
	records  []float64
	size     int
	pos      int
	filledUp bool
}

// NewRollingStats returns a RollingStats.
func NewRollingStats(size int) *RollingStats {
	return &RollingStats{
		records:  make([]float64, size),
		size:     size,
		filledUp: false,
	}
}

// Add adds an element.
func (r *RollingStats) Add(n float64) {
	r.records[r.pos] = n
	r.pos = (r.pos + 1) % r.size
	if !r.filledUp && r.pos == 0 {
		r.filledUp = true
	}
}

// Median returns the median of the records.
func (r *RollingStats) Median() float64 {
	if len(r.records) == 0 {
		return 0
	}
	records := r.records
	if !r.filledUp {
		records = r.records[:r.pos]
	}
	median, _ := stats.Median(records)
	return median
}
