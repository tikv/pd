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

package typeutil

import (
	"math"
	"sort"
	"time"
)

// MinUint64 returns the min value between two variables whose type are uint64.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint64 returns the max value between two variables whose type are uint64.
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// MinDuration returns the min value between two variables whose type are time.Duration.
func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// AreStringSlicesEqual checks if two string slices are equal. Empyt slice and nil are considered equal.
// It returns true if the slices are of the same length and all elements are identical in both slices, otherwise, it returns false.
func AreStringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// AreStringSlicesEquivalent checks if two string slices are equivalent.
// If the slices are of the same length and contain the same elements (but possibly in different order), the function returns true.
func AreStringSlicesEquivalent(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Float64Equal checks if two float64 are equal.
func Float64Equal(a, b float64) bool {
	return math.Abs(a-b) <= 1e-6
}
