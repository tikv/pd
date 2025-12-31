// Copyright 2022 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWeightAllocator(t *testing.T) {
	re := require.New(t)

	checkSumFunc := func(wa *WeightAllocator, length int) {
		sum := 0.
		for i := range length {
			sum += wa.Get(i)
		}
		re.Less(1-sum, 1e-8)
		re.Greater(1-sum, -1e-8)
	}
	checkTimeFunc := func(x1, x2, times float64) {
		re.Less(x1/x2-times, 1e-8)
		re.Greater(x1/x2-times, -1e-8)
	}

	wa := NewWeightAllocator(1, 1)
	re.Equal(1., wa.Get(0))
	checkSumFunc(wa, 1)

	wa = NewWeightAllocator(10, 10)
	checkSumFunc(wa, 10)
	for i := range 10 {
		re.Equal(1./55.*float64(10-i), wa.Get(i))
	}
	checkTimeFunc(wa.Get(0), wa.Get(9), 10)
	checkTimeFunc(wa.Get(0), wa.Get(5), 2)

	wa = NewWeightAllocator(10, 1)
	checkSumFunc(wa, 10)
	re.Equal(1./10., wa.Get(0))
	re.Equal(1./10., wa.Get(7))

	wa = NewWeightAllocator(10, 3)
	checkSumFunc(wa, 10)
	checkTimeFunc(wa.Get(0), wa.Get(3), 1)
	checkTimeFunc(wa.Get(3), wa.Get(9), 3)
	checkTimeFunc(wa.Get(6), wa.Get(9), 2)
	re.Equal(0., wa.Get(10))

	wa = NewWeightAllocator(100, 0)
	re.Equal(0., wa.Get(88))
	wa = NewWeightAllocator(0, 0)
	re.Equal(0., wa.Get(0))
	wa = NewWeightAllocator(0, 100)
	re.Equal(0., wa.Get(10))
	re.Equal(0., wa.Get(-2))

	wa = NewWeightAllocator(0, 2)
	re.Equal(0., wa.Get(0))
	re.Equal(0., wa.Get(1))
	re.Equal(0., wa.Get(2))
}
