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

package cache

// WeightAllocator is used to alloc weight
type WeightAllocator struct {
	segIndexs []int
	wights    []float64
}

// NewWeightAllocator returns a new WeightAllocator
func NewWeightAllocator(length, segNum int) *WeightAllocator {
	segLength := length / segNum
	segIndexs := make([]int, 0, segNum)
	wights := make([]float64, 0, length)

	unitCount := 0
	for i := 0; i < segNum; i++ {
		next := segLength
		if (length % segNum) > i {
			next++
		}
		unitCount += (segNum - i) * next
		segIndexs = append(segIndexs, next)
	}

	// If there are 10 results, the weight is [0.143,0.143,0.143,0.143,0.095,0.095,0.095,0.047,0.047,0.047],
	// and segIndexs is
	// If there are 3 results, the weight is [0.5,0.33,0.17]
	unitWeight := 1.0 / float64(unitCount)
	for i := 0; i < segNum; i++ {
		for j := 0; j < segIndexs[i]; j++ {
			wights = append(wights, unitWeight*float64(segNum-i))
		}
	}
	return &WeightAllocator{
		segIndexs: segIndexs,
		wights:    wights,
	}
}

// Get returns weight at pos i
func (a *WeightAllocator) Get(i int) float64 {
	if i >= len(a.wights) {
		return 0
	}
	return a.wights[i]
}
