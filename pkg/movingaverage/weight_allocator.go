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

// WeightAllocator is used to alloc weight for cache.
// When we need to use multiple items from the cache,
// we may need to assign different weights to these items.
// WeightAllocator will divide these items into some segments whose number named as segNum which should great than 0.
// And the items at first segment will be assigned more weight that is `segNum` times that of item at last segment.
// If you want assign same weights, just input segNum as 1.
// If length is 10 and segNum is 3, it will make the weight array as [3,3,3,3,2,2,2,1,1,1],
// and then uniform it : [3,3,3,3,2,2,2,1,1,1]/sum(arr)=arr/21,
// And the final weight is [0.143,0.143,0.143,0.143,0.095,0.095,0.095,0.047,0.047,0.047];
// If length is 10 and segNum is 1, the weight is [0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1];
// If length is 3 and segNum is 3, the weight is [0.5,0.33,0.17].
type WeightAllocator struct {
	weights []float64
}

// NewWeightAllocator returns a new WeightAllocator.
func NewWeightAllocator(length, segNum int) *WeightAllocator {
	if segNum < 1 || length < 1 {
		return &WeightAllocator{}
	}
	segLength := length / segNum
	// segMod is used for split seg when is length not divisible by segNum.
	segMod := length % segNum
	segIndexes := make([]int, 0, segNum)
	weights := make([]float64, 0, length)
	unitCount := 0
	for i := range segNum {
		next := segLength
		if segMod > i {
			next++
		}
		unitCount += (segNum - i) * next
		segIndexes = append(segIndexes, next)
	}
	unitWeight := 1.0 / float64(unitCount)
	for i := range segNum {
		for range segIndexes[i] {
			weights = append(weights, unitWeight*float64(segNum-i))
		}
	}
	return &WeightAllocator{
		weights: weights,
	}
}

// Get returns weight at pos i
func (a *WeightAllocator) Get(i int) float64 {
	if i >= len(a.weights) || i < 0 {
		return 0
	}
	return a.weights[i]
}
