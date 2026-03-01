// Copyright 2019 TiKV Project Authors.
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

package utils

import (
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type item struct {
	id     uint64
	values []float64
}

func (it *item) ID() uint64 {
	return it.id
}

func (it *item) Less(k int, than TopNItem) bool {
	return it.values[k] < than.(*item).values[k]
}

func TestPut(t *testing.T) {
	re := require.New(t)
	const Total, N = 10000, 50
	tn := NewTopN(DimLen, N, time.Hour)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x) + 1
	}, false /*insert*/)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x)
	}, true /*update*/)

	// check GetTopNMin
	for k := range DimLen {
		re.Equal(float64(1-N), tn.GetTopNMin(k).(*item).values[k])
	}

	{
		topns := make([]float64, N)
		// check GetAllTopN
		for _, it := range tn.GetAllTopN(0) {
			it := it.(*item)
			topns[it.id] = it.values[0]
		}
		// check update worked
		for i, v := range topns {
			re.Equal(float64(-i), v)
		}
	}

	{
		all := make([]float64, Total)
		// check GetAll
		for _, it := range tn.GetAll() {
			it := it.(*item)
			all[it.id] = it.values[0]
		}
		// check update worked
		for i, v := range all {
			re.Equal(float64(-i), v)
		}
	}

	{ // check all dimensions
		for k := 1; k < DimLen; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))

			re.Equal(all[:N], topn)
		}
	}

	// check Get
	for i := range uint64(Total) {
		it := tn.Get(i).(*item)
		re.Equal(i, it.id)
		re.Equal(-float64(i), it.values[0])
	}
}

// putPerm inserts total items into tn with randomized permutation for each dimension.
// For each item, f generates the values based on the item index.
// The randomized insertion order helps verify that TopN maintains correct ordering
// regardless of how items are inserted. If isUpdate is true, the Put method should
// indicate these are updates; otherwise, they should be new inserts.
func putPerm(re *require.Assertions, tn *TopN, total int, f func(x int) float64, isUpdate bool) {
	{ // insert
		dims := make([][]int, DimLen)
		for k := range DimLen {
			dims[k] = rand.Perm(total)
		}
		for i := range total {
			item := &item{
				id:     uint64(dims[0][i]),
				values: make([]float64, DimLen),
			}
			for k := range DimLen {
				item.values[k] = f(dims[k][i])
			}
			re.Equal(isUpdate, tn.Put(item))
		}
	}
}

func TestRemove(t *testing.T) {
	re := require.New(t)
	const Total, N = 10000, 50
	tn := NewTopN(DimLen, N, time.Hour)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	// check Remove
	for i := range Total {
		if i%3 != 0 {
			it := tn.Remove(uint64(i)).(*item)
			re.Equal(uint64(i), it.id)
		}
	}

	// check Remove worked
	for i := range Total {
		if i%3 != 0 {
			re.Nil(tn.Remove(uint64(i)))
		}
	}

	re.Equal(uint64(3*(N-1)), tn.GetTopNMin(0).(*item).id)

	{
		topns := make([]float64, N)
		for _, it := range tn.GetAllTopN(0) {
			it := it.(*item)
			topns[it.id/3] = it.values[0]
			re.Equal(uint64(0), it.id%3)
		}
		for i, v := range topns {
			re.Equal(float64(-i*3), v)
		}
	}

	{
		all := make([]float64, Total/3+1)
		for _, it := range tn.GetAll() {
			it := it.(*item)
			all[it.id/3] = it.values[0]
			re.Equal(uint64(0), it.id%3)
		}
		for i, v := range all {
			re.Equal(float64(-i*3), v)
		}
	}

	{ // check all dimensions
		for k := 1; k < DimLen; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total/3+1)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))

			re.Equal(all[:N], topn)
		}
	}

	for i := uint64(0); i < Total; i += 3 {
		it := tn.Get(i).(*item)
		re.Equal(i, it.id)
		re.Equal(-float64(i), it.values[0])
	}
}

func TestGetTopNMin(t *testing.T) {
	re := require.New(t)
	const Total, N = 1000, 60
	tn := NewTopN(DimLen, N, time.Hour)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	// For each dimension, verify that GetTopNMin is correct
	for k := range DimLen {
		minItem := tn.GetTopNMin(k)
		re.NotNil(minItem)

		// Verify minItem is less than or equal to all items in topN in dimension k
		// !(it.Less(k, minItem)) ⟹ (minItem.values[k] <= it.values[k])
		for _, it := range tn.GetAllTopN(k) {
			re.False(it.Less(k, minItem))
		}

		// Count items greater than minItem in dimension k
		allItems := tn.GetAll()
		countGreaterThanMin := 0
		for _, it := range allItems {
			if minItem.Less(k, it) {
				countGreaterThanMin++
			}
		}
		// Should be at most N-1 since minItem is the minimum in topN,
		// and there are at most N-1 other items in topN greater than it
		re.LessOrEqual(countGreaterThanMin, N-1)
	}
}

func TestGetTopNMax(t *testing.T) {
	re := require.New(t)
	const Total = 1000

	for _, N := range []int{60, 61} {
		tn := NewTopN(DimLen, N, time.Hour)

		putPerm(re, tn, Total, func(x int) float64 {
			return float64(-x)
		}, false /*insert*/)

		// For each dimension, verify that GetTopNMax is correct
		for k := range DimLen {
			maxItem := tn.GetTopNMax(k)
			re.NotNil(maxItem)

			// Verify maxItem is greater than or equal to all items in topN
			// !(maxItem.Less(k, it)) ⟹ (maxItem.values[k] >= it.values[k])
			for _, it := range tn.GetAllTopN(k) {
				re.False(maxItem.Less(k, it))
			}
		}
	}
}

func TestGetAllTopNIsSubset(t *testing.T) {
	re := require.New(t)
	const Total, N = 1000, 60
	tn := NewTopN(DimLen, N, time.Hour)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	// For each dimension, verify that GetAllTopN is a subset of GetAll
	allItems := tn.GetAll()
	for k := range DimLen {
		topNItems := tn.GetAllTopN(k)
		re.Subset(allItems, topNItems)
	}
}

func TestTTL(t *testing.T) {
	re := require.New(t)
	const Total, N = 1000, 50
	tn := NewTopN(DimLen, 50, 900*time.Millisecond)

	putPerm(re, tn, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)
	re.Len(tn.GetAll(), Total)

	time.Sleep(900 * time.Millisecond)
	{
		item := &item{id: 0, values: []float64{100}}
		for k := 1; k < DimLen; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		re.True(tn.Put(item))
	}
	re.Len(tn.RemoveExpired(), (Total-1)*DimLen)

	for i := 3; i < Total; i += 3 {
		item := &item{id: uint64(i), values: []float64{float64(-i) + 100}}
		for k := 1; k < DimLen; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		re.False(tn.Put(item))
	}

	re.Equal(Total/3+1, tn.Len())
	items := tn.GetAllTopN(0)
	v := make([]float64, N)
	for _, it := range items {
		it := it.(*item)
		re.Equal(uint64(0), it.id%3)
		v[it.id/3] = it.values[0]
	}
	for i, x := range v {
		re.Equal(float64(-i*3)+100, x)
	}

	{ // check all dimensions
		for k := 1; k < DimLen; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total/3+1)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))

			re.Equal(all[:N], topn)
		}
	}
}
