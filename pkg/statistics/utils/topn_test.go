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
	"math/rand"
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
