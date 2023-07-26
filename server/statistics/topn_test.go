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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math/rand"
	"sort"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTopNSuite{})

type testTopNSuite struct{}

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

func (s *testTopNSuite) TestPut(c *C) {
	const Total = 10000
	const K = 3
	const N = 50
	tn := NewTopN(K, N, 1*time.Hour)

	putPerm(c, tn, K, Total, func(x int) float64 {
		return float64(-x) + 1
	}, false /*insert*/)

	putPerm(c, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, true /*update*/)

	// check GetTopNMin
	for k := 0; k < K; k++ {
		c.Assert(tn.GetTopNMin(k).(*item).values[k], Equals, float64(1-N))
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
			c.Assert(v, Equals, float64(-i))
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
			c.Assert(v, Equals, float64(-i))
		}
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
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

			c.Assert(topn, DeepEquals, all[:N])
		}
	}

	// check Get
	for i := uint64(0); i < Total; i++ {
		it := tn.Get(i).(*item)
		c.Assert(it.id, Equals, i)
		c.Assert(it.values[0], Equals, -float64(i))
	}
}

func putPerm(c *C, tn *TopN, dimNum, total int, f func(x int) float64, isUpdate bool) {
	{ // insert
		dims := make([][]int, dimNum)
		for k := 0; k < dimNum; k++ {
			dims[k] = rand.Perm(total)
		}
		for i := 0; i < total; i++ {
			item := &item{
				id:     uint64(dims[0][i]),
				values: make([]float64, dimNum),
			}
			for k := 0; k < dimNum; k++ {
				item.values[k] = f(dims[k][i])
			}
			c.Assert(tn.Put(item), Equals, isUpdate)
		}
	}
}

func (s *testTopNSuite) TestRemove(c *C) {
	const Total = 10000
	const K = 3
	const N = 50
	tn := NewTopN(K, N, 1*time.Hour)

	putPerm(c, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	// check Remove
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			it := tn.Remove(uint64(i)).(*item)
			c.Assert(it.id, Equals, uint64(i))
		}
	}

	// check Remove worked
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			c.Assert(tn.Remove(uint64(i)), IsNil)
		}
	}

	c.Assert(tn.GetTopNMin(0).(*item).id, Equals, uint64(3*(N-1)))

	{
		topns := make([]float64, N)
		for _, it := range tn.GetAllTopN(0) {
			it := it.(*item)
			topns[it.id/3] = it.values[0]
			c.Assert(it.id%3, Equals, uint64(0))
		}
		for i, v := range topns {
			c.Assert(v, Equals, float64(-i*3))
		}
	}

	{
		all := make([]float64, Total/3+1)
		for _, it := range tn.GetAll() {
			it := it.(*item)
			all[it.id/3] = it.values[0]
			c.Assert(it.id%3, Equals, uint64(0))
		}
		for i, v := range all {
			c.Assert(v, Equals, float64(-i*3))
		}
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
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

			c.Assert(topn, DeepEquals, all[:N])
		}
	}

	for i := uint64(0); i < Total; i += 3 {
		it := tn.Get(i).(*item)
		c.Assert(it.id, Equals, i)
		c.Assert(it.values[0], Equals, -float64(i))
	}
}

func (s *testTopNSuite) TestTTL(c *C) {
	const Total = 1000
	const K = 3
	const N = 50
	tn := NewTopN(K, 50, 900*time.Millisecond)

	putPerm(c, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	time.Sleep(900 * time.Millisecond)
	{
		item := &item{id: 0, values: []float64{100}}
		for k := 1; k < K; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		c.Assert(tn.Put(item), IsTrue)
	}
	for i := 3; i < Total; i += 3 {
		item := &item{id: uint64(i), values: []float64{float64(-i) + 100}}
		for k := 1; k < K; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		c.Assert(tn.Put(item), IsFalse)
	}
	tn.RemoveExpired()

	c.Assert(tn.Len(), Equals, Total/3+1)
	items := tn.GetAllTopN(0)
	v := make([]float64, N)
	for _, it := range items {
		it := it.(*item)
		c.Assert(it.id%3, Equals, uint64(0))
		v[it.id/3] = it.values[0]
	}
	for i, x := range v {
		c.Assert(x, Equals, float64(-i*3)+100)
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
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

			c.Assert(topn, DeepEquals, all[:N])
		}
	}
}
