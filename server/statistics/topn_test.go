// Copyright 2019 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTopNSuite{})

type testTopNSuite struct{}

type item struct {
	id    uint64
	value float64
}

func (it *item) ID() uint64 {
	return it.id
}

func (it *item) Less(than TopNItem) bool {
	return it.value < than.(*item).value
}

func (s *testTopNSuite) TestPut(c *C) {
	const Total = 10000
	const N = 50
	tn := NewTopN(N, 1*time.Hour)
	for _, x := range rand.Perm(Total) {
		c.Assert(tn.Put(&item{id: uint64(x), value: float64(-x) + 1}), IsFalse)
	}
	for _, x := range rand.Perm(Total) {
		c.Assert(tn.Put(&item{id: uint64(x), value: float64(-x)}), IsTrue)
	}
	c.Assert(tn.GetTopNMin().(*item), DeepEquals, &item{id: N - 1, value: 1 - N})
	topns := make([]float64, N)
	for _, it := range tn.GetAllTopN() {
		it := it.(*item)
		topns[it.id] = it.value
	}
	for i, v := range topns {
		c.Assert(v, Equals, float64(-i))
	}
	all := make([]float64, Total)
	for _, it := range tn.GetAll() {
		it := it.(*item)
		all[it.id] = it.value
	}
	for i, v := range all {
		c.Assert(v, Equals, float64(-i))
	}
	for i := uint64(0); i < Total; i++ {
		it := tn.Get(i).(*item)
		c.Assert(it.id, Equals, i)
		c.Assert(it.value, Equals, -float64(i))
	}
}

func (s *testTopNSuite) TestRemove(c *C) {
	const Total = 10000
	const N = 50
	tn := NewTopN(N, 1*time.Hour)
	for _, x := range rand.Perm(Total) {
		c.Assert(tn.Put(&item{id: uint64(x), value: float64(-x)}), IsFalse)
	}
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			it := tn.Remove(uint64(i)).(*item)
			c.Assert(it.id, Equals, uint64(i))
		}
	}
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			c.Assert(tn.Remove(uint64(i)), IsNil)
		}
	}
	c.Assert(tn.GetTopNMin().(*item), DeepEquals, &item{id: 3 * (N - 1), value: 3 * (1 - N)})
	topns := make([]float64, N)
	for _, it := range tn.GetAllTopN() {
		it := it.(*item)
		topns[it.id/3] = it.value
		c.Assert(it.id%3, Equals, uint64(0))
	}
	for i, v := range topns {
		c.Assert(v, Equals, float64(-i*3))
	}
	all := make([]float64, Total/3+1)
	for _, it := range tn.GetAll() {
		it := it.(*item)
		all[it.id/3] = it.value
		c.Assert(it.id%3, Equals, uint64(0))
	}
	for i, v := range all {
		c.Assert(v, Equals, float64(-i*3))
	}
	for i := uint64(0); i < Total; i += 3 {
		it := tn.Get(i).(*item)
		c.Assert(it.id, Equals, i)
		c.Assert(it.value, Equals, -float64(i))
	}
}

func (s *testTopNSuite) TestTTL(c *C) {
	const Total = 10000
	const N = 50
	tn := NewTopN(50, 900*time.Millisecond)
	for _, x := range rand.Perm(Total) {
		c.Assert(tn.Put(&item{id: uint64(x), value: float64(-x)}), IsFalse)
	}
	time.Sleep(900 * time.Millisecond)
	c.Assert(tn.Put(&item{id: 0, value: 100}), IsTrue)
	for i := 3; i < Total; i += 3 {
		c.Assert(tn.Put(&item{id: uint64(i), value: float64(-i) + 100}), IsFalse)
	}
	tn.RemoveExpired()
	c.Assert(tn.Len(), Equals, Total/3+1)
	items := tn.GetAllTopN()
	v := make([]float64, N)
	for _, it := range items {
		it := it.(*item)
		c.Assert(it.id%3, Equals, uint64(0))
		v[it.id/3] = it.value
	}
	for i, x := range v {
		c.Assert(x, Equals, float64(-i*3)+100)
	}
}
