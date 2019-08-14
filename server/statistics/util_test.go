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

package statistics

import (
	"math/rand"
	"sort"
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRollingStats{})

type testRollingStats struct{}

func (t *testRollingStats) TestRollingMedian(c *C) {
	data := []float64{2, 4, 2, 800, 600, 6, 3}
	expected := []float64{2, 3, 2, 3, 4, 6, 6}
	stats := NewRollingStats(5)
	for i, e := range data {
		stats.Add(e)
		c.Assert(stats.Median(), Equals, expected[i])
	}
}

var _ = Suite(&testTopN{})

type testTopN struct{}

type simpleItem struct {
	id    uint64
	value int
}

func (it *simpleItem) ID() uint64 {
	return it.id
}

func (it *simpleItem) Less(than TopNItem) bool {
	return it.value < than.(*simpleItem).value
}

type sortItems []*simpleItem

func (s sortItems) Len() int {
	return len(s)
}

func (s sortItems) Less(i, j int) bool {
	return s[j].Less(s[i])
}

func (s sortItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (t *testTopN) TestTopN(c *C) {
	tot := 100
	n := 10
	items := make([]*simpleItem, tot)
	for i := 0; i < tot; i++ {
		items[i] = &simpleItem{id: uint64(i), value: rand.Int()}
	}
	tn := NewTopN(n)

	// test Add
	for _, item := range items {
		c.Assert(tn.Put(item), Equals, true)
	}

	for i, item := range items {
		newitem := &simpleItem{id: uint64(item.id), value: rand.Int()}
		items[i] = newitem
		c.Assert(tn.Put(newitem), Equals, false)
	}

	// test Count
	c.Assert(tn.Len(), Equals, tot)

	// test Get
	for i := 0; i < tot; i++ {
		item := tn.GetTopN(uint64(i))
		if item == nil {
			item = tn.GetRest(uint64(i))
		}
		c.Assert(item.(*simpleItem), Equals, items[i])
	}

	// test GetTopN
	t.checkTopNAndRest(c, tn, n, items)

	// test Fix
	for i := 0; i < 30; i++ {
		x := rand.Intn(tot)
		items[x].value = rand.Int()
		tn.Fix(items[x].id)
	}

	t.checkTopNAndRest(c, tn, n, items)

	// test Del
	cnt := tot

	// after Del, tn.Count() > n
	deln1 := 40
	for i := 0; i < deln1; i++ {
		x := rand.Intn(cnt)
		tn.Remove(items[x].id)
		items[x], items[cnt-1] = items[cnt-1], items[x]
		cnt--
	}
	t.checkTopNAndRest(c, tn, n, items[:cnt])
	c.Assert(tn.Len(), Equals, cnt)

	// after Del, tn.Count() < n
	deln2 := 55
	for i := 0; i < deln2; i++ {
		x := rand.Intn(cnt)
		tn.Remove(items[x].id)
		items[x], items[cnt-1] = items[cnt-1], items[x]
		cnt--
	}
	t.checkTopNAndRest(c, tn, n, items[:cnt])
	c.Assert(tn.Len(), Equals, cnt)
}

func (t *testTopN) checkTopNAndRest(c *C, tn *TopN, n int, items []*simpleItem) {
	sort.Sort(sortItems(items))
	realTopn := make(map[uint64]int)
	realRest := make(map[uint64]int)
	for i := 0; i < len(items); i++ {
		if i < n {
			realTopn[items[i].id] = items[i].value
		} else {
			realRest[items[i].id] = items[i].value
		}
	}

	resTopn := make(map[uint64]int)
	resRest := make(map[uint64]int)
	for _, item := range tn.GetAllTopN() {
		resTopn[item.ID()] = item.(*simpleItem).value
	}
	for _, item := range tn.GetAllRest() {
		resRest[item.ID()] = item.(*simpleItem).value
	}

	c.Assert(resTopn, DeepEquals, realTopn)
	c.Assert(resRest, DeepEquals, realRest)
}
