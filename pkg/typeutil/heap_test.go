// Copyright 2017 PingCAP, Inc.
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

package typeutil

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testHeapSuite{})

type testHeapSuite struct{}

func (s *testSizeSuite) TestPriorityQeury(c *C) {
	items := []struct {
		key, value, priority int
	}{
		{
			key:      1,
			value:    1,
			priority: 10,
		},
		{
			key:      2,
			value:    2,
			priority: 1,
		},
		{
			key:      3,
			value:    3,
			priority: 8,
		},
		{
			key:      4,
			value:    4,
			priority: 4,
		},
		{
			key:      5,
			value:    5,
			priority: 5,
		},
		{
			key:      6,
			value:    6,
			priority: 6,
		},
	}

	pq := NewPriorityQueue(5)

	for _, item := range items {
		result := pq.Push(item.key, item.value, uint64(item.priority))
		c.Assert(result, IsTrue)
	}
	c.Assert(pq.MinItem().Value, Equals, 4)
	wants := []interface{}{1, 3, 6}
	gets := pq.TopN(3)
	c.Assert(gets, DeepEquals, wants)

	//Update
	items[0].priority = 1
	pq.Push(items[0].key, items[0].value, uint64(items[0].priority))
	gets = pq.TopN(3)
	wants = []interface{}{3, 6, 5}
	c.Assert(gets, DeepEquals, wants)
}
