// Copyright 2021 TiKV Project Authors.
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

package rangelist

import (
	"testing"

	"github.com/pingcap/check"
)

func TestRangeList(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testRangeListSuite{})

type testRangeListSuite struct{}

func (s *testRangeListSuite) TestRangeList(c *check.C) {
	rl := NewBuilder().Build()
	c.Assert(rl.Len(), check.Equals, 0)
	i, data := rl.GetDataByKey([]byte("a"))
	c.Assert(i, check.Equals, -1)
	c.Assert(data, check.IsNil)
	i, data = rl.GetData([]byte("a"), []byte("b"))
	c.Assert(i, check.Equals, -1)
	c.Assert(data, check.IsNil)
	c.Assert(rl.GetSplitKeys(nil, []byte("foo")), check.IsNil)

	b := NewBuilder()
	b.AddItem(nil, nil, 1)
	rl = b.Build()
	c.Assert(rl.Len(), check.Equals, 1)
	key, data := rl.Get(0)
	c.Assert(key, check.IsNil)
	c.Assert(data, check.DeepEquals, []interface{}{1})
	i, data = rl.GetDataByKey([]byte("foo"))
	c.Assert(i, check.Equals, 0)
	c.Assert(data, check.DeepEquals, []interface{}{1})
	i, data = rl.GetData([]byte("a"), []byte("b"))
	c.Assert(i, check.Equals, 0)
	c.Assert(data, check.DeepEquals, []interface{}{1})
	c.Assert(rl.GetSplitKeys(nil, []byte("foo")), check.IsNil)
}

func (s *testRangeListSuite) TestRangeList2(c *check.C) {
	b := NewBuilder()
	b.SetCompareFunc(func(a, b interface{}) int {
		if a.(int) > b.(int) {
			return 1
		}
		if a.(int) < b.(int) {
			return -1
		}
		return 0
	})

	//       a   b   c   d   e   f   g   h   i
	// 1             |---|
	// 2 |-------|                   |-------|
	// 3 |-------|               |-------|
	// 4     |---------------|
	//   |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|
	b.AddItem([]byte("a"), []byte("e"), 4)
	b.AddItem([]byte("c"), []byte("d"), 1)
	b.AddItem([]byte("f"), []byte("h"), 3)
	b.AddItem([]byte("g"), []byte("i"), 2)
	b.AddItem([]byte(""), []byte("b"), 2)
	b.AddItem([]byte(""), []byte("b"), 3)

	expectKeys := [][]byte{
		{}, {'a'}, {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'},
	}
	expectData := [][]interface{}{
		{2, 3}, {2, 3, 4}, {4}, {1, 4}, {4}, {}, {3}, {2, 3}, {2}, {},
	}

	rl := b.Build()
	c.Assert(rl.Len(), check.Equals, len(expectKeys))
	for i := 0; i < rl.Len(); i++ {
		key, data := rl.Get(i)
		c.Assert(key, check.DeepEquals, expectKeys[i])
		c.Assert(data, check.DeepEquals, expectData[i])
	}

	getDataByKeyCases := []struct {
		key string
		pos int
	}{
		{"", 0}, {"a", 1}, {"abc", 1}, {"efg", 5}, {"z", 9},
	}
	for _, tc := range getDataByKeyCases {
		i, data := rl.GetDataByKey([]byte(tc.key))
		c.Assert(i, check.Equals, tc.pos)
		c.Assert(data, check.DeepEquals, expectData[i])
	}

	getDataCases := []struct {
		start, end string
		pos        int
	}{
		{"", "", -1}, {"", "a", 0}, {"", "aa", -1},
		{"b", "c", 2}, {"ef", "ex", 5}, {"e", "", -1},
	}
	for _, tc := range getDataCases {
		i, data := rl.GetData([]byte(tc.start), []byte(tc.end))
		c.Assert(i, check.Equals, tc.pos)
		if i >= 0 {
			c.Assert(data, check.DeepEquals, expectData[i])
		}
	}

	getSplitKeysCases := []struct {
		start, end           string
		indexStart, indexEnd int
	}{
		{"", "", 1, 10},
		{"a", "c", 2, 3},
		{"cc", "fx", 4, 7},
	}
	for _, tc := range getSplitKeysCases {
		c.Assert(rl.GetSplitKeys([]byte(tc.start), []byte(tc.end)), check.DeepEquals, expectKeys[tc.indexStart:tc.indexEnd])
	}
}
