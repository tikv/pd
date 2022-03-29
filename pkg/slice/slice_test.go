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

package slice_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/slice"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSliceSuite{})

type testSliceSuite struct {
}

func (s *testSliceSuite) Test(c *C) {
	tests := []struct {
		a      []int
		anyOf  bool
		noneOf bool
		allOf  bool
	}{
		{[]int{}, false, true, true},
		{[]int{1, 2, 3}, true, false, false},
		{[]int{1, 3}, false, true, false},
		{[]int{2, 2, 4}, true, false, true},
	}

	for _, t := range tests {
		even := func(i int) bool { return t.a[i]%2 == 0 }
		c.Assert(slice.AnyOf(t.a, even), Equals, t.anyOf)
		c.Assert(slice.NoneOf(t.a, even), Equals, t.noneOf)
		c.Assert(slice.AllOf(t.a, even), Equals, t.allOf)
	}
}

func (s *testSliceSuite) TestSliceContains(c *C) {
	ss := []string{"a", "b", "c"}
	c.Assert(slice.Contains(ss, "a"), IsTrue)
	c.Assert(slice.Contains(ss, "d"), IsFalse)

	us := []uint64{1, 2, 3}
	c.Assert(slice.Contains(us, uint64(1)), IsTrue)
	c.Assert(slice.Contains(us, uint64(4)), IsFalse)

	is := []int64{1, 2, 3}
	c.Assert(slice.Contains(is, int64(1)), IsTrue)
	c.Assert(slice.Contains(is, int64(4)), IsFalse)
}
