// Copyright 2016 PingCAP, Inc.
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

package server

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testEnsureRangeSuite{})

type testEnsureRangeSuite struct{}

type testEnsureRangeCase struct {
	v        uint64
	min      uint64
	max      uint64
	expected uint64
}

func (s *testEnsureRangeSuite) TestUint64(c *C) {
	tests := []*testEnsureRangeCase{
		{0, 1, 3, 1},
		{1, 1, 3, 1},
		{2, 1, 3, 2},
		{3, 1, 3, 3},
		{4, 1, 3, 3},
	}
	for _, t := range tests {
		c.Assert(ensureRangeUint64(t.v, t.min, t.max), Equals, t.expected)
	}
}
