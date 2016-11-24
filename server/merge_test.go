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

import . "github.com/pingcap/check"

var _ = Suite(&testMergeInfoSuite{})

type testMergeInfoSuite struct{}

func (s *testMergeInfoSuite) TestMerge(c *C) {
	m := newMergeInfo()

	id1, id2 := uint64(1), uint64(2)
	m.markRegionMerge(id1, id2)
	c.Assert(m.isRegionMerging(id1), IsTrue)
	c.Assert(m.isRegionMerging(id2), IsTrue)
	c.Assert(m.getMergingRegionCount(), Equals, 2)
	fromID, intoID, ok := m.getMergingRegions(id1)
	c.Assert(fromID, Equals, id1)
	c.Assert(intoID, Equals, id2)
	c.Assert(ok, IsTrue)
	fromID, intoID, ok = m.getMergingRegions(id2)
	c.Assert(fromID, Equals, id1)
	c.Assert(intoID, Equals, id2)
	c.Assert(ok, IsTrue)

	m.clearRegionMerge(id1)
	c.Assert(m.isRegionMerging(id1), IsFalse)
	c.Assert(m.isRegionMerging(id2), IsFalse)
	c.Assert(m.getMergingRegionCount(), Equals, 0)
	_, _, ok = m.getMergingRegions(id1)
	c.Assert(ok, IsFalse)
	_, _, ok = m.getMergingRegions(id2)
	c.Assert(ok, IsFalse)

	c.Assert(m.isRegionShutdown(id1), IsFalse)
	c.Assert(m.isRegionShutdown(id2), IsFalse)
	c.Assert(m.getShutdownRegionCount(), Equals, 0)

	m.markRegionShutdown(id1)
	m.markRegionShutdown(id2)
	c.Assert(m.isRegionShutdown(id1), IsTrue)
	c.Assert(m.isRegionShutdown(id2), IsTrue)
	c.Assert(m.getShutdownRegionCount(), Equals, 2)
}
