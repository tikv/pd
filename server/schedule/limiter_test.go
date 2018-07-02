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

package schedule

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testLimiterSuite{})

type testLimiterSuite struct{}

func (s *testLimiterSuite) TestOperatorCount(c *C) {
	l := NewLimiter()
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(0))
	c.Assert(l.OperatorCount(OpRegion), Equals, uint64(0))

	operators := make(map[uint64]*Operator)

	// init region and operator
	for i := uint64(1); i <= 3; i++ {
		op := newTestOperator(i, OpLeader|OpRegion, TransferLeader{FromStore: i + 1, ToStore: 1})
		operators[i] = op
	}

	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(1))

	delete(operators, 1)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(OpLeader|OpRegion), Equals, uint64(2))
	c.Assert(l.StoreOperatorCount(OpLeader|OpRegion, 1), Equals, uint64(2))
	c.Assert(l.StoreOperatorCount(OpLeader|OpRegion, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader|OpRegion, 3), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader|OpRegion, 4), Equals, uint64(1))

	delete(operators, 2)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(OpRegion), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpRegion, 1), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpRegion, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpRegion, 3), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpRegion, 4), Equals, uint64(1))

	delete(operators, 3)
	l.UpdateCounts(operators)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(0))
}
