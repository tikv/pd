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
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testLimiterSuite{})

type testLimiterSuite struct{}

func (s testLimiterSuite) SetUpSuite(c *C) {
	limiterCacheGCInterval = time.Second * 1
	limiterCacheTTL = time.Second * 2
}

func (s *testLimiterSuite) TestUpdateCounts(c *C) {
	l := NewLimiter()
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(0))
	c.Assert(l.OperatorCount(OpRegion), Equals, uint64(0))

	// init region and operator
	for i := uint64(1); i <= 3; i++ {
		op := newTestOperator(i, OpLeader, TransferLeader{FromStore: i + 1, ToStore: 1})
		region := newTestRegion(i, i+1, [2]uint64{1, 1}, [2]uint64{i + 1, i + 1})
		l.UpdateCounts(op, region)
	}

	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(1))

	time.Sleep(time.Second * 1)
	// operator not finished
	op := newTestOperator(3, OpLeader, TransferLeader{FromStore: 4, ToStore: 1})
	region := newTestRegion(3, 4, [2]uint64{1, 1}, [2]uint64{4, 4})
	l.UpdateCounts(op, region)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(1))
	// operator finished
	op = newTestOperator(2, OpLeader, TransferLeader{FromStore: 3, ToStore: 1})
	region = newTestRegion(2, 1, [2]uint64{1, 1}, [2]uint64{3, 3})
	l.UpdateCounts(op, region)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(3))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(1))

	// wait for ttl
	// cause there is an unfinished operator updating the counts, so expire updated.
	time.Sleep(time.Second * 2)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(1))

	// wait for ttl
	time.Sleep(time.Second * 2)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 3), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 4), Equals, uint64(0))
}

func (s *testLimiterSuite) TestRemove(c *C) {
	l := NewLimiter()

	op := newTestOperator(100, OpLeader, TransferLeader{FromStore: 100, ToStore: 1})
	region := newTestRegion(100, 100, [2]uint64{1, 1}, [2]uint64{100, 100})
	l.UpdateCounts(op, region)
	op = newTestOperator(1, OpLeader, TransferLeader{FromStore: 2, ToStore: 1})
	region = newTestRegion(1, 2, [2]uint64{1, 1}, [2]uint64{2, 2})
	l.UpdateCounts(op, region)

	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(2))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(2))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 100), Equals, uint64(1))
	l.Remove(op, region)
	c.Assert(l.OperatorCount(OpLeader), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 1), Equals, uint64(1))
	c.Assert(l.StoreOperatorCount(OpLeader, 2), Equals, uint64(0))
	c.Assert(l.StoreOperatorCount(OpLeader, 100), Equals, uint64(1))
	c.Assert(l.counts[op.Kind()][2], IsNil)
}
