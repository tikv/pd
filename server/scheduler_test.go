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
	"github.com/pingcap/pd/server/schedule"
)

var _ = Suite(&testShuffleLeaderSuite{})

type testShuffleLeaderSuite struct{}

func (s *testShuffleLeaderSuite) TestShuffle(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()
	sl, err := schedule.CreateScheduler("shuffle-leader", opt)
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(cluster), IsNil)

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 6)
	tc.addLeaderStore(2, 7)
	tc.addLeaderStore(3, 8)
	tc.addLeaderStore(4, 9)
	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.addLeaderRegion(1, 1, 2, 3, 4)
	tc.addLeaderRegion(1, 2, 3, 4, 1)
	tc.addLeaderRegion(2, 2, 3, 4, 1)
	tc.addLeaderRegion(2, 3, 4, 1, 2)
	tc.addLeaderRegion(3, 3, 4, 1, 2)
	tc.addLeaderRegion(3, 4, 1, 2, 3)
	tc.addLeaderRegion(4, 4, 1, 2, 3)
	tc.addLeaderRegion(4, 1, 2, 3, 4)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(cluster)
		sourceID := op.Step(0).(schedule.TransferLeader).FromStore
		op = sl.Schedule(cluster)
		targetID := op.Step(0).(schedule.TransferLeader).ToStore
		c.Assert(sourceID, Equals, targetID)
	}
}
