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
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testOpStatusTrackerSuite{})

type testOpStatusTrackerSuite struct{}

func (s *testOpStatusTrackerSuite) TestCreate(c *C) {
	before := time.Now()
	trk := NewOpStatusTracker()
	c.Assert(trk.Status(), Equals, CREATED)
	c.Assert(trk.ReachTime(), DeepEquals, trk.ReachTimeOf(CREATED))
	checkTimeOrder(c, before, trk.ReachTime(), time.Now())
	checkReachTime(c, &trk, CREATED)
}

func (s *testOpStatusTrackerSuite) TestNonEndTrans(c *C) {
	{
		trk := NewOpStatusTracker()
		checkInvalidTrans(c, &trk, SUCCESS, REPLACED, TIMEOUT)
		checkValidTrans(c, &trk, STARTED)
		checkInvalidTrans(c, &trk, EXPIRED)
		checkValidTrans(c, &trk, SUCCESS)
		checkReachTime(c, &trk, CREATED, STARTED, SUCCESS)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(c, &trk, CANCELED)
		checkReachTime(c, &trk, CREATED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(c, &trk, STARTED)
		checkValidTrans(c, &trk, CANCELED)
		checkReachTime(c, &trk, CREATED, STARTED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(c, &trk, STARTED)
		checkValidTrans(c, &trk, REPLACED)
		checkReachTime(c, &trk, CREATED, STARTED, REPLACED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(c, &trk, EXPIRED)
		checkReachTime(c, &trk, CREATED, EXPIRED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(c, &trk, STARTED)
		checkValidTrans(c, &trk, TIMEOUT)
		checkReachTime(c, &trk, CREATED, STARTED, TIMEOUT)
	}
}

func (s *testOpStatusTrackerSuite) TestEndStatusTrans(c *C) {
	allStatus := make([]OpStatus, 0, statusCount)
	for st := OpStatus(0); st < statusCount; st++ {
		allStatus = append(allStatus, st)
	}
	for from := firstEndStatus; from < statusCount; from++ {
		trk := NewOpStatusTracker()
		trk.current = from
		c.Assert(trk.IsEnd(), IsTrue)
		checkInvalidTrans(c, &trk, allStatus...)
	}
}

func (s *testOpStatusTrackerSuite) TestCheckExpired(c *C) {
	{
		// Not expired
		before := time.Now()
		trk := NewOpStatusTracker()
		after := time.Now()
		c.Assert(trk.CheckExpired(10*time.Second), IsFalse)
		c.Assert(trk.Status(), Equals, CREATED)
		checkTimeOrder(c, before, trk.ReachTime(), after)
	}
	{
		// Expired but status not changed
		trk := NewOpStatusTracker()
		trk.setTime(CREATED, time.Now().Add(-10*time.Second))
		c.Assert(trk.CheckExpired(5*time.Second), IsTrue)
		c.Assert(trk.Status(), Equals, EXPIRED)
	}
	{
		// Expired and status changed
		trk := NewOpStatusTracker()
		before := time.Now()
		c.Assert(trk.To(EXPIRED), IsTrue)
		after := time.Now()
		c.Assert(trk.CheckExpired(0), IsTrue)
		c.Assert(trk.Status(), Equals, EXPIRED)
		checkTimeOrder(c, before, trk.ReachTime(), after)
	}
}

func (s *testOpStatusTrackerSuite) TestCheckTimeout(c *C) {
	{
		// Not timeout
		trk := NewOpStatusTracker()
		before := time.Now()
		c.Assert(trk.To(STARTED), IsTrue)
		after := time.Now()
		c.Assert(trk.CheckTimeout(10*time.Second), IsFalse)
		c.Assert(trk.Status(), Equals, STARTED)
		checkTimeOrder(c, before, trk.ReachTime(), after)
	}
	{
		// Timeout but status not changed
		trk := NewOpStatusTracker()
		c.Assert(trk.To(STARTED), IsTrue)
		trk.setTime(STARTED, time.Now().Add(-10*time.Second))
		c.Assert(trk.CheckTimeout(5*time.Second), IsTrue)
		c.Assert(trk.Status(), Equals, TIMEOUT)
	}
	{
		// Timeout and status changed
		trk := NewOpStatusTracker()
		c.Assert(trk.To(STARTED), IsTrue)
		before := time.Now()
		c.Assert(trk.To(TIMEOUT), IsTrue)
		after := time.Now()
		c.Assert(trk.CheckTimeout(0), IsTrue)
		c.Assert(trk.Status(), Equals, TIMEOUT)
		checkTimeOrder(c, before, trk.ReachTime(), after)
	}
}

func checkTimeOrder(c *C, t1, t2, t3 time.Time) {
	c.Assert(t1.Before(t2), IsTrue)
	c.Assert(t3.After(t2), IsTrue)
}

func checkValidTrans(c *C, trk *OpStatusTracker, st OpStatus) {
	before := time.Now()
	c.Assert(trk.To(st), IsTrue)
	c.Assert(trk.Status(), Equals, st)
	c.Assert(trk.ReachTime(), DeepEquals, trk.ReachTimeOf(st))
	checkTimeOrder(c, before, trk.ReachTime(), time.Now())
}

func checkInvalidTrans(c *C, trk *OpStatusTracker, sts ...OpStatus) {
	origin := trk.Status()
	originTime := trk.ReachTime()
	sts = append(sts, statusCount, statusCount+1, statusCount+10)
	for _, st := range sts {
		c.Assert(trk.To(st), IsFalse)
		c.Assert(trk.Status(), Equals, origin)
		c.Assert(trk.ReachTime(), DeepEquals, originTime)
	}
}

func checkReachTime(c *C, trk *OpStatusTracker, reached ...OpStatus) {
	reachedMap := make(map[OpStatus]struct{}, len(reached))
	for _, st := range reached {
		c.Assert(trk.ReachTimeOf(st).IsZero(), IsFalse)
		reachedMap[st] = struct{}{}
	}
	for st := OpStatus(0); st <= statusCount+10; st++ {
		if _, ok := reachedMap[st]; ok {
			continue
		}
		c.Assert(trk.ReachTimeOf(st).IsZero(), IsTrue)
	}
}
