// Copyright 2016 TiKV Project Authors.
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
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct{}

func (s *testTimeSuite) TestParseTimestamp(c *C) {
	for i := 0; i < 3; i++ {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := Uint64ToBytes(uint64(t.UnixNano()))
		nt, err := ParseTimestamp(data)
		c.Assert(err, IsNil)
		c.Assert(nt.Equal(t), IsTrue)
	}
	data := []byte("pd")
	nt, err := ParseTimestamp(data)
	c.Assert(err, NotNil)
	c.Assert(nt.Equal(ZeroTime), IsTrue)
}

func (s *testTimeSuite) TestSubTimeByWallClock(c *C) {
	for i := 0; i < 100; i++ {
		r := rand.Int63n(1000)
		t1 := time.Now()
		// Add r seconds.
		t2 := t1.Add(time.Second * time.Duration(r))
		duration := SubRealTimeByWallClock(t2, t1)
		c.Assert(duration, Equals, time.Second*time.Duration(r))
		milliseconds := SubTSOPhysicalByWallClock(t2, t1)
		c.Assert(milliseconds, Equals, r*time.Second.Milliseconds())
		// Add r millionseconds.
		t3 := t1.Add(time.Millisecond * time.Duration(r))
		milliseconds = SubTSOPhysicalByWallClock(t3, t1)
		c.Assert(milliseconds, Equals, r)
		// Add r nanoseconds.
		t4 := t1.Add(time.Duration(-r))
		duration = SubRealTimeByWallClock(t4, t1)
		c.Assert(duration, Equals, time.Duration(-r))
		// For the millisecond comparsion, please see TestSmallTimeDifference.
	}
}

func (s *testTimeSuite) TestSmallTimeDifference(c *C) {
	t1, err := time.Parse("2006-01-02 15:04:05.999", "2021-04-26 00:44:25.682")
	c.Assert(err, IsNil)
	t2, err := time.Parse("2006-01-02 15:04:05.999", "2021-04-26 00:44:25.681918")
	c.Assert(err, IsNil)
	duration := SubRealTimeByWallClock(t1, t2)
	c.Assert(duration, Equals, time.Duration(82)*time.Microsecond)
	duration = SubRealTimeByWallClock(t2, t1)
	c.Assert(duration, Equals, time.Duration(-82)*time.Microsecond)
	milliseconds := SubTSOPhysicalByWallClock(t1, t2)
	c.Assert(milliseconds, Equals, int64(1))
	milliseconds = SubTSOPhysicalByWallClock(t2, t1)
	c.Assert(milliseconds, Equals, int64(-1))
}
