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

package movingaverage

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testAvgOverTimeSuite{})

type testAvgOverTimeSuite struct{}

func (t *testAvgOverTimeSuite) TestPulse(c *C) {
	aot := NewAvgOverTime(5 * time.Second)
	// warm up
	for i := 0; i < 5; i++ {
		aot.Add(1000, time.Second)
		aot.Add(0, time.Second)
	}
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			aot.Add(1000, time.Second)
		} else {
			aot.Add(0, time.Second)
		}
		c.Assert(aot.Get(), LessEqual, 600.)
		c.Assert(aot.Get(), GreaterEqual, 400.)
	}
}

func (t *testAvgOverTimeSuite) TestChange(c *C) {
	aot := NewAvgOverTime(5 * time.Second)

	// phase 1: 1000
	for i := 0; i < 20; i++ {
		aot.Add(1000, time.Second)
	}
	c.Assert(aot.Get(), LessEqual, 1010.)
	c.Assert(aot.Get(), GreaterEqual, 990.)

	// phase 2: 500
	for i := 0; i < 5; i++ {
		aot.Add(500, time.Second)
	}
	c.Assert(aot.Get(), LessEqual, 900.)
	c.Assert(aot.Get(), GreaterEqual, 495.)
	for i := 0; i < 15; i++ {
		aot.Add(500, time.Second)
	}

	// phase 3: 100
	for i := 0; i < 5; i++ {
		aot.Add(100, time.Second)
	}
	c.Assert(aot.Get(), LessEqual, 678.)
	c.Assert(aot.Get(), GreaterEqual, 99.)

	// clear
	aot.Set(10)
	c.Assert(aot.Get(), Equals, 10.)
}

func (t *testAvgOverTimeSuite) TestMinFilled(c *C) {
	interval := 10 * time.Second
	rate := 1.0
	for aotSize := 2; aotSize < 10; aotSize++ {
		for mfSize := 2; mfSize < 10; mfSize++ {
			tm := NewTimeMedian(aotSize, mfSize, interval)
			for i := 0; i < tm.GetFilledPeriod(); i++ {
				c.Assert(tm.Get(), Equals, 0.0)
				tm.Add(rate*interval.Seconds(), interval)
			}
			c.Assert(tm.Get(), Equals, rate)
		}
	}
}

func (t *testAvgOverTimeSuite) TestUnstableInterval(c *C) {
	aot := NewAvgOverTime(5 * time.Second)
	c.Assert(aot.Get(), Equals, 0.)
	// warm up
	for i := 0; i < 5; i++ {
		aot.Add(1000, time.Second)
	}
	// same rate, different interval
	for i := 0; i < 1000; i++ {
		r := float64(rand.Intn(5))
		aot.Add(1000*r, time.Second*time.Duration(r))
		c.Assert(aot.Get(), LessEqual, 1010.)
		c.Assert(aot.Get(), GreaterEqual, 990.)
	}
	// warm up
	for i := 0; i < 5; i++ {
		aot.Add(500, time.Second)
	}
	// different rate, same interval
	for i := 0; i < 1000; i++ {
		rate := float64(i%5*100) + 500
		aot.Add(rate*3, time.Second*3)
		c.Assert(aot.Get(), LessEqual, 910.)
		c.Assert(aot.Get(), GreaterEqual, 490.)
	}
}
