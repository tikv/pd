// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

const (
	KB = 1024
	MB = 1024 * KB
)

func TestSchedulers(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMinMaxSuite{})

type testMinMaxSuite struct{}

func (s *testMinMaxSuite) TestMinUint64(c *C) {
	c.Assert(minUint64(1, 2), Equals, uint64(1))
	c.Assert(minUint64(2, 1), Equals, uint64(1))
	c.Assert(minUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMaxUint64(c *C) {
	c.Assert(maxUint64(1, 2), Equals, uint64(2))
	c.Assert(maxUint64(2, 1), Equals, uint64(2))
	c.Assert(maxUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMinDuration(c *C) {
	c.Assert(minDuration(time.Minute, time.Second), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Minute), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Second), Equals, time.Second)
}

var _ = Suite(&testScoreInfosSuite{})

type testScoreInfosSuite struct{}

func (s *testScoreInfosSuite) TestSortScoreInfos(c *C) {
	{
		scoreInfos := NewScoreInfos()
		expectedScores := []float64{0.2, 0.4, 0.4, 0.4}

		for i, score := range expectedScores {
			scoreInfos.Add(NewScoreInfo(uint64(i+1), score))
			c.Assert(scoreInfos.isSorted, IsFalse)
		}

		c.Assert(scoreInfos.Min().score, Equals, 0.2)
		c.Assert(scoreInfos.isSorted, IsTrue)

		scoreInfos.Sort()
		for i, pair := range scoreInfos.GetScoreInfo() {
			c.Assert(pair.GetScore(), Equals, expectedScores[i])
		}

		scoreInfos.Add(NewScoreInfo(5, 0.6))
		c.Assert(scoreInfos.isSorted, IsTrue)
		scoreInfos.Add(NewScoreInfo(6, 0.4))
		c.Assert(scoreInfos.isSorted, IsFalse)

		c.Assert(math.Abs(scoreInfos.Mean()-0.4), LessEqual, 1e-7)
	}
	{
		scoreInfos := NewScoreInfos()
		c.Assert(math.Abs(scoreInfos.StdDev()), LessEqual, 1e-7)
		expectedScores := []float64{13, 5, 11, 11}

		for i, score := range expectedScores {
			scoreInfos.Add(NewScoreInfo(uint64(i+1), score))
			c.Assert(scoreInfos.isSorted, IsFalse)
		}

		c.Assert(scoreInfos.Mean(), Equals, 10.0)
		c.Assert(scoreInfos.StdDev(), Equals, 3.0)
	}
}
