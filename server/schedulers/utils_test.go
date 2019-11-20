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
	"math/rand"
	"sort"
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

type testScoreInfosSuite struct {
	num        int
	scores     []float64
	scoreInfos *ScoreInfos
}

func (s *testScoreInfosSuite) SetUpSuite(c *C) {
	rand.Seed(time.Now().Unix())
	s.num = 10
	s.scores = make([]float64, 0, s.num)
	s.scoreInfos = NewScoreInfos()
	c.Assert(s.scoreInfos.isSorted, IsTrue)
	for i := 0; i < s.num; i++ {
		score := rand.Float64()
		s.scoreInfos.Add(NewScoreInfo(uint64(i+1), score))
		s.scores = append(s.scores, score)
	}
}

func (s *testScoreInfosSuite) TestSort(c *C) {
	sort.Float64s(s.scores)
	s.scoreInfos.Sort()
	c.Assert(s.scoreInfos.Min().score, Equals, s.scores[0])

	for i := 0; i < s.num; i++ {
		c.Assert(s.scoreInfos.ToSlice()[i].GetScore(), Equals, s.scores[i])
	}
}

func (s *testScoreInfosSuite) TestMin(c *C) {
	sort.Float64s(s.scores)
	s.scoreInfos.Sort()
	c.Assert(s.scoreInfos.isSorted, IsTrue)

	last := s.scores[s.num-1]
	s.scoreInfos.Add(NewScoreInfo(uint64(s.num)+1, last+1))
	c.Assert(s.scoreInfos.isSorted, IsTrue)

	s.scoreInfos.Add(NewScoreInfo(uint64(s.num)+2, last))
	c.Assert(s.scoreInfos.isSorted, IsFalse)
}

func (s *testScoreInfosSuite) TestMeanAndStdDev(c *C) {
	sum := 0.0
	for _, score := range s.scores {
		sum += score
	}
	mean := sum / float64(s.num)

	result := 0.0
	for _, score := range s.scores {
		diff := score - mean
		result += diff * diff
	}
	result = math.Sqrt(result / float64(s.num))

	c.Assert(math.Abs(s.scoreInfos.Mean()-mean), LessEqual, 1e-7)
	c.Assert(math.Abs(s.scoreInfos.StdDev()-result), LessEqual, 1e-7)
}

func (s *testScoreInfosSuite) TestStoresStats(c *C) {
	// test NormalizeStoresStats
	sum := 0.0
	min := math.MaxFloat64
	storeStats := make(map[uint64]float64)
	expect := make(map[uint64]float64)
	for i := 0; i < s.num; i++ {
		storeID := uint64(i + 1)
		storeStats[storeID] = s.scores[i]
		sum += s.scores[i]
		if s.scores[i] < min {
			min = s.scores[i]
		}
	}

	mean := sum / float64(s.num)
	for i := 0; i < s.num; i++ {
		storeID := uint64(i + 1)
		expect[storeID] = (s.scores[i] - min) / mean
	}

	scoreInfos := NormalizeStoresStats(storeStats)
	for _, info := range scoreInfos.ToSlice() {
		c.Assert(math.Abs(info.GetScore()-expect[info.storeID]), LessEqual, 1e-7)
	}
	// test AggregateScores
	var weights []float64
	c.Assert(AggregateScores([]*ScoreInfos{scoreInfos}, weights).ToSlice(), HasLen, 0)
	{
		weights = []float64{0.0, 0.0, 2.0, 2.0}
		results := AggregateScores([]*ScoreInfos{scoreInfos}, weights).ToSlice()
		c.Assert(results, HasLen, len(scoreInfos.ToSlice()))
		c.Assert(results[0].score, Equals, 0.0)
	}
	{
		weights = []float64{1.0, 1.0, 1.0}
		results := AggregateScores([]*ScoreInfos{scoreInfos, scoreInfos}, weights).ToSlice()
		minNum := 2.0 // []*ScoreInfos{scoreInfos, scoreInfos} is less than weights
		for i, result := range results {
			c.Assert(result.score, Equals, scoreInfos.ToSlice()[i].score*minNum)
		}
	}
}
