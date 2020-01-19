package statistics

import (
	. "github.com/pingcap/check"
	"math"
	"time"
)

var _ = Suite(&testTrendSuite{})

type testTrendSuite struct{}

type testTrendCase struct {
	nums   []uint64
	result float64
}

func newTestTrendCase(result float64, nums ...uint64) *testTrendCase {
	t := &testTrendCase{
		result: result,
	}
	for _, num := range nums {
		t.nums = append(t.nums, num)
	}
	return t
}

func (s *testTopNSuite) TestGet(c *C) {
	m := newTrendMonitor()

	testCases := make([]*testTrendCase, 0)
	testCases = append(testCases, newTestTrendCase(-0.0146875, 209, 208, 205, 205, 200))
	testCases = append(testCases, newTestTrendCase(0.01875, 230, 235, 240, 245, 250))
	testCases = append(testCases, newTestTrendCase(0.015, 227, 235, 243, 248, 250))
	testCases = append(testCases, newTestTrendCase(-0.0359375, 220, 215, 214, 213, 200))

	for _, testCase := range testCases {
		nums := make([]uint64, m.capacity)
		for i := 0; i < m.capacity; i += 10 {
			nums[i] = testCase.nums[i/10]
		}
		m.interval = time.Duration(0)
		for _, num := range nums {
			m.put(num)
		}

		result := m.getTrendSpeed()
		c.Assert(math.Abs(result-testCase.result), LessEqual, 1e-7)
		m.gc()
	}
}
