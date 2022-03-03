// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ratelimit

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRateLimiterSuite{})

type testRateLimiterSuite struct {
}

func (s *testRateLimiterSuite) TestRateLimiter(c *C) {
	c.Parallel()

	limiter := NewRateLimiter(100, 100)

	c.Assert(limiter.Available(1), Equals, true)

	c.Assert(limiter.AllowN(50), Equals, true)
	c.Assert(limiter.Available(50), Equals, true)
	c.Assert(limiter.Available(100), Equals, false)
	c.Assert(limiter.Available(50), Equals, true)
	c.Assert(limiter.AllowN(50), Equals, true)
	c.Assert(limiter.Available(50), Equals, false)
	time.Sleep(time.Second)
	c.Assert(limiter.Available(1), Equals, true)
	c.Assert(limiter.AllowN(99), Equals, true)
	c.Assert(limiter.Allow(), Equals, true)
	c.Assert(limiter.Available(1), Equals, false)
}
