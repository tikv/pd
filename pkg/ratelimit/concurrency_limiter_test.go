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
	. "github.com/pingcap/check"
)

var _ = Suite(&testConcurrencyLimiterSuite{})

type testConcurrencyLimiterSuite struct {
}

func (s *testConcurrencyLimiterSuite) TestConcurrencyLimiter(c *C) {
	c.Parallel()

	cl := newConcurrencyLimiter(10)

	for i := 0; i < 10; i++ {
		c.Assert(cl.allow(), Equals, true)
	}
	c.Assert(cl.allow(), Equals, false)
	cl.release()
	c.Assert(cl.allow(), Equals, true)
	c.Assert(cl.getLimit(), Equals, uint64(10))
	cl.setLimit(5)
	c.Assert(cl.getLimit(), Equals, uint64(5))
	c.Assert(cl.getCurrent(), Equals, uint64(10))
	cl.release()
	c.Assert(cl.getCurrent(), Equals, uint64(9))
}
