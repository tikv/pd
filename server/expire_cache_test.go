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
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testExpireCacheSuite{})

type testExpireCacheSuite struct {
}

func (s *testExpireCacheSuite) TestExpireCache(c *C) {
	cache := NewExpireCache(time.Second)
	cache.set("k1", 1, 1*time.Second)
	cache.set("k2", "v2", 5*time.Second)
	cache.set("k3", 3.0, 5*time.Second)

	value, ok := cache.get("k1")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 1)

	value, ok = cache.get("k2")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.get("k3")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 3)

	time.Sleep(2 * time.Second)

	value, ok = cache.get("k1")
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.get("k2")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.get("k3")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 2)

	cache.delete("k2")

	value, ok = cache.get("k2")
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.get("k3")
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 1)
}
