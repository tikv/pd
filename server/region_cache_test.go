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

var _ = Suite(&testRegionCacheSuite{})

type testRegionCacheSuite struct {
}

func (s *testRegionCacheSuite) TestExpireRegionCache(c *C) {
	cache := newExpireRegionCache(time.Second, 2*time.Second)
	cache.setWithTTL(1, 1, 1*time.Second)
	cache.setWithTTL(2, "v2", 5*time.Second)
	cache.setWithTTL(3, 3.0, 5*time.Second)

	value, ok := cache.get(1)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 1)

	value, ok = cache.get(2)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 3)

	time.Sleep(2 * time.Second)

	value, ok = cache.get(1)
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.get(2)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 2)

	cache.delete(2)

	value, ok = cache.get(2)
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.count(), Equals, 1)
}

func (s *testRegionCacheSuite) TestLRUCache(c *C) {
	cache := newLRUCache(3)
	cache.add(1, "1")
	cache.add(2, "2")
	cache.add(3, "3")

	val, ok := cache.get(3)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "3")

	val, ok = cache.get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	c.Assert(cache.len(), Equals, 3)

	cache.add(4, "4")

	c.Assert(cache.len(), Equals, 3)

	val, ok = cache.get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	val, ok = cache.get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.get(4)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "4")

	c.Assert(cache.len(), Equals, 3)

	val, ok = cache.peek(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	elems := cache.elems()
	c.Assert(elems, HasLen, 3)
	c.Assert(elems[0].value, DeepEquals, "4")
	c.Assert(elems[1].value, DeepEquals, "2")
	c.Assert(elems[2].value, DeepEquals, "1")

	cache.remove(1)
	cache.remove(2)
	cache.remove(4)

	c.Assert(cache.len(), Equals, 0)

	val, ok = cache.get(1)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.get(2)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.get(4)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)
}

func (s *testRegionCacheSuite) TestFifoCache(c *C) {
	cache := newFifoCache(3)
	cache.add(1, "1")
	cache.add(2, "2")
	cache.add(3, "3")
	c.Assert(cache.len(), Equals, 3)

	cache.add(4, "4")
	c.Assert(cache.len(), Equals, 3)

	elems := cache.elems()
	c.Assert(elems, HasLen, 3)
	c.Assert(elems[0].value, DeepEquals, "2")
	c.Assert(elems[1].value, DeepEquals, "3")
	c.Assert(elems[2].value, DeepEquals, "4")

	elems = cache.fromElems(3)
	c.Assert(elems, HasLen, 1)
	c.Assert(elems[0].value, DeepEquals, "4")

	cache.remove()
	cache.remove()
	cache.remove()
	c.Assert(cache.len(), Equals, 0)
}
