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

package cache

import (
	"sync"
)

// ARC is a thread-safe fixed size Adaptive Replacement Cache.
// ARC is an enhancement over the standard LRU cache in that tracks both
// frequency and recency of use. This avoids a burst in access to new
// entries from evicting the frequently used older entries. It adds some
// additional tracking overhead to a standard LRU cache, computationally
// it is roughly 2x the cost, and the extra memory overhead is linear
// with the size of the cache. ARC has been patented by IBM, but is
// similar to the TwoQueueCache (2Q) which requires setting parameters.
type ARC struct {
	size int // Size is the total capacity of the cache
	p    int // P is the dynamic preference towards T1 or T2

	t1 *LRU // T1 is the LRU for recently accessed items
	b1 *LRU // B1 is the LRU for evictions from T1

	t2 *LRU // T2 is the LRU for frequently accessed items
	b2 *LRU // B2 is the LRU for evictions from T2

	lock sync.RWMutex
}

// NewARC returns a new ARC cache
func NewARC(size int) *ARC {
	b1 := NewLRU(size)
	b2 := NewLRU(size)
	t1 := NewLRU(size)
	t2 := NewLRU(size)

	return &ARC{
		size: size,
		p:    0,
		t1:   t1,
		t2:   t2,
		b1:   b1,
		b2:   b2,
	}
}

// Put puts an item to cache.
func (c *ARC) Put(key uint64, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if the value is contained in T1 (recent), and potentially
	// promote it to frequent T2
	if c.t1.contains(key) {
		c.t1.remove(key)
		c.t2.put(key, value)
		return
	}

	// Check if the value is already in T2 (frequent) and update it
	if c.t2.contains(key) {
		c.t2.put(key, value)
		return
	}

	// Check if this value was recently evicted as part of the
	// recently used list
	if c.b1.contains(key) {
		// T1 set is too small, increase P appropriately
		delta := 1
		b1Size := c.b1.size()
		b2Size := c.b2.size()
		if b2Size > b1Size {
			delta = b2Size / b1Size
		}
		if c.p+delta >= c.size {
			c.p = c.size
		} else {
			c.p += delta
		}

		// Potentially need to make room in the cache
		if c.t1.size()+c.t2.size() >= c.size {
			c.replace(false)
		}
		// Remove from B1
		c.b1.remove(key)

		// Put the key to the frequently used list
		c.t2.put(key, value)
		return
	}

	// Check if this value was recently evicted as part of the
	// frequently used list
	if c.b2.contains(key) {
		// T2 set is too small, decrease P appropriately
		delta := 1
		b1Size := c.b1.size()
		b2Size := c.b2.size()
		if b1Size > b2Size {
			delta = b1Size / b2Size
		}
		if delta >= c.p {
			c.p = 0
		} else {
			c.p -= delta
		}

		// Potentially need to make room in the cache
		if c.t1.size()+c.t2.size() >= c.size {
			c.replace(true)
		}

		// Remove from B2
		c.b2.remove(key)

		// Put the key to the frequntly used list
		c.t2.put(key, value)
		return
	}

	// Potentially need to make room in the cache
	if c.t1.size()+c.t2.size() >= c.size {
		c.replace(false)
	}

	// Keep the size of the ghost buffers trim
	if c.b1.size() > c.size-c.p {
		c.b1.removeOldest()
	}

	if c.b2.size() > c.p {
		c.b2.removeOldest()
	}

	// Put to the recently seen list
	c.t1.put(key, value)
	return
}

// replace is used to adaptively evict from either T1 or T2
// based on the current learned value of P
func (c *ARC) replace(b2ContainsKey bool) {
	t1Size := c.t1.size()
	if t1Size > 0 && (t1Size > c.p || (t1Size == c.p && b2ContainsKey)) {
		k, _, ok := c.t1.getAndRemoveOldest()
		if ok {
			c.b1.put(k, nil)
		}
	} else {
		k, _, ok := c.t2.getAndRemoveOldest()
		if ok {
			c.b2.put(k, nil)
		}
	}
}

// Get retrives an item from cache.
func (c *ARC) Get(key uint64) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Ff the value is contained in T1 (recent), then
	// promote it to T2 (frequent)
	if val, ok := c.t1.peek(key); ok {
		c.t1.remove(key)
		c.t2.put(key, val)
		return val, ok
	}

	// Check if the value is contained in T2 (frequent)
	if val, ok := c.t2.get(key); ok {
		return val, ok
	}

	// No hit
	return nil, false
}

// Peek reads an item from cache. The action is no considerd 'Use'.
func (c *ARC) Peek(key uint64) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.t1.peek(key); ok {
		return val, ok
	}
	return c.t2.peek(key)
}

// Remove eliminates an item from cache.
func (c *ARC) Remove(key uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.t1.checkAndRemove(key) {
		return
	}
	if c.t2.checkAndRemove(key) {
		return
	}
	if c.b1.checkAndRemove(key) {
		return
	}
	if c.b2.checkAndRemove(key) {
		return
	}
}

// Len returns current cache size.
func (c *ARC) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.t1.size() + c.t2.size()
}

// Elems return all items in cache.
func (c *ARC) Elems() []*Item {
	c.lock.RLock()
	defer c.lock.RUnlock()

	elems := make([]*Item, 0, c.t1.size()+c.t2.size())
	elems = append(elems, c.t1.elems()...)
	elems = append(elems, c.t2.elems()...)
	return elems
}
