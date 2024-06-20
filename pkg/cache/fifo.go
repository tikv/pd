// Copyright 2017 TiKV Project Authors.
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

package cache

import (
	"container/list"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

// FIFO is 'First-In-First-Out' cache.
type FIFO struct {
	syncutil.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll *list.List
}

// NewFIFO returns a new FIFO cache.
func NewFIFO(maxCount int) *FIFO {
	return &FIFO{
		maxCount: maxCount,
		ll:       list.New(),
	}
}

// Put puts an item into cache.
func (c *FIFO) Put(key uint64, value any) {
	c.Lock()
	defer c.Unlock()

	kv := &Item{Key: key, Value: value}
	c.ll.PushFront(kv)

	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.ll.Remove(c.ll.Back())
	}
}

// Remove takes the oldest item out.
func (c *FIFO) Remove() {
	c.Lock()
	defer c.Unlock()

	c.ll.Remove(c.ll.Back())
}

// Elems returns all items in cache.
func (c *FIFO) Elems() []*Item {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*Item, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		elems = append(elems, ele.Value.(*Item))
	}

	return elems
}

// FromElems returns all items that has a key greater than the specified one.
func (c *FIFO) FromElems(key uint64) []*Item {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*Item, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		kv := ele.Value.(*Item)
		if kv.Key > key {
			elems = append(elems, ele.Value.(*Item))
		}
	}

	return elems
}

// FromLastSameElems returns continuous items that have the same comparable attribute with the last one.
func (c *FIFO) FromLastSameElems(checkFunc func(any) (bool, string)) []*Item {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*Item, 0, c.ll.Len())
	var lastItem any
	for ele := c.ll.Front(); ele != nil; ele = ele.Next() {
		kv := ele.Value.(*Item)
		if lastItem == nil {
			elems = append(elems, kv)
			lastItem = kv.Value
			continue
		}
		ok1, value1 := checkFunc(kv.Value)
		ok2, value2 := checkFunc(lastItem)
		if ok1 && ok2 && value1 == value2 {
			elems = append(elems, kv)
			lastItem = kv.Value
		} else {
			break
		}
	}
	return elems
}

// Len returns current cache size.
func (c *FIFO) Len() int {
	c.RLock()
	defer c.RUnlock()

	return c.ll.Len()
}
