// Copyright 2018 PingCAP, Inc.
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
	"container/list"
)

const (
	probationSegment int = iota
	protectedSegment
)

const (
	protectedRatio = 0.8
)

// SLRUItem is the cache entry.
type SLRUItem struct {
	Key    uint64
	Value  interface{}
	status int
}

// SLRU is a segmented LRU.
type SLRU struct {
	probationCap int
	protectedCap int

	probation *list.List
	protected *list.List
	cache     map[uint64]*list.Element
}

func newSLRU(cap int) *SLRU {
	probationCap := int(float64(cap) * (1 - protectedRatio))
	if probationCap < 1 {
		probationCap = 1
	}
	return &SLRU{
		protectedCap: cap - probationCap,
		probationCap: probationCap,
		protected:    list.New(),
		probation:    list.New(),
		cache:        make(map[uint64]*list.Element),
	}
}

// Put puts an item into cache.
func (c *SLRU) Put(key uint64, value interface{}) {
	elem, ok := c.cache[key]
	if ok {
		item := elem.Value.(*SLRUItem)
		item.Value = value
		c.update(elem)
		return
	}

	newItem := &SLRUItem{Key: key, Value: value, status: probationSegment}
	if c.probation.Len() < c.probationCap || c.Len() < c.probationCap+c.protectedCap {
		c.cache[key] = c.probation.PushFront(newItem)
		return
	}

	back := c.probation.Back()
	if back == nil {
		return
	}

	item := back.Value.(*SLRUItem)
	delete(c.cache, item.Key)
	item = newItem
	c.cache[item.Key] = back
	c.probation.MoveToFront(back)
}

// Get retrives an item from cache.
func (c *SLRU) Get(key uint64) (interface{}, bool) {
	elem := c.cache[key]
	if elem == nil {
		return nil, false
	}

	item := elem.Value.(*SLRUItem)
	if item.status == protectedSegment {
		c.protected.MoveToFront(elem)
		return elem.Value.(*SLRUItem).Value, true
	}

	if c.protected.Len() < c.protectedCap {
		c.probation.Remove(elem)
		item.status = protectedSegment
		c.cache[key] = c.protected.PushFront(item)
		return elem.Value.(*SLRUItem).Value, true
	}

	back := c.protected.Back()
	backItem := back.Value.(*SLRUItem)
	*backItem, *item = *item, *backItem

	item.status = probationSegment
	backItem.status = protectedSegment

	c.cache[key] = back
	c.cache[item.Key] = elem
	c.protected.MoveToFront(back)
	c.probation.MoveToFront(elem)
	return back.Value.(*SLRUItem).Value, true
}

func (c *SLRU) update(elem *list.Element) {
	item := elem.Value.(*SLRUItem)
	if item.status == protectedSegment {
		c.protected.MoveToFront(elem)
	}

	if c.protected.Len() < c.protectedCap {
		c.probation.Remove(elem)
		item.status = protectedSegment
		c.cache[item.Key] = c.protected.PushFront(item)
	}

	back := c.protected.Back()
	backItem := back.Value.(*SLRUItem)
	*backItem, *item = *item, *backItem

	item.status = probationSegment
	backItem.status = protectedSegment

	c.cache[backItem.Key] = back
	c.cache[item.Key] = elem
	c.protected.MoveToFront(back)
	c.probation.MoveToFront(elem)
}

// Peek reads an item from cache. The action is no considered 'Use'.
func (c *SLRU) Peek(key uint64) (interface{}, bool) {
	if elem, ok := c.cache[key]; ok {
		return elem.Value.(*SLRUItem).Value, true
	}
	return nil, false
}

// Remove eliminates an item from cache.
func (c *SLRU) Remove(key uint64) {
	c.remove(key)
}

func (c *SLRU) remove(key uint64) bool {
	if elem, ok := c.cache[key]; ok {
		c.removeElement(elem)
		return ok
	}
	return false
}

func (c *SLRU) removeElement(elem *list.Element) {
	item := elem.Value.(*SLRUItem)
	if item.status == protectedSegment {
		c.protected.Remove(elem)
	} else {
		c.probation.Remove(elem)
	}
	delete(c.cache, item.Key)
}

// Len returns current cache size.
func (c *SLRU) Len() int {
	return c.protected.Len() + c.probation.Len()
}

// Elems return all items in cache.
func (c *SLRU) Elems() []*Item {
	elems := make([]*Item, 0, c.Len())
	for elem := c.protected.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*SLRUItem)
		elems = append(elems, &Item{
			Key:   item.Key,
			Value: item.Value,
		})
	}
	for elem := c.probation.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*SLRUItem)
		elems = append(elems, &Item{
			Key:   item.Key,
			Value: item.Value,
		})
	}
	return elems
}

func (c *SLRU) victim() *SLRUItem {
	if c.Len() < c.probationCap+c.protectedCap {
		return nil
	}

	v := c.probation.Back()
	if v == nil {
		return nil
	}
	return v.Value.(*SLRUItem)
}
