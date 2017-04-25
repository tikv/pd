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
	"container/list"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

type cacheItem struct {
	key    uint64
	value  interface{}
	expire time.Time
}

type idCache struct {
	*expireRegionCache
}

func newIDCache(interval, ttl time.Duration) *idCache {
	return &idCache{
		expireRegionCache: newExpireRegionCache(interval, ttl),
	}
}

func (c *idCache) set(id uint64) {
	c.expireRegionCache.set(id, nil)
}

func (c *idCache) get(id uint64) bool {
	_, ok := c.expireRegionCache.get(id)
	return ok
}

// expireRegionCache is an expired region cache.
type expireRegionCache struct {
	sync.RWMutex

	items      map[uint64]cacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// newExpireRegionCache returns a new expired region cache.
func newExpireRegionCache(gcInterval time.Duration, ttl time.Duration) *expireRegionCache {
	c := &expireRegionCache{
		items:      make(map[uint64]cacheItem),
		ttl:        ttl,
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

func (c *expireRegionCache) get(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		return nil, false
	}

	return item.value, true
}

func (c *expireRegionCache) set(key uint64, value interface{}) {
	c.setWithTTL(key, value, c.ttl)
}

func (c *expireRegionCache) setWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

func (c *expireRegionCache) delete(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *expireRegionCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *expireRegionCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := 0
			now := time.Now()
			c.Lock()
			for key := range c.items {
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						count++
						delete(c.items, key)
					}
				}
			}
			c.Unlock()

			log.Debugf("GC %d items", count)
		}
	}
}

type lruCache struct {
	sync.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll    *list.List
	cache map[uint64]*list.Element
}

// newLRUCache returns a new lru cache.
func newLRUCache(maxCount int) *lruCache {
	return &lruCache{
		maxCount: maxCount,
		ll:       list.New(),
		cache:    make(map[uint64]*list.Element),
	}
}

func (c *lruCache) add(key uint64, value interface{}) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*cacheItem).value = value
		return
	}

	kv := &cacheItem{key: key, value: value}
	ele := c.ll.PushFront(kv)
	c.cache[key] = ele
	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.removeOldest()
	}
}

func (c *lruCache) get(key uint64) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*cacheItem).value, true
	}

	return nil, false
}

func (c *lruCache) peek(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	if ele, ok := c.cache[key]; ok {
		return ele.Value.(*cacheItem).value, true
	}

	return nil, false
}

func (c *lruCache) remove(key uint64) {
	c.Lock()
	defer c.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
	}
}

func (c *lruCache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *lruCache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	kv := ele.Value.(*cacheItem)
	delete(c.cache, kv.key)
}

func (c *lruCache) elems() []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Front(); ele != nil; ele = ele.Next() {
		clone := *(ele.Value.(*cacheItem))
		elems = append(elems, &clone)
	}

	return elems
}

func (c *lruCache) len() int {
	c.RLock()
	defer c.RUnlock()

	return c.ll.Len()
}

type fifoCache struct {
	sync.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll *list.List
}

// newFifoCache returns a new fifo cache.
func newFifoCache(maxCount int) *fifoCache {
	return &fifoCache{
		maxCount: maxCount,
		ll:       list.New(),
	}
}

func (c *fifoCache) add(key uint64, value interface{}) {
	c.Lock()
	defer c.Unlock()

	kv := &cacheItem{key: key, value: value}
	c.ll.PushFront(kv)

	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.ll.Remove(c.ll.Back())
	}
}

func (c *fifoCache) remove() {
	c.Lock()
	defer c.Unlock()

	c.ll.Remove(c.ll.Back())
}

func (c *fifoCache) elems() []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		elems = append(elems, ele.Value.(*cacheItem))
	}

	return elems
}

func (c *fifoCache) fromElems(key uint64) []*cacheItem {
	c.RLock()
	defer c.RUnlock()

	elems := make([]*cacheItem, 0, c.ll.Len())
	for ele := c.ll.Back(); ele != nil; ele = ele.Prev() {
		kv := ele.Value.(*cacheItem)
		if kv.key > key {
			elems = append(elems, ele.Value.(*cacheItem))
		}
	}

	return elems
}

func (c *fifoCache) len() int {
	c.RLock()
	defer c.RUnlock()

	return c.ll.Len()
}
