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
	"sync"
	"time"
)

type cacheItem struct {
	key    uint64
	value  interface{}
	expire time.Time
}

// ExpireRegionCache is an expired region cache.
type ExpireRegionCache struct {
	sync.RWMutex

	items      map[uint64]cacheItem
	gcInterval time.Duration
}

// NewExpireRegionCache returns a new expired region cache.
func NewExpireRegionCache(gcInterval time.Duration) *ExpireRegionCache {
	c := &ExpireRegionCache{
		items:      make(map[uint64]cacheItem),
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

func (c *ExpireRegionCache) get(key uint64) (interface{}, bool) {
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

func (c *ExpireRegionCache) set(key uint64, value interface{}, expire time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(expire),
	}
}

func (c *ExpireRegionCache) delete(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *ExpireRegionCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *ExpireRegionCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			keys := []uint64{}
			c.RLock()
			for k := range c.items {
				keys = append(keys, k)
			}
			c.RUnlock()

			now := time.Now()
			for _, key := range keys {
				c.Lock()
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						delete(c.items, key)
					}
				}
				c.Unlock()
			}
		}
	}
}
