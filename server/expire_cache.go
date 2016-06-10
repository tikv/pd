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
	key    string
	value  interface{}
	expire time.Time
}

// ExpireCache is an expired cache.
type ExpireCache struct {
	sync.RWMutex

	items      map[string]cacheItem
	gcInterval time.Duration
}

// NewExpireCache returns a new expired cache.
func NewExpireCache(gcInterval time.Duration) *ExpireCache {
	c := &ExpireCache{
		items:      make(map[string]cacheItem),
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

func (c *ExpireCache) get(key string) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		delete(c.items, key)
		return nil, false
	}

	return item.value, true
}

func (c *ExpireCache) set(key string, value interface{}, expire time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(expire),
	}
}

func (c *ExpireCache) delete(key string) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *ExpireCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *ExpireCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			keys := []string{}
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
