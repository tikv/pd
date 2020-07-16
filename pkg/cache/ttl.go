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
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ttlCacheItem struct {
	value  interface{}
	expire time.Time
}

// TTL is a cache that assigns TTL(Time-To-Live) for each items.
type ttl struct {
	sync.RWMutex
	ctx context.Context

	items      map[interface{}]ttlCacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// NewTTL returns a new TTL cache.
func newTTL(ctx context.Context, gcInterval time.Duration, duration time.Duration) *ttl {
	c := &ttl{
		ctx:        ctx,
		items:      make(map[interface{}]ttlCacheItem),
		ttl:        duration,
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

// Put puts an item into cache.
func (c *ttl) put(key interface{}, value interface{}) {
	c.putWithTTL(key, value, c.ttl)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *ttl) putWithTTL(key interface{}, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = ttlCacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

// Get retrives an item from cache.
func (c *ttl) get(key interface{}) (interface{}, bool) {
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

// GetKeys returns all keys that are not expired.
func (c *ttl) getKeys() []interface{} {
	c.RLock()
	defer c.RUnlock()

	var keys []interface{}

	now := time.Now()
	for key, item := range c.items {
		if item.expire.After(now) {
			keys = append(keys, key)
		}
	}
	return keys
}

// Remove eliminates an item from cache.
func (c *ttl) remove(key interface{}) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

// Len returns current cache size.
func (c *ttl) Len() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

// Clear removes all items in the ttl cache.
func (c *ttl) Clear() {
	c.Lock()
	defer c.Unlock()

	for k := range c.items {
		delete(c.items, k)
	}
}

func (c *ttl) doGC() {
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
			log.Debug("TTL GC items", zap.Int("count", count))
		case <-c.ctx.Done():
			return
		}
	}
}

// TTLUint64 is simple TTL saves only uint64s.
type TTLUint64 struct {
	*ttl
}

// NewIDTTL creates a new TTLUint64 cache.
func NewIDTTL(ctx context.Context, gcInterval, ttl time.Duration) *TTLUint64 {
	return &TTLUint64{
		ttl: newTTL(ctx, gcInterval, ttl),
	}
}

// Get return the value by key id
func (c *TTLUint64) Get(id uint64) (interface{}, bool) {
	return c.ttl.get(id)
}

// Put saves an ID in cache.
func (c *TTLUint64) Put(id uint64, value interface{}) {
	c.ttl.put(id, value)
}

// GetAllID returns all ids.
func (c *TTLUint64) GetAllID() []uint64 {
	keys := c.ttl.getKeys()
	var ids []uint64
	for _, key := range keys {
		id, ok := key.(uint64)
		if ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// Exists checks if an ID exists in cache.
func (c *TTLUint64) Exists(id uint64) bool {
	_, ok := c.ttl.get(id)
	return ok
}

// Remove remove key
func (c *TTLUint64) Remove(key uint64) {
	c.ttl.remove(key)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *TTLUint64) PutWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.ttl.putWithTTL(key, value, ttl)
}

// TTLString is simple TTL saves key string and value.
type TTLString struct {
	*ttl
}

// NewStringTTL creates a new TTLString cache.
func NewStringTTL(ctx context.Context, gcInterval, ttl time.Duration) *TTLString {
	return &TTLString{
		ttl: newTTL(ctx, gcInterval, ttl),
	}
}

// Put put the string key with the value
func (c *TTLString) Put(key string, value interface{}) {
	c.ttl.put(key, value)
}

// PopOneKeyValue pop one key/value that is not expired
func (c *TTLString) PopOneKeyValue() (string, interface{}) {
	c.Lock()
	defer c.Unlock()
	now := time.Now()
	for k, item := range c.items {
		key, ok := k.(string)
		if ok && item.expire.After(now) {
			value := item.value
			delete(c.items, k)
			return key, value
		}
	}
	return "", nil
}
