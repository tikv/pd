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
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type ttlCacheItem struct {
	value  any
	expire time.Time
}

// ttlCache is a cache that assigns TTL (Time-To-Live) for each items.
type ttlCache struct {
	syncutil.RWMutex
	ctx context.Context

	items      map[any]ttlCacheItem
	ttl        time.Duration
	gcInterval time.Duration
	// isGCRunning is used to avoid running GC multiple times.
	isGCRunning atomic.Bool
}

// NewTTL returns a new TTL cache.
func newTTL(ctx context.Context, gcInterval time.Duration, duration time.Duration) *ttlCache {
	c := &ttlCache{
		ctx:         ctx,
		items:       make(map[any]ttlCacheItem),
		ttl:         duration,
		gcInterval:  gcInterval,
		isGCRunning: atomic.Bool{},
	}
	return c
}

// Put puts an item into cache.
func (c *ttlCache) put(key any, value any) {
	c.putWithTTL(key, value, c.ttl)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *ttlCache) putWithTTL(key any, value any, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()
	if len(c.items) == 0 && c.isGCRunning.CompareAndSwap(false, true) {
		go c.doGC()
	}
	c.items[key] = ttlCacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

// Get retrieves an item from cache.
func (c *ttlCache) get(key any) (any, bool) {
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
func (c *ttlCache) getKeys() []any {
	c.RLock()
	defer c.RUnlock()

	var keys []any

	now := time.Now()
	for key, item := range c.items {
		if item.expire.After(now) {
			keys = append(keys, key)
		}
	}
	return keys
}

// Remove eliminates an item from cache.
func (c *ttlCache) remove(key any) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

// pop one key/value that is not expired. If boolean is false, it means that it didn't find the valid one.
func (c *ttlCache) pop() (key, value any, exist bool) {
	c.Lock()
	defer c.Unlock()
	now := time.Now()
	for k, item := range c.items {
		if item.expire.After(now) {
			value := item.value
			delete(c.items, k)
			return k, value, true
		}
	}
	return nil, nil, false
}

// Len returns current cache size.
func (c *ttlCache) Len() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

// Clear removes all items in the ttl cache.
func (c *ttlCache) Clear() {
	c.Lock()
	defer c.Unlock()

	for k := range c.items {
		delete(c.items, k)
	}
}

func (c *ttlCache) doGC() {
	defer logutil.LogPanic()
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
			if len(c.items) == 0 && c.isGCRunning.CompareAndSwap(true, false) {
				c.Unlock()
				log.Debug("TTL GC items is empty exit")
				return
			}
			c.Unlock()
			log.Debug("TTL GC items", zap.Int("count", count))
		case <-c.ctx.Done():
			return
		}
	}
}

// UpdateTTL updates the TTL for the cache.
func (c *ttlCache) UpdateTTL(duration time.Duration) {
	c.Lock()
	defer c.Unlock()
	if c.ttl == duration {
		return
	}

	for key := range c.items {
		c.items[key] = ttlCacheItem{
			value:  c.items[key].value,
			expire: time.Now().Add(duration),
		}
	}
	c.ttl = duration
}

// TTLUint64 is simple TTL saves only uint64s.
type TTLUint64 struct {
	*ttlCache
}

// NewIDTTL creates a new TTLUint64 cache.
func NewIDTTL(ctx context.Context, gcInterval, ttl time.Duration) *TTLUint64 {
	return &TTLUint64{
		ttlCache: newTTL(ctx, gcInterval, ttl),
	}
}

// Get return the value by key id
func (c *TTLUint64) Get(id uint64) (any, bool) {
	return c.get(id)
}

// Put saves an ID in cache.
func (c *TTLUint64) Put(id uint64, value any) {
	c.put(id, value)
}

// GetAllID returns all ids.
func (c *TTLUint64) GetAllID() []uint64 {
	keys := c.getKeys()
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
	_, ok := c.get(id)
	return ok
}

// Remove remove key
func (c *TTLUint64) Remove(key uint64) {
	c.remove(key)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *TTLUint64) PutWithTTL(key uint64, value any, ttl time.Duration) {
	c.putWithTTL(key, value, ttl)
}

// TTLString is simple TTL saves key string and value.
type TTLString struct {
	*ttlCache
}

// NewStringTTL creates a new TTLString cache.
func NewStringTTL(ctx context.Context, gcInterval, ttl time.Duration) *TTLString {
	return &TTLString{
		ttlCache: newTTL(ctx, gcInterval, ttl),
	}
}

// Put put the string key with the value
func (c *TTLString) Put(key string, value any) {
	c.put(key, value)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *TTLString) PutWithTTL(key string, value any, ttl time.Duration) {
	c.putWithTTL(key, value, ttl)
}

// Pop one key/value that is not expired
func (c *TTLString) Pop() (string, any, bool) {
	k, v, success := c.pop()
	if !success {
		return "", nil, false
	}
	key, ok := k.(string)
	if !ok {
		return "", nil, false
	}
	return key, v, true
}

// Get return the value by key id
func (c *TTLString) Get(id string) (any, bool) {
	return c.get(id)
}

// GetAllID returns all key ids
func (c *TTLString) GetAllID() []string {
	keys := c.getKeys()
	var ids []string
	for _, key := range keys {
		id, ok := key.(string)
		if ok {
			ids = append(ids, id)
		}
	}
	return ids
}
