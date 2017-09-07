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

// Cache is an interface for cache system.
type Cache interface {
	Put(key uint64, value interface{})
	Get(key uint64) (interface{}, bool)
	Peek(key uint64) (interface{}, bool)
	Remove(key uint64)
	Elems() []*Item
	Len() int
}

const (
	// LRUCache means use LRU cache
	LRUCache int = 1
	// ARCCache means use ARC cache
	ARCCache int = 2
)

var (
	// DefaultCache set default cache type for NewDefaultCache function
	DefaultCache = ARCCache
)

// NewCache create cache by type
func NewCache(size int, cacheType int) Cache {
	switch cacheType {
	case LRUCache:
		return NewLRU(size)
	case ARCCache:
		return NewARC(size)
	}
	panic("Unknown cache type")
}

// NewDefaultCache create cache by default type
func NewDefaultCache(size int) Cache {
	return NewCache(size, DefaultCache)
}
