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

package schedule

import (
	"sync"
	"time"

	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
)

// Limiter is a counter that limits the number of operators
type Limiter struct {
	sync.RWMutex
	counts map[OperatorKind]map[uint64]*cache.TTLUint64
}

// NewLimiter creates a schedule limiter
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[OperatorKind]map[uint64]*cache.TTLUint64),
	}
}

var (
	limiterCacheGCInterval = time.Second * 5
	limiterCacheTTL        = time.Minute * 1
)

// UpdateCounts updates resouce counts using current pending operators.
func (l *Limiter) UpdateCounts(op *Operator, region *core.RegionInfo) {
	l.Lock()
	defer l.Unlock()

	if _, ok := l.counts[op.Kind()]; !ok {
		l.counts[op.Kind()] = make(map[uint64]*cache.TTLUint64)
	}

	for _, store := range op.InvolvedStores(region) {
		ttl, ok := l.counts[op.Kind()][store]
		if !ok {
			ttl = cache.NewIDTTL(limiterCacheGCInterval, limiterCacheTTL)
			l.counts[op.Kind()][store] = ttl
		}
		ttl.Put(op.RegionID())
	}
}

// Remove deletes related items from all involved stores cache.
func (l *Limiter) Remove(op *Operator, region *core.RegionInfo) {
	l.Lock()
	defer l.Unlock()

	for _, store := range op.InvolvedStores(region) {
		ttl, ok := l.counts[op.Kind()][store]
		if ok {
			ttl.Remove(op.RegionID())
			if ttl.Len() == 0 {
				delete(l.counts[op.Kind()], store)
			}
		}
	}
}

// OperatorCount gets the max count of operators of all involved stores filtered by mask.
func (l *Limiter) OperatorCount(mask OperatorKind) uint64 {
	l.RLock()
	defer l.RUnlock()

	var max uint64
	for k, stores := range l.counts {
		if k&mask != 0 {
			for _, store := range stores {
				if max < uint64(store.Len()) {
					max = uint64(store.Len())
				}
			}
		}
	}
	return max
}

// StoreOperatorCount gets the count of operators for specific store filtered by mask.
func (l *Limiter) StoreOperatorCount(mask OperatorKind, storeID uint64) uint64 {
	l.RLock()
	defer l.RUnlock()

	var total uint64
	for k, stores := range l.counts {
		if k&mask != 0 {
			if store, ok := stores[storeID]; ok {
				total += uint64(store.Len())
			}
		}
	}
	return total
}
