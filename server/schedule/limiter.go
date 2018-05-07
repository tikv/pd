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

	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// Limiter is a counter that limits the number of operators.
type Limiter struct {
	sync.RWMutex
	counts map[OperatorKind]map[uint64]uint64
}

// NewLimiter creates a schedule limiter.
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[OperatorKind]map[uint64]uint64),
	}
}

// AddOperator increases the count by kind.
func (l *Limiter) AddOperator(op *Operator, region *core.RegionInfo) {
	l.Lock()
	defer l.Unlock()

	if _, ok := l.counts[op.Kind()]; !ok {
		l.counts[op.Kind()] = make(map[uint64]uint64)
	}

	for _, store := range op.InvolvedStores(region) {
		l.counts[op.Kind()][store]++
	}
}

// RemoveOperator decreases the count by kind.
func (l *Limiter) RemoveOperator(op *Operator) {
	l.Lock()
	defer l.Unlock()

	for _, store := range op.InvolvedStores(nil) {
		_, ok := l.counts[op.Kind()][store]
		if ok {
			if l.counts[op.Kind()][store] == 0 {
				log.Fatal("the limiter is already 0, no operators need to remove")
			}
			l.counts[op.Kind()][store]--
		} else {
			log.Fatalf("operator count decrease on nonexisted store %d", store)
		}
	}
}

// OperatorCount gets the max count of operators of all stores filtered by mask.
func (l *Limiter) OperatorCount(mask OperatorKind) uint64 {
	l.RLock()
	defer l.RUnlock()

	var max uint64
	counts := make(map[uint64]uint64)
	for k, stores := range l.counts {
		if k&mask != 0 {
			for storeID, count := range stores {
				counts[storeID] += count
				if max < counts[storeID] {
					max = counts[storeID]
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
			if count, ok := stores[storeID]; ok {
				total += count
			}
		}
	}
	return total
}
