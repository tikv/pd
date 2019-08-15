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

package statistics

import (
	"container/heap"
	"sync"

	"github.com/montanaflynn/stats"
)

// RollingStats provides rolling statistics with specified window size.
// There are window size records for calculating.
type RollingStats struct {
	records []float64
	size    int
	count   int
}

// NewRollingStats returns a RollingStats.
func NewRollingStats(size int) *RollingStats {
	return &RollingStats{
		records: make([]float64, size),
		size:    size,
	}
}

// Add adds an element.
func (r *RollingStats) Add(n float64) {
	r.records[r.count%r.size] = n
	r.count++
}

// Median returns the median of the records.
// it can be used to filter noise.
// References: https://en.wikipedia.org/wiki/Median_filter.
func (r *RollingStats) Median() float64 {
	if r.count == 0 {
		return 0
	}
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	median, _ := stats.Median(records)
	return median
}

// TopNItem represents a single object in TopN.
type TopNItem interface {
	// ID is used to check identity.
	//
	// If two items have the same ID, then they are
	// different versions of the same object.
	ID() uint64
	// Less tests whether the current item is less than the given argument.
	Less(then TopNItem) bool
}

// indexedHeap is a heap with index.
type indexedHeap struct {
	rev   bool
	items []TopNItem
	index map[uint64]int
}

func newTopNHeap() *indexedHeap {
	return &indexedHeap{
		rev:   false,
		items: []TopNItem{},
		index: make(map[uint64]int),
	}
}

func newRevTopNHeap() *indexedHeap {
	return &indexedHeap{
		rev:   true,
		items: []TopNItem{},
		index: make(map[uint64]int),
	}
}

// Implementing heap.Interface.
func (hp *indexedHeap) Len() int {
	return len(hp.items)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Less(i, j int) bool {
	if !hp.rev {
		return hp.items[i].Less(hp.items[j])
	}
	return hp.items[j].Less(hp.items[i])
}

// Implementing heap.Interface.
func (hp *indexedHeap) Swap(i, j int) {
	lid := hp.items[i].ID()
	rid := hp.items[j].ID()
	hp.items[i], hp.items[j] = hp.items[j], hp.items[i]
	hp.index[lid] = j
	hp.index[rid] = i
}

// Implementing heap.Interface.
func (hp *indexedHeap) Push(x interface{}) {
	item := x.(TopNItem)
	hp.index[item.ID()] = hp.Len()
	hp.items = append(hp.items, item)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Pop() interface{} {
	l := hp.Len()
	item := hp.items[l-1]
	hp.items = hp.items[:l-1]
	delete(hp.index, item.ID())
	return item
}

// Top returns the top item.
func (hp *indexedHeap) Top() TopNItem {
	if hp.Len() <= 0 {
		return nil
	}
	return hp.items[0]
}

// Get returns item with the given ID.
func (hp *indexedHeap) Get(id uint64) TopNItem {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := hp.items[idx]
	return item.(TopNItem)
}

// GetAll returns all the items.
//
// Callers should not mutate the returned slice.
func (hp *indexedHeap) GetAll() []TopNItem {
	return hp.items
}

// Put inserts item or updates the old item if it exists.
func (hp *indexedHeap) Put(item TopNItem) (isNew bool) {
	idx, ok := hp.index[item.ID()]
	if !ok {
		heap.Push(hp, item)
	} else {
		hp.items[idx] = item
		heap.Fix(hp, idx)
	}
	return !ok
}

// Fix fixes the heap, returns false if there is no item has the given ID.
func (hp *indexedHeap) Fix(id uint64) (ok bool) {
	idx, ok := hp.index[id]
	if ok {
		heap.Fix(hp, idx)
	}
	return
}

// Remove deletes item by ID and returns it.
func (hp *indexedHeap) Remove(id uint64) TopNItem {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := heap.Remove(hp, idx)
	return item.(TopNItem)
}

func maxInt(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// TopN maintains the N largest items.
// NOTE: TopN is thread-safe.
type TopN struct {
	rw   sync.RWMutex
	n    int
	topn *indexedHeap
	rest *indexedHeap
}

// NewTopN returns a TopN working on the items.
func NewTopN(n int) *TopN {
	return &TopN{
		n:    maxInt(n, 1),
		topn: newTopNHeap(),
		rest: newRevTopNHeap(),
	}
}

// Len returns number of all items.
func (tn *TopN) Len() int {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topn.Len() + tn.rest.Len()
}

// GetTopN returns the item with the given ID if it's in top N.
func (tn *TopN) GetTopN(id uint64) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topn.Get(id)
}

// GetRest returns the item with the given ID if it's in the rest.
func (tn *TopN) GetRest(id uint64) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.rest.Get(id)
}

// GetTopNMin returns the min item in top N.
func (tn *TopN) GetTopNMin() TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topn.Top()
}

// GetRestMax returns the mx item in rest.
func (tn *TopN) GetRestMax() TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.rest.Top()
}

// GetAllTopN returns the top N items.
func (tn *TopN) GetAllTopN() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	topn := tn.topn.GetAll()
	res := make([]TopNItem, len(topn))
	copy(res, topn)
	return res
}

// GetAllRest returns the rest items.
func (tn *TopN) GetAllRest() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	rest := tn.rest.GetAll()
	res := make([]TopNItem, len(rest))
	copy(res, rest)
	return res
}

// Put inserts item or updates the old item if it exists.
func (tn *TopN) Put(item TopNItem) (isNew bool) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	if tn.topn.Get(item.ID()) != nil {
		isNew = false
		tn.topn.Put(item)
	} else {
		isNew = tn.rest.Put(item)
	}
	tn.maintain()
	return
}

// Fix fixes the heaps, returns false if there is no item has the given ID.
func (tn *TopN) Fix(id uint64) (ok bool) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	ok = tn.topn.Fix(id) || tn.rest.Fix(id)
	if ok {
		tn.maintain()
	}
	return
}

// Remove deletes the item by given ID and returns it.
func (tn *TopN) Remove(id uint64) TopNItem {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	item := tn.topn.Remove(id)
	if item == nil {
		return tn.rest.Remove(id)
	}
	tn.maintain()
	return item
}

func (tn *TopN) promote() {
	heap.Push(tn.topn, heap.Pop(tn.rest))
}

func (tn *TopN) demote() {
	heap.Push(tn.rest, heap.Pop(tn.topn))
}

func (tn *TopN) maintain() {
	for tn.topn.Len() < tn.n && tn.rest.Len() > 0 {
		tn.promote()
	}
	rest1 := tn.rest.Top()
	if rest1 == nil {
		return
	}
	for top1 := tn.topn.Top(); top1.Less(rest1); {
		tn.demote()
		tn.promote()
		rest1 = tn.rest.Top()
		top1 = tn.topn.Top()
	}
}
