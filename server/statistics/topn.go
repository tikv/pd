// Copyright 2019 PingCAP, Inc.
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
	"container/list"
	"sync"
	"time"
)

// TopNItem represents a single object in TopN.
type TopNItem interface {
	// ID is used to check identity.
	ID() uint64
	// Less tests whether the current item is less than the given argument.
	Less(then TopNItem) bool
}

// TopN maintains the N largest items.
type TopN struct {
	rw     sync.RWMutex
	n      int
	topn   *indexedHeap
	rest   *indexedHeap
	ttlLst *ttlList
}

// NewTopN returns a TopN with given TTL.
func NewTopN(n int, ttl time.Duration) *TopN {
	return &TopN{
		n:      maxInt(n, 1),
		topn:   newTopNHeap(n),
		rest:   newRevTopNHeap(n),
		ttlLst: newTTLList(ttl),
	}
}

// Len returns number of all items.
func (tn *TopN) Len() int {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.ttlLst.Len()
}

// GetTopNMin returns the min item in top N.
func (tn *TopN) GetTopNMin() TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topn.Top()
}

// GetAllTopN returns the top N items.
func (tn *TopN) GetAllTopN() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topn.GetAll()
}

// GetAll returns all items.
func (tn *TopN) GetAll() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	topn := tn.topn.GetAll()
	return append(topn, tn.rest.GetAll()...)
}

// Get returns the item with given id, nil if there is no such item.
func (tn *TopN) Get(id uint64) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	if item := tn.topn.Get(id); item != nil {
		return item
	}
	return tn.rest.Get(id)
}

// Put inserts item or updates the old item if it exists.
func (tn *TopN) Put(item TopNItem) (isUpdate bool) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	if tn.topn.Get(item.ID()) != nil {
		isUpdate = true
		tn.topn.Put(item)
	} else {
		isUpdate = tn.rest.Put(item)
	}
	tn.ttlLst.Put(item.ID())
	tn.maintain()
	return
}

func (tn *TopN) removeItemLocked(id uint64) TopNItem {
	item := tn.topn.Remove(id)
	if item == nil {
		item = tn.rest.Remove(id)
	}
	return item
}

// RemoveExpired deletes all expired items.
func (tn *TopN) RemoveExpired() {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	tn.maintain()
}

// Remove deletes the item by given ID and returns it.
func (tn *TopN) Remove(id uint64) TopNItem {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	item := tn.removeItemLocked(id)
	_ = tn.ttlLst.Remove(id)
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
	for _, id := range tn.ttlLst.takeExpired() {
		_ = tn.removeItemLocked(id)
	}
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

// indexedHeap is a heap with index.
type indexedHeap struct {
	rev   bool
	items []TopNItem
	index map[uint64]int
}

func newTopNHeap(hint int) *indexedHeap {
	return &indexedHeap{
		rev:   false,
		items: make([]TopNItem, 0, hint),
		index: map[uint64]int{},
	}
}

func newRevTopNHeap(hint int) *indexedHeap {
	return &indexedHeap{
		rev:   true,
		items: make([]TopNItem, 0, hint),
		index: map[uint64]int{},
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
func (hp *indexedHeap) GetAll() []TopNItem {
	all := make([]TopNItem, len(hp.items))
	copy(all, hp.items)
	return all
}

// Put inserts item or updates the old item if it exists.
func (hp *indexedHeap) Put(item TopNItem) (isUpdate bool) {
	if idx, ok := hp.index[item.ID()]; ok {
		hp.items[idx] = item
		heap.Fix(hp, idx)
		return true
	}
	heap.Push(hp, item)
	return false
}

// Remove deletes item by ID and returns it.
func (hp *indexedHeap) Remove(id uint64) TopNItem {
	if idx, ok := hp.index[id]; ok {
		item := heap.Remove(hp, idx)
		return item.(TopNItem)
	}
	return nil
}

type ttlItem struct {
	id     uint64
	expire time.Time
}

type ttlList struct {
	ttl   time.Duration
	lst   *list.List
	index map[uint64]*list.Element
}

func newTTLList(ttl time.Duration) *ttlList {
	return &ttlList{
		ttl:   ttl,
		lst:   list.New(),
		index: map[uint64]*list.Element{},
	}
}

func (tl *ttlList) Len() int {
	return tl.lst.Len()
}

func (tl *ttlList) takeExpired() []uint64 {
	expired := []uint64{}
	now := time.Now()
	for ele := tl.lst.Front(); ele != nil; ele = tl.lst.Front() {
		item := ele.Value.(ttlItem)
		if item.expire.After(now) {
			break
		}
		expired = append(expired, item.id)
		_ = tl.lst.Remove(ele)
		delete(tl.index, item.id)
	}
	return expired
}

func (tl *ttlList) Put(id uint64) (isUpdate bool) {
	item := ttlItem{id: id}
	if ele, ok := tl.index[id]; ok {
		isUpdate = true
		_ = tl.lst.Remove(ele)
	}
	item.expire = time.Now().Add(tl.ttl)
	tl.index[id] = tl.lst.PushBack(item)
	return
}

func (tl *ttlList) Remove(id uint64) (removed bool) {
	if ele, ok := tl.index[id]; ok {
		_ = tl.lst.Remove(ele)
		delete(tl.index, id)
		removed = true
	}
	return
}
