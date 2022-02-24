// Copyright 2019 TiKV Project Authors.
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

package statistics

import (
	"container/heap"
	"container/list"
	"fmt"
	"sync"
	"time"
)

// TopNItem represents a single object in TopN.
type TopNItem interface {
	// ID is used to check identity.
	ID() uint64
	// Less tests whether the current item is less than the given argument in the `k`th dimension.
	Less(k int, than TopNItem) bool
}

// TopN maintains the N largest items of multiple dimensions.
type TopN struct {
	rw     sync.RWMutex
	topns  []*singleTopN
	ttlLst *ttlList
}

// NewTopN returns a k-dimensional TopN with given TTL.
// NOTE: panic if k <= 0 or n <= 0.
func NewTopN(k, n int, ttl time.Duration) *TopN {
	if k <= 0 || n <= 0 {
		panic(fmt.Sprintf("invalid arguments for NewTopN: k = %d, n = %d", k, n))
	}
	ret := &TopN{
		topns:  make([]*singleTopN, k),
		ttlLst: newTTLList(ttl),
	}
	for i := 0; i < k; i++ {
		ret.topns[i] = newSingleTopN(i, n)
	}
	return ret
}

// Len returns number of all items.
func (tn *TopN) Len() int {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.ttlLst.Len()
}

// GetTopNMin returns the min item in top N of the `k`th dimension.
func (tn *TopN) GetTopNMin(k int) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[k].GetTopNMin()
}

// GetAllTopN returns the top N items of the `k`th dimension.
func (tn *TopN) GetAllTopN(k int) []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[k].GetAllTopN()
}

// GetAll returns all items.
func (tn *TopN) GetAll() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[0].GetAll()
}

// Get returns the item with given id, nil if there is no such item.
func (tn *TopN) Get(id uint64) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[0].Get(id)
}

// Put inserts item or updates the old item if it exists.
func (tn *TopN) Put(item TopNItem) (isUpdate bool) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	for _, stn := range tn.topns {
		isUpdate = stn.Put(item)
	}
	tn.ttlLst.Put(item.ID())
	tn.maintain()
	return
}

// RemoveExpired deletes all expired items.
func (tn *TopN) RemoveExpired() {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	tn.maintain()
}

// Remove deletes the item by given ID and returns it.
func (tn *TopN) Remove(id uint64) (item TopNItem) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	for _, stn := range tn.topns {
		item = stn.Remove(id)
	}
	_ = tn.ttlLst.Remove(id)
	tn.maintain()
	return
}

func (tn *TopN) maintain() {
	for _, id := range tn.ttlLst.TakeExpired() {
		for _, stn := range tn.topns {
			stn.Remove(id)
		}
	}
}

type singleTopN struct {
	k    int
	n    int
	topn *indexedHeap
	rest *indexedHeap
}

func newSingleTopN(k, n int) *singleTopN {
	return &singleTopN{
		k:    k,
		n:    n,
		topn: newTopNHeap(k, n),
		rest: newRevTopNHeap(k, n),
	}
}

func (stn *singleTopN) Len() int {
	return stn.topn.Len() + stn.rest.Len()
}

func (stn *singleTopN) GetTopNMin() TopNItem {
	return stn.topn.Top()
}

func (stn *singleTopN) GetAllTopN() []TopNItem {
	return stn.topn.GetAll()
}

func (stn *singleTopN) GetAll() []TopNItem {
	topn := stn.topn.GetAll()
	return append(topn, stn.rest.GetAll()...)
}

func (stn *singleTopN) Get(id uint64) TopNItem {
	if item := stn.topn.Get(id); item != nil {
		return item
	}
	return stn.rest.Get(id)
}

func (stn *singleTopN) Put(item TopNItem) (isUpdate bool) {
	if stn.topn.Get(item.ID()) != nil {
		isUpdate = true
		stn.topn.Put(item)
	} else {
		isUpdate = stn.rest.Put(item)
	}
	stn.maintain()
	return
}

func (stn *singleTopN) Remove(id uint64) TopNItem {
	item := stn.topn.Remove(id)
	if item == nil {
		item = stn.rest.Remove(id)
	}
	stn.maintain()
	return item
}

func (stn *singleTopN) promote() {
	heap.Push(stn.topn, heap.Pop(stn.rest))
}

func (stn *singleTopN) demote() {
	heap.Push(stn.rest, heap.Pop(stn.topn))
}

func (stn *singleTopN) maintain() {
	for stn.topn.Len() < stn.n && stn.rest.Len() > 0 {
		stn.promote()
	}
	rest1 := stn.rest.Top()
	if rest1 == nil {
		return
	}
	for topn1 := stn.topn.Top(); topn1.Less(stn.k, rest1); {
		stn.demote()
		stn.promote()
		rest1 = stn.rest.Top()
		topn1 = stn.topn.Top()
	}
}

// indexedHeap is a heap with index.
type indexedHeap struct {
	k     int
	rev   bool
	items []TopNItem
	index map[uint64]int
}

func newTopNHeap(k, hint int) *indexedHeap {
	return &indexedHeap{
		k:     k,
		rev:   false,
		items: make([]TopNItem, 0, hint),
		index: map[uint64]int{},
	}
}

func newRevTopNHeap(k, hint int) *indexedHeap {
	return &indexedHeap{
		k:     k,
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
		return hp.items[i].Less(hp.k, hp.items[j])
	}
	return hp.items[j].Less(hp.k, hp.items[i])
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
	return item
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

func (tl *ttlList) TakeExpired() []uint64 {
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
