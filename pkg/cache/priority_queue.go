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
	"github.com/tikv/pd/pkg/btree"
)

// defaultDegree default btree degree, the depth is h<log(degree)(capacity+1)/2
const defaultDegree = 4

// PriorityQueue queue has priority  and preempt
type PriorityQueue struct {
	items    map[uint64]*Entry
	btree    *btree.BTree
	capacity int
}

// NewPriorityQueue construct of priority queue
func NewPriorityQueue(capacity int) *PriorityQueue {
	return &PriorityQueue{
		items:    make(map[uint64]*Entry),
		btree:    btree.New(defaultDegree),
		capacity: capacity,
	}
}

// PriorityQueueItem avoid convert cost
type PriorityQueueItem interface {
	ID() uint64
}

// Put put value with priority into queue
func (pq *PriorityQueue) Put(priority int, value PriorityQueueItem) bool {
	id := value.ID()
	entry, ok := pq.items[id]
	if !ok {
		entry = &Entry{Priority: priority, Value: value}
		if pq.Len() >= pq.capacity {
			min := pq.btree.Min()
			// avoid to capacity equal 0
			if min == nil || !min.Less(entry) {
				return false
			}
			pq.Remove(min.(*Entry).Value.ID())
		}
	} else if entry.Priority != priority { // delete before update
		pq.btree.Delete(entry)
		entry.Priority = priority
	}

	pq.btree.ReplaceOrInsert(entry)
	pq.items[id] = entry
	return true
}

// Get find entry by id from queue
func (pq *PriorityQueue) Get(id uint64) *Entry {
	return pq.items[id]
}

// Peek return the highest priority entry
func (pq *PriorityQueue) Peek() *Entry {
	if max, ok := pq.btree.Max().(*Entry); ok {
		return max
	}
	return nil
}

// Tail return the lowest priority entry
func (pq *PriorityQueue) Tail() *Entry {
	if min, ok := pq.btree.Min().(*Entry); ok {
		return min
	}
	return nil
}

// Elems return all elements in queue
func (pq *PriorityQueue) Elems() []*Entry {
	rs := make([]*Entry, pq.Len())
	count := 0
	pq.btree.Descend(func(i btree.Item) bool {
		rs[count] = i.(*Entry)
		count++
		return true
	})
	return rs
}

// Remove remove value from queue
func (pq *PriorityQueue) Remove(id uint64) {
	if v, ok := pq.items[id]; ok {
		pq.btree.Delete(v)
		delete(pq.items, id)
	}
}

// Len return queue size
func (pq *PriorityQueue) Len() int {
	return pq.btree.Len()
}

// Entry a pair of region and it's priority
type Entry struct {
	Priority int
	Value    PriorityQueueItem
}

// Less return true if the entry has smaller priority
func (r *Entry) Less(other btree.Item) bool {
	left := r.Priority
	right := other.(*Entry).Priority
	return left > right
}
