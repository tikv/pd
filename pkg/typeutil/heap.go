// copyright 2017 pingcap, inc.
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
//
//     http://www.apache.org/licenses/license-2.0
//
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// see the license for the specific language governing permissions and
// limitations under the license.

package typeutil

import (
	"container/heap"
	"sort"
	"sync"
)

// ItemNode is one node for priority queue
type ItemNode struct {
	Key      interface{}
	Value    interface{}
	priority uint64
	index    int
}

type itemSlice struct {
	items    []*ItemNode
	itemsMap map[interface{}]*ItemNode
}

func (s itemSlice) Len() int { return len(s.items) }

func (s itemSlice) Less(i, j int) bool {
	return s.items[i].priority < s.items[j].priority
}

func (s itemSlice) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
	s.items[i].index = i
	s.items[j].index = j
	if s.itemsMap != nil {
		s.itemsMap[s.items[i].Key] = s.items[i]
		s.itemsMap[s.items[j].Key] = s.items[j]
	}
}

func (s *itemSlice) Push(x interface{}) {
	n := len(s.items)
	item := x.(*ItemNode)
	item.index = n
	s.items = append(s.items, item)
	s.itemsMap[item.Key] = item
}

func (s *itemSlice) Pop() interface{} {
	old := s.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	delete(s.itemsMap, item.Key)
	s.items = old[0 : n-1]
	return item
}

// update modifies the priority and value of an item in the queue.
func (s *itemSlice) update(key interface{}, value interface{}, priority uint64) {
	item := s.getItemByKey(key)
	if item != nil {
		s.updateItem(item, value, priority)
	}
}

func (s *itemSlice) updateItem(item *ItemNode, value interface{}, priority uint64) {
	item.Value = value
	item.priority = priority
	heap.Fix(s, item.index)
}

func (s *itemSlice) getItemByKey(key interface{}) *ItemNode {
	if item, found := s.itemsMap[key]; found {
		return item
	}
	return nil
}

// PriorityQueue implements heap.Interface
type PriorityQueue struct {
	*sync.RWMutex

	slice   itemSlice
	maxSize int
}

// NewPriorityQueue returns a priority queue
func NewPriorityQueue(maxSize int) *PriorityQueue {
	return &PriorityQueue{
		RWMutex: &sync.RWMutex{},
		slice: itemSlice{
			items:    make([]*ItemNode, 0, maxSize),
			itemsMap: make(map[interface{}]*ItemNode),
		},
		maxSize: maxSize,
	}
}

// Len return the length of items
func (pq PriorityQueue) Len() int {
	pq.RLock()
	defer pq.RUnlock()
	size := pq.slice.Len()
	return size
}

func (pq *PriorityQueue) minItem() *ItemNode {
	len := pq.slice.Len()
	if len == 0 {
		return nil
	}
	return pq.slice.items[0]
}

// MinItem return the minimal ItemNode
func (pq *PriorityQueue) MinItem() *ItemNode {
	pq.RLock()
	defer pq.RUnlock()
	return pq.minItem()
}

// Push a pair (key,value) with priority into the priority queue
func (pq *PriorityQueue) Push(key, value interface{}, priority uint64) bool {
	pq.Lock()
	defer pq.Unlock()
	item := pq.slice.getItemByKey(key)
	if item != nil {
		pq.slice.updateItem(item, value, priority)
		return true
	}
	item = &ItemNode{
		Key:      key,
		Value:    value,
		priority: priority,
		index:    -1,
	}
	if pq.maxSize <= 0 || pq.slice.Len() < pq.maxSize {
		heap.Push(&(pq.slice), item)
		return true
	}
	min := pq.minItem()
	if min.priority >= priority {
		return false
	}
	heap.Pop(&(pq.slice))
	heap.Push(&(pq.slice), item)
	return true
}

// TopN get the top n values
func (pq PriorityQueue) TopN(n int) []interface{} {
	size := pq.Len()
	if size > n {
		size = n
	}

	items := pq.GetOrderItems()
	ret := make([]interface{}, 0, n)
	for i := 0; i < size; i++ {
		ret = append(ret, items[i].Value)
	}
	return ret
}

// GetOrderItems return a reverse sort list of the items in PriorityQueue
func (pq PriorityQueue) GetOrderItems() []*ItemNode {
	size := pq.Len()
	if size == 0 {
		return []*ItemNode{}
	}
	s := itemSlice{}
	s.items = make([]*ItemNode, size)
	pq.RLock()
	for i := 0; i < size; i++ {
		s.items[i] = &ItemNode{
			Key:      pq.slice.items[i].Key,
			Value:    pq.slice.items[i].Value,
			priority: pq.slice.items[i].priority,
		}
	}
	pq.RUnlock()
	sort.Sort(sort.Reverse(s))
	return s.items
}
