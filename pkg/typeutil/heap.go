package typeutil

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
)

type Item struct {
	key      interface{}
	value    interface{}
	priority uint64
	index    int
}

type Items []*Item

func (items Items) String() string {
	var ret string
	for _, i := range items {
		ret = ret + fmt.Sprintf("{key:%d,value:%d,priority:%d,index:%d} ", i.key, i.value, i.priority, i.index)
	}
	return ret
}

type ItemSlice struct {
	items    Items
	itemsMap map[interface{}]*Item
}

func (s ItemSlice) Len() int { return len(s.items) }

func (s ItemSlice) Less(i, j int) bool {
	return s.items[i].priority < s.items[j].priority
}

func (s ItemSlice) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
	s.items[i].index = i
	s.items[j].index = j
	if s.itemsMap != nil {
		s.itemsMap[s.items[i].key] = s.items[i]
		s.itemsMap[s.items[j].key] = s.items[j]
	}
}

func (s *ItemSlice) Push(x interface{}) {
	n := len(s.items)
	item := x.(*Item)
	item.index = n
	s.items = append(s.items, item)
	s.itemsMap[item.key] = item
}

func (s *ItemSlice) Pop() interface{} {
	old := s.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	delete(s.itemsMap, item.key)
	s.items = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (s *ItemSlice) update(key interface{}, value interface{}, priority uint64) {
	item := s.getItemByKey(key)
	if item != nil {
		s.updateItem(item, value, priority)
	}
}

func (s *ItemSlice) updateItem(item *Item, value interface{}, priority uint64) {
	item.value = value
	item.priority = priority
	heap.Fix(s, item.index)
}

func (s *ItemSlice) getItemByKey(key interface{}) *Item {
	if item, found := s.itemsMap[key]; found {
		return item
	}
	return nil
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	slice   ItemSlice
	maxSize int
	mutex   sync.RWMutex
}

func NewPriorityQuery(maxSize int) *PriorityQueue {
	return &PriorityQueue{
		slice: ItemSlice{
			items:    make([]*Item, 0, maxSize),
			itemsMap: make(map[interface{}]*Item),
		},
		maxSize: maxSize,
	}
}
func (pq *PriorityQueue) Init(maxSize int) {
}

func (pq PriorityQueue) Len() int {
	pq.mutex.RLock()
	size := pq.slice.Len()
	pq.mutex.RUnlock()
	return size
}

func (pq *PriorityQueue) minItem() *Item {
	len := pq.slice.Len()
	if len == 0 {
		return nil
	}
	return pq.slice.items[0]
}

func (pq *PriorityQueue) MinItem() *Item {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	return pq.minItem()
}

func (pq *PriorityQueue) Push(key, value interface{}, priority uint64) bool {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	item := pq.slice.getItemByKey(key)
	if item != nil {
		pq.slice.updateItem(item, value, priority)
		return true
	}
	item = &Item{
		key:      key,
		value:    value,
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

func (pq PriorityQueue) TopN(n int) []interface{} {
	size := pq.Len()
	if size > n {
		size = n
	}

	items := pq.GetOrderItems()
	ret := make([]interface{}, 0, n)
	for i := 0; i < size; i++ {
		ret = append(ret, items[i].value)
	}
	return ret
}

func (pq PriorityQueue) GetOrderItems() []*Item {
	size := pq.Len()
	if size == 0 {
		return []*Item{}
	}
	s := ItemSlice{}
	s.items = make([]*Item, size)
	pq.mutex.RLock()
	for i := 0; i < size; i++ {
		s.items[i] = &Item{
			value:    pq.slice.items[i].value,
			priority: pq.slice.items[i].priority,
		}
	}
	pq.mutex.RUnlock()
	sort.Sort(sort.Reverse(s))
	return s.items
}
