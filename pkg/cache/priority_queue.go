package cache

import (
	"container/heap"
)

// PriorityQueue queue has priority, the first element has the highest priority
// the priority is lowest that has highest priority
type PriorityQueue struct {
	queue    *priorityHeap
	items    map[interface{}]*Entry
	capacity int
}

// NewPriorityQueue construct of priority queue
func NewPriorityQueue(capacity int) *PriorityQueue {
	return &PriorityQueue{
		queue:    &priorityHeap{},
		items:    make(map[interface{}]*Entry),
		capacity: capacity,
	}
}

// Push push element into pq with priority
// it will refuse if used > capacity and the tail priority bigger than the given priority
func (pq *PriorityQueue) Push(priority int, value interface{}) bool {
	v, ok := pq.items[value]
	if !ok {
		if pq.Size() >= pq.capacity {
			tail := pq.queue.tail()
			if tail.Priority < priority {
				return false
			}
			pq.RemoveValue(tail.Value)
		}
		v = &Entry{Value: value, Priority: priority}
		heap.Push(pq.queue, v)
	} else {
		pq.UpdatePriority(v, priority)
	}
	pq.items[value] = v
	return true
}

// Pop pop the high Priority element from queue
func (pq *PriorityQueue) Pop() *Entry {
	if pq.Size() <= 0 {
		return nil
	}
	entry, ok := heap.Pop(pq.queue).(*Entry)
	if !ok || entry == nil {
		return nil
	}
	delete(pq.items, entry.Value)
	return entry
}

// Peek return the highest priority element
func (pq *PriorityQueue) Peek() *Entry {
	return pq.queue.peek()
}

// UpdatePriority update the priority of the given element
func (pq *PriorityQueue) UpdatePriority(old *Entry, priority int) {
	old.Priority = priority
	pq.queue.fix(old)
}

// GetAll return limited Entries
func (pq *PriorityQueue) GetAll() []*Entry {
	return pq.queue.GetAll()
}

// RemoveValue remove element
func (pq *PriorityQueue) RemoveValue(value interface{}) {
	if entry, ok := pq.items[value]; ok {
		pq.queue.remove(entry.index)
		delete(pq.items, value)
	}
}

// RemoveValues remove elements
func (pq *PriorityQueue) RemoveValues(values []interface{}) {
	for _, v := range values {
		pq.RemoveValue(v)
	}
}

// Has it will return true if queue has the value
func (pq *PriorityQueue) Has(value interface{}) bool {
	if v, ok := pq.items[value]; ok && v.index != -1 {
		return true
	}
	return false
}

// Size return the size of queue
func (pq *PriorityQueue) Size() int {
	return pq.queue.Len()
}

// Entry internal struct for record element
type Entry struct {
	Value    interface{}
	Priority int
	index    int
}

// priorityHeap implements heap.Interface and holds entries.
type priorityHeap []*Entry

// Len
func (pq priorityHeap) Len() int { return len(pq) }

// Less
func (pq priorityHeap) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// Swap
func (pq priorityHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push push element to heap
func (pq *priorityHeap) Push(value interface{}) {
	entry := value.(*Entry)
	entry.index = len(*pq)
	*pq = append(*pq, entry)
}

// Pop pop the highest Priority element from heap
func (pq *priorityHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// peek peek the highest priority element from heap
func (pq *priorityHeap) peek() *Entry {
	if pq.Len() <= 0 {
		return nil
	}
	old := *pq
	return old[0]
}

// tail peek the lowest priority element from heap
func (pq *priorityHeap) tail() *Entry {
	if pq.Len() <= 0 {
		return nil
	}
	old := *pq
	return old[len(old)-1]
}

// GetAll return the limited size of elements
func (pq *priorityHeap) GetAll() []*Entry {
	tmp := *pq
	return tmp[:]
}

// remove return the index element from heap
func (pq *priorityHeap) remove(index int) interface{} {
	if index >= pq.Len() {
		return nil
	}
	result := heap.Remove(pq, index).(*Entry)
	result.index = -1
	return result
}

// fix the entry index
func (pq *priorityHeap) fix(entry *Entry) {
	if entry.index >= pq.Len() {
		return
	}
	heap.Fix(pq, entry.index)
}
