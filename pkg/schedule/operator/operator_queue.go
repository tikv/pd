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

package operator

import (
	"container/heap"
	"sync"
	"time"
)

type operatorWithTime struct {
	op   *Operator
	time time.Time
}

type operatorQueue []*operatorWithTime

// Len implements heap.Interface.
func (opn operatorQueue) Len() int { return len(opn) }

// Less implements heap.Interface.
func (opn operatorQueue) Less(i, j int) bool {
	return opn[i].time.Before(opn[j].time)
}

// Swap implements heap.Interface.
func (opn operatorQueue) Swap(i, j int) {
	opn[i], opn[j] = opn[j], opn[i]
}

// Push implements heap.Interface.
func (opn *operatorQueue) Push(x any) {
	item := x.(*operatorWithTime)
	*opn = append(*opn, item)
}

// Pop implements heap.Interface.
func (opn *operatorQueue) Pop() any {
	old := *opn
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*opn = old[0 : n-1]
	return item
}

type concurrentHeapOpQueue struct {
	sync.Mutex
	heap operatorQueue
}

func newConcurrentHeapOpQueue() *concurrentHeapOpQueue {
	return &concurrentHeapOpQueue{heap: make(operatorQueue, 0)}
}

func (ch *concurrentHeapOpQueue) len() int {
	ch.Lock()
	defer ch.Unlock()
	return len(ch.heap)
}

func (ch *concurrentHeapOpQueue) push(x *operatorWithTime) {
	ch.Lock()
	defer ch.Unlock()
	heap.Push(&ch.heap, x)
}

func (ch *concurrentHeapOpQueue) pop() (*operatorWithTime, bool) {
	ch.Lock()
	defer ch.Unlock()
	if len(ch.heap) == 0 {
		return nil, false
	}
	x := heap.Pop(&ch.heap).(*operatorWithTime)
	return x, true
}
