// Copyright 2023 TiKV Project Authors.
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

package balancer

import (
	"sync"
	"sync/atomic"
)

// RoundRobin is a balancer that selects nodes in a round-robin fashion.
type RoundRobin[T uint32 | string] struct {
	sync.RWMutex
	nodes  []T
	exists map[T]struct{}
	next   uint32
}

// NewRoundRobin creates a balancer that selects nodes in a round-robin fashion.
func NewRoundRobin[T uint32 | string]() *RoundRobin[T] {
	return &RoundRobin[T]{
		nodes:  make([]T, 0),
		exists: make(map[T]struct{}),
	}
}

// Next returns next address
func (r *RoundRobin[T]) Next() (t T) {
	r.RLock()
	defer r.RUnlock()
	if len(r.nodes) == 0 {
		return
	}
	next := atomic.AddUint32(&r.next, 1)
	node := r.nodes[(int(next)-1)%len(r.nodes)]
	return node
}

// GetAll returns all nodes
func (r *RoundRobin[T]) GetAll() []T {
	r.RLock()
	defer r.RUnlock()
	// return a copy to avoid data race
	return append(r.nodes[:0:0], r.nodes...)
}

// Put puts one into balancer.
func (r *RoundRobin[T]) Put(node T) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.exists[node]; !ok {
		r.nodes = append(r.nodes, node)
		r.exists[node] = struct{}{}
	}
}

// Delete deletes one from balancer.
func (r *RoundRobin[T]) Delete(node T) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.exists[node]; ok {
		for i, n := range r.nodes {
			if n == node {
				r.nodes = append(r.nodes[:i], r.nodes[i+1:]...)
				delete(r.exists, node)
				break
			}
		}
	}
}

// Len returns the length of nodes.
func (r *RoundRobin[T]) Len() int {
	r.RLock()
	defer r.RUnlock()
	return len(r.nodes)
}
