// Copyright 2014-2022 Google Inc.
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

// In Go 1.18 and beyond, a BTreeG generic is created, and BTree is a specific
// instantiation of that generic for the Item interface, with a backwards-
// compatible API.  Before go1.18, generics are not supported,
// and BTree is just an implementation based around the Item interface.

// Package btree implements in-memory B-Trees of arbitrary degree.
//
// btree implements an in-memory B-Tree for use as an ordered data structure.
// It is not meant for persistent storage solutions.
//
// It has a flatter structure than an equivalent red-black or other binary tree,
// which in some cases yields better memory usage and/or performance.
// See some discussion on the matter here:
//
//	http://google-opensource.blogspot.com/2013/01/c-containers-that-save-memory-and-time.html
//
// Note, though, that this project is in no way related to the C++ B-Tree
// implementation written about there.
//
// Within this tree, each node contains a slice of items and a (possibly nil)
// slice of children.  For basic numeric values or raw structs, this can cause
// efficiency differences when compared to equivalent C++ template code that
// stores values in arrays within the node:
//   - Due to the overhead of storing values as interfaces (each
//     value needs to be stored as the value itself, then 2 words for the
//     interface pointing to that value and its type), resulting in higher
//     memory use.
//   - Since interfaces can point to values anywhere in memory, values are
//     most likely not stored in contiguous blocks, resulting in a higher
//     number of cache misses.
//
// These issues don't tend to matter, though, when working with strings or other
// heap-allocated structures, since C++-equivalent structures also must store
// pointers and also distribute their values across the heap.
//
// This implementation is designed to be a drop-in replacement to gollrb.LLRB
// trees, (http://github.com/petar/gollrb), an excellent and probably the most
// widely used ordered tree implementation in the Go ecosystem currently.
// Its functions, therefore, exactly mirror those of
// llrb.LLRB where possible.  Unlike gollrb, though, we currently don't
// support storing multiple equivalent values.
//
// There are two implementations; those suffixed with 'G' are generics, usable
// for any type, and require a passed-in "less" function to define their ordering.
// Those without this prefix are specific to the 'Item' interface, and use
// its 'Less' function for ordering.

// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nolint
package btree

import (
	"sort"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

// Item represents a single object in the tree.
type Item[T any] interface {
	// Less tests whether the current item is less than the given argument.
	//
	// This must provide a strict weak ordering.
	// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
	// hold one of either a or b in the tree).
	Less(than T) bool
}

const (
	// DefaultFreeListSize is the default size of free list.
	DefaultFreeListSize = 32
)

// FreeListG represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTrees can share the same
// FreeList, in particular when they're created with Clone.
// Two Btrees using the same freelist are safe for concurrent write access.
type FreeListG[T Item[T]] struct {
	mu       syncutil.Mutex
	freelist []*node[T]
}

// NewFreeListG creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeListG[T Item[T]](size int) *FreeListG[T] {
	return &FreeListG[T]{freelist: make([]*node[T], 0, size)}
}

func (f *FreeListG[T]) newNode() (n *node[T]) {
	f.mu.Lock()
	index := len(f.freelist) - 1
	if index < 0 {
		f.mu.Unlock()
		return new(node[T])
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	f.mu.Unlock()
	return
}

func (f *FreeListG[T]) freeNode(n *node[T]) (out bool) {
	f.mu.Lock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	f.mu.Unlock()
	return
}

// ItemIteratorG allows callers of Ascend* to iterate in-order over portions of
// the tree.  When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIteratorG[T Item[T]] func(i T) bool

// NewG creates a new B-Tree with the given degree.
//
// NewG(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
//
// The passed-in LessFunc determines how objects of type T are ordered.
func NewG[T Item[T]](degree int) *BTreeG[T] {
	return NewWithFreeListG(degree, NewFreeListG[T](DefaultFreeListSize))
}

// NewWithFreeListG creates a new B-Tree that uses the given node free list.
func NewWithFreeListG[T Item[T]](degree int, f *FreeListG[T]) *BTreeG[T] {
	if degree <= 1 {
		panic("bad degree")
	}
	return &BTreeG[T]{
		degree: degree,
		cow:    &copyOnWriteContext[T]{freelist: f},
	}
}

// items stores items in a node.
type items[T Item[T]] []T

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items[T]) insertAt(index int, item T) {
	var zero T
	*s = append(*s, zero)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items[T]) removeAt(index int) T {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	var zero T
	(*s)[len(*s)-1] = zero
	*s = (*s)[:len(*s)-1]
	return item
}

// pop removes and returns the last element in the list.
func (s *items[T]) pop() (out T) {
	index := len(*s) - 1
	out = (*s)[index]
	var zero T
	(*s)[index] = zero
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items[T]) truncate(index int) {
	var i int
	var x T
	var toClear items[T]
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		for i = 0; i < 16 && i < len(toClear); i++ {
			toClear[i] = x
		}
		toClear = toClear[i:]
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s items[T]) find(item T) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return item.Less(s[i])
	})
	if i > 0 && !s[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

// indices stores indices of items in a node.
// If the node has any children, indices[i] is the index of items[i] in the subtree.
// We have following formulas:
//
//	indices[i] = if i == 0 { children[0].length() }
//	             else { indices[i-1] + 1 + children[i].length() }
type indices []int

func (s *indices) addAt(index int, delta int) {
	for i := index; i < len(*s); i++ {
		(*s)[i] += delta
	}
}

func (s *indices) insertAt(index int, sz int) {
	*s = append(*s, -1)
	for i := len(*s) - 1; i >= index && i > 0; i-- {
		(*s)[i] = (*s)[i-1] + sz + 1
	}
	if index == 0 {
		(*s)[0] = sz
	}
}

func (s *indices) push(sz int) {
	if len(*s) == 0 {
		*s = append(*s, sz)
	} else {
		*s = append(*s, (*s)[len(*s)-1]+1+sz)
	}
}

// children[i] is split.
func (s *indices) split(index, nextSize int) {
	s.insertAt(index+1, -1)
	(*s)[index] -= 1 + nextSize
}

// children[i] and children[i+1] is merged
func (s *indices) merge(index int) {
	for i := index; i < len(*s)-1; i++ {
		(*s)[i] = (*s)[i+1]
	}
	*s = (*s)[:len(*s)-1]
}

func (s *indices) removeAt(index int) int {
	sz := (*s)[index]
	if index > 0 {
		sz = sz - (*s)[index-1] - 1
	}
	for i := index + 1; i < len(*s); i++ {
		(*s)[i-1] = (*s)[i] - sz - 1
	}
	*s = (*s)[:len(*s)-1]
	return sz
}

func (s *indices) pop() int {
	l := len(*s)
	out := (*s)[l-1]
	if l != 1 {
		out -= (*s)[l-2] + 1
	}
	*s = (*s)[:len(*s)-1]
	return out
}

func (s *indices) truncate(index int) {
	*s = (*s)[:index]
	// no need to clear
}

func (s indices) find(k int) (index int, found bool) {
	i := sort.SearchInts(s, k)
	return i, s[i] == k
}

// children stores child nodes in a node.
type children[T Item[T]] []*node[T]

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children[T]) insertAt(index int, n *node[T]) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children[T]) removeAt(index int) *node[T] {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children[T]) pop() (out *node[T]) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index children. index must be less than or equal to length.
func (s *children[T]) truncate(index int) {
	var i int
	var toClear children[T]
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		for i = 0; i < 16 && i < len(toClear); i++ {
			toClear[i] = nil
		}
		toClear = toClear[i:]
	}
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   - len(children) == 0, len(items) unconstrained
//   - len(children) == len(items) + 1
type node[T Item[T]] struct {
	items    items[T]
	children children[T]
	indices  indices
	cow      *copyOnWriteContext[T]
}

func (n *node[T]) length() int {
	if len(n.indices) == 0 {
		return len(n.items)
	}
	return n.indices[len(n.indices)-1]
}

func (n *node[T]) initSize() {
	l := len(n.children)
	if l <= 0 {
		n.indices.truncate(0)
		return
	} else if l <= cap(n.indices) {
		n.indices = n.indices[:l]
	} else {
		n.indices = make([]int, l)
	}
	n.indices[0] = n.children[0].length()
	for i := 1; i < l; i++ {
		n.indices[i] = n.indices[i-1] + 1 + n.children[i].length()
	}
}

func (n *node[T]) mutableFor(cow *copyOnWriteContext[T]) *node[T] {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	if cap(out.items) >= len(n.items) {
		out.items = out.items[:len(n.items)]
	} else {
		out.items = make(items[T], len(n.items), cap(n.items))
	}
	copy(out.items, n.items)
	// Copy children
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children[T], len(n.children), cap(n.children))
	}
	copy(out.children, n.children)
	// Copy indices
	if cap(out.indices) >= len(n.indices) {
		out.indices = out.indices[:len(n.indices)]
	} else {
		out.indices = make(indices, len(n.indices), cap(n.indices))
	}
	copy(out.indices, n.indices)
	return out
}

func (n *node[T]) mutableChild(i int) *node[T] {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *node[T]) split(i int) (T, *node[T]) {
	item := n.items[i]
	next := n.cow.newNode()
	next.items = append(next.items, n.items[i+1:]...)
	n.items.truncate(i)
	if len(n.children) > 0 {
		next.children = append(next.children, n.children[i+1:]...)
		next.initSize()
		n.children.truncate(i + 1)
		n.indices.truncate(i + 1)
	}
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node[T]) maybeSplitChild(i, maxItems int) bool {
	if len(n.children[i].items) < maxItems {
		return false
	}
	first := n.mutableChild(i)
	item, second := first.split(maxItems / 2)
	n.items.insertAt(i, item)
	n.children.insertAt(i+1, second)
	n.indices.split(i, second.length())
	return true
}

// insert inserts an item into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxItems items.  Should an equivalent item be
// be found/replaced by insert, it will be returned.
func (n *node[T]) insert(item T, maxItems int) (_ T, _ bool) {
	i, found := n.items.find(item)
	if found {
		out := n.items[i]
		n.items[i] = item
		return out, true
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		return
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.items[i]
		switch {
		case item.Less(inTree):
			// no change, we want first split node
		case inTree.Less(item):
			i++ // we want second split node
		default:
			out := n.items[i]
			n.items[i] = item
			return out, true
		}
	}
	out, outb := n.mutableChild(i).insert(item, maxItems)
	if !outb {
		n.indices.addAt(i, 1)
	}
	return out, outb
}

// get finds the given key in the subtree and returns it.
func (n *node[T]) get(key T) (_ T, _ bool) {
	i, found := n.items.find(key)
	if found {
		return n.items[i], true
	} else if len(n.children) > 0 {
		return n.children[i].get(key)
	}
	return
}

func (n *node[T]) getAt(k int) T {
	if k >= n.length() || k < 0 {
		var zero T
		return zero
	}
	if len(n.children) == 0 {
		return n.items[k]
	}
	i, found := n.indices.find(k)
	if found {
		return n.items[i]
	}
	if i == 0 {
		return n.children[0].getAt(k)
	}
	return n.children[i].getAt(k - n.indices[i-1] - 1)
}

// index is the number of items < key
func (n *node[T]) getWithIndex(key T) (_ T, _ int) {
	i, found := n.items.find(key)
	if found {
		rk := i
		if len(n.indices) > 0 {
			rk = n.indices[i]
		}
		return n.items[i], rk
	} else if len(n.children) > 0 {
		out, rk := n.children[i].getWithIndex(key)
		if i > 0 {
			rk += n.indices[i-1] + 1
		}
		return out, rk
	}
	var zero T
	return zero, i
}

// min returns the first item in the subtree.
func min[T Item[T]](n *node[T]) (_ T, found bool) {
	if n == nil {
		return
	}
	for len(n.children) > 0 {
		n = n.children[0]
	}
	if len(n.items) == 0 {
		return
	}
	return n.items[0], true
}

// max returns the last item in the subtree.
func max[T Item[T]](n *node[T]) (_ T, found bool) {
	if n == nil {
		return
	}
	for len(n.children) > 0 {
		n = n.children[len(n.children)-1]
	}
	if len(n.items) == 0 {
		return
	}
	return n.items[len(n.items)-1], true
}

// toRemove details what item to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given item
	removeMin                  // removes smallest item in the subtree
	removeMax                  // removes largest item in the subtree
)

// remove removes an item from the subtree rooted at this node.
func (n *node[T]) remove(item T, minItems int, typ toRemove) (_ T, _ bool) {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			return n.items.pop(), true
		}
		i = len(n.items)
	case removeMin:
		if len(n.children) == 0 {
			return n.items.removeAt(0), true
		}
		i = 0
	case removeItem:
		i, found = n.items.find(item)
		if len(n.children) == 0 {
			if found {
				return n.items.removeAt(i), true
			}
			return
		}
	default:
		panic("invalid type")
	}
	// If we get to here, we have children.
	if len(n.children[i].items) <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ)
	}
	child := n.mutableChild(i)
	// Either we had enough items to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	var (
		out  T
		outb bool
	)
	if found {
		// The item exists at index 'i', and the child we've selected can give us a
		// predecessor, since if we've gotten here it's got > minItems items in it.
		out = n.items[i]
		// We use our special-case 'remove' call with typ=maxItem to pull the
		// predecessor of item i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the item from.
		var zero T
		n.items[i], outb = child.remove(zero, minItems, removeMax)
	} else {
		// Final recursive call.  Once we're here, we know that the item isn't in this
		// node and that the child is big enough to remove from.
		out, outb = child.remove(item, minItems, typ)
	}
	if outb {
		n.indices.addAt(i, -1)
	}
	return out, outb
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// item from it while keeping it at minItems, then calls remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casing:
//  1. item is in this node
//  2. item is in child
//
// In both cases, we need to handle the two subcases:
//
//	A) node has enough values that it can spare one
//	B) node doesn't have enough values
//
// For the latter, we have to check:
//
//	a) left sibling has node to spare
//	b) right sibling has node to spare
//	c) we must merge
//
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough items, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough items and can guarantee
// that we hit case A.
func (n *node[T]) growChildAndRemove(i int, item T, minItems int, typ toRemove) (T, bool) {
	if i > 0 && len(n.children[i-1].items) > minItems {
		// Steal from left child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i - 1)
		stolenItem := stealFrom.items.pop()
		child.items.insertAt(0, n.items[i-1])
		n.items[i-1] = stolenItem
		n.indices[i-1] -= 1
		if len(stealFrom.children) > 0 {
			child.children.insertAt(0, stealFrom.children.pop())
			stealSize := stealFrom.indices.pop()
			n.indices[i-1] -= stealSize
			child.indices.insertAt(0, stealSize)
		}
	} else if i < len(n.items) && len(n.children[i+1].items) > minItems {
		// steal from right child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i + 1)
		stolenItem := stealFrom.items.removeAt(0)
		child.items = append(child.items, n.items[i])
		n.items[i] = stolenItem
		n.indices[i] += 1
		if len(stealFrom.children) > 0 {
			child.children = append(child.children, stealFrom.children.removeAt(0))
			stealSize := stealFrom.indices.removeAt(0)
			n.indices[i] += stealSize
			child.indices.push(stealSize)
		}
	} else {
		if i >= len(n.items) {
			i--
		}
		child := n.mutableChild(i)
		// merge with right child
		mergeItem := n.items.removeAt(i)
		mergeChild := n.children.removeAt(i + 1)
		child.items = append(child.items, mergeItem)
		child.items = append(child.items, mergeChild.items...)
		child.children = append(child.children, mergeChild.children...)
		for _, nn := range mergeChild.children {
			child.indices.push(nn.length())
		}
		n.indices.merge(i)
		n.cow.freeNode(mergeChild)
	}
	return n.remove(item, minItems, typ)
}

type direction int

const (
	descend = direction(-1)
	ascend  = direction(+1)
)

// iterate provides a simple method for iterating over elements in the tree.
//
// When ascending, the 'start' should be less than 'stop' and when descending,
// the 'start' should be greater than 'stop'. Setting 'includeStart' to true
// will force the iterator to include the first item when it equals 'start',
// thus creating a "greaterOrEqual" or "lessThanEqual" rather than just a
// "greaterThan" or "lessThan" queries.
func (n *node[T]) iterate(dir direction, start, stop *T, includeStart bool, hit bool, iter ItemIteratorG[T]) (bool, bool) {
	var ok, found bool
	var index int
	switch dir {
	case ascend:
		if start != nil {
			index, _ = n.items.find(*start)
		}
		for i := index; i < len(n.items); i++ {
			if len(n.children) > 0 {
				if hit, ok = n.children[i].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if !includeStart && !hit && start != nil && !(*start).Less(n.items[i]) {
				hit = true
				continue
			}
			hit = true
			if stop != nil && !n.items[i].Less(*stop) {
				return hit, false
			}
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[len(n.children)-1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	case descend:
		if start != nil {
			index, found = n.items.find(*start)
			if !found {
				index--
			}
		} else {
			index = len(n.items) - 1
		}
		for i := index; i >= 0; i-- {
			if start != nil && !n.items[i].Less(*start) {
				if !includeStart || hit || (*start).Less(n.items[i]) {
					continue
				}
			}
			if len(n.children) > 0 {
				if hit, ok = n.children[i+1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if stop != nil && !(*stop).Less(n.items[i]) {
				return hit, false //	continue
			}
			hit = true
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[0].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	}
	return hit, true
}

// BTreeG is a generic implementation of a B-Tree.
//
// BTreeG stores items of type T in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTreeG[T Item[T]] struct {
	degree int
	length int
	root   *node[T]
	cow    *copyOnWriteContext[T]
}

// copyOnWriteContext pointers determine node ownership... a tree with a write
// context equivalent to a node's write context is allowed to modify that node.
// A tree whose write context does not match a node's is not allowed to modify
// it, and must create a new, writable copy (IE: it's a Clone).
//
// When doing any write operation, we maintain the invariant that the current
// node's context is equal to the context of the tree that requested the write.
// We do this by, before we descend into any node, creating a copy with the
// correct context if the contexts don't match.
//
// Since the node we're currently visiting on any write has the requesting
// tree's context, that node is modifiable in place.  Children of that node may
// not share context, but before we descend into them, we'll make a mutable
// copy.
type copyOnWriteContext[T Item[T]] struct {
	freelist *FreeListG[T]
}

// Clone clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degradation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *BTreeG[T]) Clone() (t2 *BTreeG[T]) {
	// Create two entirely new copy-on-write contexts.
	// This operation effectively creates three trees:
	//   the original, shared nodes (old b.cow)
	//   the new b.cow nodes
	//   the new out.cow nodes
	cow1, cow2 := *t.cow, *t.cow
	out := *t
	t.cow = &cow1
	out.cow = &cow2
	return &out
}

// maxItems returns the max number of items to allow per node.
func (t *BTreeG[T]) maxItems() int {
	return t.degree*2 - 1
}

// minItems returns the min number of items to allow per node (ignored for the
// root node).
func (t *BTreeG[T]) minItems() int {
	return t.degree - 1
}

func (c *copyOnWriteContext[T]) newNode() (n *node[T]) {
	n = c.freelist.newNode()
	n.cow = c
	return
}

type freeType int

const (
	ftFreelistFull freeType = iota // node was freed (available for GC, not stored in freelist)
	ftStored                       // node was stored in the freelist for later use
	ftNotOwned                     // node was ignored by COW, since it's owned by another one
)

// freeNode frees a node within a given COW context, if it's owned by that
// context.  It returns what happened to the node (see freeType const
// documentation).
func (c *copyOnWriteContext[T]) freeNode(n *node[T]) freeType {
	if n.cow == c {
		// clear to allow GC
		n.items.truncate(0)
		n.children.truncate(0)
		n.indices.truncate(0)
		n.cow = nil
		if c.freelist.freeNode(n) {
			return ftStored
		}
		return ftFreelistFull
	}
	return ftNotOwned
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (t *BTreeG[T]) ReplaceOrInsert(item T) (_ T, _ bool) {
	if t.root == nil {
		t.root = t.cow.newNode()
		t.root.items = append(t.root.items, item)
		t.length++
		return
	}
	t.root = t.root.mutableFor(t.cow)
	if len(t.root.items) >= t.maxItems() {
		item2, second := t.root.split(t.maxItems() / 2)
		oldroot := t.root
		t.root = t.cow.newNode()
		t.root.items = append(t.root.items, item2)
		t.root.children = append(t.root.children, oldroot, second)
		t.root.initSize()
	}
	out, found := t.root.insert(item, t.maxItems())
	if !found {
		t.length++
	}
	return out, found
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) Delete(item T) (T, bool) {
	return t.deleteItem(item, removeItem)
}

// DeleteMin removes the smallest item in the tree and returns it.
// If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) DeleteMin() (T, bool) {
	var zero T
	return t.deleteItem(zero, removeMin)
}

// DeleteMax removes the largest item in the tree and returns it.
// If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) DeleteMax() (T, bool) {
	var zero T
	return t.deleteItem(zero, removeMax)
}

func (t *BTreeG[T]) deleteItem(item T, typ toRemove) (_ T, _ bool) {
	if t.root == nil || len(t.root.items) == 0 {
		return
	}
	t.root = t.root.mutableFor(t.cow)
	out, found := t.root.remove(item, t.minItems(), typ)
	if len(t.root.items) == 0 && len(t.root.children) > 0 {
		oldroot := t.root
		t.root = t.root.children[0]
		t.cow.freeNode(oldroot)
	}
	if found {
		t.length--
	}
	return out, found
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *BTreeG[T]) AscendRange(greaterOrEqual, lessThan T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, &greaterOrEqual, &lessThan, true, false, iterator)
}

// AscendLessThan calls the iterator for every value in the tree within the range
// [first, pivot), until iterator returns false.
func (t *BTreeG[T]) AscendLessThan(pivot T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, &pivot, false, false, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (t *BTreeG[T]) AscendGreaterOrEqual(pivot T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, &pivot, nil, true, false, iterator)
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (t *BTreeG[T]) Ascend(iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, nil, false, false, iterator)
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (t *BTreeG[T]) DescendRange(lessOrEqual, greaterThan T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, &lessOrEqual, &greaterThan, true, false, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the tree within the range
// [pivot, first], until iterator returns false.
func (t *BTreeG[T]) DescendLessOrEqual(pivot T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, &pivot, nil, true, false, iterator)
}

// DescendGreaterThan calls the iterator for every value in the tree within
// the range [last, pivot), until iterator returns false.
func (t *BTreeG[T]) DescendGreaterThan(pivot T, iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, &pivot, false, false, iterator)
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (t *BTreeG[T]) Descend(iterator ItemIteratorG[T]) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, nil, false, false, iterator)
}

// Get looks for the key item in the tree, returning it.  It returns
// (zeroValue, false) if unable to find that item.
func (t *BTreeG[T]) Get(key T) (_ T, _ bool) {
	if t.root == nil {
		return
	}
	return t.root.get(key)
}

// GetWithIndex gets the key and its index.
// If the key is not in the tree, the the index is the number of items < key.
func (t *BTreeG[T]) GetWithIndex(key T) (_ T, _ int) {
	if t.root == nil {
		var zero T
		return zero, 0
	}
	return t.root.getWithIndex(key)
}

// GetAt returns the item with index k. If k < 0 or k >= t.Len(), returns nil.
func (t *BTreeG[T]) GetAt(k int) T {
	if t.root == nil {
		var zero T
		return zero
	}
	return t.root.getAt(k)
}

// Min returns the smallest item in the tree, or (zeroValue, false) if the tree is empty.
func (t *BTreeG[T]) Min() (_ T, _ bool) {
	return min(t.root)
}

// Max returns the largest item in the tree, or (zeroValue, false) if the tree is empty.
func (t *BTreeG[T]) Max() (_ T, _ bool) {
	return max(t.root)
}

// Has returns true if the given key is in the tree.
func (t *BTreeG[T]) Has(key T) bool {
	_, found := t.Get(key)
	return found
}

// Len returns the number of items currently in the tree.
func (t *BTreeG[T]) Len() int {
	return t.length
}

// function for test.
func (t *BTreeG[T]) getRootLength() int {
	if t.root == nil {
		return 0
	}
	return t.root.length()
}

// Clear removes all items from the btree.  If addNodesToFreelist is true,
// t's nodes are added to its freelist as part of this call, until the freelist
// is full.  Otherwise, the root node is simply dereferenced and the subtree
// left to Go's normal GC processes.
//
// This can be much faster
// than calling Delete on all elements, because that requires finding/removing
// each element in the tree and updating the tree accordingly.  It also is
// somewhat faster than creating a new tree to replace the old one, because
// nodes from the old tree are reclaimed into the freelist for use by the new
// one, instead of being lost to the garbage collector.
//
// This call takes:
//
//	O(1): when addNodesToFreelist is false, this is a single operation.
//	O(1): when the freelist is already full, it breaks out immediately
//	O(freelist size):  when the freelist is empty and the nodes are all owned
//	    by this tree, nodes are added to the freelist until full.
//	O(tree size):  when all nodes are owned by another tree, all nodes are
//	    iterated over looking for nodes to add to the freelist, and due to
//	    ownership, none are.
func (t *BTreeG[T]) Clear(addNodesToFreelist bool) {
	if t.root != nil && addNodesToFreelist {
		t.root.reset(t.cow)
	}
	t.root, t.length = nil, 0
}

// reset returns a subtree to the freelist.  It breaks out immediately if the
// freelist is full, since the only benefit of iterating is to fill that
// freelist up.  Returns true if parent reset call should continue.
func (n *node[T]) reset(c *copyOnWriteContext[T]) bool {
	for _, child := range n.children {
		if !child.reset(c) {
			return false
		}
	}
	return c.freeNode(n) != ftFreelistFull
}

// Int implements the Item interface for integers.
type Int int

// Less returns true if int(a) < int(b).
func (a Int) Less(b Int) bool {
	return a < b
}
