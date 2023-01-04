// Copyright 2022 TiKV Project Authors.
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

package rangetree

import (
	"bytes"

	"github.com/tikv/pd/pkg/btree"
)

// RangeItem is the item of the range tree.
type RangeItem interface {
	Less(RangeItem) bool
	GetStartKey() []byte
	GetEndKey() []byte
}

// DebrisFactory is the factory that generates some debris when updating items.
type DebrisFactory func(startKey, EndKey []byte, item RangeItem) []RangeItem

// RangeTree is the tree contains RangeItems.
type RangeTree struct {
	tree    *btree.BTreeG[RangeItem]
	factory DebrisFactory
}

// NewRangeTree is the constructor of the range tree.
func NewRangeTree(degree int, factory DebrisFactory) *RangeTree {
	return &RangeTree{
		tree:    btree.NewG[RangeItem](degree),
		factory: factory,
	}
}

// Update insert the item and delete overlaps.
func (r *RangeTree) Update(item RangeItem) []RangeItem {
	overlaps := r.GetOverlaps(item)
	for _, old := range overlaps {
		r.tree.Delete(old)
		children := r.factory(item.GetStartKey(), item.GetEndKey(), old)
		for _, child := range children {
			if c := bytes.Compare(child.GetStartKey(), child.GetEndKey()); c < 0 {
				r.tree.ReplaceOrInsert(child)
			} else if c > 0 && len(child.GetEndKey()) == 0 {
				r.tree.ReplaceOrInsert(child)
			}
		}
	}
	r.tree.ReplaceOrInsert(item)
	return overlaps
}

// GetOverlaps returns the range items that has some intersections with the given items.
func (r *RangeTree) GetOverlaps(item RangeItem) []RangeItem {
	// note that Find() gets the last item that is less or equal than the item.
	// in the case: |_______a_______|_____b_____|___c___|
	// new item is     |______d______|
	// Find() will return RangeItem of item_a
	// and both startKey of item_a and item_b are less than endKey of item_d,
	// thus they are regarded as overlapped items.
	result := r.Find(item)
	if result == nil {
		result = item
	}
	endKey := item.GetEndKey()
	var overlaps []RangeItem
	r.tree.AscendGreaterOrEqual(result, func(i RangeItem) bool {
		if len(endKey) > 0 && bytes.Compare(endKey, i.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, i)
		return true
	})
	return overlaps
}

// Find returns the range item contains the start key.
func (r *RangeTree) Find(item RangeItem) RangeItem {
	var result RangeItem
	r.tree.DescendLessOrEqual(item, func(i RangeItem) bool {
		result = i
		return false
	})

	if result == nil || !contains(result, item.GetStartKey()) {
		return nil
	}

	return result
}

func contains(item RangeItem, key []byte) bool {
	start, end := item.GetStartKey(), item.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

// Remove removes the given item and return the deleted item.
func (r *RangeTree) Remove(item RangeItem) RangeItem {
	i, _ := r.tree.Delete(item)
	return i
}

// Len returns the count of the range tree.
func (r *RangeTree) Len() int {
	return r.tree.Len()
}

// ScanRange scan the start item util the result of the function is false.
func (r *RangeTree) ScanRange(start RangeItem, f func(_ RangeItem) bool) {
	// Find if there is one item with key range [s, d), s < startKey < d
	startItem := r.Find(start)
	if startItem == nil {
		startItem = start
	}
	r.tree.AscendGreaterOrEqual(startItem, func(item RangeItem) bool {
		return f(item)
	})
}

// GetAdjacentItem returns the adjacent range item.
func (r *RangeTree) GetAdjacentItem(item RangeItem) (prev RangeItem, next RangeItem) {
	r.tree.AscendGreaterOrEqual(item, func(i RangeItem) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		next = i
		return false
	})
	r.tree.DescendLessOrEqual(item, func(i RangeItem) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		prev = i
		return false
	})
	return prev, next
}

// GetAt returns the given index item.
func (r *RangeTree) GetAt(index int) RangeItem {
	return r.tree.GetAt(index)
}

// GetWithIndex returns index and item for the given item.
func (r *RangeTree) GetWithIndex(item RangeItem) (RangeItem, int) {
	return r.tree.GetWithIndex(item)
}
