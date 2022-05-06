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

package cache

import (
	"bytes"
	"fmt"

	"github.com/tikv/pd/pkg/btree"
)

// Ring is a ring buffer, the key range must be continuous.
type Ring struct {
	tree *btree.BTree
}

// NewRing creates a new ring buffer.
func NewRing(degree int) *Ring {
	return &Ring{
		tree: btree.New(degree),
	}
}

// RingItem is a ring item.
type RingItem interface {
	Less(than btree.Item) bool
	EndKey() []byte
	Debris(startKey, endKey []byte) []RingItem
	StartKey() []byte
	String() string
}

// Len returns the length of the ring.
func (r *Ring) Len() int {
	return r.tree.Len()
}

// GetRange returns the items that belong the key range.
func (r *Ring) GetRange(item RingItem) []RingItem {
	var res []RingItem

	var first RingItem
	r.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		first = i.(RingItem)
		return false
	})

	if first == nil || !(bytes.Compare(first.StartKey(), item.StartKey()) <= 0 && bytes.Compare(item.StartKey(), first.EndKey()) < 0) {
		first = item
	}

	r.tree.AscendGreaterOrEqual(first, func(i btree.Item) bool {
		ringItem := i.(RingItem)
		if len(item.EndKey()) > 0 && bytes.Compare(ringItem.StartKey(), item.EndKey()) >= 0 {
			return false
		}
		res = append(res, ringItem)
		return true
	})
	return res
}

// Put puts a new item into the ring.
func (r *Ring) Put(item RingItem) {
	overlaps := r.GetRange(item)
	for _, overlap := range overlaps {
		r.tree.Delete(overlap)
		others := overlap.Debris(item.StartKey(), item.EndKey())
		for _, other := range others {
			if bytes.Equal(other.StartKey(), other.EndKey()) {
				r.tree.Delete(other)
			} else {
				r.tree.ReplaceOrInsert(other)
			}
		}
	}
	r.tree.ReplaceOrInsert(item)
}

type simpleRingItem struct {
	startKey []byte
	endKey   []byte
}

func newSimpleRingItem(startKey, endKey []byte) *simpleRingItem {
	return &simpleRingItem{
		startKey: startKey,
		endKey:   endKey,
	}
}

// String
func (s *simpleRingItem) String() string {
	return fmt.Sprintf("key-range: [%s, %s]", s.startKey, s.endKey)
}

// Less returns true if the start key of the item is less than the start key of the argument.
func (s *simpleRingItem) Less(than btree.Item) bool {
	return bytes.Compare(s.StartKey(), than.(RingItem).StartKey()) < 0
}

// Debris returns the debris of the item.
func (s simpleRingItem) Debris(startKey, endKey []byte) []RingItem {
	var res []RingItem

	left := maxKey(startKey, s.startKey)
	right := minKey(endKey, s.endKey)
	if bytes.Compare(left, right) > 0 {
		return nil
	}
	if !bytes.Equal(s.startKey, left) {
		res = append(res, newSimpleRingItem(s.startKey, left))
	}

	if !bytes.Equal(right, s.endKey) {
		res = append(res, newSimpleRingItem(right, s.endKey))
	}
	return res
}

// EndKey returns the end key of the item.
func (s *simpleRingItem) EndKey() []byte {
	return s.endKey
}

// StartKey returns the start key of the item.
func (s *simpleRingItem) StartKey() []byte {
	return s.startKey
}

func minKey(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func maxKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}
