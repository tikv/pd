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

package buckets

import (
	"bytes"

	"github.com/tikv/pd/pkg/btree"
)

// BucketTree is a buffer, the key range must be continuous.
type BucketTree struct {
	tree *btree.BTree
}

// NewBucketTree creates a new bucket tree.
func NewBucketTree(degree int) *BucketTree {
	return &BucketTree{
		tree: btree.New(degree),
	}
}

// BucketItem is bucket tree item.
type BucketItem interface {
	Less(than btree.Item) bool
	StartKey() []byte
	EndKey() []byte
	// Debris returns the debris after replacing the key range.
	Debris(startKey, endKey []byte) []BucketItem
	String() string
}

// Len returns the length of the bucket tree.
func (r *BucketTree) Len() int {
	return r.tree.Len()
}

// GetRange returns the items that belong the key range.
// cache key range:  |001-----100|100-----200|
// request key range:   |005-----120|
// return items:     |001-----100|100-----200|
func (r *BucketTree) GetRange(item BucketItem) []BucketItem {
	var res []BucketItem

	var first BucketItem
	r.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		first = i.(BucketItem)
		return false
	})

	// find the first item that contains the start key. if not found,
	// it will use the param.
	if first == nil || !(bytes.Compare(first.StartKey(), item.StartKey()) <= 0 &&
		bytes.Compare(item.StartKey(), first.EndKey()) < 0) {
		first = item
	}

	// find the next item util the item greater than end key.
	r.tree.AscendGreaterOrEqual(first, func(i btree.Item) bool {
		ringItem := i.(BucketItem)
		if len(item.EndKey()) > 0 && bytes.Compare(ringItem.StartKey(), item.EndKey()) >= 0 {
			return false
		}
		res = append(res, ringItem)
		return true
	})
	return res
}

// Put puts a new item into the bucket tree.
func (r *BucketTree) Put(item BucketItem) {
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
