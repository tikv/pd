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
	"testing"

	"github.com/stretchr/testify/require"
)

type simpleBucketItem struct {
	startKey []byte
	endKey   []byte
}

func newSimpleBucketItem(startKey, endKey []byte) *simpleBucketItem {
	return &simpleBucketItem{
		startKey: startKey,
		endKey:   endKey,
	}
}

// Less returns true if the start key of the item is less than the start key of the argument.
func (s *simpleBucketItem) Less(than RangeItem) bool {
	return bytes.Compare(s.GetStartKey(), than.GetStartKey()) < 0
}

// StartKey returns the start key of the item.
func (s *simpleBucketItem) GetStartKey() []byte {
	return s.startKey
}

// EndKey returns the end key of the item.
func (s *simpleBucketItem) GetEndKey() []byte {
	return s.endKey
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

// Debris returns the debris of the item.
// details: https://leetcode.cn/problems/interval-list-intersections/
func bucketDebrisFactory(startKey, endKey []byte, item RangeItem) []RangeItem {
	var res []RangeItem

	left := maxKey(startKey, item.GetStartKey())
	right := minKey(endKey, item.GetEndKey())
	// they have no intersection if they are neighbors like |010 - 100| and |100 - 200|.
	if bytes.Compare(left, right) >= 0 {
		return nil
	}
	// the left has one intersection like |010 - 100| and |020 - 100|.
	if !bytes.Equal(item.GetStartKey(), left) {
		res = append(res, newSimpleBucketItem(item.GetStartKey(), left))
	}
	// the right has one intersection like |010 - 100| and |010 - 099|.
	if !bytes.Equal(right, item.GetEndKey()) {
		res = append(res, newSimpleBucketItem(right, item.GetEndKey()))
	}
	return res
}

func TestRingPutItem(t *testing.T) {
	re := require.New(t)
	bucketTree := NewRangeTree(2, bucketDebrisFactory)
	bucketTree.Update(newSimpleBucketItem([]byte("002"), []byte("100")))
	re.Equal(1, bucketTree.Len())
	bucketTree.Update(newSimpleBucketItem([]byte("100"), []byte("200")))
	re.Equal(2, bucketTree.Len())

	// init key range: [002,100], [100,200]
	re.Empty(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("000"), []byte("002"))))
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("000"), []byte("009"))), 1)
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), 1)
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("110"))), 2)
	re.Empty(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("200"), []byte("300"))))

	// test1: insert one key range, the old overlaps will retain like split buckets.
	// key range: [002,010],[010,090],[090,100],[100,200]
	bucketTree.Update(newSimpleBucketItem([]byte("010"), []byte("090")))
	re.Equal(4, bucketTree.Len())
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), 1)

	// test2: insert one key range, the old overlaps will retain like merge .
	// key range: [001,080], [080,090],[090,100],[100,200]
	bucketTree.Update(newSimpleBucketItem([]byte("001"), []byte("080")))
	re.Equal(4, bucketTree.Len())
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), 2)

	// test2: insert one keyrange, the old overlaps will retain like merge .
	// key range: [001,120],[120,200]
	bucketTree.Update(newSimpleBucketItem([]byte("001"), []byte("120")))
	re.Equal(2, bucketTree.Len())
	re.Len(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), 1)
}

func TestDebris(t *testing.T) {
	re := require.New(t)
	ringItem := newSimpleBucketItem([]byte("010"), []byte("090"))
	var overlaps []RangeItem
	overlaps = bucketDebrisFactory([]byte("000"), []byte("100"), ringItem)
	re.Empty(overlaps)
	overlaps = bucketDebrisFactory([]byte("000"), []byte("080"), ringItem)
	re.Len(overlaps, 1)
	overlaps = bucketDebrisFactory([]byte("020"), []byte("080"), ringItem)
	re.Len(overlaps, 2)
	overlaps = bucketDebrisFactory([]byte("010"), []byte("090"), ringItem)
	re.Empty(overlaps)
	overlaps = bucketDebrisFactory([]byte("010"), []byte("100"), ringItem)
	re.Empty(overlaps)
	overlaps = bucketDebrisFactory([]byte("100"), []byte("200"), ringItem)
	re.Empty(overlaps)
}
