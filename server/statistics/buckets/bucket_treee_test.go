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
	"fmt"
	"github.com/tikv/pd/server/core"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/btree"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBucketSuite{})

type testBucketSuite struct {
}

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

// String implements String.
func (s *simpleBucketItem) String() string {
	return fmt.Sprintf("key-range: [%s, %s]", s.startKey, s.endKey)
}

// Less returns true if the start key of the item is less than the start key of the argument.
func (s *simpleBucketItem) Less(than btree.Item) bool {
	return bytes.Compare(s.GetStartKey(), than.(core.RegionItem).GetStartKey()) < 0
}

// Debris returns the debris of the item.
// details: https://leetcode.cn/problems/interval-list-intersections/
func (s simpleBucketItem) Debris(startKey, endKey []byte) []core.RegionItem {
	var res []core.RegionItem

	left := maxKey(startKey, s.startKey)
	right := minKey(endKey, s.endKey)
	// they have no intersection if they are neighbour like |010 - 100| and |100 - 200|.
	if bytes.Compare(left, right) >= 0 {
		return nil
	}
	// the left has oen intersection like |010 - 100| and |020 - 100|.
	if !bytes.Equal(s.startKey, left) {
		res = append(res, newSimpleBucketItem(s.startKey, left))
	}
	// the right has oen intersection like |010 - 100| and |010 - 099|.
	if !bytes.Equal(right, s.endKey) {
		res = append(res, newSimpleBucketItem(right, s.endKey))
	}
	return res
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

func (bs *testBucketSuite) TestRingPutItem(c *C) {
	bucketTree := core.NewRegionTree(2)
	bucketTree.Update(newSimpleBucketItem([]byte("002"), []byte("100")))
	c.Assert(bucketTree.Len(), Equals, 1)
	bucketTree.Update(newSimpleBucketItem([]byte("100"), []byte("200")))
	c.Assert(bucketTree.Len(), Equals, 2)

	// init key range: [002,100], [100,200]
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("000"), []byte("002"))), HasLen, 0)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("000"), []byte("009"))), HasLen, 1)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), HasLen, 1)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("110"))), HasLen, 2)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("200"), []byte("300"))), HasLen, 0)

	// test1： insert one key range, the old overlaps will retain like split buckets.
	// key range: [002,010],[010,090],[090,100],[100,200]
	bucketTree.Update(newSimpleBucketItem([]byte("010"), []byte("090")))
	c.Assert(bucketTree.Len(), Equals, 4)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), HasLen, 1)

	// test2: insert one key range, the old overlaps will retain like merge .
	// key range: [001,080], [080,090],[090,100],[100,200]
	bucketTree.Update(newSimpleBucketItem([]byte("001"), []byte("080")))
	c.Assert(bucketTree.Len(), Equals, 4)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), HasLen, 2)

	// test2: insert one keyrange, the old overlaps will retain like merge .
	// key range: [001,120],[120,200]
	bucketTree.Update(newSimpleBucketItem([]byte("001"), []byte("120")))
	c.Assert(bucketTree.Len(), Equals, 2)
	c.Assert(bucketTree.GetOverlaps(newSimpleBucketItem([]byte("010"), []byte("090"))), HasLen, 1)
}

func (bs *testBucketSuite) TestDebris(c *C) {
	ringItem := newSimpleBucketItem([]byte("010"), []byte("090"))
	var overlaps []core.RegionItem
	overlaps = ringItem.Debris([]byte("000"), []byte("100"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("000"), []byte("080"))
	c.Assert(overlaps, HasLen, 1)
	overlaps = ringItem.Debris([]byte("020"), []byte("080"))
	c.Assert(overlaps, HasLen, 2)
	overlaps = ringItem.Debris([]byte("010"), []byte("090"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("010"), []byte("100"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("100"), []byte("200"))
	c.Assert(overlaps, HasLen, 0)
}
