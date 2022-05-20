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
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testHotBucketCache{})

type testHotBucketCache struct{}

func (t *testHotBucketCache) TestPutItem(c *C) {
	cache := NewBucketsCache(context.Background())
	testdata := []struct {
		regionID    uint64
		keys        [][]byte
		regionCount int
		treeLen     int
		version     uint64
	}{{
		regionID:    1,
		keys:        [][]byte{[]byte(""), []byte("")},
		regionCount: 1,
		treeLen:     1,
	}, {
		// case1: region split
		// origin:  |""-----------------------""|
		// new: 	      |10--20--30|
		// tree:    |""----10--20--30--------""|
		regionID:    1,
		keys:        [][]byte{[]byte("10"), []byte("20"), []byte("30")},
		regionCount: 1,
		version:     2,
		treeLen:     3,
	}, {
		// case2: region split
		// origin:  |""--10-----------30---""|
		// new:              |15 20|
		// tree:    |""--10--15--20--30--""|
		regionID:    2,
		keys:        [][]byte{[]byte("15"), []byte("20")},
		regionCount: 1,
		treeLen:     5,
	}, {
		// case 3: region split
		// origin:  |""--10--15--20--30--""|
		// new:                 |20 ---- ""|
		// tree:   |""--10--15--20------ ""|
		regionID:    1,
		keys:        [][]byte{[]byte("20"), []byte("")},
		version:     3,
		regionCount: 2,
		treeLen:     4,
	}, {
		// case 4: region split
		// tree: |""--10--15--20------ ""|
		// new:  |""----------20|
		// tree: |""----------20--------""|
		regionID:    3,
		keys:        [][]byte{[]byte(""), []byte("20")},
		regionCount: 2,
		treeLen:     2,
	}, {
		// // case 5: region 1,2,3 will be merged.
		regionID:    4,
		keys:        [][]byte{[]byte(""), []byte("")},
		regionCount: 1,
		treeLen:     1,
	}}
	for _, v := range testdata {
		bucket := convertToBucketTreeItem(newTestBuckets(v.regionID, v.version, v.keys, 10))
		c.Assert(bucket.GetStartKey(), BytesEquals, v.keys[0])
		c.Assert(bucket.GetEndKey(), BytesEquals, v.keys[len(v.keys)-1])
		cache.putItem(bucket, cache.getBucketsByKeyRange(bucket.GetStartKey(), bucket.GetEndKey()))
		c.Assert(cache.bucketsOfRegion, HasLen, v.regionCount)
		c.Assert(cache.tree.Len(), Equals, v.treeLen)
		c.Assert(cache.bucketsOfRegion[v.regionID], NotNil)
		c.Assert(cache.getBucketsByKeyRange([]byte("10"), nil), NotNil)
	}
}

func (t *testHotBucketCache) TestConvertToBucketTreeStat(c *C) {
	buckets := &metapb.Buckets{
		RegionId: 1,
		Version:  0,
		Keys:     [][]byte{{'1'}, {'2'}, {'3'}, {'4'}, {'5'}},
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{1, 2, 3, 4},
			ReadKeys:   []uint64{1, 2, 3, 4},
			ReadQps:    []uint64{1, 2, 3, 4},
			WriteBytes: []uint64{1, 2, 3, 4},
			WriteKeys:  []uint64{1, 2, 3, 4},
			WriteQps:   []uint64{1, 2, 3, 4},
		},
		PeriodInMs: 1000,
	}
	item := convertToBucketTreeItem(buckets)
	c.Assert(item.startKey, BytesEquals, []byte{'1'})
	c.Assert(item.endKey, BytesEquals, []byte{'5'})
	c.Assert(item.regionID, Equals, uint64(1))
	c.Assert(item.version, Equals, uint64(0))
	c.Assert(item.stats, HasLen, 4)
}

func (t *testHotBucketCache) TestGetBucketsByKeyRange(c *C) {
	cache := NewBucketsCache(context.Background())
	bucket1 := newTestBuckets(1, 1, [][]byte{[]byte(""), []byte("015")}, 0)
	bucket2 := newTestBuckets(2, 1, [][]byte{[]byte("015"), []byte("020")}, 0)
	bucket3 := newTestBuckets(3, 1, [][]byte{[]byte("020"), []byte("")}, 0)
	cache.putItem(cache.checkBucketsFlow(bucket1))
	cache.putItem(cache.checkBucketsFlow(bucket2))
	cache.putItem(cache.checkBucketsFlow(bucket3))
	c.Assert(cache.getBucketsByKeyRange([]byte(""), []byte("100")), HasLen, 3)
	c.Assert(cache.getBucketsByKeyRange([]byte("030"), []byte("100")), HasLen, 1)
	c.Assert(cache.getBucketsByKeyRange([]byte("010"), []byte("030")), HasLen, 3)
	c.Assert(cache.getBucketsByKeyRange([]byte("015"), []byte("020")), HasLen, 1)
	c.Assert(cache.getBucketsByKeyRange([]byte("001"), []byte("")), HasLen, 3)
	c.Assert(cache.bucketsOfRegion, HasLen, 3)
}

func (t *testHotBucketCache) TestInherit(c *C) {
	originBucketItem := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte(""), []byte("20"), []byte("50"), []byte("")}, 0))
	originBucketItem.stats[0].HotDegree = 3
	originBucketItem.stats[1].HotDegree = 2
	originBucketItem.stats[2].HotDegree = 10

	testdata := []struct {
		buckets *metapb.Buckets
		expect  []int
	}{{
		// case1: one bucket can be inherited by many buckets.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte(""), []byte("20"), []byte("30"), []byte("40"), []byte("50")}, 0),
		expect:  []int{3, 2, 2, 2},
	}, {
		// case2: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("20"), []byte("45"), []byte("50")}, 0),
		expect:  []int{2, 2},
	}, {
		// case3: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("00"), []byte("05")}, 0),
		expect:  []int{3},
	}, {
		// case4: newItem starKey is greater than old.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("80"), []byte("")}, 0),
		expect:  []int{10},
	}, {
		buckets: newTestBuckets(1, 1, [][]byte{[]byte(""), []byte("")}, 0),
		expect:  []int{10},
	}}

	// init: key range |10--20---50---60|(3 2 10)
	for _, v := range testdata {
		buckets := convertToBucketTreeItem(v.buckets)
		buckets.inherit([]*BucketTreeItem{originBucketItem})
		c.Assert(buckets.stats, HasLen, len(v.expect))
		for k, v := range v.expect {
			c.Assert(buckets.stats[k].HotDegree, Equals, v)
		}
	}
}

func (t *testHotBucketCache) TestBucketTreeItemClone(c *C) {
	// bucket range: [010,020][020,100]
	origin := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("010"), []byte("020"), []byte("100")}, uint64(0)))
	testdata := []struct {
		startKey []byte
		endKey   []byte
		count    int
		strict   bool
	}{{
		startKey: []byte("010"),
		endKey:   []byte("100"),
		count:    2,
		strict:   true,
	}, {
		startKey: []byte("000"),
		endKey:   []byte("010"),
		count:    0,
		strict:   false,
	}, {
		startKey: []byte("100"),
		endKey:   []byte("200"),
		count:    0,
		strict:   false,
	}, {
		startKey: []byte("000"),
		endKey:   []byte("020"),
		count:    1,
		strict:   false,
	}, {
		startKey: []byte("015"),
		endKey:   []byte("095"),
		count:    2,
		strict:   true,
	}, {
		startKey: []byte("015"),
		endKey:   []byte("200"),
		count:    2,
		strict:   false,
	}}
	for _, v := range testdata {
		copy := origin.cloneBucketItemByRange(v.startKey, v.endKey)
		c.Assert(copy.startKey, BytesEquals, v.startKey)
		c.Assert(copy.endKey, BytesEquals, v.endKey)
		c.Assert(copy.stats, HasLen, v.count)
		if v.count > 0 && v.strict {
			c.Assert(copy.stats[0].StartKey, BytesEquals, v.startKey)
			c.Assert(copy.stats[len(copy.stats)-1].EndKey, BytesEquals, v.endKey)
		}
	}
}

func (t *testHotBucketCache) TestCalculateHotDegree(c *C) {
	origin := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("010"), []byte("100")}, uint64(0)))
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].HotDegree, Equals, -1)

	// case1: the dimension of read will be hot
	origin.stats[0].Loads = []uint64{minHotThresholds[0] + 1, minHotThresholds[1] + 1, 0, 0, 0, 0}
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].HotDegree, Equals, 0)

	// case1: the dimension of write will be hot
	origin.stats[0].Loads = []uint64{0, 0, 0, minHotThresholds[3] + 1, minHotThresholds[4] + 1, 0}
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].HotDegree, Equals, 1)
}

func newTestBuckets(regionID uint64, version uint64, keys [][]byte, flow uint64) *metapb.Buckets {
	flows := make([]uint64, len(keys)-1)
	for i := range keys {
		if i == len(keys)-1 {
			continue
		}
		flows[i] = flow
	}
	rst := &metapb.Buckets{RegionId: regionID, Version: version, Keys: keys, PeriodInMs: 1000,
		Stats: &metapb.BucketStats{
			ReadBytes:  flows,
			ReadKeys:   flows,
			ReadQps:    flows,
			WriteBytes: flows,
			WriteKeys:  flows,
			WriteQps:   flows,
		}}
	return rst
}
