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
	// case1: region split
	// origin:  |10|20|30|
	// new: 	|10|15|20|30|
	// when report bucket[15:20], the origin should be truncate into two region
	cache := NewBucketsCache(context.Background())
	testdata := []struct {
		regionID    uint64
		keys        [][]byte
		regionCount int
		treeLen     int
		version     uint64
	}{{
		regionID:    1,
		keys:        [][]byte{[]byte("10"), []byte("20"), []byte("30")},
		regionCount: 1,
		treeLen:     1,
	}, {
		regionID:    2,
		keys:        [][]byte{[]byte("15"), []byte("20")},
		regionCount: 1,
		treeLen:     3,
	}, {
		regionID:    1,
		keys:        [][]byte{[]byte("20"), []byte("30")},
		version:     2,
		regionCount: 2,
		treeLen:     3,
	}, {
		regionID:    3,
		keys:        [][]byte{[]byte("10"), []byte("15")},
		regionCount: 3,
		treeLen:     3,
	}, {
		// region 1,2,3 will be merged.
		regionID:    4,
		keys:        [][]byte{[]byte("10"), []byte("30")},
		regionCount: 1,
		treeLen:     1,
	}}
	for _, v := range testdata {
		bucket := convertToBucketTreeItem(newTestBuckets(v.regionID, v.version, v.keys, 0))
		c.Assert(bucket.StartKey(), BytesEquals, v.keys[0])
		c.Assert(bucket.EndKey(), BytesEquals, v.keys[len(v.keys)-1])
		cache.putItem(bucket, cache.getBucketsByKeyRange(bucket.StartKey(), bucket.EndKey()))
		c.Assert(cache.bucketsOfRegion, HasLen, v.regionCount)
		c.Assert(cache.ring.Len(), Equals, v.treeLen)
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
	bucket1 := newTestBuckets(1, 1, [][]byte{[]byte("010"), []byte("015")}, 0)
	bucket2 := newTestBuckets(2, 1, [][]byte{[]byte("015"), []byte("020")}, 0)
	bucket3 := newTestBuckets(3, 1, [][]byte{[]byte("020"), []byte("030")}, 0)
	cache.putItem(cache.checkBucketsFlow(bucket1))
	cache.putItem(cache.checkBucketsFlow(bucket2))
	cache.putItem(cache.checkBucketsFlow(bucket3))
	c.Assert(cache.getBucketsByKeyRange([]byte("010"), []byte("100")), NotNil)
	c.Assert(cache.getBucketsByKeyRange([]byte("030"), []byte("100")), IsNil)
	c.Assert(cache.getBucketsByKeyRange([]byte("010"), []byte("030")), HasLen, 3)
	c.Assert(cache.getBucketsByKeyRange([]byte("010"), []byte("020")), HasLen, 2)
	c.Assert(cache.getBucketsByKeyRange([]byte("001"), []byte("010")), HasLen, 0)
	c.Assert(cache.bucketsOfRegion, HasLen, 3)
}

func (t *testHotBucketCache) TestInherit(c *C) {
	originBucketItem := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("50"), []byte("60")}, 0))
	originBucketItem.stats[0].hotDegree = 3
	originBucketItem.stats[1].hotDegree = 2
	originBucketItem.stats[2].hotDegree = 10

	testdata := []struct {
		buckets *metapb.Buckets
		expect  []int
	}{{
		// case1: one bucket can be inherited by many buckets.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30"), []byte("40"), []byte("50")}, 0),
		expect:  []int{3, 2, 2, 2},
	}, {
		// case2: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("20"), []byte("45"), []byte("50")}, 0),
		expect:  []int{2, 2},
	}, {
		// case3: the first start key is less than the end key of old item.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("00"), []byte("05")}, 0),
		expect:  []int{0},
	}, {
		// case4: newItem starKey is greater than old.
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("80"), []byte("90")}, 0),
		expect:  []int{0},
	}}

	for _, v := range testdata {
		buckets := convertToBucketTreeItem(v.buckets)
		buckets.inherit([]*BucketTreeItem{originBucketItem})
		c.Assert(buckets.stats, HasLen, len(v.expect))
		for k, v := range v.expect {
			c.Assert(buckets.stats[k].hotDegree, Equals, v)
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
		copy := origin.clone(v.startKey, v.endKey)
		c.Assert(copy.startKey, BytesEquals, v.startKey)
		c.Assert(copy.endKey, BytesEquals, v.endKey)
		c.Assert(copy.stats, HasLen, v.count)
		if v.count > 0 && v.strict {
			c.Assert(copy.stats[0].startKey, BytesEquals, v.startKey)
			c.Assert(copy.stats[len(copy.stats)-1].endKey, BytesEquals, v.endKey)
		}
	}
}

func (t *testHotBucketCache) TestClip(c *C) {
	origins := []*BucketTreeItem{
		convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("60")}, uint64(0))),
		convertToBucketTreeItem(newTestBuckets(2, 1, [][]byte{[]byte("80"), []byte("100")}, uint64(0))),
	}

	testdata := []struct {
		buckets  *metapb.Buckets
		count    int
		startKey []byte
	}{{
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}, 0),
		startKey: []byte("10"),
		count:    3,
	}, {
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("20"), []byte("50")}, 0),
		startKey: []byte("20"),
		count:    2,
	}, {
		buckets:  newTestBuckets(1, 1, [][]byte{[]byte("80"), []byte("100")}, 0),
		startKey: []byte("50"),
		count:    0,
	}}
	for _, v := range testdata {
		item := convertToBucketTreeItem(v.buckets)
		stats := item.clip(origins)
		c.Assert(stats, HasLen, v.count)
		if v.count > 0 {
			c.Assert(stats[0].startKey, BytesEquals, v.startKey)
		}
	}
}

func (t *testHotBucketCache) TestCalculateHotDegree(c *C) {
	origin := convertToBucketTreeItem(newTestBuckets(1, 1, [][]byte{[]byte("010"), []byte("100")}, uint64(0)))
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].hotDegree, Equals, -1)

	// case1: the dimension of read will be hot
	origin.stats[0].loads = []uint64{minHotThresholds[0] + 1, minHotThresholds[1] + 1, 0, 0, 0, 0}
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].hotDegree, Equals, 0)

	// case1: the dimension of write will be hot
	origin.stats[0].loads = []uint64{0, 0, 0, minHotThresholds[3] + 1, minHotThresholds[4] + 1, 0}
	origin.calculateHotDegree()
	c.Assert(origin.stats[0].hotDegree, Equals, 1)
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
