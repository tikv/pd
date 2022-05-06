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
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var _ = Suite(&testHotBucketTaskCache{})

type testHotBucketTaskCache struct {
}

func (s *testHotBucketTaskCache) SetUpSuite(_ *C) {
}

func (s *testHotBucketTaskCache) TearDownTest(_ *C) {
}

func getAllBucketStats(ctx context.Context, hotCache *HotBucketCache) map[uint64][]*BucketStat {
	task := NewCollectBucketStatsTask(-100)
	hotCache.CheckAsync(task)
	return task.WaitRet(ctx)
}

func (s *testHotBucketTaskCache) TestColdHot(c *C) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	hotCache := NewBucketsCache(ctx)
	testdata := []struct {
		buckets *metapb.Buckets
		isHot   bool
	}{{
		buckets: newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}, 0),
		isHot:   false,
	}, {
		buckets: newTestBuckets(2, 1, [][]byte{[]byte("20"), []byte("30")}, math.MaxUint64),
		isHot:   true,
	}}
	for _, v := range testdata {
		for i := 0; i < 100; i++ {
			task := NewCheckPeerTask(v.buckets)
			c.Assert(hotCache.CheckAsync(task), IsTrue)
			hotBuckets := getAllBucketStats(ctx, hotCache)
			time.Sleep(time.Millisecond * 10)
			item := hotBuckets[v.buckets.RegionId]
			c.Assert(item, NotNil)
			if v.isHot {
				c.Assert(item[0].hotDegree, Equals, i+1)
			} else {
				c.Assert(item[0].hotDegree, Equals, -i-1)
			}
		}
	}
}

func (s *testHotBucketTaskCache) TestCheckBucketsTask(c *C) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	hotCache := NewBucketsCache(ctx)
	// case1： add bucket successfully
	buckets := newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30")}, 0)
	task := NewCheckPeerTask(buckets)
	c.Assert(hotCache.CheckAsync(task), IsTrue)
	time.Sleep(time.Millisecond * 10)

	hotBuckets := getAllBucketStats(ctx, hotCache)
	c.Assert(hotBuckets, HasLen, 1)
	item := hotBuckets[uint64(1)]
	c.Assert(item, NotNil)
	c.Assert(item, HasLen, 2)
	c.Assert(item[0].hotDegree, Equals, -1)
	c.Assert(item[1].hotDegree, Equals, -1)

	// case2: add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(2, 1, [][]byte{[]byte("20"), []byte("30")}, 0)
	task = NewCheckPeerTask(buckets)
	c.Assert(hotCache.CheckAsync(task), IsTrue)
	hotBuckets = getAllBucketStats(ctx, hotCache)
	time.Sleep(time.Millisecond * 10)
	item = hotBuckets[uint64(2)]
	c.Assert(item, HasLen, 1)
	c.Assert(item[0].hotDegree, Equals, -2)

	// case3：add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}, 0)
	task = NewCheckPeerTask(buckets)
	c.Assert(hotCache.CheckAsync(task), IsTrue)
	hotBuckets = getAllBucketStats(ctx, hotCache)
	time.Sleep(time.Millisecond * 10)
	item = hotBuckets[uint64(1)]
	c.Assert(item, HasLen, 1)
	c.Assert(item[0].hotDegree, Equals, -2)
}

func (s *testHotBucketTaskCache) TestCollectBucketStatsTask(c *C) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	hotCache := NewBucketsCache(ctx)
	// case1： add bucket successfully
	for i := uint64(0); i < 10; i++ {
		buckets := convertToBucketTreeItem(newTestBuckets(i, 1, [][]byte{[]byte(strconv.FormatUint(i*10, 10)),
			[]byte(strconv.FormatUint((i+1)*10, 10))}, 0))
		hotCache.putItem(buckets, hotCache.getBucketsByKeyRange(buckets.startKey, buckets.endKey))
	}
	time.Sleep(time.Millisecond * 10)
	task := NewCollectBucketStatsTask(-100)
	c.Assert(hotCache.CheckAsync(task), IsTrue)
	stats := task.WaitRet(ctx)
	c.Assert(stats, HasLen, 10)
	task = NewCollectBucketStatsTask(1)
	c.Assert(hotCache.CheckAsync(task), IsTrue)
	stats = task.WaitRet(ctx)
	c.Assert(stats, HasLen, 0)
}
