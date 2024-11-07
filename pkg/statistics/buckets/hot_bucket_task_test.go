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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

func getAllBucketStats(ctx context.Context, hotCache *HotBucketCache) map[uint64][]*BucketStat {
	task := NewCollectBucketStatsTask(minHotDegree)
	hotCache.CheckAsync(task)
	return task.WaitRet(ctx)
}

func TestColdHot(t *testing.T) {
	re := require.New(t)
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
		for i := range 20 {
			task := NewCheckPeerTask(v.buckets)
			re.True(hotCache.CheckAsync(task))
			hotBuckets := getAllBucketStats(ctx, hotCache)
			time.Sleep(time.Millisecond * 10)
			item := hotBuckets[v.buckets.RegionId]
			re.NotNil(item)
			if v.isHot {
				re.Equal(i+1, item[0].HotDegree)
			} else {
				re.Equal(-i-1, item[0].HotDegree)
			}
		}
	}
}

func TestCheckBucketsTask(t *testing.T) {
	re := require.New(t)
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	hotCache := NewBucketsCache(ctx)
	// case1: add bucket successfully
	buckets := newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30")}, 0)
	task := NewCheckPeerTask(buckets)
	re.True(hotCache.CheckAsync(task))
	time.Sleep(time.Millisecond * 10)

	hotBuckets := getAllBucketStats(ctx, hotCache)
	re.Len(hotBuckets, 1)
	item := hotBuckets[uint64(1)]
	re.NotNil(item)

	re.Len(item, 2)
	re.Equal(-1, item[0].HotDegree)
	re.Equal(-1, item[1].HotDegree)

	// case2: add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(2, 1, [][]byte{[]byte("20"), []byte("30")}, 0)
	task = NewCheckPeerTask(buckets)
	re.True(hotCache.CheckAsync(task))
	hotBuckets = getAllBucketStats(ctx, hotCache)
	time.Sleep(time.Millisecond * 10)
	item = hotBuckets[uint64(2)]
	re.Len(item, 1)
	re.Equal(-2, item[0].HotDegree)

	// case3: add bucket successful and the hot degree should inherit from the old one.
	buckets = newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}, 0)
	task = NewCheckPeerTask(buckets)
	re.True(hotCache.CheckAsync(task))
	hotBuckets = getAllBucketStats(ctx, hotCache)
	time.Sleep(time.Millisecond * 10)
	item = hotBuckets[uint64(1)]
	re.Len(item, 1)
	re.Equal(-2, item[0].HotDegree)
}

func TestCollectBucketStatsTask(t *testing.T) {
	re := require.New(t)
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	hotCache := NewBucketsCache(ctx)
	// case1: add bucket successfully
	for i := range uint64(10) {
		buckets := convertToBucketTreeItem(newTestBuckets(i, 1, [][]byte{[]byte(strconv.FormatUint(i*10, 10)),
			[]byte(strconv.FormatUint((i+1)*10, 10))}, 0))
		hotCache.putItem(buckets, hotCache.getBucketsByKeyRange(buckets.startKey, buckets.endKey))
	}
	time.Sleep(time.Millisecond * 10)
	task := NewCollectBucketStatsTask(-100)
	re.True(hotCache.CheckAsync(task))
	stats := task.WaitRet(ctx)
	re.Len(stats, 10)
	task = NewCollectBucketStatsTask(1)
	re.True(hotCache.CheckAsync(task))
	stats = task.WaitRet(ctx)
	re.Empty(stats)
}
