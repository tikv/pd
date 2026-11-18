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
	"fmt"
	"math"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func getAllBucketStats(ctx context.Context, hotCache *HotBucketCache) map[uint64][]*BucketStat {
	task := NewCollectBucketStatsTask(minHotDegree)
	hotCache.CheckAsync(task)
	return task.WaitRet(ctx)
}

func TestColdHot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
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
				synctest.Sleep(time.Millisecond * 10)
				item := hotBuckets[v.buckets.RegionId]
				re.NotNil(item)
				if v.isHot {
					re.Equal(i+1, item[0].HotDegree)
				} else {
					re.Equal(-i-1, item[0].HotDegree)
				}
			}
		}
	})
}

func TestCheckBucketsTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
		hotCache := NewBucketsCache(ctx)
		// case1: add bucket successfully
		buckets := newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20"), []byte("30")}, 0)
		task := NewCheckPeerTask(buckets)
		re.True(hotCache.CheckAsync(task))
		synctest.Sleep(time.Millisecond * 10)

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
		synctest.Sleep(time.Millisecond * 10)
		item = hotBuckets[uint64(2)]
		re.Len(item, 1)
		re.Equal(-2, item[0].HotDegree)

		// case3: add bucket successful and the hot degree should inherit from the old one.
		buckets = newTestBuckets(1, 1, [][]byte{[]byte("10"), []byte("20")}, 0)
		task = NewCheckPeerTask(buckets)
		re.True(hotCache.CheckAsync(task))
		hotBuckets = getAllBucketStats(ctx, hotCache)
		synctest.Sleep(time.Millisecond * 10)
		item = hotBuckets[uint64(1)]
		re.Len(item, 1)
		re.Equal(-2, item[0].HotDegree)
	})
}

func TestCheckBucketsTaskIgnoresIncompleteStats(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*metapb.Buckets)
	}{
		{name: "missing-stats", mutate: func(b *metapb.Buckets) { b.Stats = nil }},
		{name: "empty-stats", mutate: func(b *metapb.Buckets) { b.Stats = &metapb.BucketStats{} }},
		{name: "short-read-bytes", mutate: func(b *metapb.Buckets) { b.Stats.ReadBytes = b.Stats.ReadBytes[:1] }},
		{name: "short-read-keys", mutate: func(b *metapb.Buckets) { b.Stats.ReadKeys = b.Stats.ReadKeys[:1] }},
		{name: "short-read-qps", mutate: func(b *metapb.Buckets) { b.Stats.ReadQps = b.Stats.ReadQps[:1] }},
		{name: "short-write-bytes", mutate: func(b *metapb.Buckets) { b.Stats.WriteBytes = b.Stats.WriteBytes[:1] }},
		{name: "short-write-keys", mutate: func(b *metapb.Buckets) { b.Stats.WriteKeys = b.Stats.WriteKeys[:1] }},
		{name: "short-write-qps", mutate: func(b *metapb.Buckets) { b.Stats.WriteQps = b.Stats.WriteQps[:1] }},
		{name: "missing-keys", mutate: func(b *metapb.Buckets) { b.Keys = nil }},
		{name: "single-key", mutate: func(b *metapb.Buckets) { b.Keys = b.Keys[:1] }},
		{name: "nil-buckets"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				cache := NewBucketsCache(t.Context())
				keys := [][]byte{[]byte("10"), []byte("20"), []byte("30")}
				initial := newTestBuckets(1, 1, keys, 0)
				re.True(cache.CheckAsync(NewCheckPeerTask(initial)))
				before := getAllBucketStats(t.Context(), cache)
				re.Len(before[1], 2)

				var report *metapb.Buckets
				if testCase.mutate != nil {
					report = newTestBuckets(1, 2, [][]byte{[]byte("10"), []byte("15"), []byte("30")}, 1000)
					testCase.mutate(report)
				}
				re.True(cache.CheckAsync(NewCheckPeerTask(report)))
				re.Equal(before, getAllBucketStats(t.Context(), cache))

				valid := newTestBuckets(1, 3, keys, 1000)
				re.True(cache.CheckAsync(NewCheckPeerTask(valid)))
				after := getAllBucketStats(t.Context(), cache)
				re.Len(after[1], 2)
				for _, stat := range after[1] {
					re.Equal([]uint64{1000, 1000, 1000, 1000, 1000, 1000}, stat.Loads)
				}
			})
		})
	}
}

func TestCheckBucketsTaskAcceptsCompleteStats(t *testing.T) {
	for _, period := range []uint64{0, 1000} {
		for _, extraStats := range []bool{false, true} {
			t.Run(fmt.Sprintf("period-%d/extra-stats-%t", period, extraStats), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					re := require.New(t)
					cache := NewBucketsCache(t.Context())
					keys := [][]byte{[]byte("10"), []byte("20"), []byte("30")}
					if extraStats {
						keys = append(keys, []byte("40"))
					}
					report := newTestBuckets(1, 1, keys, 1000)
					report.Keys = report.Keys[:3]
					report.PeriodInMs = period
					re.True(cache.CheckAsync(NewCheckPeerTask(report)))
					stats := getAllBucketStats(t.Context(), cache)
					re.Len(stats[1], 2)

					interval := period
					if interval == 0 {
						interval = 10000
					}
					load := 1000 * 1000 / interval
					for i, stat := range stats[1] {
						re.Equal(report.Keys[i], stat.StartKey)
						re.Equal(report.Keys[i+1], stat.EndKey)
						re.Equal(interval, stat.Interval)
						re.Equal([]uint64{load, load, load, load, load, load}, stat.Loads)
					}
				})
			})
		}
	}
}

func TestCollectBucketStatsTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
		hotCache := NewBucketsCache(ctx)
		// case1: add bucket successfully
		for i := range uint64(10) {
			buckets := convertToBucketTreeItem(newTestBuckets(i, 1, [][]byte{[]byte(strconv.FormatUint(i*10, 10)),
				[]byte(strconv.FormatUint((i+1)*10, 10))}, 0))
			hotCache.putItem(buckets, hotCache.getBucketsByKeyRange(buckets.startKey, buckets.endKey))
		}
		synctest.Sleep(time.Millisecond * 10)
		task := NewCollectBucketStatsTask(-100)
		re.True(hotCache.CheckAsync(task))
		stats := task.WaitRet(ctx)
		re.Len(stats, 10)
		task = NewCollectBucketStatsTask(1)
		re.True(hotCache.CheckAsync(task))
		stats = task.WaitRet(ctx)
		re.Empty(stats)
	})
}
