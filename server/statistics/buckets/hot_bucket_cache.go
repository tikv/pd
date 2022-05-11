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
	"context"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

type status int

const (
	alive status = iota
	archive
)

const (
	// queue is the length of the channel used to send the statistics.
	queue = 20000
	// bucketBtreeDegree is the degree of the btree used to store the bucket.
	bucketBtreeDegree = 10

	// the range of the hot degree should be [-100, 100]
	minHotDegree = -100
	maxHotDegree = 100
)

var minHotThresholds = [statistics.RegionStatCount]uint64{
	statistics.RegionReadBytes:  8 * 1024,
	statistics.RegionReadKeys:   128,
	statistics.RegionReadQuery:  128,
	statistics.RegionWriteBytes: 1 * 1024,
	statistics.RegionWriteKeys:  32,
	statistics.RegionWriteQuery: 32,
}

// HotBucketCache is the cache of hot stats.
type HotBucketCache struct {
	ring            *cache.Ring                // regionId -> BucketsStats
	bucketsOfRegion map[uint64]*BucketTreeItem // regionId -> BucketsStats
	taskQueue       chan flowBucketsItemTask
	ctx             context.Context
}

// NewBucketsCache creates a new hot spot cache.
func NewBucketsCache(ctx context.Context) *HotBucketCache {
	bucketCache := &HotBucketCache{
		ctx:             ctx,
		bucketsOfRegion: make(map[uint64]*BucketTreeItem),
		ring:            cache.NewRing(bucketBtreeDegree),
		taskQueue:       make(chan flowBucketsItemTask, queue),
	}
	go bucketCache.updateItems()
	return bucketCache
}

// BucketStats returns the hot stats of the regions that great than degree.
func (h *HotBucketCache) BucketStats(degree int) map[uint64][]*BucketStat {
	rst := make(map[uint64][]*BucketStat)
	for _, item := range h.bucketsOfRegion {
		stats := make([]*BucketStat, 0)
		for _, b := range item.stats {
			if b.HotDegree >= degree {
				stats = append(stats, b)
			}
		}
		if len(stats) > 0 {
			rst[item.regionID] = stats
		}
	}
	return rst
}

// putItem puts the item into the cache.
func (h *HotBucketCache) putItem(item *BucketTreeItem, overlaps []*BucketTreeItem) {
	// only update origin if the key range is same.
	if origin := h.bucketsOfRegion[item.regionID]; item.compareKeyRange(origin) {
		*origin = *item
		return
	}
	for _, overlap := range overlaps {
		if overlap.status == alive {
			log.Info("delete buckets from cache", zap.Uint64("region-id", overlap.regionID))
			delete(h.bucketsOfRegion, overlap.regionID)
		}
	}
	log.Info("put buckets into cache", zap.Stringer("region-id", item))
	h.bucketsOfRegion[item.regionID] = item
	h.ring.Put(item)
}

// CheckAsync returns true if the task queue is not full.
func (h *HotBucketCache) CheckAsync(task flowBucketsItemTask) bool {
	select {
	case h.taskQueue <- task:
		return true
	default:
		return false
	}
}

func (h *HotBucketCache) updateItems() {
	defer logutil.LogPanic()
	for {
		select {
		case <-h.ctx.Done():
			return
		case task := <-h.taskQueue:
			start := time.Now()
			task.runTask(h)
			bucketsHotHandlerDuration.WithLabelValues(task.taskType().String()).Observe(time.Since(start).Seconds())
		}
	}
}

// checkBucketsFlow returns the new item tree and the overlaps.
func (h *HotBucketCache) checkBucketsFlow(buckets *metapb.Buckets) (newItem *BucketTreeItem, overlaps []*BucketTreeItem) {
	newItem = convertToBucketTreeItem(buckets)
	// origin is existed and the version is same.
	if origin := h.bucketsOfRegion[buckets.GetRegionId()]; newItem.compareKeyRange(origin) {
		overlaps = []*BucketTreeItem{origin}
	} else {
		overlaps = h.getBucketsByKeyRange(newItem.startKey, newItem.endKey)
	}
	newItem.inherit(overlaps)
	newItem.calculateHotDegree()
	h.collectBucketsMetrics(newItem)
	return newItem, overlaps
}

func (b *BucketTreeItem) calculateHotDegree() {
	for _, stat := range b.stats {
		// todo： qps should be considered, tikv will report this in next sprint
		readLoads := stat.Loads[:2]
		readHot := slice.AllOf(readLoads, func(i int) bool {
			return readLoads[i] > minHotThresholds[i]
		})
		writeLoads := stat.Loads[3:5]
		writeHot := slice.AllOf(writeLoads, func(i int) bool {
			return writeLoads[i] > minHotThresholds[3+i]
		})
		hot := readHot || writeHot
		if hot && stat.HotDegree < maxHotDegree {
			stat.HotDegree++
		}
		if !hot && stat.HotDegree > minHotDegree {
			stat.HotDegree--
		}
	}
}

// getBucketsByKeyRange returns the overlaps with the key range.
func (h *HotBucketCache) getBucketsByKeyRange(startKey, endKey []byte) (items []*BucketTreeItem) {
	item := &BucketTreeItem{startKey: startKey, endKey: endKey}
	ringItems := h.ring.GetRange(item)
	for _, item := range ringItems {
		bucketItem := item.(*BucketTreeItem)
		items = append(items, bucketItem)
	}
	return
}

// collectBucketsMetrics collects the metrics of the hot stats.
func (h *HotBucketCache) collectBucketsMetrics(stats *BucketTreeItem) {
	bucketsHeartbeatIntervalHist.Observe(float64(stats.interval) / 1000)
	for _, bucket := range stats.stats {
		bucketsHotDegreeHist.Observe(float64(bucket.HotDegree))
	}
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	HotDegree int
	Interval  uint64
	// see statistics.RegionStatKind
	Loads []uint64
}

func (b *BucketStat) clone() *BucketStat {
	c := &BucketStat{
		StartKey:  b.StartKey,
		EndKey:    b.EndKey,
		RegionID:  b.RegionID,
		HotDegree: b.HotDegree,
		Interval:  b.Interval,
		Loads:     make([]uint64, len(b.Loads)),
	}
	copy(c.Loads, b.Loads)
	return c
}

// BucketTreeItem is the item of the bucket btree.
type BucketTreeItem struct {
	regionID uint64
	startKey []byte
	endKey   []byte
	stats    []*BucketStat
	interval uint64
	version  uint64
	status   status
}

// StartKey implements the TreeItem interface.
func (b *BucketTreeItem) StartKey() []byte {
	return b.startKey
}

// EndKey implements the TreeItem interface.
func (b *BucketTreeItem) EndKey() []byte {
	return b.endKey
}

// String implements the fmt.Stringer interface.
func (b *BucketTreeItem) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key:%s]",
		b.regionID, core.HexRegionKeyStr(b.startKey), core.HexRegionKeyStr(b.endKey))
}

// Debris returns the debris of the item.
func (b *BucketTreeItem) Debris(startKey, endKey []byte) []cache.RingItem {
	var res []cache.RingItem
	left := maxKey(startKey, b.startKey)
	right := minKey(endKey, b.endKey)
	// has no intersection
	if bytes.Compare(left, right) > 0 {
		return nil
	}
	// there will be no debris if the left is equal to the start key.
	if !bytes.Equal(b.startKey, left) {
		res = append(res, b.clone(b.startKey, left))
	}

	// there will be no debris if the right is equal to the end key.
	if !bytes.Equal(b.endKey, right) {
		res = append(res, b.clone(right, b.endKey))
	}
	return res
}

// Less returns true if the start key is less than the other.
func (b *BucketTreeItem) Less(than btree.Item) bool {
	return bytes.Compare(b.startKey, than.(*BucketTreeItem).startKey) < 0
}

// compareKeyRange returns whether the key range is overlaps with the item.
func (b *BucketTreeItem) compareKeyRange(origin *BucketTreeItem) bool {
	if origin == nil {
		return false
	}
	// key range must be same if the version is same.
	if b.version == origin.version {
		return true
	}
	return bytes.Equal(b.startKey, origin.startKey) && bytes.Equal(b.endKey, origin.endKey)
}

// Clone returns a new item with the same key range.
// item must have some debris for the given key range
func (b *BucketTreeItem) clone(startKey, endKey []byte) *BucketTreeItem {
	item := &BucketTreeItem{
		regionID: b.regionID,
		startKey: startKey,
		endKey:   endKey,
		interval: b.interval,
		version:  b.version,
		stats:    make([]*BucketStat, 0, len(b.stats)),
		status:   archive,
	}

	for _, stat := range b.stats {
		//  insert if the stat has debris with the key range.
		left := maxKey(stat.StartKey, startKey)
		right := minKey(stat.EndKey, endKey)
		if bytes.Compare(left, right) < 0 {
			copy := stat.clone()
			copy.StartKey = left
			copy.EndKey = right
			item.stats = append(item.stats, copy)
		}
	}
	return item
}

func (b *BucketTreeItem) contains(key []byte) bool {
	return bytes.Compare(b.startKey, key) <= 0 && bytes.Compare(key, b.endKey) < 0
}

// inherit the hot stats from the old item to the new item.
// rule1: if one cross buckets are hot , it will inherit the hottest one.
// rule2: if the cross buckets are not hot, it will inherit the coldest one.
// rule3: if some cross buckets are hot and the others are cold, it will inherit the hottest one.
func (b *BucketTreeItem) inherit(origins []*BucketTreeItem) {
	if len(origins) == 0 || len(b.stats) == 0 || bytes.Compare(b.endKey, origins[0].startKey) < 0 {
		return
	}

	newItems := b.stats
	oldItems := make([]*BucketStat, 0)
	for _, bucketTree := range origins {
		oldItems = append(oldItems, bucketTree.stats...)
	}
	// details: https://leetcode.cn/problems/interval-list-intersections/solution/jiu-pa-ni-bu-dong-shuang-zhi-zhen-by-hyj8/
	for p1, p2 := 0, 0; p1 < len(newItems) && p2 < len(oldItems); {
		newItem, oldItem := newItems[p1], oldItems[p2]
		left := maxKey(newItem.StartKey, oldItems[p2].StartKey)
		right := minKey(newItem.EndKey, oldItems[p2].EndKey)

		// bucket should inherit the old bucket hot degree if they have some intersection.
		// skip if the left is equal to the right key, such as [10 20] [20 30].
		if bytes.Compare(left, right) < 0 {
			log.Info("inherit bucket %s from %s", zap.ByteString("left", left), zap.ByteString("right", right))
			oldDegree := oldItems[p2].HotDegree
			newDegree := newItems[p1].HotDegree
			// new bucket should interim old if the hot degree of the new bucket is less than zero.
			if oldDegree < 0 && newDegree <= 0 && oldDegree < newDegree {
				newItem.HotDegree = oldDegree
			}
			// if oldDegree is greater than zero and the new bucket, the new bucket should inherit the old hot degree.
			if oldDegree > 0 && oldDegree > newDegree {
				newItem.HotDegree = oldDegree
			}
		}
		// move the left item to the next, old should move first if they are equal.
		if bytes.Compare(newItem.EndKey, oldItem.EndKey) > 0 {
			p2++
		} else {
			p1++
		}
	}
}

func (b *BucketStat) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key-key:%s][hot-degree:%d][Interval:%d(ms)][Loads:%v]",
		b.RegionID, core.HexRegionKeyStr(b.StartKey), core.HexRegionKeyStr(b.EndKey), b.HotDegree, b.Interval, b.Loads)
}

// convertToBucketTreeItem converts the bucket stat to bucket tree item.
func convertToBucketTreeItem(buckets *metapb.Buckets) *BucketTreeItem {
	items := make([]*BucketStat, len(buckets.Keys)-1)
	interval := buckets.PeriodInMs
	// Interval may be zero after the tikv initial.
	if interval == 0 {
		interval = 10 * 1000
	}
	for i := 0; i < len(buckets.Keys)-1; i++ {
		loads := []uint64{
			buckets.Stats.ReadBytes[i] * 1000 / interval,
			buckets.Stats.ReadKeys[i] * 1000 / interval,
			buckets.Stats.ReadQps[i] * 1000 / interval,
			buckets.Stats.WriteBytes[i] * 1000 / interval,
			buckets.Stats.WriteKeys[i] * 1000 / interval,
			buckets.Stats.WriteQps[i] * 1000 / interval,
		}
		items[i] = &BucketStat{
			RegionID:  buckets.RegionId,
			StartKey:  buckets.Keys[i],
			EndKey:    buckets.Keys[i+1],
			HotDegree: 0,
			Loads:     loads,
			Interval:  interval,
		}
	}
	return &BucketTreeItem{
		startKey: getStartKey(buckets),
		endKey:   getEndKey(buckets),
		regionID: buckets.RegionId,
		stats:    items,
		interval: buckets.GetPeriodInMs(),
		version:  buckets.Version,
		status:   alive,
	}
}

func getEndKey(buckets *metapb.Buckets) []byte {
	if len(buckets.GetKeys()) == 0 {
		return nil
	}
	return buckets.Keys[len(buckets.Keys)-1]
}

func getStartKey(buckets *metapb.Buckets) []byte {
	if len(buckets.GetKeys()) == 0 {
		return nil
	}
	return buckets.Keys[0]
}

func maxKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func minKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return b
	}
	return a
}
