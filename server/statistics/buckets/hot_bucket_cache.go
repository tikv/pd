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
	"sort"

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
			log.Info("delete region from cache", zap.Uint64("RegionID", overlap.regionID))
			delete(h.bucketsOfRegion, overlap.regionID)
		}
	}
	log.Info("put item into cache", zap.Uint64("RegionID", item.regionID), zap.ByteString("StartKey", item.startKey), zap.ByteString("EndKey", item.endKey))
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
			task.runTask(h)
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
	bucketsHeartbeatIntervalHist.Observe(float64(stats.interval))
	for _, bucket := range stats.stats {
		log.Info("collect bucket hot degree metrics ", zap.Any("bucket", bucket))
		bucketsHotDegreeHist.Observe(float64(bucket.HotDegree))
	}
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	HotDegree int
	interval  uint64
	// see statistics.RegionStatKind
	Loads []uint64
}

// GetHotDegree returns the hot degree of the bucket stat.
func (b *BucketStat) GetHotDegree() int {
	return b.HotDegree
}
func (b *BucketStat) clone() *BucketStat {
	c := &BucketStat{
		StartKey:  b.StartKey,
		EndKey:    b.EndKey,
		RegionID:  b.RegionID,
		HotDegree: b.HotDegree,
		interval:  b.interval,
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
	// it will not inherit if the end key of the new item is less than the start key of the old item.
	// such as: new item: |----a----| |----b----|
	// origins item:      |----c----| |----d----|
	if len(origins) == 0 || len(b.stats) == 0 || bytes.Compare(b.endKey, origins[0].startKey) < 0 {
		return
	}

	newItem := b.stats
	bucketStats := b.clip(origins)

	// p1/p2: the hot stats of the new/old index
	for p1, p2 := 0, 0; p1 < len(newItem) && p2 < len(bucketStats); {
		oldDegree := bucketStats[p2].HotDegree
		newDegree := newItem[p1].HotDegree
		// new bucket should interim old if the hot degree of the new bucket is less than zero.
		if oldDegree < 0 && newDegree <= 0 && oldDegree < newDegree {
			newItem[p1].HotDegree = oldDegree
		}
		// if oldDegree is greater than zero and the new bucket, the new bucket should inherit the old hot degree.
		if oldDegree > 0 && oldDegree > newDegree {
			newItem[p1].HotDegree = oldDegree
		}

		if bytes.Compare(newItem[p1].EndKey, bucketStats[p2].EndKey) > 0 {
			p2++
		} else if bytes.Equal(newItem[p1].EndKey, bucketStats[p2].EndKey) {
			p2++
			p1++
		} else {
			p1++
		}
	}
}

// clip origins bucket to BucketStat array
func (b *BucketTreeItem) clip(origins []*BucketTreeItem) []*BucketStat {
	// the first buckets should contain the start key.
	if len(origins) == 0 || !origins[0].contains(b.startKey) {
		return nil
	}
	bucketStats := make([]*BucketStat, 0)
	index := sort.Search(len(origins[0].stats), func(i int) bool {
		return bytes.Compare(b.startKey, origins[0].stats[i].EndKey) < 0
	})
	bucketStats = append(bucketStats, origins[0].stats[index:]...)
	for i := 1; i < len(origins); i++ {
		bucketStats = append(bucketStats, origins[i].stats...)
	}
	return bucketStats
}

func (b *BucketStat) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key-key:%s][hot-degree:%d][interval:%d][Loads:%v]",
		b.RegionID, b.StartKey, b.EndKey, b.HotDegree, b.interval, b.Loads)
}

// convertToBucketTreeItem converts the bucket stat to bucket tree item.
func convertToBucketTreeItem(buckets *metapb.Buckets) *BucketTreeItem {
	items := make([]*BucketStat, len(buckets.Keys)-1)
	interval := buckets.PeriodInMs / 1000
	for i := 0; i < len(buckets.Keys)-1; i++ {
		loads := []uint64{
			buckets.Stats.ReadBytes[i] / interval,
			buckets.Stats.ReadKeys[i] / interval,
			buckets.Stats.ReadQps[i] / interval,
			buckets.Stats.WriteBytes[i] / interval,
			buckets.Stats.WriteKeys[i] / interval,
			buckets.Stats.WriteQps[i] / interval,
		}
		items[i] = &BucketStat{
			RegionID:  buckets.RegionId,
			StartKey:  buckets.Keys[i],
			EndKey:    buckets.Keys[i+1],
			HotDegree: 0,
			Loads:     loads,
			interval:  interval,
		}
	}
	return &BucketTreeItem{
		startKey: getStartKey(buckets),
		endKey:   getEndKey(buckets),
		regionID: buckets.RegionId,
		stats:    items,
		interval: buckets.GetPeriodInMs() / 1000,
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
