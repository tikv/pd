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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core/rangetree"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/logutil"
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
	minHotDegree = -20
	maxHotDegree = 20
)

// HotBucketCache is the cache of hot stats.
type HotBucketCache struct {
	tree            *rangetree.RangeTree       // regionId -> BucketTreeItem
	bucketsOfRegion map[uint64]*BucketTreeItem // regionId -> BucketTreeItem
	taskQueue       chan flowBucketsItemTask
	ctx             context.Context
}

// GetHotBucketStats returns the hot stats of the regionIDs that great than degree.
func (h *HotBucketCache) GetHotBucketStats(degree int, regionIDs []uint64) map[uint64][]*BucketStat {
	rst := make(map[uint64][]*BucketStat)
	appendItems := func(item *BucketTreeItem) {
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
	if len(regionIDs) == 0 {
		for _, item := range h.bucketsOfRegion {
			appendItems(item)
		}
	} else {
		for _, region := range regionIDs {
			if item, ok := h.bucketsOfRegion[region]; ok {
				appendItems(item)
			}
		}
	}

	return rst
}

// bucketDebrisFactory returns the debris if the key range of the item is bigger than the given key range.
// start and end key:    | 001------------------------200|
// the split key range:              |050---150|
// returns debris:       |001-----050|         |150------200|
func bucketDebrisFactory(startKey, endKey []byte, item rangetree.RangeItem) []rangetree.RangeItem {
	var res []rangetree.RangeItem
	left := keyutil.MaxKey(startKey, item.GetStartKey())
	right := keyutil.MinKey(endKey, item.GetEndKey())
	if len(endKey) == 0 {
		right = item.GetEndKey()
	}
	if len(item.GetEndKey()) == 0 {
		right = endKey
	}
	// they have no intersection.
	// key range:   |001--------------100|
	// bucket tree:                      |100-----------200|
	if bytes.Compare(left, right) > 0 && len(right) != 0 {
		return nil
	}

	bt := item.(*BucketTreeItem)
	// there will be no debris if the left is equal to the start key.
	if !bytes.Equal(item.GetStartKey(), left) {
		res = append(res, bt.cloneBucketItemByRange(item.GetStartKey(), left))
	}
	// there will be no debris if the right is equal to the end key.
	if !bytes.Equal(item.GetEndKey(), right) || len(right) == 0 {
		if len(right) == 0 {
			res = append(res, bt.cloneBucketItemByRange(item.GetEndKey(), right))
		} else {
			res = append(res, bt.cloneBucketItemByRange(right, item.GetEndKey()))
		}
	}
	return res
}

// NewBucketsCache is the constructor for HotBucketCache.
func NewBucketsCache(ctx context.Context) *HotBucketCache {
	bucketCache := &HotBucketCache{
		ctx:             ctx,
		bucketsOfRegion: make(map[uint64]*BucketTreeItem),
		tree:            rangetree.NewRangeTree(bucketBtreeDegree, bucketDebrisFactory),
		taskQueue:       make(chan flowBucketsItemTask, queue),
	}
	go bucketCache.schedule()
	return bucketCache
}

// putItem puts the item into the cache.
func (h *HotBucketCache) putItem(item *BucketTreeItem, overlaps []*BucketTreeItem) {
	// only update origin if the key range is same.
	if origin := h.bucketsOfRegion[item.regionID]; item.equals(origin) {
		*origin = *item
		return
	}
	for _, overlap := range overlaps {
		if overlap.status == alive {
			log.Debug("delete buckets from cache",
				zap.Uint64("region-id", overlap.regionID),
				logutil.ZapRedactByteString("start-key", overlap.GetStartKey()),
				logutil.ZapRedactByteString("end-key", overlap.GetEndKey()))
			delete(h.bucketsOfRegion, overlap.regionID)
		}
	}
	h.bucketsOfRegion[item.regionID] = item
	h.tree.Update(item)
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

// BucketsStats returns hot region's buckets stats.
func (h *HotBucketCache) BucketsStats(degree int, regionIDs ...uint64) map[uint64][]*BucketStat {
	task := NewCollectBucketStatsTask(degree, regionIDs...)
	if !h.CheckAsync(task) {
		return nil
	}
	return task.WaitRet(h.ctx)
}

func (h *HotBucketCache) schedule() {
	defer logutil.LogPanic()

	for {
		select {
		case <-h.ctx.Done():
			return
		case task := <-h.taskQueue:
			start := time.Now()
			task.runTask(h)
			bucketsTaskDuration.WithLabelValues(task.taskType().String()).Observe(time.Since(start).Seconds())
		}
	}
}

// checkBucketsFlow returns the preprocessor bucket tree item and the overlaps.
// step1: convert to bucket tree item.
// step2: inherit old bucket states.
// step3: update bucket states.
func (h *HotBucketCache) checkBucketsFlow(buckets *metapb.Buckets) (newItem *BucketTreeItem, overlaps []*BucketTreeItem) {
	newItem = convertToBucketTreeItem(buckets)
	// origin is existed and the version is same.
	if origin := h.bucketsOfRegion[buckets.GetRegionId()]; newItem.equals(origin) {
		overlaps = []*BucketTreeItem{origin}
	} else {
		overlaps = h.getBucketsByKeyRange(newItem.startKey, newItem.endKey)
	}
	newItem.inherit(overlaps)
	newItem.calculateHotDegree()
	return newItem, overlaps
}

// getBucketsByKeyRange returns the overlaps with the key range.
func (h *HotBucketCache) getBucketsByKeyRange(startKey, endKey []byte) (items []*BucketTreeItem) {
	item := &BucketTreeItem{startKey: startKey, endKey: endKey}
	ringItems := h.tree.GetOverlaps(item)
	for _, item := range ringItems {
		bucketItem := item.(*BucketTreeItem)
		items = append(items, bucketItem)
	}
	return
}

// convertToBucketTreeItem converts the bucket stat to bucket tree item.
func convertToBucketTreeItem(buckets *metapb.Buckets) *BucketTreeItem {
	items := make([]*BucketStat, len(buckets.Keys)-1)
	interval := buckets.PeriodInMs
	// Interval may be zero after the tikv init.
	if interval == 0 {
		interval = 10 * 1000
	}
	for i := range len(buckets.Keys) - 1 {
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
		interval: interval,
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
