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

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/rangetree"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

var minHotThresholds [utils.RegionStatCount]uint64

func init() {
	for i := range minHotThresholds {
		minHotThresholds[i] = uint64(utils.MinHotThresholds[i])
	}
}

// BucketStatInformer is used to get the bucket statistics.
type BucketStatInformer interface {
	BucketsStats(degree int, regions ...uint64) map[uint64][]*BucketStat
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	HotDegree int
	Interval  uint64
	// the order should see utils.RegionStatKind
	Loads []uint64
}

func (b *BucketStat) clone() *BucketStat {
	c := &BucketStat{
		RegionID:  b.RegionID,
		HotDegree: b.HotDegree,
		Interval:  b.Interval,
		StartKey:  b.StartKey,
		EndKey:    b.EndKey,
		Loads:     make([]uint64, len(b.Loads)),
	}
	copy(c.Loads, b.Loads)
	return c
}

func (b *BucketStat) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key-key:%s][hot-degree:%d][Interval:%d(ms)][Loads:%v]",
		b.RegionID, core.HexRegionKeyStr(b.StartKey), core.HexRegionKeyStr(b.EndKey), b.HotDegree, b.Interval, b.Loads)
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

// GetStartKey returns the start key of the bucket tree.
func (b *BucketTreeItem) GetStartKey() []byte {
	return b.startKey
}

// GetEndKey return the end key of the bucket tree item.
func (b *BucketTreeItem) GetEndKey() []byte {
	return b.endKey
}

// String implements the fmt.Stringer interface.
func (b *BucketTreeItem) String() string {
	return fmt.Sprintf("[region-id:%d][start-key:%s][end-key:%s]",
		b.regionID, core.HexRegionKeyStr(b.startKey), core.HexRegionKeyStr(b.endKey))
}

// Less returns true if the start key is less than the other.
func (b *BucketTreeItem) Less(than rangetree.RangeItem) bool {
	return bytes.Compare(b.startKey, than.(*BucketTreeItem).startKey) < 0
}

// equals returns whether the key range is overlaps with the item.
func (b *BucketTreeItem) equals(origin *BucketTreeItem) bool {
	if origin == nil {
		return false
	}
	return bytes.Equal(b.startKey, origin.startKey) && bytes.Equal(b.endKey, origin.endKey)
}

// cloneBucketItemByRange returns a new item with the same key range.
// item must have some debris for the given key range
func (b *BucketTreeItem) cloneBucketItemByRange(startKey, endKey []byte) *BucketTreeItem {
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
		left := keyutil.MaxKey(stat.StartKey, startKey)
		right := keyutil.MinKey(stat.EndKey, endKey)
		if len(endKey) == 0 {
			right = stat.EndKey
		}
		if bytes.Compare(left, right) < 0 {
			copy := stat.clone()
			copy.StartKey = left
			copy.EndKey = right
			item.stats = append(item.stats, copy)
		}
	}
	return item
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
	// given two list of closed intervals like newItems and oldItems, where items[i]=[start-key,end-key],
	// and each item are disjoint and sorted order.
	// It should calculate the value if some item has intersection.
	for p1, p2 := 0, 0; p1 < len(newItems) && p2 < len(oldItems); {
		newItem, oldItem := newItems[p1], oldItems[p2]
		left := keyutil.MaxKey(newItem.StartKey, oldItem.StartKey)
		right := keyutil.MinKey(newItem.EndKey, oldItem.EndKey)

		// bucket should inherit the old bucket hot degree if they have some intersection.
		// skip if the left is equal to the right key, such as [10 20] [20 30].
		// new bucket:         					|10 ---- 20 |
		// old bucket: 					| 5 ---------15|
		// they has one intersection 			|10--15|.
		if bytes.Compare(left, right) < 0 || len(right) == 0 {
			oldDegree := oldItem.HotDegree
			newDegree := newItem.HotDegree
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
		if bytes.Compare(newItem.EndKey, oldItem.EndKey) > 0 || len(newItem.EndKey) == 0 {
			p2++
		} else {
			p1++
		}
	}
}

func (b *BucketTreeItem) calculateHotDegree() {
	for _, stat := range b.stats {
		// TODO: qps should be considered, tikv will report this in next sprint
		// the order: read [bytes keys qps] and write[bytes keys qps]
		readLoads := stat.Loads[:2]
		// keep same with the hot region hot degree
		// https://github.com/tikv/pd/blob/6f6f545a6716840f7e2c7f4d8ed9b49f613a5cd8/pkg/statistics/hot_peer_cache.go#L220-L222
		readHot := slice.AnyOf(readLoads, func(i int) bool {
			return readLoads[i] > minHotThresholds[i]
		})
		writeLoads := stat.Loads[3:5]
		writeHot := slice.AnyOf(writeLoads, func(i int) bool {
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

// collectBucketsMetrics collects the metrics of the hot stats.
func (b *BucketTreeItem) collectBucketsMetrics() {
	for _, bucket := range b.stats {
		bucketsHotDegreeHist.Observe(float64(bucket.HotDegree))
	}
}
