// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	//"github.com/tikv/pd/pkg/btree"
	"github.com/tidwall/btree"
	"github.com/tikv/pd/pkg/logutil"
	"go.uber.org/zap"
)

type regionItem struct {
	region *RegionInfo
}

// byRegionItem is a comparison function that compares regionItems and returns true
// when a is less than b.
func byRegionItem(a, b interface{}) bool {
	i1, i2 := a.(*regionItem), b.(*regionItem)
	return i1.Less(i2)
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other *regionItem) bool {
	left := r.region.GetStartKey()
	right := other.region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	tree *btree.BTree
	// Statistics
	totalSize           int64
	totalWriteBytesRate float64
	totalWriteKeysRate  float64
}

func newRegionTree() *regionTree {
	return &regionTree{
		tree:                btree.New(byRegionItem),
		totalSize:           0,
		totalWriteBytesRate: 0,
		totalWriteKeysRate:  0,
	}
}

func (t *regionTree) length() int {
	if t == nil {
		return 0
	}
	return t.tree.Len()
}

// getOverlaps gets the regions which are overlapped with the specified region range.
func (t *regionTree) getOverlaps(region *RegionInfo) []*RegionInfo {
	item := &regionItem{region: region}

	// note that find() gets the first(minimum) item that is less or equal than the region.
	// in the case: |_______a_______|_____b_____|___c___|
	// new region is     |______d______|
	// find() will return regionItem of region_a
	// and both startKey of region_a and region_b are less than endKey of region_d,
	// thus they are regarded as overlapped regions.
	result := t.find(region)
	if result == nil {
		result = item
	}

	var overlaps []*RegionInfo

	// t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
	// 	over := i.(*regionItem)
	// 	if len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), over.region.GetStartKey()) <= 0 {
	// 		return false
	// 	}
	// 	overlaps = append(overlaps, over.region)
	// 	return true
	// })
	t.tree.Ascend(result, func(i interface{}) bool {
		over := i.(*regionItem)
		if len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), over.region.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, over.region)
		return true
	})
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(item *regionItem) []*RegionInfo {
	region := item.region
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	overlaps := t.getOverlaps(region)
	for _, old := range overlaps {
		log.Debug("overlapping region",
			zap.Uint64("region-id", old.GetID()),
			logutil.ZapRedactStringer("delete-region", RegionToHexMeta(old.GetMeta())),
			logutil.ZapRedactStringer("update-region", RegionToHexMeta(region.GetMeta())))
		t.tree.Delete(&regionItem{old})
		t.totalSize -= old.approximateSize
		regionWriteBytesRate, regionWriteKeysRate = old.GetWriteRate()
		t.totalWriteBytesRate -= regionWriteBytesRate
		t.totalWriteKeysRate -= regionWriteKeysRate
	}
	t.tree.Set(item)
	return overlaps
}

// updateStat is used to update statistics when regionItem.region is directly replaced.
func (t *regionTree) updateStat(origin *RegionInfo, region *RegionInfo) {
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	t.totalSize -= origin.approximateSize
	regionWriteBytesRate, regionWriteKeysRate = origin.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionTree) remove(region *RegionInfo) {
	if t.length() == 0 {
		return
	}
	result := t.find(region)
	if result == nil || result.region.GetID() != region.GetID() {
		return
	}

	t.totalSize -= region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *regionTree) search(regionKey []byte) *RegionInfo {
	region := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	result := t.find(region)
	if result == nil {
		return nil
	}
	return result.region
}

// searchPrev returns the previous region of the region where the regionKey is located.
func (t *regionTree) searchPrev(regionKey []byte) *RegionInfo {
	curRegion := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	curRegionItem := t.find(curRegion)
	if curRegionItem == nil {
		return nil
	}
	prevRegionItem, _ := t.getAdjacentRegions(curRegionItem.region)
	if prevRegionItem == nil {
		return nil
	}
	if !bytes.Equal(prevRegionItem.region.GetEndKey(), curRegionItem.region.GetStartKey()) {
		return nil
	}
	return prevRegionItem.region
}

// find is a helper function to find an item that contains the regions start
// key.
func (t *regionTree) find(region *RegionInfo) *regionItem {
	item := &regionItem{region: region}

	var result *regionItem
	t.tree.Descend(item, func(i interface{}) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || !result.Contains(region.GetStartKey()) {
		return nil
	}

	return result
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// find if there is a region with key range [s, d), s < startKey < d
	startItem := t.find(region)
	if startItem == nil {
		startItem = &regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}}
	}
	t.tree.Ascend(startItem, func(item interface{}) bool {
		return f(item.(*regionItem).region)
	})
}

func (t *regionTree) scanRanges() []*RegionInfo {
	if t.length() == 0 {
		return nil
	}
	var res []*RegionInfo
	t.scanRange([]byte(""), func(region *RegionInfo) bool {
		res = append(res, region)
		return true
	})
	return res
}

func (t *regionTree) getAdjacentRegions(region *RegionInfo) (*regionItem, *regionItem) {
	item := &regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: region.GetStartKey()}}}
	var prev, next *regionItem
	t.tree.Ascend(item, func(i interface{}) bool {
		if bytes.Equal(item.region.GetStartKey(), i.(*regionItem).region.GetStartKey()) {
			return true
		}
		next = i.(*regionItem)
		return false
	})
	t.tree.Descend(item, func(i interface{}) bool {
		if bytes.Equal(item.region.GetStartKey(), i.(*regionItem).region.GetStartKey()) {
			return true
		}
		prev = i.(*regionItem)
		return false
	})
	return prev, next
}

// RandomRegion is used to get a random region within ranges.
func (t *regionTree) RandomRegion(ranges []KeyRange) *RegionInfo {
	if t.length() == 0 {
		return nil
	}

	if len(ranges) == 0 {
		ranges = []KeyRange{NewKeyRange("", "")}
	}

	println("tree len", t.tree.Len())
	if t.tree.Len() == 0 {
		return nil
	}
	for _, i := range rand.Perm(len(ranges)) {
		startKey, endKey := ranges[i].StartKey, ranges[i].EndKey
		// if len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
		// 	log.Error("wrong range keys",
		// 		logutil.ZapRedactString("start-key", string(HexRegionKey(startKey))),
		// 		logutil.ZapRedactString("end-key", string(HexRegionKey(endKey))),
		// 		errs.ZapError(errs.ErrWrongRangeKeys))
		// 	continue
		// }

		var curRegion *regionItem
		item := &regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}}
		t.tree.Ascend(item, func(i interface{}) bool {
			curRegion = i.(*regionItem)
			return false
		})

		if curRegion == nil {
			curRegion = t.find(&RegionInfo{meta: &metapb.Region{StartKey: endKey}})
		}
		if curRegion == nil {
			curRegion = t.tree.GetAt(0).(*regionItem)
		}
		region := curRegion.region

		if isInvolved(region, startKey, endKey) {
			return region
		}
		println("startkey", string(startKey), "endkeyL", string(endKey))
		println("regioninfo", string(region.GetStartKey()), "end:", string(region.GetEndKey()))
		if len(startKey) == 0 && len(endKey) == 0 {
			println("xxxxxx", region.GetStartKey())
		}
	}
	return nil
}

func (t *regionTree) RandomRegions(n int, ranges []KeyRange) []*RegionInfo {
	if t.length() == 0 {
		return nil
	}

	regions := make([]*RegionInfo, 0, n)

	for i := 0; i < n; i++ {
		if region := t.RandomRegion(ranges); region != nil {
			regions = append(regions, region)
		}
	}
	return regions
}

func (t *regionTree) TotalSize() int64 {
	if t.length() == 0 {
		return 0
	}
	return t.totalSize
}

func (t *regionTree) TotalWriteRate() (bytesRate, keysRate float64) {
	if t.length() == 0 {
		return 0, 0
	}
	return t.totalWriteBytesRate, t.totalWriteKeysRate
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
