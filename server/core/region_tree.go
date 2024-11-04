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
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"go.uber.org/zap"
)

type regionItem struct {
	*RegionInfo
}

// GetStartKey returns the start key of the region.
func (r *regionItem) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetEndKey returns the end key of the region.
func (r *regionItem) GetEndKey() []byte {
	return r.meta.EndKey
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other *regionItem) bool {
	left := r.meta.StartKey
	right := other.meta.StartKey
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.GetStartKey(), r.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	tree *btree.BTreeG[*regionItem]
	// Statistics
	totalSize           int64
	totalWriteBytesRate float64
	totalWriteKeysRate  float64
	// count the number of regions that not loaded from storage.
	notFromStorageRegionsCnt int
}

func newRegionTree() *regionTree {
	return &regionTree{
		tree:                     btree.NewG[*regionItem](defaultBTreeDegree),
		totalSize:                0,
		totalWriteBytesRate:      0,
		totalWriteKeysRate:       0,
		notFromStorageRegionsCnt: 0,
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
	item := &regionItem{RegionInfo: region}
	result := t.overlaps(item)
	overlaps := make([]*RegionInfo, len(result))
	for i, r := range result {
		overlaps[i] = r.RegionInfo
	}
	return overlaps
}

// GetOverlaps returns the range items that has some intersections with the given items.
func (t *regionTree) overlaps(item *regionItem) []*regionItem {
	// note that Find() gets the last item that is less or equal than the item.
	// in the case: |_______a_______|_____b_____|___c___|
	// new item is     |______d______|
	// Find() will return RangeItem of item_a
	// and both startKey of item_a and item_b are less than endKey of item_d,
	// thus they are regarded as overlapped items.
	result := t.find(item)
	if result == nil {
		result = item
	}
	endKey := item.GetEndKey()
	var overlaps []*regionItem
	t.tree.AscendGreaterOrEqual(result, func(i *regionItem) bool {
		if len(endKey) > 0 && bytes.Compare(endKey, i.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, i)
		return true
	})
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(item *regionItem) []*RegionInfo {
	region := item.RegionInfo
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate
	if !region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt++
	}

	overlaps := t.overlaps(item)
	for _, old := range overlaps {
		t.tree.Delete(old)
	}
	t.tree.ReplaceOrInsert(item)
	result := make([]*RegionInfo, len(overlaps))
	for i, overlap := range overlaps {
		old := overlap.RegionInfo
		result[i] = old
		log.Debug("overlapping region",
			zap.Uint64("region-id", old.GetID()),
			logutil.ZapRedactStringer("delete-region", RegionToHexMeta(old.GetMeta())),
			logutil.ZapRedactStringer("update-region", RegionToHexMeta(region.GetMeta())))
		t.totalSize -= old.approximateSize
		regionWriteBytesRate, regionWriteKeysRate = old.GetWriteRate()
		t.totalWriteBytesRate -= regionWriteBytesRate
		t.totalWriteKeysRate -= regionWriteKeysRate
		if !old.LoadedFromStorage() {
			t.notFromStorageRegionsCnt--
		}
	}

	return result
}

// updateStat is used to update statistics when regionItem.RegionInfo is directly replaced.
func (t *regionTree) updateStat(origin *RegionInfo, region *RegionInfo) {
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	t.totalSize -= origin.approximateSize
	regionWriteBytesRate, regionWriteKeysRate = origin.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate

	// If the region meta information not loaded from storage anymore, decrease the counter.
	if origin.LoadedFromStorage() && !region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt++
	}
	// If the region meta information updated to load from storage, increase the counter.
	if !origin.LoadedFromStorage() && region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt--
	}
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionTree) remove(region *RegionInfo) {
	if t.length() == 0 {
		return
	}
	item := &regionItem{RegionInfo: region}
	result := t.find(item)
	if result == nil || result.GetID() != region.GetID() {
		return
	}

	t.totalSize -= result.GetApproximateSize()
	regionWriteBytesRate, regionWriteKeysRate := result.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
	if !region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt--
	}
	t.tree.Delete(item)
}

// search returns a region that contains the key.
func (t *regionTree) search(regionKey []byte) *RegionInfo {
	region := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	result := t.find(&regionItem{RegionInfo: region})
	if result == nil {
		return nil
	}
	return result.RegionInfo
}

// searchPrev returns the previous region of the region where the regionKey is located.
func (t *regionTree) searchPrev(regionKey []byte) *RegionInfo {
	curRegion := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	curRegionItem := t.find(&regionItem{RegionInfo: curRegion})
	if curRegionItem == nil {
		return nil
	}
	prevRegionItem, _ := t.getAdjacentRegions(curRegionItem.RegionInfo)
	if prevRegionItem == nil {
		return nil
	}
	if !bytes.Equal(prevRegionItem.GetEndKey(), curRegionItem.GetStartKey()) {
		return nil
	}
	return prevRegionItem.RegionInfo
}

// find returns the range item contains the start key.
func (t *regionTree) find(item *regionItem) *regionItem {
	var result *regionItem
	t.tree.DescendLessOrEqual(item, func(i *regionItem) bool {
		result = i
		return false
	})

	if result == nil || !result.Contains(item.GetStartKey()) {
		return nil
	}

	return result
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// find if there is a region with key range [s, d), s < startKey < d
	fn := func(item *regionItem) bool {
		r := item
		return f(r.RegionInfo)
	}
	start := &regionItem{RegionInfo: region}
	startItem := t.find(start)
	if startItem == nil {
		startItem = start
	}
	t.tree.AscendGreaterOrEqual(startItem, func(item *regionItem) bool {
		return fn(item)
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
	item := &regionItem{RegionInfo: &RegionInfo{meta: &metapb.Region{StartKey: region.GetStartKey()}}}
	return t.getAdjacentItem(item)
}

// GetAdjacentItem returns the adjacent range item.
func (t *regionTree) getAdjacentItem(item *regionItem) (prev *regionItem, next *regionItem) {
	t.tree.AscendGreaterOrEqual(item, func(i *regionItem) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		next = i
		return false
	})
	t.tree.DescendLessOrEqual(item, func(i *regionItem) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		prev = i
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

	for _, i := range rand.Perm(len(ranges)) {
		var endIndex int
		startKey, endKey := ranges[i].StartKey, ranges[i].EndKey
		startRegion, startIndex := t.tree.GetWithIndex(&regionItem{RegionInfo: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}})

		if len(endKey) != 0 {
			_, endIndex = t.tree.GetWithIndex(&regionItem{RegionInfo: &RegionInfo{meta: &metapb.Region{StartKey: endKey}}})
		} else {
			endIndex = t.tree.Len()
		}

		// Consider that the item in the tree may not be continuous,
		// we need to check if the previous item contains the key.
		if startIndex != 0 && startRegion == nil && t.tree.GetAt(startIndex-1).Contains(startKey) {
			startIndex--
		}

		if endIndex <= startIndex {
			if len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
				log.Error("wrong range keys",
					logutil.ZapRedactString("start-key", string(HexRegionKey(startKey))),
					logutil.ZapRedactString("end-key", string(HexRegionKey(endKey))),
					errs.ZapError(errs.ErrWrongRangeKeys))
			}
			continue
		}
		index := rand.Intn(endIndex-startIndex) + startIndex
		region := t.tree.GetAt(index).RegionInfo
		if region.isInvolved(startKey, endKey) {
			return region
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
	rand.New(rand.NewSource(time.Now().UnixNano()))
}
