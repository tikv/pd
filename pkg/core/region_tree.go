// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"bytes"
	"math/rand"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
)

type regionItem struct {
	*RegionInfo
}

// GetStartKey returns the start key of the region.
func (r *regionItem) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetID returns the ID of the region.
func (r *regionItem) GetID() uint64 {
	return r.meta.GetId()
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
	// count reference of RegionInfo
	countRef bool
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

func newRegionTreeWithCountRef() *regionTree {
	return &regionTree{
		tree:                     btree.NewG[*regionItem](defaultBTreeDegree),
		totalSize:                0,
		totalWriteBytesRate:      0,
		totalWriteKeysRate:       0,
		notFromStorageRegionsCnt: 0,
		countRef:                 true,
	}
}

func (t *regionTree) length() int {
	if t == nil {
		return 0
	}
	return t.tree.Len()
}

func (t *regionTree) notFromStorageRegionsCount() int {
	if t == nil {
		return 0
	}
	return t.notFromStorageRegionsCnt
}

// GetOverlaps returns the range items that has some intersections with the given items.
func (t *regionTree) overlaps(item *regionItem) []*RegionInfo {
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
	var overlaps []*RegionInfo
	t.tree.AscendGreaterOrEqual(result, func(i *regionItem) bool {
		if len(endKey) > 0 && bytes.Compare(endKey, i.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, i.RegionInfo)
		return true
	})
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(item *regionItem, withOverlaps bool, overlaps ...*RegionInfo) []*RegionInfo {
	region := item.RegionInfo
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate
	if !region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt++
	}

	if !withOverlaps {
		overlaps = t.overlaps(item)
	}

	for _, old := range overlaps {
		t.tree.Delete(&regionItem{RegionInfo: old})
	}
	t.tree.ReplaceOrInsert(item)
	if t.countRef {
		item.RegionInfo.IncRef()
	}
	result := make([]*RegionInfo, len(overlaps))
	for i, overlap := range overlaps {
		old := overlap
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
		if t.countRef {
			old.DecRef()
		}
	}

	return result
}

// updateStat is used to update statistics when RegionInfo is directly replaced.
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
	if t.countRef {
		origin.DecRef()
		region.IncRef()
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
	if t.countRef {
		result.RegionInfo.DecRef()
	}
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

// searchByKeys searches the regions by keys and return a slice of `*RegionInfo` whose order is the same as the input keys.
func (t *regionTree) searchByKeys(keys [][]byte) []*RegionInfo {
	regions := make([]*RegionInfo, len(keys))
	// TODO: do we need to deduplicate the input keys?
	for idx, key := range keys {
		regions[idx] = t.search(key)
	}
	return regions
}

// searchByPrevKeys searches the regions by prevKeys and return a slice of `*RegionInfo` whose order is the same as the input keys.
func (t *regionTree) searchByPrevKeys(prevKeys [][]byte) []*RegionInfo {
	regions := make([]*RegionInfo, len(prevKeys))
	// TODO: do we need to deduplicate the input keys?
	for idx, key := range prevKeys {
		regions[idx] = t.searchPrev(key)
	}
	return regions
}

// find returns the range item contains the start key.
func (t *regionTree) find(item *regionItem) *regionItem {
	var result *regionItem
	t.tree.DescendLessOrEqual(item, func(i *regionItem) bool {
		result = i
		return false
	})

	if result == nil || !result.contain(item.GetStartKey()) {
		return nil
	}

	return result
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// find if there is a region with key range [s, d), s <= startKey < d
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

func (t *regionTree) randomRegion(ranges []KeyRange) *RegionInfo {
	regions := t.RandomRegions(1, ranges)
	if len(regions) == 0 {
		return nil
	}
	return regions[0]
}

// RandomRegions get n random regions within the given ranges.
func (t *regionTree) RandomRegions(n int, ranges []KeyRange) []*RegionInfo {
	treeLen := t.length()
	if treeLen == 0 || n < 1 {
		return nil
	}
	// Pre-allocate the variables to reduce the temporary memory allocations.
	var (
		startKey, endKey []byte
		// By default, we set the `startIndex` and `endIndex` to the whole tree range.
		startIndex, endIndex = 0, treeLen
		randIndex            int
		startItem            *regionItem
		pivotItem            = &regionItem{&RegionInfo{meta: &metapb.Region{}}}
		region               *RegionInfo
		regions              = make([]*RegionInfo, 0, n)
		rangeLen, curLen     = len(ranges), len(regions)
		// setStartEndIndices is a helper function to set `startIndex` and `endIndex`
		// according to the `startKey` and `endKey` and check if the range is invalid
		// to skip the iteration.
		// TODO: maybe we could cache the `startIndex` and `endIndex` for each range.
		setAndCheckStartEndIndices = func() (skip bool) {
			startKeyLen, endKeyLen := len(startKey), len(endKey)
			if startKeyLen == 0 && endKeyLen == 0 {
				startIndex, endIndex = 0, treeLen
				return false
			}
			pivotItem.meta.StartKey = startKey
			startItem, startIndex = t.tree.GetWithIndex(pivotItem)
			if endKeyLen > 0 {
				pivotItem.meta.StartKey = endKey
				_, endIndex = t.tree.GetWithIndex(pivotItem)
			} else {
				endIndex = treeLen
			}
			// Consider that the item in the tree may not be continuous,
			// we need to check if the previous item contains the key.
			if startIndex != 0 && startItem == nil {
				region = t.tree.GetAt(startIndex - 1).RegionInfo
				if region.contain(startKey) {
					startIndex--
				}
			}
			// Check whether the `startIndex` and `endIndex` are valid.
			if endIndex <= startIndex {
				if endKeyLen > 0 && bytes.Compare(startKey, endKey) > 0 {
					log.Error("wrong range keys",
						logutil.ZapRedactString("start-key", string(HexRegionKey(startKey))),
						logutil.ZapRedactString("end-key", string(HexRegionKey(endKey))),
						errs.ZapError(errs.ErrWrongRangeKeys))
				}
				return true
			}
			return false
		}
	)
	// This is a fast path to reduce the unnecessary iterations when we only have one range.
	if rangeLen <= 1 {
		if rangeLen == 1 {
			startKey, endKey = ranges[0].StartKey, ranges[0].EndKey
			if setAndCheckStartEndIndices() {
				return regions
			}
		}
		for curLen < n {
			randIndex = rand.Intn(endIndex-startIndex) + startIndex
			region = t.tree.GetAt(randIndex).RegionInfo
			if region.isInvolved(startKey, endKey) {
				regions = append(regions, region)
				curLen++
			}
			// No region found, directly break to avoid infinite loop.
			if curLen == 0 {
				break
			}
		}
		return regions
	}
	// When there are multiple ranges provided,
	// keep retrying until we get enough regions.
	for curLen < n {
		// Shuffle the ranges to increase the randomness.
		for _, i := range rand.Perm(rangeLen) {
			startKey, endKey = ranges[i].StartKey, ranges[i].EndKey
			if setAndCheckStartEndIndices() {
				continue
			}

			randIndex = rand.Intn(endIndex-startIndex) + startIndex
			region = t.tree.GetAt(randIndex).RegionInfo
			if region.isInvolved(startKey, endKey) {
				regions = append(regions, region)
				curLen++
				if curLen == n {
					return regions
				}
			}
		}
		// No region found, directly break to avoid infinite loop.
		if curLen == 0 {
			break
		}
	}
	return regions
}

// TotalSize returns the total size of all regions.
func (t *regionTree) TotalSize() int64 {
	if t.length() == 0 {
		return 0
	}
	return t.totalSize
}

// TotalWriteRate returns the total write bytes rate and the total write keys
// rate of all regions.
func (t *regionTree) TotalWriteRate() (bytesRate, keysRate float64) {
	if t.length() == 0 {
		return 0, 0
	}
	return t.totalWriteBytesRate, t.totalWriteKeysRate
}
