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
	"github.com/tikv/pd/pkg/rangetree"
	"go.uber.org/zap"
)

var _ rangetree.RangeItem = &regionItem{}

type regionItem struct {
	region *RegionInfo
	// it may be 0 if it's not in store dedicated region tree
	storeID uint64
}

// GetStartKey returns the start key of the region.
func (r *regionItem) GetStartKey() []byte {
	return r.region.GetStartKey()
}

// GetEndKey returns the end key of the region.
func (r *regionItem) GetEndKey() []byte {
	return r.region.GetEndKey()
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(rangetree.RangeItem).GetStartKey()
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
	tree *rangetree.RangeTree
	// Statistics
	totalSize           int64
	totalWriteBytesRate float64
	totalWriteKeysRate  float64
}

func newRegionTree() *regionTree {
	factory := func(_, _ []byte, _ rangetree.RangeItem) []rangetree.RangeItem {
		return nil
	}
	return &regionTree{
		tree:                rangetree.NewRangeTree(defaultBTreeDegree, factory),
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
	result := t.tree.GetOverlaps(item)
	overlaps := make([]*RegionInfo, len(result))
	for i, r := range result {
		overlaps[i] = r.(*regionItem).region
	}
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(item *regionItem) []*RegionInfo {
	region := item.region
	t.totalSize += region.GetStorePeerApproximateSize(item.storeID)
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	overlaps := t.tree.Update(item)
	result := make([]*RegionInfo, len(overlaps))
	for i, overlap := range overlaps {
		old := overlap.(*regionItem).region
		result[i] = old
		log.Debug("overlapping region",
			zap.Uint64("region-id", old.GetID()),
			logutil.ZapRedactStringer("delete-region", RegionToHexMeta(old.GetMeta())),
			logutil.ZapRedactStringer("update-region", RegionToHexMeta(region.GetMeta())))
		t.totalSize -= old.GetStorePeerApproximateSize(overlap.(*regionItem).storeID)
		regionWriteBytesRate, regionWriteKeysRate = old.GetWriteRate()
		t.totalWriteBytesRate -= regionWriteBytesRate
		t.totalWriteKeysRate -= regionWriteKeysRate
	}

	return result
}

// updateStat is used to update statistics when regionItem.region is directly replaced.
func (t *regionTree) updateStat(origin *RegionInfo, region *RegionInfo) {
	t.totalSize += region.GetApproximateSize()
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	t.totalSize -= origin.GetApproximateSize()
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
	item := &regionItem{region: region}
	result := t.tree.Find(item).(*regionItem)
	if result == nil || result.region.GetID() != region.GetID() {
		return
	}

	t.totalSize -= result.region.GetStorePeerApproximateSize(result.storeID)
	regionWriteBytesRate, regionWriteKeysRate := result.region.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
	t.tree.Remove(result)
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
	item := t.tree.Find(&regionItem{region: region})
	if item == nil {
		return nil
	}
	if result, ok := item.(*regionItem); ok && result.Contains(region.GetStartKey()) {
		return result
	}
	return nil
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// find if there is a region with key range [s, d), s < startKey < d
	fn := func(item rangetree.RangeItem) bool {
		r := item.(*regionItem)
		return f(r.region)
	}
	t.tree.ScanRange(&regionItem{region: region}, fn)
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
	prevItem, nextItem := t.tree.GetAdjacentItem(item)
	var prev, next *regionItem
	if prevItem != nil {
		prev = prevItem.(*regionItem)
	}
	if nextItem != nil {
		next = nextItem.(*regionItem)
	}
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
		startRegion, startIndex := t.tree.GetWithIndex(&regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}})

		if len(endKey) != 0 {
			_, endIndex = t.tree.GetWithIndex(&regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: endKey}}})
		} else {
			endIndex = t.tree.Len()
		}

		// Consider that the item in the tree may not be continuous,
		// we need to check if the previous item contains the key.
		if startIndex != 0 && startRegion == nil && t.tree.GetAt(startIndex-1).(*regionItem).Contains(startKey) {
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
		region := t.tree.GetAt(index).(*regionItem).region
		if isInvolved(region, startKey, endKey) {
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
	rand.Seed(time.Now().UnixNano())
}
