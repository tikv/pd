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
	"math/rand/v2"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// regionItem is the value a region tree stores. It holds a *RegionInfo and
// supplies the btree ordering for it.
//
// # Ordering immutability
//
// Once a regionItem has been inserted into a tree, its key range must never
// change. The RegionInfo it holds may only be replaced by one whose start and end
// keys are identical, see RegionInfo.rangeEqualsTo. A range change must allocate a
// fresh regionItem instead; setRegionLocked does that.
//
// This is what lets an O(1) copy-on-write btree Clone serve as a usable read-only
// snapshot: a clone shares regionItem pointers with the live tree, so changing a
// shared item's range would leave it at the wrong position in the clone and
// silently break the clone's ordering. See rootRangeSnapshot.
//
// # Publish-once
//
// The RegionInfo is held atomically so that snapshot readers can walk a cloned
// tree without holding the tree's lock. The atomic protects the slot only. It does
// not protect the RegionInfo, its metapb.Region, or its key slices, so a
// *RegionInfo that has been stored here must never be mutated afterwards. The
// exceptions are the fields that carry their own atomics: RegionInfo.ref and
// RegionInfo.reportBuckets.
type regionItem struct {
	region atomic.Pointer[RegionInfo]
}

// newRegionItem returns a regionItem holding the given region.
func newRegionItem(region *RegionInfo) *regionItem {
	item := &regionItem{}
	item.region.Store(region)
	return item
}

// getRegion returns the RegionInfo held by the item.
func (r *regionItem) getRegion() *RegionInfo {
	return r.region.Load()
}

// setRegion replaces the RegionInfo held by the item.
//
// The caller must hold the write lock of every tree the item belongs to, and the
// new region must cover the same key range as the current one. See the ordering
// immutability note on regionItem.
func (r *regionItem) setRegion(region *RegionInfo) {
	r.region.Store(region)
}

// GetStartKey returns the start key of the region.
func (r *regionItem) GetStartKey() []byte {
	return r.getRegion().meta.StartKey
}

// GetID returns the ID of the region.
func (r *regionItem) GetID() uint64 {
	return r.getRegion().meta.GetId()
}

// GetEndKey returns the end key of the region.
func (r *regionItem) GetEndKey() []byte {
	return r.getRegion().meta.EndKey
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other *regionItem) bool {
	left := r.getRegion().meta.StartKey
	right := other.getRegion().meta.StartKey
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

// GetCountByRange returns the number of regions in the range [startKey, endKey).
func (t *regionTree) GetCountByRange(startKey, endKey []byte) int {
	start := newRegionItem(&RegionInfo{meta: &metapb.Region{StartKey: startKey}})
	end := newRegionItem(&RegionInfo{meta: &metapb.Region{StartKey: endKey}})
	// it returns 0 if startKey is nil.
	item, startIndex := t.tree.GetWithIndex(start)
	// if item is nil, it means that the startKey is not found in the tree, we need to check the previous item, avoid
	// to the startKey in the previous iterm.
	// regions: [a c] [c f] [f h], startKey: b
	// the first item is index 2 [c,f]
	if item == nil {
		item = t.tree.GetAt(startIndex - 1)
		// if the item is not nil and the start key in the previous item range, the previous should be included.
		if item != nil && bytes.Compare(item.GetEndKey(), startKey) > 0 {
			startIndex--
		}
	}
	var endIndex int
	// it should return the length of the tree if endKey is nil.
	if len(endKey) == 0 {
		endIndex = t.tree.Len()
	} else {
		_, endIndex = t.tree.GetWithIndex(end)
	}
	return endIndex - startIndex
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
		overlaps = append(overlaps, i.getRegion())
		return true
	})
	return overlaps
}

// updateRef transfers the reference from origin to region when an item is
// replaced in place, i.e. the tree keeps the same item and only its embedded
// RegionInfo changes. It is a no-op for trees that do not count references.
func (t *regionTree) updateRef(origin, region *RegionInfo) {
	if !t.countRef {
		return
	}
	origin.DecRef()
	region.IncRef()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(item *regionItem, withOverlaps bool, overlaps ...*RegionInfo) []*RegionInfo {
	region := item.getRegion()
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
		t.tree.Delete(newRegionItem(old))
	}
	t.tree.ReplaceOrInsert(item)
	if t.countRef {
		item.getRegion().IncRef()
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
	t.updateRef(origin, region)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionTree) remove(region *RegionInfo) {
	if t.length() == 0 {
		return
	}
	item := newRegionItem(region)
	result := t.find(item)
	if result == nil || result.GetID() != region.GetID() {
		return
	}

	t.totalSize -= result.getRegion().GetApproximateSize()
	regionWriteBytesRate, regionWriteKeysRate := result.getRegion().GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
	if t.countRef {
		result.getRegion().DecRef()
	}
	if !region.LoadedFromStorage() {
		t.notFromStorageRegionsCnt--
	}
	t.tree.Delete(item)
}

// search returns a region that contains the key.
func (t *regionTree) search(regionKey []byte) *RegionInfo {
	region := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	result := t.find(newRegionItem(region))
	if result == nil {
		return nil
	}
	return result.getRegion()
}

// searchPrev returns the previous region of the region where the regionKey is located.
func (t *regionTree) searchPrev(regionKey []byte) *RegionInfo {
	curRegion := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	curRegionItem := t.find(newRegionItem(curRegion))
	if curRegionItem == nil {
		return nil
	}
	prevRegionItem, _ := t.getAdjacentRegions(curRegionItem.getRegion())
	if prevRegionItem == nil {
		return nil
	}
	if !bytes.Equal(prevRegionItem.GetEndKey(), curRegionItem.GetStartKey()) {
		return nil
	}
	return prevRegionItem.getRegion()
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

// rootRangeSnapshot is a read-only view of a region tree, produced by an O(1)
// copy-on-write btree Clone. A caller can scan an arbitrarily large key range
// from a snapshot without holding the tree's lock for the duration of the scan.
//
// # What a snapshot freezes
//
// The tree's shape: the set of items, each item's key range, and therefore the
// key-space coverage, the iteration order and the length. Regions created, split,
// merged or removed after the snapshot was taken are invisible to it, and regions
// removed after it was taken stay visible to it.
//
// This is what a chunked scan cannot offer. A scan that releases the lock and
// re-enters the tree at the last end key can see the key space change underneath
// it: a split at a chunk boundary yields a region counted twice, and a merge that
// swallows the boundary region leaves a range no chunk covers. A snapshot has no
// second scan to be inconsistent with.
//
// # What a snapshot does not freeze
//
// The values. The RegionInfo behind an item may be replaced concurrently by the
// heartbeat path, but only by one covering the same key range, see regionItem. So
// a scan observes exactly one region per key, and for each of them either the
// value that was live when the snapshot was taken or a later value for the same
// range, with fresher epoch, leader, peers or statistics.
//
// A scan of a live tree under one read lock is stronger than this: there, every
// region comes from a single instant. Callers that need agreement between two
// different regions' values, rather than agreement about which key ranges exist,
// must not use a snapshot.
//
// # References and lifetime
//
// A snapshot deliberately takes no reference on the regions it holds, see
// RegionInfo.IncRef. Doing so would make it O(n), and the reference count carries
// the functional "already present in the subtree" signal that RaftCluster relies
// on. A region reached through a snapshot may therefore already be gone from the
// live tree.
//
// A snapshot pins the btree nodes that existed when it was taken. Those nodes can
// no longer be recycled through the shared free list, and the regions they point
// at stay reachable. Keep a snapshot function-scoped and short-lived; never store
// one in a long-lived struct.
type rootRangeSnapshot struct {
	tree *btree.BTreeG[*regionItem]
}

// snapshot returns a read-only view of the tree.
//
// The caller must hold the tree's *write* lock. btree Clone rewrites the source
// tree's copy-on-write context, so it must not run concurrently with a write or
// with another Clone; see btree.BTreeG.Clone.
func (t *regionTree) snapshot() *rootRangeSnapshot {
	return &rootRangeSnapshot{tree: t.tree.Clone()}
}

// scanRange scans from the first region containing or behind the start key until
// f returns false. It takes no lock.
func (s *rootRangeSnapshot) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	scanTreeRange(s.tree, startKey, f)
}

// length returns the number of regions the snapshot holds.
func (s *rootRangeSnapshot) length() int {
	return s.tree.Len()
}

// findItem returns the item in tree whose key range contains the start key of
// the given item.
//
// It only reads tree, so it is also safe to call on a snapshot. See
// rootRangeSnapshot.
func findItem(tree *btree.BTreeG[*regionItem], item *regionItem) *regionItem {
	var result *regionItem
	tree.DescendLessOrEqual(item, func(i *regionItem) bool {
		result = i
		return false
	})

	if result == nil || !result.getRegion().contain(item.GetStartKey()) {
		return nil
	}

	return result
}

// scanTreeRange scans tree from the first region containing or behind the start
// key until f returns false.
//
// It only reads tree, so it is also safe to call on a snapshot. See
// rootRangeSnapshot.
func scanTreeRange(tree *btree.BTreeG[*regionItem], startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	start := newRegionItem(region)
	// find if there is a region with key range [s, d), s <= startKey < d
	startItem := findItem(tree, start)
	if startItem == nil {
		startItem = start
	}
	tree.AscendGreaterOrEqual(startItem, func(item *regionItem) bool {
		return f(item.getRegion())
	})
}

// find returns the range item contains the start key.
func (t *regionTree) find(item *regionItem) *regionItem {
	return findItem(t.tree, item)
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	scanTreeRange(t.tree, startKey, f)
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

func (t *regionTree) getAdjacentRegions(region *RegionInfo) (prev, next *regionItem) {
	item := newRegionItem(&RegionInfo{meta: &metapb.Region{StartKey: region.GetStartKey()}})
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

func (t *regionTree) randomRegion(ranges []keyutil.KeyRange) *RegionInfo {
	regions := t.RandomRegions(1, ranges)
	if len(regions) == 0 {
		return nil
	}
	return regions[0]
}

// RandomRegions get n random regions within the given ranges.
func (t *regionTree) RandomRegions(n int, ranges []keyutil.KeyRange) []*RegionInfo {
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
		// pivotRegion is the region behind pivotItem. pivotItem is a scratch item
		// that is only ever used to look up a key, and is never inserted into the
		// tree, so its start key may be rewritten in place through pivotRegion.
		// That does not violate the ordering immutability of tree-resident items.
		pivotRegion = &RegionInfo{meta: &metapb.Region{}}
		pivotItem   = newRegionItem(pivotRegion)
		region      *RegionInfo
		regions     = make([]*RegionInfo, 0, n)
		curLen      = len(regions)
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
			pivotRegion.meta.StartKey = startKey
			startItem, startIndex = t.tree.GetWithIndex(pivotItem)
			if endKeyLen > 0 {
				pivotRegion.meta.StartKey = endKey
				_, endIndex = t.tree.GetWithIndex(pivotItem)
			} else {
				endIndex = treeLen
			}
			// Consider that the item in the tree may not be continuous,
			// we need to check if the previous item contains the key.
			if startIndex != 0 && startItem == nil {
				region = t.tree.GetAt(startIndex - 1).getRegion()
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
	if len(ranges) <= 1 {
		if len(ranges) == 1 {
			startKey, endKey = ranges[0].StartKey, ranges[0].EndKey
			if setAndCheckStartEndIndices() {
				return regions
			}
		}
		for curLen < n {
			randIndex = rand.IntN(endIndex-startIndex) + startIndex
			region = t.tree.GetAt(randIndex).getRegion()
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
		for _, i := range rand.Perm(len(ranges)) {
			startKey, endKey = ranges[i].StartKey, ranges[i].EndKey
			if setAndCheckStartEndIndices() {
				continue
			}

			randIndex = rand.IntN(endIndex-startIndex) + startIndex
			region = t.tree.GetAt(randIndex).getRegion()
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
