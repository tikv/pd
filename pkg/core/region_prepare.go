// Copyright 2026 TiKV Project Authors.
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

import "github.com/pingcap/failpoint"

const (
	// Bound work under the tree read locks while checking preparation.
	regionPreparationScanBatchSize = 16
	// Bound snapshot traversal scratch space independently of the Region count.
	regionPreparationSnapshotBatchSize = 1024
	// Bound the temporary topology journal during a preparation query.
	regionPreparationMaxChanges = 1 << 16
	// Bound retries when changes arrive during the incremental recheck.
	regionPreparationMaxRounds = 3
)

func sameRegionPreparation(left, right *RegionInfo) bool {
	if left == right {
		return true
	}
	return left != nil && right != nil && left.rangeEqualsTo(right) &&
		left.peersEqualTo(right) && left.LoadedFromStorage() == right.LoadedFromStorage()
}

// updateRegionPreparation is called under the write lock of the tree being updated.
func (r *RegionsInfo) updateRegionPreparation(origin, region *RegionInfo, root bool) {
	if r.preparationChecking {
		r.recordRegionPreparationChange(origin, region, root)
	}
}

func (r *RegionsInfo) recordRegionPreparationChange(origin, region *RegionInfo, root bool) {
	if sameRegionPreparation(origin, region) {
		return
	}
	var id uint64
	if region != nil {
		id = region.GetID()
	} else {
		id = origin.GetID()
	}
	if root {
		if len(r.preparationRootChanges) == regionPreparationMaxChanges {
			r.preparationRootInvalid.Store(true)
			return
		}
		r.preparationRootChanges = append(r.preparationRootChanges, id)
	} else {
		if len(r.preparationSubChanges) == regionPreparationMaxChanges {
			r.preparationSubInvalid.Store(true)
			return
		}
		r.preparationSubChanges = append(r.preparationSubChanges, id)
	}
}

// IsRegionLeaderPrepared verifies that all cached regions have leaders and matching subtrees.
// Concurrent changes that exceed the check's budget make it return false for a later retry.
func (r *RegionsInfo) IsRegionLeaderPrepared() bool {
	r.preparationMu.Lock()
	defer r.preparationMu.Unlock()
	r.t.Lock()
	r.st.Lock()
	if !r.regionLeaderCountsPreparedLocked() {
		r.st.Unlock()
		r.t.Unlock()
		return false
	}
	if len(r.regions) == 0 {
		r.st.Unlock()
		r.t.Unlock()
		return true
	}
	r.preparationChecking = true
	rootSnapshot, subSnapshot := r.tree.tree.Clone(), r.overlapTree.tree.Clone()
	r.st.Unlock()
	r.t.Unlock()
	defer func() {
		r.t.RLock()
		r.st.RLock()
		r.preparationChecking = false
		r.preparationRootChanges = nil
		r.preparationSubChanges = nil
		r.preparationRootInvalid.Store(false)
		r.preparationSubInvalid.Store(false)
		r.st.RUnlock()
		r.t.RUnlock()
	}()
	// Traverse the cloned structure by rank in bounded batches. Looking up a
	// key would read mutable regionItem values outside their owning tree lock.
	var items [regionPreparationSnapshotBatchSize]*regionItem
	var changed map[uint64]struct{}
	mark := func(id uint64) bool {
		if _, ok := changed[id]; ok {
			return true
		}
		if len(changed) == regionPreparationMaxChanges {
			return false
		}
		if changed == nil {
			changed = make(map[uint64]struct{})
		}
		changed[id] = struct{}{}
		return true
	}
	valid, count := true, 0
	subSnapshot.Ascend(func(subItem *regionItem) bool {
		if count == rootSnapshot.Len() {
			valid = false
			return false
		}
		if count%len(items) == 0 {
			filled := 0
			rootSnapshot.AscendFromIndex(count, func(item *regionItem) bool {
				items[filled] = item
				filled++
				return filled < len(items) && !r.preparationRootInvalid.Load() && !r.preparationSubInvalid.Load()
			})
			if filled != min(len(items), rootSnapshot.Len()-count) {
				valid = false
				return false
			}
		}
		root, subtree := items[count%len(items)].loadRegionInfo(), subItem.loadRegionInfo()
		if root == nil || subtree == nil {
			valid = false
			return false
		}
		if root != subtree && (root.GetID() != subtree.GetID() || !sameRegionPreparation(root, subtree)) {
			if !mark(root.GetID()) || !mark(subtree.GetID()) {
				valid = false
				return false
			}
		}
		count++
		if count%regionPreparationScanBatchSize == 0 {
			failpoint.InjectCall("regionPreparationScanBatch", root.GetID())
		}
		valid = !r.preparationRootInvalid.Load() && !r.preparationSubInvalid.Load()
		return valid
	})
	if !valid || count != rootSnapshot.Len() {
		return false
	}
	if count%regionPreparationScanBatchSize != 0 {
		failpoint.InjectCall("regionPreparationScanBatch", items[(count-1)%len(items)].loadRegionInfo().GetID())
	}
	// Recheck captured IDs in small batches outside the tree locks.
	var batch [regionPreparationScanBatchSize]struct {
		id            uint64
		root, subtree *regionItem
	}
	checkBatch := func(count int) bool {
		r.t.RLock()
		for i := range count {
			batch[i].root = r.regions[batch[i].id]
		}
		r.t.RUnlock()
		r.st.RLock()
		for i := range count {
			batch[i].subtree = r.subRegions[batch[i].id]
		}
		r.st.RUnlock()
		for _, pair := range batch[:count] {
			var root, subtree *RegionInfo
			if pair.root != nil {
				root = pair.root.loadRegionInfo()
			}
			if pair.subtree != nil {
				subtree = pair.subtree.loadRegionInfo()
			}
			if !sameRegionPreparation(root, subtree) {
				return false
			}
		}
		failpoint.InjectCall("regionPreparationScanBatch", batch[count-1].id)
		return !r.preparationRootInvalid.Load() && !r.preparationSubInvalid.Load()
	}
	for range regionPreparationMaxRounds {
		rootChanges, subChanges, ok := func() ([]uint64, []uint64, bool) {
			r.t.RLock()
			r.st.RLock()
			defer r.t.RUnlock()
			defer r.st.RUnlock()
			if r.preparationRootInvalid.Load() || r.preparationSubInvalid.Load() {
				return nil, nil, false
			}
			root, subtree := r.preparationRootChanges, r.preparationSubChanges
			r.preparationRootChanges, r.preparationSubChanges = nil, nil
			return root, subtree, true
		}()
		if !ok {
			return false
		}
		for _, ids := range [][]uint64{rootChanges, subChanges} {
			for _, id := range ids {
				if !mark(id) {
					return false
				}
			}
		}
		count = 0
		for id := range changed {
			batch[count].id = id
			count++
			if count == len(batch) {
				if !checkBatch(count) {
					return false
				}
				count = 0
			}
		}
		if count > 0 && !checkBatch(count) {
			return false
		}
		// Finish a small final set under both locks. This permits preparation
		// during matched topology updates without waiting for a quiet interval.
		done, prepared := func() (bool, bool) {
			r.t.RLock()
			r.st.RLock()
			defer r.t.RUnlock()
			defer r.st.RUnlock()
			if r.preparationRootInvalid.Load() || r.preparationSubInvalid.Load() {
				return true, false
			}
			if len(r.preparationRootChanges)+len(r.preparationSubChanges) > regionPreparationScanBatchSize {
				return false, false
			}
			for _, ids := range [][]uint64{r.preparationRootChanges, r.preparationSubChanges} {
				for _, id := range ids {
					var root, subtree *RegionInfo
					if item := r.regions[id]; item != nil {
						root = item.RegionInfo
					}
					if item := r.subRegions[id]; item != nil {
						subtree = item.RegionInfo
					}
					if !sameRegionPreparation(root, subtree) {
						return true, false
					}
				}
			}
			return true, r.regionLeaderCountsPreparedLocked()
		}()
		if done {
			return prepared
		}
		clear(changed)
	}
	return false
}

// regionLeaderCountsPreparedLocked requires both tree locks.
func (r *RegionsInfo) regionLeaderCountsPreparedLocked() bool {
	count := 0
	for _, leaders := range r.leaders {
		count += leaders.length()
	}
	return count == len(r.subRegions) && count == len(r.regions) &&
		r.tree.length() == count && r.overlapTree.length() == count
}
