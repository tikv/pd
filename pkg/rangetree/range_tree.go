package rangetree

import (
	"bytes"

	"github.com/tikv/pd/pkg/btree"
)

// RangeItem is one key range tree item.
type RangeItem interface {
	btree.Item
	GetStartKey() []byte
	GetEndKey() []byte
	// Debris returns the debris after replacing the key range.
	Debris(startKey, endKey []byte) []RangeItem
}

// RangeTree is the tree contains RangeItems.
type RangeTree struct {
	tree *btree.BTree
}

// NewRangeTree is the constructor of the range tree.
func NewRangeTree(degree int) *RangeTree {
	return &RangeTree{
		tree: btree.New(degree),
	}
}

// Update insert the item and delete overlaps.
func (r *RangeTree) Update(item RangeItem) []RangeItem {
	overlaps := r.GetOverlaps(item)
	for _, old := range overlaps {
		r.tree.Delete(old)
		debris := old.Debris(item.GetStartKey(), item.GetEndKey())
		for _, child := range debris {
			if bytes.Compare(child.GetStartKey(), child.GetEndKey()) < 0 {
				r.tree.ReplaceOrInsert(child)
			}
		}
	}
	r.tree.ReplaceOrInsert(item)
	return overlaps
}

// GetOverlaps returns the range items that has some intersections with the given items.
func (r *RangeTree) GetOverlaps(item RangeItem) []RangeItem {
	// note that Find() gets the last item that is less or equal than the region.
	// in the case: |_______a_______|_____b_____|___c___|
	// new region is     |______d______|
	// Find() will return RangeItem of region_a
	// and both startKey of region_a and region_b are less than endKey of region_d,
	// thus they are regarded as overlapped regions.
	result := r.Find(item)
	if result == nil {
		result = item
	}

	var overlaps []RangeItem
	r.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(RangeItem)
		if len(item.GetEndKey()) > 0 && bytes.Compare(item.GetEndKey(), over.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

func (r *RangeTree) Find(item RangeItem) RangeItem {
	var result RangeItem
	r.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(RangeItem)
		return false
	})

	if result == nil || !Contains(result, item.GetStartKey()) {
		return nil
	}

	return result
}

func Contains(item RangeItem, key []byte) bool {
	start, end := item.GetStartKey(), item.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (r *RangeTree) Remove(item RangeItem) RangeItem {
	return r.tree.Delete(item).(RangeItem)
}

func (r *RangeTree) Len() int {
	return r.tree.Len()
}

func (r *RangeTree) ScanRange(region RangeItem, f func(item2 RangeItem) bool) {
	// Find if there is a region with key range [s, d), s < startKey < d
	startItem := r.Find(region)
	if startItem == nil {
		startItem = region
	}
	r.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		return f(item.(RangeItem))
	})
}

func (r *RangeTree) GetAdjacentRegions(item RangeItem) (RangeItem, RangeItem) {
	var prev, next RangeItem
	r.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.GetStartKey(), i.(RangeItem).GetStartKey()) {
			return true
		}
		next = i.(RangeItem)
		return false
	})
	r.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.GetStartKey(), i.(RangeItem).GetStartKey()) {
			return true
		}
		prev = i.(RangeItem)
		return false
	})
	return prev, next
}

func (r *RangeTree) GetAt(index int) RangeItem {
	return r.tree.GetAt(index).(RangeItem)
}

func (r *RangeTree) GetWithIndex(item RangeItem) (RangeItem, int) {
	rst, index := r.tree.GetWithIndex(item)
	if rst == nil {
		return nil, index
	}
	return rst.(RangeItem), index
}
