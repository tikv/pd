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

package pkdbforcemerge

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage/endpoint"
)

const defaultBTreeDegree = 64

// RangeItem is the managed range unit for force merge.
type RangeItem struct {
	StartKey    []byte `json:"-"`
	StartKeyHex string `json:"start_key"`
	EndKey      []byte `json:"-"`
	EndKeyHex   string `json:"end_key"`
}

// Less returns true when the start key is smaller than the other's start key.
func (r *RangeItem) Less(other *RangeItem) bool {
	return bytes.Compare(r.StartKey, other.StartKey) < 0
}

// String returns the JSON form of the range item.
func (r *RangeItem) String() string {
	data, _ := json.Marshal(r)
	return string(data)
}

// Manager manages force merge ranges in memory.
type Manager struct {
	syncutil.Mutex
	tree    *btree.BTreeG[*RangeItem]
	storage endpoint.ForceMergeStorage
}

// NewManager creates a force merge manager.
func NewManager(storage endpoint.ForceMergeStorage) (*Manager, error) {
	manager := &Manager{
		tree:    btree.NewG[*RangeItem](defaultBTreeDegree),
		storage: storage,
	}
	if err := manager.loadRanges(); err != nil {
		log.Error("failed to initialize force merge manager", zap.Error(err))
		return nil, err
	}
	return manager, nil
}

func newRangeItem(startKeyHex, endKeyHex string) (*RangeItem, error) {
	item := &RangeItem{
		StartKeyHex: startKeyHex,
		EndKeyHex:   endKeyHex,
	}
	if err := adjustRangeItem(item); err != nil {
		return nil, err
	}
	return item, nil
}

func adjustRangeItem(item *RangeItem) error {
	if item.EndKeyHex == "" {
		return errs.ErrForceMergeRangeContent.FastGenByArgs("end key should not be empty")
	}

	startKey, err := decodeHexKey(item.StartKeyHex)
	if err != nil {
		return err
	}
	endKey, err := decodeHexKey(item.EndKeyHex)
	if err != nil {
		return err
	}
	if bytes.Compare(endKey, startKey) <= 0 {
		return errs.ErrForceMergeRangeContent.FastGenByArgs("end key should be greater than start key")
	}

	item.StartKey = startKey
	item.StartKeyHex = hex.EncodeToString(startKey)
	item.EndKey = endKey
	item.EndKeyHex = hex.EncodeToString(endKey)
	return nil
}

func decodeHexKey(keyHex string) ([]byte, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, errs.ErrHexDecodingString.FastGenByArgs(keyHex)
	}
	return key, nil
}

func cloneBytes(data []byte) []byte {
	return append([]byte(nil), data...)
}

func rangeItemsLogString(items []*RangeItem) string {
	data, _ := json.Marshal(items)
	return string(data)
}

// coveredByRange assumes endKey is non-empty.
func coveredByRange(item *RangeItem, startKey, endKey []byte) bool {
	if bytes.Compare(item.StartKey, startKey) < 0 {
		return false
	}
	return bytes.Compare(item.EndKey, endKey) <= 0
}

// coversEnd assumes endKey is non-empty.
func coversEnd(item *RangeItem, endKey []byte) bool {
	if bytes.Compare(item.StartKey, endKey) > 0 {
		return false
	}
	return bytes.Compare(item.EndKey, endKey) > 0
}

// searchRangeLocked finds the ranges around the queried range.
// The caller must ensure endKey is non-empty.
func (m *Manager) searchRangeLocked(startKey, endKey []byte) (left, right *RangeItem, overlaps []*RangeItem) {
	if m.tree.Len() == 0 {
		return nil, nil, nil
	}

	var candidate *RangeItem
	m.tree.DescendLessOrEqual(&RangeItem{StartKey: startKey}, func(item *RangeItem) bool {
		candidate = item
		return false
	})
	if candidate != nil && bytes.Compare(candidate.StartKey, startKey) < 0 && bytes.Compare(startKey, candidate.EndKey) <= 0 {
		left = candidate
	}

	m.tree.AscendGreaterOrEqual(&RangeItem{StartKey: startKey}, func(item *RangeItem) bool {
		if bytes.Compare(item.StartKey, endKey) > 0 {
			return false
		}
		if coveredByRange(item, startKey, endKey) {
			overlaps = append(overlaps, item)
			return true
		}
		if coversEnd(item, endKey) {
			right = item
		}
		return false
	})
	return left, right, overlaps
}

func (m *Manager) loadRanges() error {
	if m.storage == nil {
		return nil
	}

	var (
		toSave   []*RangeItem
		toDelete []string
	)
	err := m.storage.LoadForceMergeRanges(func(key, value string) {
		item := &RangeItem{}
		if err := json.Unmarshal([]byte(value), item); err != nil {
			log.Error("failed to unmarshal force merge range",
				zap.String("range-key", key),
				zap.String("range-value", value),
				errs.ZapError(errs.ErrLoadForceMergeRange))
			toDelete = append(toDelete, key)
			return
		}
		if err := adjustRangeItem(item); err != nil {
			log.Error("force merge range is in bad format",
				zap.String("range-key", key),
				zap.String("range-value", value),
				errs.ZapError(errs.ErrLoadForceMergeRange, err))
			toDelete = append(toDelete, key)
			return
		}
		if key != item.EndKeyHex {
			log.Error("mismatch force merge range key, need to restore",
				zap.String("range-key", key),
				zap.String("expected-key", item.EndKeyHex),
				errs.ZapError(errs.ErrLoadForceMergeRange))
			toDelete = append(toDelete, key)
			toSave = append(toSave, item)
		}
		m.tree.ReplaceOrInsert(item)
	})
	if err != nil {
		log.Error("failed to load force merge ranges", errs.ZapError(errs.ErrLoadForceMergeRange, err))
		return err
	}
	for _, item := range toSave {
		if err := m.storage.SaveForceMergeRange(item.EndKeyHex, item); err != nil {
			log.Error("failed to restore force merge range",
				logutil.ZapRedactString("start-key", item.StartKeyHex),
				logutil.ZapRedactString("end-key", item.EndKeyHex),
				zap.Error(err))
			return err
		}
	}
	for _, key := range toDelete {
		if err := m.storage.DeleteForceMergeRange(key); err != nil {
			log.Error("failed to delete invalid force merge range",
				zap.String("range-key", key),
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (m *Manager) removeRangeLocked(item *RangeItem) error {
	if m.storage != nil {
		if err := m.storage.DeleteForceMergeRange(item.EndKeyHex); err != nil {
			log.Error("failed to persist force merge range deletion",
				logutil.ZapRedactString("start-key", item.StartKeyHex),
				logutil.ZapRedactString("end-key", item.EndKeyHex),
				zap.Error(err))
			return err
		}
	}
	m.tree.Delete(item)
	return nil
}

// addRangeLocked adds a range and merges all overlapped ranges.
func (m *Manager) addRangeLocked(item *RangeItem) error {
	left, right, overlaps := m.searchRangeLocked(item.StartKey, item.EndKey)
	exactOverlap := len(overlaps) == 1 && bytes.Equal(overlaps[0].StartKey, item.StartKey) && bytes.Equal(overlaps[0].EndKey, item.EndKey)
	if exactOverlap ||
		(left != nil && bytes.Compare(left.EndKey, item.EndKey) >= 0) ||
		(right != nil && bytes.Equal(right.StartKey, item.StartKey)) {
		return nil
	}
	if left != nil {
		if err := m.removeRangeLocked(left); err != nil {
			return err
		}
		item.StartKey = cloneBytes(left.StartKey)
		item.StartKeyHex = left.StartKeyHex
	}
	for _, overlap := range overlaps {
		if err := m.removeRangeLocked(overlap); err != nil {
			return err
		}
	}
	if right != nil {
		if err := m.removeRangeLocked(right); err != nil {
			return err
		}
		item.EndKey = cloneBytes(right.EndKey)
		item.EndKeyHex = right.EndKeyHex
	}

	if m.storage != nil {
		if err := m.storage.SaveForceMergeRange(item.EndKeyHex, item); err != nil {
			log.Error("failed to persist force merge range",
				logutil.ZapRedactString("start-key", item.StartKeyHex),
				logutil.ZapRedactString("end-key", item.EndKeyHex),
				zap.Error(err))
			return err
		}
	}
	m.tree.ReplaceOrInsert(item)
	return nil
}

// AddRanges validates and adds the ranges into the manager.
func (m *Manager) AddRanges(startKeysHex, endKeysHex []string) error {
	if len(startKeysHex) != len(endKeysHex) {
		return errs.ErrForceMergeRangeContent.FastGenByArgs(
			fmt.Sprintf("start key count %d does not match end key count %d", len(startKeysHex), len(endKeysHex)))
	}

	items := make([]*RangeItem, 0, len(startKeysHex))
	var prevStartKey, prevEndKey []byte
	for i := range startKeysHex {
		item, err := newRangeItem(startKeysHex[i], endKeysHex[i])
		if err != nil {
			return err
		}
		if i > 0 {
			if bytes.Compare(prevStartKey, item.StartKey) > 0 {
				return errs.ErrForceMergeRangeContent.FastGenByArgs("start keys should be sorted")
			}
			if bytes.Compare(prevEndKey, item.EndKey) > 0 {
				return errs.ErrForceMergeRangeContent.FastGenByArgs("end keys should be sorted")
			}
			if bytes.Compare(prevEndKey, item.StartKey) > 0 {
				return errs.ErrForceMergeRangeContent.FastGenByArgs("ranges should not overlap")
			}
		}
		prevStartKey = item.StartKey
		prevEndKey = item.EndKey
		if len(items) > 0 && bytes.Equal(items[len(items)-1].EndKey, item.StartKey) {
			items[len(items)-1].EndKey = item.EndKey
			items[len(items)-1].EndKeyHex = item.EndKeyHex
			continue
		}
		items = append(items, item)
	}

	m.Lock()
	defer m.Unlock()
	for i, item := range items {
		if err := m.addRangeLocked(item); err != nil {
			log.Error("failed to add force merge range",
				logutil.ZapRedactString("failed-start-key", item.StartKeyHex),
				logutil.ZapRedactString("failed-end-key", item.EndKeyHex),
				logutil.ZapRedactString("pending-ranges", rangeItemsLogString(items[i+1:])),
				zap.Error(err))
			return err
		}
	}
	return nil
}

// SolveRegion checks whether the region should force merge and clears merged ranges.
// Regions with empty end key are ignored so the internal range helpers can rely on non-empty endKey.
// Exactly matched ranges are kept so the region can continue merging with its neighbors.
func (m *Manager) SolveRegion(region *core.RegionInfo) (needForceMerge bool) {
	if region == nil {
		return false
	}
	if len(region.GetEndKey()) == 0 {
		return false
	}

	m.Lock()
	defer m.Unlock()

	startKey := region.GetStartKey()
	endKey := region.GetEndKey()
	left, right, overlaps := m.searchRangeLocked(startKey, endKey)
	exactOverlap := len(overlaps) == 1 && bytes.Equal(overlaps[0].StartKey, startKey) && bytes.Equal(overlaps[0].EndKey, endKey)
	if exactOverlap {
		return true
	}
	for _, overlap := range overlaps {
		if err := m.removeRangeLocked(overlap); err != nil {
			return false
		}
	}
	return (left != nil && bytes.Compare(left.EndKey, endKey) >= 0) ||
		(right != nil && bytes.Equal(right.StartKey, startKey))
}
