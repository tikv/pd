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

package keyspace

import (
	"bytes"
	"cmp"
	"slices"
	"sort"

	"github.com/google/btree"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/codec"
	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	legacyRegionBound   = txnRegionBound + 1
	boundaryLookupLimit = 10
)

// IDRange is an inclusive range of keyspace IDs.
type IDRange struct{ Start, End uint32 }

// BoundaryFinder is an optional scheduling cluster capability.
type BoundaryFinder interface {
	// GetKeyspaceBoundaryIDs returns up to limit descending IDs per mode from
	// one snapshot. A nil range skips that mode. ok is false when the metadata
	// is unavailable and a merge cannot be verified.
	GetKeyspaceBoundaryIDs(raw, txn *IDRange, limit int) (rawIDs, txnIDs []uint32, ok bool)
}

// MetadataSplitKeys returns metadata boundaries strictly inside [start, end).
// A false result means the metadata is unavailable.
func MetadataSplitKeys(start, end []byte, finder BoundaryFinder) ([][]byte, bool) {
	return findMetadataSplitKeys(start, end, finder, boundaryLookupLimit)
}

// CanMergeAcrossMetadata reports whether metadata confirms there is no
// keyspace boundary strictly inside [start, end).
func CanMergeAcrossMetadata(start, end []byte, finder BoundaryFinder) bool {
	keys, ok := findMetadataSplitKeys(start, end, finder, 1)
	return ok && len(keys) == 0
}

func findMetadataSplitKeys(start, end []byte, finder BoundaryFinder, limit int) ([][]byte, bool) {
	if len(end) != 0 && bytes.Compare(start, end) >= 0 {
		return nil, true
	}
	raw := boundaryIDRange(start, end, codec.RawKeyspaceModePrefix)
	txn := boundaryIDRange(start, end, codec.TxnKeyspaceModePrefix)
	if raw == nil && txn == nil {
		return nil, true
	}
	rawIDs, txnIDs, ok := finder.GetKeyspaceBoundaryIDs(raw, txn, limit)
	if !ok {
		return nil, false
	}
	var keys [][]byte
	for mode, ids := range [2][]uint32{rawIDs, txnIDs} {
		for _, id := range ids {
			left, right := regionBoundType(mode).bounds(MakeRegionBound(id))
			if keyutil.Between(start, end, left) {
				keys = append(keys, left)
			}
			if keyutil.Between(start, end, right) {
				keys = append(keys, right)
			}
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal), true
}

func boundaryIDRange(start, end []byte, prefix byte) *IDRange {
	left := codec.EncodeKeyspaceBoundary(prefix, constant.StartKeyspaceID)
	right := codec.EncodeKeyspaceBoundary(prefix, constant.MaxValidKeyspaceID+1)
	if bytes.Compare(start, right[:]) >= 0 || (len(end) != 0 && bytes.Compare(end, left[:]) <= 0) {
		return nil
	}
	startID := constant.StartKeyspaceID
	if bytes.Compare(start, left[:]) >= 0 {
		startID = resolveBoundaryID(start, prefix)
		// No boundary can lie inside a range confined to one keyspace.
		next := codec.EncodeKeyspaceBoundary(prefix, startID+1)
		if len(end) != 0 && bytes.Compare(end, next[:]) <= 0 {
			return nil
		}
	}
	endID := constant.MaxValidKeyspaceID
	if len(end) != 0 && bytes.Compare(end, right[:]) < 0 {
		endID = resolveBoundaryID(end, prefix)
		fence := codec.EncodeKeyspaceBoundary(prefix, endID)
		if bytes.Equal(end, fence[:]) {
			endID-- // end is strictly above the block's left bound.
		}
	}
	return &IDRange{Start: startID, End: endID}
}

// resolveBoundaryID finds the ID whose left boundary is at or before key.
// A foreign or partial key within a mode block must still be compared by bytes.
func resolveBoundaryID(key []byte, prefix byte) uint32 {
	if mode, id, ok := codec.DecodeKeyspaceKey(key); ok && mode == prefix {
		return id
	}
	next := sort.Search(int(constant.MaxValidKeyspaceID)+1, func(id int) bool {
		left := codec.EncodeKeyspaceBoundary(prefix, uint32(id))
		return bytes.Compare(left[:], key) > 0
	})
	return uint32(next - 1)
}

// BoundaryCache indexes the live keyspaces from the scheduling metadata watch.
// Legacy IDs have their own tree so fallback changes do not require reindexing.
type BoundaryCache struct {
	mu    syncutil.RWMutex
	index boundaryIndex
	ready bool
}

type boundaryIndex struct {
	modes [legacyRegionBound + 1]*btree.BTreeG[uint32]
	byID  map[uint32]regionBoundType
}

// Invalidate makes boundary lookups unavailable until a full load succeeds.
func (c *BoundaryCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = false
}

// Replace installs a complete snapshot and makes boundary lookups available.
func (c *BoundaryCache) Replace(metas []*keyspacepb.KeyspaceMeta) {
	index := boundaryIndex{byID: make(map[uint32]regionBoundType, len(metas))}
	for _, meta := range metas {
		index.apply(meta)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index, c.ready = index, true
}

// Apply updates the index atomically for one batch of watch events. A delete is
// represented by a tombstone, and events for the same ID are applied in order.
func (c *BoundaryCache) Apply(metas []*keyspacepb.KeyspaceMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready {
		for _, meta := range metas {
			c.index.apply(meta)
		}
	}
}

func (index *boundaryIndex) apply(meta *keyspacepb.KeyspaceMeta) {
	id := meta.GetId()
	if prev, ok := index.byID[id]; ok {
		index.modes[prev].Delete(id)
		delete(index.byID, id)
	}
	if meta.GetState() == keyspacepb.KeyspaceState_TOMBSTONE {
		return
	}
	mode := legacyRegionBound
	if value, ok := meta.GetConfig()[RegionBoundType]; ok {
		// Preserve the historical interpretation of explicit values.
		mode = keyTypeStringToRegionBoundType(value)
	}
	if index.modes[mode] == nil {
		index.modes[mode] = btree.NewOrderedG[uint32](2)
	}
	index.modes[mode].ReplaceOrInsert(id)
	index.byID[id] = mode
}

// Lookup returns the requested mode-specific IDs from the same snapshot.
func (c *BoundaryCache) Lookup(raw, txn *IDRange, limit int, pdKeyType coreconstant.KeyType) (rawIDs, txnIDs []uint32, ready bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.ready {
		return nil, nil, false
	}
	fallback := keyTypeToRegionBoundType(pdKeyType)
	return c.index.lookup(raw, limit, rawRegionBound, fallback), c.index.lookup(txn, limit, txnRegionBound, fallback), true
}

func (index *boundaryIndex) lookup(r *IDRange, limit int, mode, fallback regionBoundType) []uint32 {
	if r == nil {
		return nil
	}
	ids := index.appendIDs(nil, r, limit, mode)
	if mode == fallback && index.modes[legacyRegionBound] != nil {
		ids = index.appendIDs(ids, r, limit, legacyRegionBound)
		slices.SortFunc(ids, func(a, b uint32) int { return cmp.Compare(b, a) })
	}
	if limit > 0 && len(ids) > limit {
		return ids[:limit]
	}
	return ids
}

func (index *boundaryIndex) appendIDs(ids []uint32, r *IDRange, limit int, mode regionBoundType) []uint32 {
	if tree := index.modes[mode]; tree != nil {
		remaining := limit
		tree.DescendLessOrEqual(r.End, func(id uint32) bool {
			if id < r.Start {
				return false
			}
			ids = append(ids, id)
			remaining--
			return limit <= 0 || remaining > 0
		})
	}
	return ids
}
