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

package keyspace

import (
	"container/heap"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

const (
	spaceIDMax = ^uint32(0) >> 8 // 16777215 (Uint24Max) is the maximum value of spaceID.
	// namePattern is a regex that specifies acceptable characters of the keyspace name.
	// Name must be non-empty and contains only alphanumerical, `_` and `-`.
	namePattern = "^[-A-Za-z0-9_]+$"
)

var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.New("keyspace does not exist")
	// ErrRegionSplitTimeout indices to split region timeout
	ErrRegionSplitTimeout = errors.New("region split timeout")
	// ErrRegionSplitFailed indices to split region failed
	ErrRegionSplitFailed = errors.New("region split failed")
	// ErrKeyspaceExists indicates target keyspace already exists.
	// It's used when creating a new keyspace.
	ErrKeyspaceExists = errors.New("keyspace already exists")
	// ErrKeyspaceGroupExists indicates target keyspace group already exists.
	ErrKeyspaceGroupExists = errors.New("keyspace group already exists")
	// ErrKeyspaceGroupNotExists is used to indicate target keyspace group does not exist.
	ErrKeyspaceGroupNotExists = func(groupID uint32) error {
		return errors.Errorf("keyspace group %v does not exist", groupID)
	}
	// ErrKeyspaceGroupInSplit is used to indicate target keyspace group is in split state.
	ErrKeyspaceGroupInSplit = func(groupID uint32) error {
		return errors.Errorf("keyspace group %v is in split state", groupID)
	}
	// ErrKeyspaceGroupNotInSplit is used to indicate target keyspace group is not in split state.
	ErrKeyspaceGroupNotInSplit = func(groupID uint32) error {
		return errors.Errorf("keyspace group %v is not in split state", groupID)
	}
	// ErrKeyspaceGroupInMerging is used to indicate target keyspace group is in merging state.
	ErrKeyspaceGroupInMerging = func(groupID uint32) error {
		return errors.Errorf("keyspace group %v is in merging state", groupID)
	}
	// ErrKeyspaceGroupNotInMerging is used to indicate target keyspace group is not in merging state.
	ErrKeyspaceGroupNotInMerging = func(groupID uint32) error {
		return errors.Errorf("keyspace group %v is not in merging state", groupID)
	}
	// ErrKeyspaceNotInKeyspaceGroup is used to indicate target keyspace is not in this keyspace group.
	ErrKeyspaceNotInKeyspaceGroup = errors.New("keyspace is not in this keyspace group")
	// ErrKeyspaceNotInAnyKeyspaceGroup is used to indicate target keyspace is not in any keyspace group.
	ErrKeyspaceNotInAnyKeyspaceGroup = errors.New("keyspace is not in any keyspace group")
	// ErrNodeNotInKeyspaceGroup is used to indicate the tso node is not in this keyspace group.
	ErrNodeNotInKeyspaceGroup = errors.New("the tso node is not in this keyspace group")
	// ErrKeyspaceGroupNotEnoughReplicas is used to indicate not enough replicas in the keyspace group.
	ErrKeyspaceGroupNotEnoughReplicas = errors.New("not enough replicas in the keyspace group")
	// ErrKeyspaceGroupWithEmptyKeyspace is used to indicate keyspace group with empty keyspace.
	ErrKeyspaceGroupWithEmptyKeyspace = errors.New("keyspace group with empty keyspace")
	// ErrModifyDefaultKeyspaceGroup is used to indicate that default keyspace group cannot be modified.
	ErrModifyDefaultKeyspaceGroup = errors.New("default keyspace group cannot be modified")
	// ErrNoAvailableNode is used to indicate no available node in the keyspace group.
	ErrNoAvailableNode = errors.New("no available node")
	// ErrExceedMaxEtcdTxnOps is used to indicate the number of etcd txn operations exceeds the limit.
	ErrExceedMaxEtcdTxnOps = errors.New("exceed max etcd txn operations")
	// ErrModifyDefaultKeyspace is used to indicate that default keyspace cannot be modified.
	ErrModifyDefaultKeyspace = errors.New("cannot modify default keyspace's state")
	errIllegalOperation      = errors.New("unknown operation")

	// ErrUnsupportedOperationInKeyspace is used to indicate this is an unsupported operation.
	ErrUnsupportedOperationInKeyspace = errors.New("it's a unsupported operation")

	// stateTransitionTable lists all allowed next state for the given current state.
	// Note that transit from any state to itself is allowed for idempotence.
	stateTransitionTable = map[keyspacepb.KeyspaceState][]keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED:   {keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED},
		keyspacepb.KeyspaceState_DISABLED:  {keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_ARCHIVED},
		keyspacepb.KeyspaceState_ARCHIVED:  {keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE},
		keyspacepb.KeyspaceState_TOMBSTONE: {keyspacepb.KeyspaceState_TOMBSTONE},
	}
	// Only keyspaces in the state specified by allowChangeConfig are allowed to change their config.
	allowChangeConfig = []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED}

	// ErrKeyspaceGroupPrimaryNotFound is used to indicate primary of target keyspace group does not exist.
	ErrKeyspaceGroupPrimaryNotFound = errors.New("primary of keyspace group does not exist")
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(id uint32) error {
	if id > spaceIDMax {
		return errors.Errorf("illegal keyspace id %d, larger than spaceID Max %d", id, spaceIDMax)
	}
	if id == constant.DefaultKeyspaceID {
		return errors.Errorf("illegal keyspace id %d, collides with default keyspace id", id)
	}
	return nil
}

// validateName check if user provided name is legal.
// It throws errIllegalName when name contains illegal character,
// or if it collides with reserved name.
func validateName(name string) error {
	isValid, err := regexp.MatchString(namePattern, name)
	if err != nil {
		return err
	}
	if !isValid {
		return errors.Errorf("illegal keyspace name %s, should contain only alphanumerical and underline", name)
	}
	if name == constant.DefaultKeyspaceName {
		return errors.Errorf("illegal keyspace name %s, collides with default keyspace name", name)
	}
	return nil
}

// MaskKeyspaceID is used to hash the spaceID inside the lockGroup.
// A simple mask is applied to spaceID to use its last byte as map key,
// limiting the maximum map length to 256.
// Since keyspaceID is sequentially allocated, this can also reduce the chance
// of collision when comparing with random hashes.
func MaskKeyspaceID(id uint32) uint32 {
	return id & 0xFF
}

// RegionBound represents the region boundary of the given keyspace.
// For a keyspace with id ['a', 'b', 'c'], it has four boundaries:
//
//	Lower bound for raw mode: ['r', 'a', 'b', 'c']
//	Upper bound for raw mode: ['r', 'a', 'b', 'c + 1']
//	Lower bound for txn mode: ['x', 'a', 'b', 'c']
//	Upper bound for txn mode: ['x', 'a', 'b', 'c + 1']
//
// From which it shares the lower bound with keyspace with id ['a', 'b', 'c-1'].
// And shares upper bound with keyspace with id ['a', 'b', 'c + 1'].
// These repeated bound will not cause any problem, as repetitive bound will be ignored during rangeListBuild,
// but provides guard against hole in keyspace allocations should it occur.
type RegionBound struct {
	RawLeftBound  []byte
	RawRightBound []byte
	TxnLeftBound  []byte
	TxnRightBound []byte
}

// MakeRegionBound constructs the correct region boundaries of the given keyspace.
func MakeRegionBound(id uint32) *RegionBound {
	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)
	return &RegionBound{
		RawLeftBound:  codec.EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...)),
		RawRightBound: codec.EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...)),
		TxnLeftBound:  codec.EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...)),
		TxnRightBound: codec.EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...)),
	}
}

// MakeKeyRanges encodes keyspace ID to correct LabelRule data.
func MakeKeyRanges(id uint32) []any {
	regionBound := MakeRegionBound(id)
	return []any{
		map[string]any{
			"start_key": hex.EncodeToString(regionBound.RawLeftBound),
			"end_key":   hex.EncodeToString(regionBound.RawRightBound),
		},
		map[string]any{
			"start_key": hex.EncodeToString(regionBound.TxnLeftBound),
			"end_key":   hex.EncodeToString(regionBound.TxnRightBound),
		},
	}
}

// getRegionLabelID returns the region label id of the target keyspace.
func getRegionLabelID(id uint32) string {
	return regionLabelIDPrefix + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
}

// MakeLabelRule makes the label rule for the given keyspace id.
func MakeLabelRule(id uint32) *labeler.LabelRule {
	return &labeler.LabelRule{
		ID:    getRegionLabelID(id),
		Index: 0,
		Labels: []labeler.RegionLabel{
			{
				Key:   regionLabelKey,
				Value: strconv.FormatUint(uint64(id), endpoint.SpaceIDBase),
			},
		},
		RuleType: labeler.KeyRange,
		Data:     MakeKeyRanges(id),
	}
}

// indexedHeap is a heap with index.
type indexedHeap struct {
	items []*endpoint.KeyspaceGroup
	// keyspace group id -> position in items
	index map[uint32]int
}

func newIndexedHeap(hint int) *indexedHeap {
	return &indexedHeap{
		items: make([]*endpoint.KeyspaceGroup, 0, hint),
		index: map[uint32]int{},
	}
}

// Implementing heap.Interface.
func (hp *indexedHeap) Len() int {
	return len(hp.items)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Less(i, j int) bool {
	// Gives the keyspace group with the least number of keyspaces first
	return len(hp.items[j].Keyspaces) > len(hp.items[i].Keyspaces)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Swap(i, j int) {
	lid := hp.items[i].ID
	rid := hp.items[j].ID
	hp.items[i], hp.items[j] = hp.items[j], hp.items[i]
	hp.index[lid] = j
	hp.index[rid] = i
}

// Implementing heap.Interface.
func (hp *indexedHeap) Push(x any) {
	item := x.(*endpoint.KeyspaceGroup)
	hp.index[item.ID] = hp.Len()
	hp.items = append(hp.items, item)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Pop() any {
	l := hp.Len()
	item := hp.items[l-1]
	hp.items = hp.items[:l-1]
	delete(hp.index, item.ID)
	return item
}

// Top returns the top item.
func (hp *indexedHeap) Top() *endpoint.KeyspaceGroup {
	if hp.Len() <= 0 {
		return nil
	}
	return hp.items[0]
}

// Get returns item with the given ID.
func (hp *indexedHeap) Get(id uint32) *endpoint.KeyspaceGroup {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := hp.items[idx]
	return item
}

// GetAll returns all the items.
func (hp *indexedHeap) GetAll() []*endpoint.KeyspaceGroup {
	all := make([]*endpoint.KeyspaceGroup, len(hp.items))
	copy(all, hp.items)
	return all
}

// Put inserts item or updates the old item if it exists.
func (hp *indexedHeap) Put(item *endpoint.KeyspaceGroup) (isUpdate bool) {
	if idx, ok := hp.index[item.ID]; ok {
		hp.items[idx] = item
		heap.Fix(hp, idx)
		return true
	}
	heap.Push(hp, item)
	return false
}

// Remove deletes item by ID and returns it.
func (hp *indexedHeap) Remove(id uint32) *endpoint.KeyspaceGroup {
	if idx, ok := hp.index[id]; ok {
		item := heap.Remove(hp, idx)
		return item.(*endpoint.KeyspaceGroup)
	}
	return nil
}
