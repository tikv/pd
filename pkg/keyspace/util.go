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
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/codec"
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
	// ErrKeyspaceExists indicates target keyspace already exists.
	// It's used when creating a new keyspace.
	ErrKeyspaceExists = errors.New("keyspace already exists")
	// ErrKeyspaceGroupExists indicates target keyspace group already exists.
	ErrKeyspaceGroupExists = errors.New("keyspace group already exists")
	// ErrKeyspaceGroupNotFound is used to indicate target keyspace group does not exist.
	ErrKeyspaceGroupNotFound = errors.New("keyspace group does not exist")
	// ErrKeyspaceGroupNotInSplit is used to indicate target keyspace group is not in split state.
	ErrKeyspaceGroupNotInSplit = errors.New("keyspace group is not in split state")
	// ErrKeyspaceNotInKeyspaceGroup is used to indicate target keyspace is not in this keyspace group.
	ErrKeyspaceNotInKeyspaceGroup = errors.New("keyspace is not in this keyspace group")
	errModifyDefault              = errors.New("cannot modify default keyspace's state")
	errIllegalOperation           = errors.New("unknown operation")

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
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(id uint32) error {
	if id > spaceIDMax {
		return errors.Errorf("illegal keyspace id %d, larger than spaceID Max %d", id, spaceIDMax)
	}
	if id == DefaultKeyspaceID {
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
	if name == DefaultKeyspaceName {
		return errors.Errorf("illegal keyspace name %s, collides with default keyspace name", name)
	}
	return nil
}

// keyspaceIDHash is used to hash the spaceID inside the lockGroup.
// A simple mask is applied to spaceID to use its last byte as map key,
// limiting the maximum map length to 256.
// Since keyspaceID is sequentially allocated, this can also reduce the chance
// of collision when comparing with random hashes.
func keyspaceIDHash(id uint32) uint32 {
	return id & 0xFF
}

// makeKeyRanges encodes keyspace ID to correct LabelRule data.
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
func makeKeyRanges(id uint32) []interface{} {
	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)
	rawLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...)))
	rawRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...)))
	txnLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...)))
	txnRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...)))
	return []interface{}{
		map[string]interface{}{
			"start_key": rawLeftBound,
			"end_key":   rawRightBound,
		},
		map[string]interface{}{
			"start_key": txnLeftBound,
			"end_key":   txnRightBound,
		},
	}
}

// getRegionLabelID returns the region label id of the target keyspace.
func getRegionLabelID(id uint32) string {
	return regionLabelIDPrefix + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
}

// makeLabelRule makes the label rule for the given keyspace id.
func makeLabelRule(id uint32) *labeler.LabelRule {
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
		Data:     makeKeyRanges(id),
	}
}
