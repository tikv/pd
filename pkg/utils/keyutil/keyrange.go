// Copyright 2025 TiKV Project Authors.
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

package keyutil

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// KeyRange is a key range.
type KeyRange struct {
	StartKey []byte `json:"start-key"`
	EndKey   []byte `json:"end-key"`
}

// OverLapped return true if the two KeyRanges are overlapped.
// if the two KeyRanges are continuous, it will also return true.
func (kr *KeyRange) OverLapped(other *KeyRange) bool {
	leftMax := MaxStartKey(kr.StartKey, other.StartKey)
	rightMin := MinEndKey(kr.EndKey, other.EndKey)
	if len(leftMax) == 0 {
		return true
	}
	if bytes.Equal(leftMax, rightMin) {
		return true
	}
	return less(leftMax, rightMin, right)
}

// IsAdjacent returns true if the two KeyRanges are adjacent.
func (kr *KeyRange) IsAdjacent(other *KeyRange) bool {
	// Check if the end of one range is equal to the start of the other range
	return bytes.Equal(kr.EndKey, other.StartKey) || bytes.Equal(other.EndKey, kr.StartKey)
}

var _ json.Marshaler = &KeyRange{}
var _ json.Unmarshaler = &KeyRange{}

// MarshalJSON marshals to json.
func (kr *KeyRange) MarshalJSON() ([]byte, error) {
	m := map[string]string{
		"start-key": hex.EncodeToString(kr.StartKey),
		"end-key":   hex.EncodeToString(kr.EndKey),
	}
	return json.Marshal(m)
}

// UnmarshalJSON unmarshals from json.
func (kr *KeyRange) UnmarshalJSON(data []byte) error {
	m := make(map[string]string)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	startKey, err := hex.DecodeString(m["start-key"])
	if err != nil {
		return err
	}
	endKey, err := hex.DecodeString(m["end-key"])
	if err != nil {
		return err
	}
	kr.StartKey = startKey
	kr.EndKey = endKey
	return nil
}

// NewKeyRange create a KeyRange with the given start key and end key.
func NewKeyRange(startKey, endKey string) KeyRange {
	return KeyRange{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

// KeyRanges is a slice of monotonically increasing KeyRange.
type KeyRanges struct {
	krs []*KeyRange
}

// NewKeyRanges creates a KeyRanges.
func NewKeyRanges(ranges []KeyRange) *KeyRanges {
	krs := make([]*KeyRange, 0, len(ranges))
	for _, kr := range ranges {
		krs = append(krs, &kr)
	}
	return &KeyRanges{
		krs,
	}
}

// NewKeyRangesWithSize creates a KeyRanges with the hint size.
func NewKeyRangesWithSize(size int) *KeyRanges {
	return &KeyRanges{
		krs: make([]*KeyRange, 0, size),
	}
}

// IsEmpty returns true if the KeyRanges is empty.
func (rs *KeyRanges) IsEmpty() bool {
	return len(rs.krs) == 0
}

// Append appends a KeyRange.
func (rs *KeyRanges) Append(startKey, endKey []byte) {
	rs.krs = append(rs.krs, &KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	})
}

// SortAndDeduce sorts the KeyRanges and deduces the overlapped KeyRanges.
func (rs *KeyRanges) SortAndDeduce() {
	if len(rs.krs) <= 1 {
		return
	}
	sort.Slice(rs.krs, func(i, j int) bool {
		return less(rs.krs[i].StartKey, rs.krs[j].StartKey, left)
	})
	res := make([]*KeyRange, 0)
	res = append(res, rs.krs[0])
	for i := 1; i < len(rs.krs); i++ {
		last := res[len(res)-1]
		if last.IsAdjacent(rs.krs[i]) {
			last.EndKey = rs.krs[i].EndKey
		} else {
			res = append(res, rs.krs[i])
		}
	}
	rs.krs = res
}

// Delete deletes the KeyRange from the KeyRanges.
func (rs *KeyRanges) Delete(base *KeyRange) {
	res := make([]*KeyRange, 0)
	for _, r := range rs.krs {
		if !r.OverLapped(base) {
			res = append(res, r)
			continue
		}
		if less(r.StartKey, base.StartKey, left) {
			res = append(res, &KeyRange{StartKey: r.StartKey, EndKey: MinEndKey(r.EndKey, base.StartKey)})
		}

		if less(base.EndKey, r.EndKey, right) {
			startKey := MaxStartKey(r.StartKey, base.EndKey)
			if len(r.StartKey) == 0 {
				startKey = base.EndKey
			}
			res = append(res, &KeyRange{StartKey: startKey, EndKey: r.EndKey})
		}
	}
	rs.krs = res
}

// SubtractKeyRanges returns the KeyRanges that are not overlapped with the given KeyRange.
func (rs *KeyRanges) SubtractKeyRanges(base *KeyRange) []KeyRange {
	res := make([]KeyRange, 0)
	start := base.StartKey
	for _, kr := range rs.krs {
		// if the last range is not overlapped with the current range, we can skip it.
		if !base.OverLapped(kr) {
			continue
		}
		// add new key range if start<StartKey
		if less(start, kr.StartKey, left) {
			r := &KeyRange{StartKey: start, EndKey: MinEndKey(kr.StartKey, base.EndKey)}
			res = append(res, *r)
		}
		if len(start) == 0 {
			start = kr.EndKey
		} else {
			start = MaxStartKey(start, kr.EndKey)
		}
		// break if startKey<base.EndKey
		if !less(start, base.EndKey, right) {
			break
		}
	}
	if less(start, base.EndKey, right) {
		r := &KeyRange{StartKey: start, EndKey: base.EndKey}
		res = append(res, *r)
	}
	return res
}

// Ranges returns the slice of KeyRange.
func (rs *KeyRanges) Ranges() []*KeyRange {
	if rs == nil {
		return nil
	}
	return rs.krs
}

// Merge merges the continuous KeyRanges.
func (rs *KeyRanges) Merge() {
	if len(rs.krs) == 0 {
		return
	}
	merged := make([]*KeyRange, 0, len(rs.krs))
	start := rs.krs[0].StartKey
	end := rs.krs[0].EndKey
	for _, kr := range rs.krs[1:] {
		if bytes.Equal(end, kr.StartKey) {
			end = kr.EndKey
		} else {
			merged = append(merged, &KeyRange{
				StartKey: start,
				EndKey:   end,
			})
			start = kr.StartKey
			end = kr.EndKey
		}
	}
	merged = append(merged, &KeyRange{
		StartKey: start,
		EndKey:   end,
	})
	rs.krs = merged
}
