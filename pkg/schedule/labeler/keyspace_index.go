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

package labeler

import (
	"bytes"
	"math/bits"
	"sort"
	"strconv"
	"strings"
)

const (
	keyspaceRuleIDPrefix = "keyspaces/"
	keyspaceIDLabelKey   = "id"
	keyspaceRawMode      = byte('r')
	keyspaceTxnMode      = byte('x')
	keyspaceMaxID        = uint32(1<<24 - 1)

	keyspaceChunkBits  = 10
	keyspaceChunkSize  = 1 << keyspaceChunkBits
	keyspaceChunkMask  = keyspaceChunkSize - 1
	keyspaceChunkCount = (int(keyspaceMaxID) + 1) / keyspaceChunkSize
	keyspaceSlotWords  = (int(keyspaceMaxID) + 1) / 64
	keyspaceBoundCount = int(keyspaceMaxID) + 2
)

// A keyspace rule has one fixed-width range per enabled API mode. Keeping
// these ranges in sparse slots avoids materializing two split points and a
// segment for every keyspace. The bitset is used only to find range boundaries;
// the rule remains owned by RegionLabeler's labelRules map.
type keyspaceRuleSet struct {
	chunks []*[keyspaceChunkSize]*LabelRule
	bits   []uint64
}

func (s *keyspaceRuleSet) get(id uint32) *LabelRule {
	if len(s.chunks) == 0 || id > keyspaceMaxID {
		return nil
	}
	chunk := s.chunks[int(id)>>keyspaceChunkBits]
	if chunk == nil {
		return nil
	}
	return chunk[int(id)&keyspaceChunkMask]
}

func (s *keyspaceRuleSet) set(id uint32, rule *LabelRule) {
	if len(s.chunks) == 0 {
		s.chunks = make([]*[keyspaceChunkSize]*LabelRule, keyspaceChunkCount)
		s.bits = make([]uint64, keyspaceSlotWords)
	}
	chunkID := int(id) >> keyspaceChunkBits
	if s.chunks[chunkID] == nil {
		s.chunks[chunkID] = new([keyspaceChunkSize]*LabelRule)
	}
	s.chunks[chunkID][int(id)&keyspaceChunkMask] = rule
	s.bits[int(id)>>6] |= uint64(1) << (id & 63)
}

func (s *keyspaceRuleSet) clear(id uint32) {
	if len(s.chunks) == 0 || id > keyspaceMaxID {
		return
	}
	chunk := s.chunks[int(id)>>keyspaceChunkBits]
	if chunk == nil {
		return
	}
	chunk[int(id)&keyspaceChunkMask] = nil
	s.bits[int(id)>>6] &^= uint64(1) << (id & 63)
}

// boundaryWord returns the boundaries contributed by the slots in word.
// A slot at ID n contributes both boundary n and boundary n+1.
func (s *keyspaceRuleSet) boundaryWord(word int) uint64 {
	if len(s.bits) == 0 || word < 0 || word > len(s.bits) {
		return 0
	}
	var current uint64
	if word < len(s.bits) {
		current = s.bits[word]
	}
	boundaries := current | current<<1
	if word > 0 && s.bits[word-1]&(uint64(1)<<63) != 0 {
		boundaries |= 1
	}
	return boundaries
}

func (s *keyspaceRuleSet) forEachBoundary(lo, hi int, fn func(id uint32) bool) {
	if len(s.bits) == 0 || lo >= hi {
		return
	}
	lo = max(lo, 0)
	hi = min(hi, keyspaceBoundCount)
	for word := lo >> 6; word <= (hi-1)>>6; word++ {
		boundaries := s.boundaryWord(word)
		if offset := lo & 63; word == lo>>6 && offset != 0 {
			boundaries &= ^uint64(0) << offset
		}
		for boundaries != 0 {
			bit := bits.TrailingZeros64(boundaries)
			id := word<<6 + bit
			if id >= hi || !fn(uint32(id)) {
				return
			}
			boundaries &= boundaries - 1
		}
	}
}

type keyspaceRuleRange struct {
	mode byte
	id   uint32
}

// keyspaceRuleIndex indexes the deterministic keyspace label rules. It only
// accepts the exact rule shape produced by pkg/keyspace; all other rules stay
// in the generic range list.
type keyspaceRuleIndex struct {
	raw keyspaceRuleSet
	txn keyspaceRuleSet
}

func (i *keyspaceRuleIndex) ruleSet(mode byte) *keyspaceRuleSet {
	switch mode {
	case keyspaceRawMode:
		return &i.raw
	case keyspaceTxnMode:
		return &i.txn
	default:
		return nil
	}
}

// Add indexes a deterministic keyspace rule. It returns false when the rule
// is not canonical or a different rule already occupies one of its slots.
func (i *keyspaceRuleIndex) Add(rule *LabelRule) bool {
	ranges, count, ok := keyspaceRanges(rule)
	if !ok {
		return false
	}
	for _, r := range ranges[:count] {
		if i.ruleSet(r.mode).get(r.id) != nil {
			return false
		}
	}
	for _, r := range ranges[:count] {
		i.ruleSet(r.mode).set(r.id, rule)
	}
	return true
}

// Remove deletes all slots owned by rule for the keyspace rule ID. It returns
// false when the ID is not canonical or no indexed slot is owned by rule.
func (i *keyspaceRuleIndex) Remove(ruleID string, rule *LabelRule) bool {
	id, _, ok := parseKeyspaceRuleID(ruleID)
	if !ok {
		return false
	}
	removed := false
	for _, mode := range []byte{keyspaceRawMode, keyspaceTxnMode} {
		set := i.ruleSet(mode)
		if set.get(id) == rule {
			set.clear(id)
			removed = true
		}
	}
	return removed
}

// Contains reports whether all canonical slots are owned by rule.
func (i *keyspaceRuleIndex) Contains(rule *LabelRule) bool {
	ranges, count, ok := keyspaceRanges(rule)
	if !ok {
		return false
	}
	for _, r := range ranges[:count] {
		if i.ruleSet(r.mode).get(r.id) != rule {
			return false
		}
	}
	return true
}

// GetRule returns the keyspace rule covering the whole range.
func (i *keyspaceRuleIndex) GetRule(start, end []byte) *LabelRule {
	if len(start) < 9 || start[8] < 0xfb || len(end) == 0 {
		return nil
	}
	mode := start[0]
	set := i.ruleSet(mode)
	if set == nil {
		return nil
	}
	id := uint32(start[1])<<16 | uint32(start[2])<<8 | uint32(start[3])
	rule := set.get(id)
	if rule == nil {
		return nil
	}
	left := keyspaceBoundary(mode, id)
	right := keyspaceBoundary(mode, id+1)
	if bytes.Compare(start, left[:]) < 0 || bytes.Compare(end, right[:]) > 0 {
		return nil
	}
	return rule
}

// HasSplitKey reports whether a keyspace boundary exists in (start, end).
func (i *keyspaceRuleIndex) HasSplitKey(start, end []byte) bool {
	for _, mode := range []byte{keyspaceRawMode, keyspaceTxnMode} {
		set := i.ruleSet(mode)
		if len(set.bits) == 0 {
			continue
		}
		lo, hi := keyspaceBoundaryRange(mode, start, end)
		found := false
		set.forEachBoundary(lo, hi, func(uint32) bool {
			found = true
			return false
		})
		if found {
			return true
		}
	}
	return false
}

// GetSplitKeys returns all indexed keyspace boundaries in (start, end).
func (i *keyspaceRuleIndex) GetSplitKeys(start, end []byte) [][]byte {
	var keys [][]byte
	for _, mode := range []byte{keyspaceRawMode, keyspaceTxnMode} {
		set := i.ruleSet(mode)
		if len(set.bits) == 0 {
			continue
		}
		lo, hi := keyspaceBoundaryRange(mode, start, end)
		set.forEachBoundary(lo, hi, func(id uint32) bool {
			keys = append(keys, keyspaceBoundaryBytes(mode, id))
			return true
		})
	}
	return keys
}

func keyspaceRanges(rule *LabelRule) ([2]keyspaceRuleRange, int, bool) {
	var ranges [2]keyspaceRuleRange
	if rule == nil || rule.Index != 0 || rule.RuleType != KeyRange || len(rule.Labels) != 1 {
		return ranges, 0, false
	}
	label := rule.Labels[0]
	if label.Key != keyspaceIDLabelKey || label.TTL != "" || label.StartAt != "" || label.expire != nil {
		return ranges, 0, false
	}
	id, idText, ok := parseKeyspaceRuleID(rule.ID)
	if !ok || label.Value != idText {
		return ranges, 0, false
	}
	keyRanges := rule.GetKeyRanges()
	if len(keyRanges) == 0 || len(keyRanges) > 2 {
		return ranges, 0, false
	}
	var seenRaw, seenTxn bool
	count := 0
	for _, keyRange := range keyRanges {
		mode, ok := canonicalKeyspaceRange(keyRange, id)
		if !ok {
			return ranges, 0, false
		}
		switch mode {
		case keyspaceRawMode:
			if seenRaw {
				return ranges, 0, false
			}
			seenRaw = true
		case keyspaceTxnMode:
			if seenTxn {
				return ranges, 0, false
			}
			seenTxn = true
		}
		switch count {
		case 0:
			ranges[0] = keyspaceRuleRange{mode: mode, id: id}
		case 1:
			ranges[1] = keyspaceRuleRange{mode: mode, id: id}
		default:
			return ranges, 0, false
		}
		count++
	}
	return ranges, count, true
}

func parseKeyspaceRuleID(ruleID string) (uint32, string, bool) {
	if !strings.HasPrefix(ruleID, keyspaceRuleIDPrefix) {
		return 0, "", false
	}
	idText := strings.TrimPrefix(ruleID, keyspaceRuleIDPrefix)
	id64, err := strconv.ParseUint(idText, 10, 32)
	hasLeadingZero := len(idText) > 1 && idText[0] == '0'
	if err != nil || id64 > uint64(keyspaceMaxID) || hasLeadingZero {
		return 0, "", false
	}
	return uint32(id64), idText, true
}

func canonicalKeyspaceRange(keyRange *KeyRangeRule, id uint32) (byte, bool) {
	if keyRange == nil {
		return 0, false
	}
	for _, mode := range []byte{keyspaceRawMode, keyspaceTxnMode} {
		start := keyspaceBoundary(mode, id)
		end := keyspaceBoundary(mode, id+1)
		if bytes.Equal(keyRange.StartKey, start[:]) && bytes.Equal(keyRange.EndKey, end[:]) {
			return mode, true
		}
	}
	return 0, false
}

// keyspaceBoundary returns codec.EncodeBytes([mode, keyspace-id]). The input is
// always four bytes, so its memcomparable encoding is a fixed nine-byte value.
// max-id+1 is the exclusive fencepost in the next mode byte.
func keyspaceBoundary(mode byte, id uint32) [9]byte {
	if id > keyspaceMaxID {
		mode++
		id = 0
	}
	return [9]byte{
		mode,
		byte(id >> 16),
		byte(id >> 8),
		byte(id),
		0, 0, 0, 0,
		0xfb,
	}
}

func keyspaceBoundaryBytes(mode byte, id uint32) []byte {
	key := keyspaceBoundary(mode, id)
	return key[:]
}

func keyspaceBoundaryRange(mode byte, start, end []byte) (lo, hi int) {
	lo = sort.Search(keyspaceBoundCount, func(id int) bool {
		key := keyspaceBoundary(mode, uint32(id))
		return bytes.Compare(key[:], start) > 0
	})
	hi = keyspaceBoundCount
	if len(end) > 0 {
		hi = sort.Search(keyspaceBoundCount, func(id int) bool {
			key := keyspaceBoundary(mode, uint32(id))
			return bytes.Compare(key[:], end) >= 0
		})
	}
	return lo, hi
}

func mergeSplitKeys(left, right [][]byte) [][]byte {
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	merged := make([][]byte, 0, len(left)+len(right))
	for len(left) > 0 || len(right) > 0 {
		var next []byte
		switch {
		case len(left) == 0:
			next, right = right[0], right[1:]
		case len(right) == 0:
			next, left = left[0], left[1:]
		case bytes.Compare(left[0], right[0]) <= 0:
			next, left = left[0], left[1:]
		default:
			next, right = right[0], right[1:]
		}
		if len(merged) == 0 || !bytes.Equal(merged[len(merged)-1], next) {
			merged = append(merged, next)
		}
	}
	return merged
}
