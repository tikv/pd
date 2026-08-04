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
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

const (
	keyspaceChunkBits  = 10
	keyspaceChunkSize  = 1 << keyspaceChunkBits
	keyspaceChunkMask  = keyspaceChunkSize - 1
	keyspaceChunkWords = keyspaceChunkSize / 64

	keyspaceChunkCount              = (int(constant.MaxValidKeyspaceID) + 1 + keyspaceChunkSize - 1) / keyspaceChunkSize
	keyspaceChunkBitmapWords        = (keyspaceChunkCount + 63) / 64
	keyspaceChunkBitmapSummaryWords = (keyspaceChunkBitmapWords + 63) / 64
	keyspaceBoundaryCount           = int(constant.MaxValidKeyspaceID) + 2
)

// A keyspace rule has one fixed-width range per enabled API mode. Keeping
// these ranges in sparse slots avoids materializing two split points and a
// segment for every keyspace. Slot presence bits live with their sparse chunks,
// while a compact two-level chunk bitmap lets range scans skip unallocated ID
// spans. The rule remains owned by labelRuleIndex's authoritative map.
type keyspaceRuleChunk struct {
	rules [keyspaceChunkSize]*LabelRule
	bits  [keyspaceChunkWords]uint64
	count uint16
}

type keyspaceRuleSet struct {
	chunks                     []*keyspaceRuleChunk
	nonEmptyChunkBitmap        [keyspaceChunkBitmapWords]uint64
	nonEmptyChunkBitmapSummary [keyspaceChunkBitmapSummaryWords]uint64
}

func (s *keyspaceRuleSet) get(id uint32) *LabelRule {
	if len(s.chunks) == 0 || id > constant.MaxValidKeyspaceID {
		return nil
	}
	chunkID := int(id) >> keyspaceChunkBits
	if chunkID >= len(s.chunks) {
		return nil
	}
	chunk := s.chunks[chunkID]
	if chunk == nil {
		return nil
	}
	return chunk.rules[int(id)&keyspaceChunkMask]
}

func (s *keyspaceRuleSet) set(id uint32, rule *LabelRule) {
	// Add checks that the target slot is empty before calling set.
	chunkID := int(id) >> keyspaceChunkBits
	if chunkID >= len(s.chunks) {
		s.chunks = append(s.chunks, make([]*keyspaceRuleChunk, chunkID+1-len(s.chunks))...)
	}
	if s.chunks[chunkID] == nil {
		s.chunks[chunkID] = new(keyspaceRuleChunk)
		word := chunkID >> 6
		if s.nonEmptyChunkBitmap[word] == 0 {
			s.nonEmptyChunkBitmapSummary[word>>6] |= uint64(1) << (word & 63)
		}
		s.nonEmptyChunkBitmap[word] |= uint64(1) << (chunkID & 63)
	}
	chunk := s.chunks[chunkID]
	slot := int(id) & keyspaceChunkMask
	chunk.count++
	chunk.rules[slot] = rule
	chunk.bits[slot>>6] |= uint64(1) << (id & 63)
}

func (s *keyspaceRuleSet) replace(id uint32, rule *LabelRule) {
	chunk := s.chunks[int(id)>>keyspaceChunkBits]
	chunk.rules[int(id)&keyspaceChunkMask] = rule
}

func (s *keyspaceRuleSet) clear(id uint32) {
	// Remove checks that the target slot is owned before calling clear.
	if len(s.chunks) == 0 || id > constant.MaxValidKeyspaceID {
		return
	}
	chunkID := int(id) >> keyspaceChunkBits
	if chunkID >= len(s.chunks) {
		return
	}
	chunk := s.chunks[chunkID]
	if chunk == nil {
		return
	}
	slot := int(id) & keyspaceChunkMask
	chunk.rules[slot] = nil
	chunk.bits[slot>>6] &^= uint64(1) << (id & 63)
	chunk.count--
	if chunk.count == 0 {
		s.chunks[chunkID] = nil
		word := chunkID >> 6
		s.nonEmptyChunkBitmap[word] &^= uint64(1) << (chunkID & 63)
		if s.nonEmptyChunkBitmap[word] == 0 {
			s.nonEmptyChunkBitmapSummary[word>>6] &^= uint64(1) << (word & 63)
		}
		for len(s.chunks) > 0 && s.chunks[len(s.chunks)-1] == nil {
			s.chunks = s.chunks[:len(s.chunks)-1]
		}
	}
}

// forEachSlot visits populated keyspace IDs in ascending order within [lo, hi),
// stopping early when fn returns false.
func (s *keyspaceRuleSet) forEachSlot(lo, hi int, fn func(id uint32) bool) {
	if len(s.chunks) == 0 || lo >= hi {
		return
	}
	lo = max(lo, 0)
	hi = min(hi, int(constant.MaxValidKeyspaceID)+1)
	if lo >= hi {
		return
	}

	firstChunk, lastChunk := lo>>keyspaceChunkBits, (hi-1)>>keyspaceChunkBits
	firstChunkWord, lastChunkWord := firstChunk>>6, lastChunk>>6
	firstSummaryWord, lastSummaryWord := firstChunkWord>>6, lastChunkWord>>6
	for summaryWord := firstSummaryWord; summaryWord <= lastSummaryWord; summaryWord++ {
		chunkWords := s.nonEmptyChunkBitmapSummary[summaryWord]
		if offset := firstChunkWord & 63; summaryWord == firstSummaryWord && offset != 0 {
			chunkWords &= ^uint64(0) << offset
		}
		if offset := (lastChunkWord & 63) + 1; summaryWord == lastSummaryWord && offset != 64 {
			chunkWords &= (uint64(1) << offset) - 1
		}
		for chunkWords != 0 {
			chunkWordBit := bits.TrailingZeros64(chunkWords)
			chunkWord := (summaryWord << 6) + chunkWordBit
			chunks := s.nonEmptyChunkBitmap[chunkWord]
			if offset := firstChunk & 63; chunkWord == firstChunkWord && offset != 0 {
				chunks &= ^uint64(0) << offset
			}
			if offset := (lastChunk & 63) + 1; chunkWord == lastChunkWord && offset != 64 {
				chunks &= (uint64(1) << offset) - 1
			}
			for chunks != 0 {
				chunkBit := bits.TrailingZeros64(chunks)
				chunkID := (chunkWord << 6) + chunkBit
				chunk := s.chunks[chunkID]
				chunkStart := chunkID << keyspaceChunkBits
				localLo := max(lo-chunkStart, 0)
				localHi := min(hi-chunkStart, keyspaceChunkSize)
				firstWord, lastWord := localLo>>6, (localHi-1)>>6
				for word := firstWord; word <= lastWord; word++ {
					slots := chunk.bits[word]
					if offset := localLo & 63; word == firstWord && offset != 0 {
						slots &= ^uint64(0) << offset
					}
					if offset := localHi & 63; word == lastWord && offset != 0 {
						slots &= (uint64(1) << offset) - 1
					}
					for slots != 0 {
						bit := bits.TrailingZeros64(slots)
						id := chunkStart + (word << 6) + bit
						if !fn(uint32(id)) {
							return
						}
						slots &= slots - 1
					}
				}
				chunks &= chunks - 1
			}
			chunkWords &= chunkWords - 1
		}
	}
}

func (s *keyspaceRuleSet) forEachBoundary(lo, hi int, fn func(id uint32) bool) {
	if len(s.chunks) == 0 || lo >= hi {
		return
	}
	lo = max(lo, 0)
	hi = min(hi, keyspaceBoundaryCount)
	lastBoundary := -1
	emit := func(boundary int) bool {
		if boundary < lo || boundary >= hi || boundary == lastBoundary {
			return true
		}
		lastBoundary = boundary
		return fn(uint32(boundary))
	}
	s.forEachSlot(lo-1, hi, func(id uint32) bool {
		boundary := int(id)
		return emit(boundary) && emit(boundary+1)
	})
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

func (i *keyspaceRuleIndex) isEmpty() bool {
	return len(i.raw.chunks) == 0 && len(i.txn.chunks) == 0
}

func (i *keyspaceRuleIndex) ruleSet(mode byte) *keyspaceRuleSet {
	switch mode {
	case codec.RawKeyspaceModePrefix:
		return &i.raw
	case codec.TxnKeyspaceModePrefix:
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

// Replace updates the slots owned by old to point to rule. It returns false
// when old is not indexed, rule is not canonical, or a different rule occupies
// one of rule's slots.
func (i *keyspaceRuleIndex) Replace(old, rule *LabelRule) bool {
	ranges, count, ok := keyspaceRanges(rule)
	if !ok {
		return false
	}
	id := ranges[0].id
	owned := false
	for _, mode := range codec.KeyspaceModes() {
		if i.ruleSet(mode).get(id) == old {
			owned = true
		}
	}
	if !owned {
		return false
	}
	for _, r := range ranges[:count] {
		if current := i.ruleSet(r.mode).get(r.id); current != nil && current != old {
			return false
		}
	}

	for _, mode := range codec.KeyspaceModes() {
		set := i.ruleSet(mode)
		current := set.get(id)
		wanted := false
		for _, r := range ranges[:count] {
			if r.mode == mode {
				wanted = true
				break
			}
		}
		switch {
		case current == old && wanted:
			set.replace(id, rule)
		case current == old:
			set.clear(id)
		case wanted:
			set.set(id, rule)
		}
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
	for _, mode := range codec.KeyspaceModes() {
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
	rule, _ := i.matchRule(start, end)
	return rule
}

func (i *keyspaceRuleIndex) matchRule(start, end []byte) (*LabelRule, bool) {
	if len(end) == 0 {
		return nil, false
	}
	mode, id, ok := codec.DecodeKeyspaceKey(start)
	if !ok {
		return nil, false
	}
	set := i.ruleSet(mode)
	if set == nil {
		return nil, false
	}
	rule := set.get(id)
	if len(end) >= codec.KeyspacePrefixLen && bytes.Equal(start[:codec.KeyspacePrefixLen], end[:codec.KeyspacePrefixLen]) {
		return rule, true
	}
	right := keyspaceBoundary(mode, id+1)
	// DecodeKeyspaceKey guarantees that start is not below this ID's boundary.
	if bytes.Compare(end, right[:]) > 0 {
		return nil, false
	}
	return rule, true
}

// HasSplitKey reports whether a keyspace boundary exists in (start, end).
func (i *keyspaceRuleIndex) HasSplitKey(start, end []byte) bool {
	for _, mode := range codec.KeyspaceModes() {
		set := i.ruleSet(mode)
		if len(set.chunks) == 0 || !keyspaceModeOverlapsRange(mode, start, end) {
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
	for _, mode := range codec.KeyspaceModes() {
		set := i.ruleSet(mode)
		if len(set.chunks) == 0 || !keyspaceModeOverlapsRange(mode, start, end) {
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
	if label.Key != constant.RegionLabelKey || label.TTL != "" || label.StartAt != "" || label.expire != nil {
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
		case codec.RawKeyspaceModePrefix:
			if seenRaw {
				return ranges, 0, false
			}
			seenRaw = true
		case codec.TxnKeyspaceModePrefix:
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
	idText, ok := strings.CutPrefix(ruleID, constant.RegionLabelIDPrefix)
	if !ok {
		return 0, "", false
	}
	id64, err := strconv.ParseUint(idText, 10, 32)
	hasLeadingZero := len(idText) > 1 && idText[0] == '0'
	if err != nil || id64 > uint64(constant.MaxValidKeyspaceID) || hasLeadingZero {
		return 0, "", false
	}
	return uint32(id64), idText, true
}

func canonicalKeyspaceRange(keyRange *KeyRangeRule, id uint32) (byte, bool) {
	if keyRange == nil {
		return 0, false
	}
	for _, mode := range codec.KeyspaceModes() {
		start := keyspaceBoundary(mode, id)
		end := keyspaceBoundary(mode, id+1)
		if bytes.Equal(keyRange.StartKey, start[:]) && bytes.Equal(keyRange.EndKey, end[:]) {
			return mode, true
		}
	}
	return 0, false
}

func keyspaceBoundary(mode byte, id uint32) [9]byte {
	return codec.EncodeKeyspaceBoundary(mode, id)
}

func keyspaceBoundaryBytes(mode byte, id uint32) []byte {
	key := keyspaceBoundary(mode, id)
	return key[:]
}

func keyspaceBoundaryRange(mode byte, start, end []byte) (lo, hi int) {
	lo = keyspaceBoundaryBound(mode, start, true)
	hi = keyspaceBoundaryCount
	if len(end) > 0 {
		hi = keyspaceBoundaryBound(mode, end, false)
	}
	return lo, hi
}

func keyspaceModeOverlapsRange(mode byte, start, end []byte) bool {
	first := keyspaceBoundary(mode, 0)
	if len(end) > 0 && bytes.Compare(end, first[:]) <= 0 {
		return false
	}
	fence := keyspaceBoundary(mode, uint32(keyspaceBoundaryCount-1))
	return bytes.Compare(start, fence[:]) < 0
}

func keyspaceModesOverlapRange(start, end []byte) bool {
	if len(end) > 0 {
		switch {
		case end[0] < codec.RawKeyspaceModePrefix:
			return false
		case end[0] == codec.RawKeyspaceModePrefix:
			first := keyspaceBoundary(codec.RawKeyspaceModePrefix, 0)
			if bytes.Compare(end, first[:]) <= 0 {
				return false
			}
		}
	}
	if len(start) == 0 || start[0] < codec.TxnKeyspaceModePrefix+1 {
		return true
	}
	if start[0] > codec.TxnKeyspaceModePrefix+1 {
		return false
	}
	fence := keyspaceBoundary(codec.TxnKeyspaceModePrefix, uint32(keyspaceBoundaryCount-1))
	return bytes.Compare(start, fence[:]) < 0
}

func keyspaceBoundaryBound(mode byte, key []byte, upper bool) int {
	first := keyspaceBoundary(mode, 0)
	if cmp := bytes.Compare(first[:], key); cmp > 0 || cmp == 0 && !upper {
		return 0
	}

	fenceID := uint32(keyspaceBoundaryCount - 1)
	fence := keyspaceBoundary(mode, fenceID)
	if cmp := bytes.Compare(fence[:], key); cmp < 0 || cmp == 0 && upper {
		return keyspaceBoundaryCount
	} else if cmp == 0 {
		return int(fenceID)
	}

	// A key between the last boundary of this mode and its fence belongs at
	// the fence. This also handles a fence prefix shorter than the boundary.
	if key[0] != mode {
		return int(fenceID)
	}

	var id uint32
	if len(key) > 1 {
		id |= uint32(key[1]) << 16
	}
	if len(key) > 2 {
		id |= uint32(key[2]) << 8
	}
	if len(key) > 3 {
		id |= uint32(key[3])
	}
	boundary := keyspaceBoundary(mode, id)
	if cmp := bytes.Compare(boundary[:], key); cmp > 0 || cmp == 0 && !upper {
		return int(id)
	}
	return int(id) + 1
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
