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
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/rangelist"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

func makeKeyspaceRuleForTest(id uint32, modes ...byte) *LabelRule {
	ranges := make([]any, 0, len(modes))
	for _, mode := range modes {
		start := keyspaceBoundary(mode, id)
		end := keyspaceBoundary(mode, id+1)
		ranges = append(ranges, map[string]any{
			"start_key": hex.EncodeToString(start[:]),
			"end_key":   hex.EncodeToString(end[:]),
		})
	}
	return &LabelRule{
		ID:       fmt.Sprintf("%s%d", constant.RegionLabelIDPrefix, id),
		Labels:   []RegionLabel{{Key: constant.RegionLabelKey, Value: strconv.FormatUint(uint64(id), 10)}},
		RuleType: KeyRange,
		Data:     ranges,
	}
}

func makeRegionForKeyspace(id uint32, mode byte) *core.RegionInfo {
	start := codec.EncodeBytes(append(codec.MakeKeyspacePrefix(mode, id), 'a'))
	end := codec.EncodeBytes(append(codec.MakeKeyspacePrefix(mode, id), 'z'))
	return core.NewTestRegionInfo(1, 1, start, end)
}

func TestKeyspaceRuleIndex(t *testing.T) {
	re := require.New(t)
	rule := makeKeyspaceRuleForTest(42, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
	re.NoError(rule.CheckAndAdjust())

	var index keyspaceRuleIndex
	re.True(index.Add(rule))
	re.True(index.Contains(rule))
	re.Same(rule, index.GetRule(
		makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix).GetStartKey(),
		makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix).GetEndKey(),
	))
	re.Same(rule, index.GetRule(
		makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix).GetStartKey(),
		makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix).GetEndKey(),
	))

	rawStart := keyspaceBoundary(codec.RawKeyspaceModePrefix, 41)
	rawEnd := keyspaceBoundary(codec.RawKeyspaceModePrefix, 44)
	splitKeys := index.GetSplitKeys(rawStart[:], rawEnd[:])
	re.Equal([][]byte{
		keyspaceBoundaryBytes(codec.RawKeyspaceModePrefix, 42),
		keyspaceBoundaryBytes(codec.RawKeyspaceModePrefix, 43),
	}, splitKeys)
	re.True(index.HasSplitKey(rawStart[:], rawEnd[:]))

	// A rejected multi-range add must not populate its free slot before it
	// discovers a collision in another slot.
	txnOwner := makeKeyspaceRuleForTest(43, codec.TxnKeyspaceModePrefix)
	re.NoError(txnOwner.CheckAndAdjust())
	re.True(index.Add(txnOwner))
	collision := makeKeyspaceRuleForTest(43, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
	re.NoError(collision.CheckAndAdjust())
	re.False(index.Add(collision))
	re.True(index.Contains(txnOwner))
	re.False(index.Contains(collision))
	re.Nil(index.GetRule(
		makeRegionForKeyspace(43, codec.RawKeyspaceModePrefix).GetStartKey(),
		makeRegionForKeyspace(43, codec.RawKeyspaceModePrefix).GetEndKey(),
	))
	re.Same(txnOwner, index.GetRule(
		makeRegionForKeyspace(43, codec.TxnKeyspaceModePrefix).GetStartKey(),
		makeRegionForKeyspace(43, codec.TxnKeyspaceModePrefix).GetEndKey(),
	))
	re.True(index.Remove(txnOwner.ID, txnOwner))

	re.True(index.Remove(rule.ID, rule))
	re.False(index.Contains(rule))
	re.Empty(index.GetSplitKeys(rawStart[:], rawEnd[:]))
}

func TestKeyspaceRuleIndexRejectsNonCanonicalID(t *testing.T) {
	re := require.New(t)
	rule := makeKeyspaceRuleForTest(1, codec.TxnKeyspaceModePrefix)
	rule.ID = constant.RegionLabelIDPrefix + "01"
	rule.Labels[0].Value = "01"
	re.NoError(rule.CheckAndAdjust())

	var index keyspaceRuleIndex
	re.False(index.Add(rule))
}

func TestKeyspaceRuleIndexBoundaries(t *testing.T) {
	re := require.New(t)
	ids := []uint32{0, 63, 64, 1023, 1024, constant.MaxValidKeyspaceID}
	var index keyspaceRuleIndex
	expectedByKey := make(map[string][]byte)
	for _, id := range ids {
		rule := makeKeyspaceRuleForTest(id, codec.TxnKeyspaceModePrefix)
		re.NoError(rule.CheckAndAdjust())
		re.True(index.Add(rule))
		for _, boundaryID := range []uint32{id, id + 1} {
			key := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, boundaryID)
			expectedByKey[string(key)] = key
		}

		left := keyspaceBoundary(codec.TxnKeyspaceModePrefix, id)
		right := keyspaceBoundary(codec.TxnKeyspaceModePrefix, id+1)
		re.Same(rule, index.GetRule(left[:], right[:]))
		region := makeRegionForKeyspace(id, codec.TxnKeyspaceModePrefix)
		re.Same(rule, index.GetRule(region.GetStartKey(), region.GetEndKey()))
	}

	expected := make([][]byte, 0, len(expectedByKey))
	for _, key := range expectedByKey {
		expected = append(expected, key)
	}
	sort.Slice(expected, func(i, j int) bool {
		return string(expected[i]) < string(expected[j])
	})
	re.Equal(expected, index.GetSplitKeys(nil, nil))
}

func TestKeyspaceRuleIndexMatchesIncompletePrefix(t *testing.T) {
	re := require.New(t)
	rule255 := makeKeyspaceRuleForTest(255, codec.TxnKeyspaceModePrefix)
	rule256 := makeKeyspaceRuleForTest(256, codec.TxnKeyspaceModePrefix)
	maxRule := makeKeyspaceRuleForTest(
		constant.MaxValidKeyspaceID,
		codec.RawKeyspaceModePrefix,
		codec.TxnKeyspaceModePrefix,
	)

	var index keyspaceRuleIndex
	for _, rule := range []*LabelRule{rule255, rule256, maxRule} {
		re.NoError(rule.checkAndAdjust())
		re.True(index.Add(rule))
	}

	b256 := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 256)
	b257 := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 257)
	below256 := append([]byte(nil), b256...)
	below256[len(below256)-1]--
	above256Malformed := append([]byte(nil), b256...)
	above256Malformed[4] = 1
	above256Malformed[len(above256Malformed)-1]--
	inside256 := codec.EncodeBytes([]byte{codec.TxnKeyspaceModePrefix, 0, 1, 0, 'a'})
	rawFence := keyspaceBoundaryBytes(codec.RawKeyspaceModePrefix, constant.MaxValidKeyspaceID+1)
	txnFirst := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 0)
	txnFence := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID+1)

	for _, testCase := range []struct {
		name       string
		start      []byte
		end        []byte
		want       *LabelRule
		withinMode bool
	}{
		{name: "incomplete-marker-before-boundary", start: below256, end: b256, want: rule255, withinMode: true},
		{name: "short-boundary-prefix", start: b256[:codec.KeyspacePrefixLen], end: b256, want: rule255, withinMode: true},
		{name: "incomplete-prefix-crosses-boundary", start: below256, end: inside256},
		{name: "malformed-marker-after-boundary", start: above256Malformed, end: b257, want: rule256, withinMode: true},
		{name: "raw-fence-prefix", start: rawFence[:1], end: rawFence, want: maxRule, withinMode: true},
		{name: "txn-fence-prefix", start: txnFence[:8], end: txnFence, want: maxRule, withinMode: true},
		{name: "before-first-boundary", start: txnFirst[:codec.KeyspacePrefixLen], end: txnFirst},
		{name: "at-fence", start: txnFence, end: []byte{'z'}},
		{name: "unbounded-end", start: below256},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			got, withinMode := index.matchRule(testCase.start, testCase.end)
			if testCase.want == nil {
				re.Nil(got)
			} else {
				re.Same(testCase.want, got)
			}
			re.Equal(testCase.withinMode, withinMode)
		})
	}

	allocs := testing.AllocsPerRun(100, func() {
		got, withinMode := index.matchRule(below256, b256)
		if got != rule255 || !withinMode {
			panic("incomplete keyspace prefix did not match")
		}
	})
	re.Zero(allocs)
}

func TestKeyspaceRuleIndexSparseGap(t *testing.T) {
	re := require.New(t)
	rule := makeKeyspaceRuleForTest(constant.MaxValidKeyspaceID, codec.TxnKeyspaceModePrefix)
	re.NoError(rule.CheckAndAdjust())

	var index keyspaceRuleIndex
	re.True(index.Add(rule))
	gapRegion := makeRegionForKeyspace(1, codec.TxnKeyspaceModePrefix)
	matched, withinKeyspace := index.matchRule(gapRegion.GetStartKey(), gapRegion.GetEndKey())
	re.Nil(matched)
	re.True(withinKeyspace)
	labelIndex := newLabelRuleIndex(map[string]*LabelRule{rule.ID: rule})
	rangeRules, keyspaceRule, ok := labelIndex.getRangeRules(gapRegion.GetStartKey(), gapRegion.GetEndKey())
	re.True(ok)
	re.Empty(rangeRules)
	re.Nil(keyspaceRule)
	rangeRules, keyspaceRule, ok = labelIndex.getRangeRules([]byte{'a'}, []byte{'b'})
	re.True(ok)
	re.Empty(rangeRules)
	re.Nil(keyspaceRule)
	start := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 0)
	end := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID)
	re.Empty(index.GetSplitKeys(start, end))
	re.False(index.HasSplitKey(start, end))
	afterFence := []byte{'z'}
	re.Empty(index.GetSplitKeys(afterFence, nil))
	re.False(index.HasSplitKey(afterFence, nil))
}

func TestKeyspaceBoundaryBoundMatchesBinarySearch(t *testing.T) {
	re := require.New(t)
	keys := [][]byte{nil, {'a'}, {'z'}, {0xff}}
	for _, mode := range codec.KeyspaceModes() {
		keys = append(keys, []byte{mode}, []byte{mode, 1}, []byte{mode + 1})
		for _, id := range []uint32{0, 1, 63, 64, 1023, 1024, constant.MaxValidKeyspaceID, constant.MaxValidKeyspaceID + 1} {
			boundary := keyspaceBoundary(mode, id)
			exact := append([]byte(nil), boundary[:]...)
			before := append([]byte(nil), boundary[:]...)
			before[len(before)-1]--
			after := append([]byte(nil), boundary[:]...)
			after[len(after)-1]++
			keys = append(keys, exact, before, after)
			for length := 1; length < len(boundary); length++ {
				keys = append(keys, append([]byte(nil), boundary[:length]...))
			}
		}
	}

	for _, mode := range codec.KeyspaceModes() {
		for _, key := range keys {
			lower := sort.Search(keyspaceBoundaryCount, func(id int) bool {
				boundary := keyspaceBoundary(mode, uint32(id))
				return bytes.Compare(boundary[:], key) >= 0
			})
			upper := sort.Search(keyspaceBoundaryCount, func(id int) bool {
				boundary := keyspaceBoundary(mode, uint32(id))
				return bytes.Compare(boundary[:], key) > 0
			})
			re.Equal(lower, keyspaceBoundaryBound(mode, key, false), "lower bound for %x", key)
			re.Equal(upper, keyspaceBoundaryBound(mode, key, true), "upper bound for %x", key)
		}
	}
}

func TestKeyspaceRuleSetUsesSparseChunks(t *testing.T) {
	re := require.New(t)
	var set keyspaceRuleSet
	rules := []*LabelRule{{ID: "low"}, {ID: "next-chunk"}, {ID: "max"}}

	set.set(1, rules[0])
	re.Len(set.chunks, 1)

	set.set(keyspaceChunkSize, rules[1])
	re.Len(set.chunks, 2)

	set.set(constant.MaxValidKeyspaceID, rules[2])
	re.Len(set.chunks, (int(constant.MaxValidKeyspaceID)>>keyspaceChunkBits)+1)

	set.clear(constant.MaxValidKeyspaceID)
	re.Len(set.chunks, 2)
	set.clear(keyspaceChunkSize)
	re.Len(set.chunks, 1)
	set.clear(1)
	re.Empty(set.chunks)
	re.Zero(set.nonEmptyChunkBitmap)
	re.Zero(set.nonEmptyChunkBitmapSummary)
}

func TestKeyspaceRuleSetIteratesChunkBitmapBoundaries(t *testing.T) {
	re := require.New(t)
	ids := []uint32{
		0,
		63 << keyspaceChunkBits,
		64 << keyspaceChunkBits,
		4095 << keyspaceChunkBits,
		4096 << keyspaceChunkBits,
		constant.MaxValidKeyspaceID,
	}
	var set keyspaceRuleSet
	for _, id := range ids {
		set.set(id, &LabelRule{ID: strconv.FormatUint(uint64(id), 10)})
	}

	collect := func(lo, hi int) []uint32 {
		var got []uint32
		set.forEachSlot(lo, hi, func(id uint32) bool {
			got = append(got, id)
			return true
		})
		return got
	}

	re.Equal(ids, collect(0, int(constant.MaxValidKeyspaceID)+1))
	re.Equal(ids[2:5], collect(int(ids[1])+1, int(ids[4])+1))
	set.clear(ids[2])
	set.clear(ids[4])
	re.Equal([]uint32{ids[1], ids[3]}, collect(int(ids[1]), int(ids[4])+1))
}

func TestKeyspaceRuleIndexReplaceReusesSparseStorage(t *testing.T) {
	for _, id := range []uint32{1, constant.MaxValidKeyspaceID} {
		t.Run(strconv.FormatUint(uint64(id), 10), func(t *testing.T) {
			re := require.New(t)
			first := makeKeyspaceRuleForTest(id, codec.TxnKeyspaceModePrefix)
			second := makeKeyspaceRuleForTest(id, codec.TxnKeyspaceModePrefix)
			re.NoError(first.CheckAndAdjust())
			re.NoError(second.CheckAndAdjust())

			var index keyspaceRuleIndex
			re.True(index.Add(first))
			chunkID := int(id) >> keyspaceChunkBits
			chunksLen := len(index.txn.chunks)
			chunk := index.txn.chunks[chunkID]
			current, replacement := first, second
			allocs := testing.AllocsPerRun(100, func() {
				if !index.Replace(current, replacement) {
					panic("failed to replace keyspace rule")
				}
				current, replacement = replacement, current
			})

			re.Zero(allocs)
			re.Len(index.txn.chunks, chunksLen)
			re.Same(chunk, index.txn.chunks[chunkID])
			re.Same(current, index.txn.get(id))
		})
	}
}

func buildLegacyRangeList(rules []*LabelRule) rangelist.List {
	builder := rangelist.NewBuilder()
	for _, rule := range rules {
		for _, keyRange := range rule.GetKeyRanges() {
			builder.AddItem(keyRange.StartKey, keyRange.EndKey, rule)
		}
	}
	return builder.Build()
}

func getLegacyRegionLabels(list rangelist.List, region *core.RegionInfo) map[string]string {
	labels := make(map[string]string)
	indexes := make(map[string]int)
	if index, data := list.GetData(region.GetStartKey(), region.GetEndKey()); index != -1 {
		for _, item := range data {
			rule := item.(*LabelRule)
			for _, label := range rule.Labels {
				if oldIndex, ok := indexes[label.Key]; !ok || oldIndex < rule.Index {
					labels[label.Key] = label.Value
					indexes[label.Key] = rule.Index
				}
			}
		}
	}
	return labels
}

func TestKeyspaceRuleIndexPreservesLegacyResults(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionLabeler, err := NewRegionLabeler(ctx, store, time.Hour)
	re.NoError(err)

	var rules []*LabelRule
	for _, id := range []uint32{0, 1, 63, 64, 255, 256, 1023, 1024, 65535, constant.MaxValidKeyspaceID} {
		rule := makeKeyspaceRuleForTest(id, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
		rules = append(rules, rule)
		re.NoError(regionLabeler.SetLabelRule(rule))
	}
	genericRule := &LabelRule{
		ID:       "generic",
		Index:    1,
		Labels:   []RegionLabel{{Key: "generic", Value: "yes"}},
		RuleType: KeyRange,
		Data:     MakeKeyRanges("", ""),
	}
	rules = append(rules, genericRule)
	re.NoError(regionLabeler.SetLabelRule(genericRule))
	overlayStart := keyspaceBoundary(codec.TxnKeyspaceModePrefix, 64)
	overlayEnd := keyspaceBoundary(codec.TxnKeyspaceModePrefix, 65)
	overlayRule := &LabelRule{
		ID:       "generic-overlay",
		Index:    2,
		Labels:   []RegionLabel{{Key: constant.RegionLabelKey, Value: "override"}},
		RuleType: KeyRange,
		Data: MakeKeyRanges(
			hex.EncodeToString(overlayStart[:]),
			hex.EncodeToString(overlayEnd[:]),
		),
	}
	rules = append(rules, overlayRule)
	re.NoError(regionLabeler.SetLabelRule(overlayRule))

	legacy := buildLegacyRangeList(rules)
	var regions []*core.RegionInfo
	for _, id := range []uint32{0, 1, 42, 63, 64, 1023, 1024, constant.MaxValidKeyspaceID} {
		for _, mode := range codec.KeyspaceModes() {
			regions = append(regions, makeRegionForKeyspace(id, mode))
		}
	}
	for _, id := range []uint32{0, 63, 1023} {
		for _, mode := range codec.KeyspaceModes() {
			left, right := makeRegionForKeyspace(id, mode), makeRegionForKeyspace(id+1, mode)
			regions = append(regions, core.NewTestRegionInfo(2, 1, left.GetStartKey(), right.GetEndKey()))
		}
	}
	b256 := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 256)
	incomplete256 := codec.EncodeBytes([]byte{codec.TxnKeyspaceModePrefix, 0, 1})
	inside256 := codec.EncodeBytes([]byte{codec.TxnKeyspaceModePrefix, 0, 1, 0, 'a'})
	txnFence := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID+1)
	regions = append(regions,
		core.NewTestRegionInfo(3, 1, incomplete256, b256),
		core.NewTestRegionInfo(4, 1, incomplete256, inside256),
		core.NewTestRegionInfo(5, 1, txnFence[:1], txnFence),
	)
	for _, region := range regions {
		want := getLegacyRegionLabels(legacy, region)
		got := make(map[string]string)
		for _, label := range regionLabeler.GetRegionLabels(region) {
			got[label.Key] = label.Value
		}
		re.Equal(want, got, "start=%x end=%x", region.GetStartKey(), region.GetEndKey())
	}

	for _, keyRange := range [][2][]byte{
		{nil, nil},
		{keyspaceBoundaryBytes(codec.RawKeyspaceModePrefix, 0), keyspaceBoundaryBytes(codec.RawKeyspaceModePrefix, 65)},
		{keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 62), keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 65)},
		{keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 1023), keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 1025)},
		{keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID), nil},
	} {
		re.Equal(
			legacy.GetSplitKeys(keyRange[0], keyRange[1]),
			regionLabeler.GetSplitKeys(keyRange[0], keyRange[1]),
		)
	}
}

func TestRegionLabelerUpdatesKeyspaceRulesIncrementally(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionLabeler, err := NewRegionLabeler(ctx, store, time.Hour)
	re.NoError(err)

	genericRule := &LabelRule{
		ID:       "generic",
		Index:    1,
		Labels:   []RegionLabel{{Key: "generic", Value: "yes"}},
		RuleType: KeyRange,
		Data:     MakeKeyRanges("", ""),
	}
	re.NoError(regionLabeler.SetLabelRule(genericRule))
	re.Len(regionLabeler.ruleIndex.generic, 1)

	rule := makeKeyspaceRuleForTest(42, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
	re.NoError(regionLabeler.SetLabelRule(rule))
	re.False(regionLabeler.ruleIndex.rangesDirty)
	re.Len(regionLabeler.ruleIndex.generic, 1)
	re.True(regionLabeler.ruleIndex.keyspaces.Contains(rule))

	for _, mode := range codec.KeyspaceModes() {
		region := makeRegionForKeyspace(42, mode)
		re.Equal("42", regionLabeler.GetRegionLabel(region, constant.RegionLabelKey))
		re.Equal("yes", regionLabeler.GetRegionLabel(region, "generic"))
	}
	startRegion := makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix)
	endRegion := makeRegionForKeyspace(43, codec.TxnKeyspaceModePrefix)
	crossingRegion := core.NewTestRegionInfo(2, 1, startRegion.GetStartKey(), endRegion.GetEndKey())
	re.Empty(regionLabeler.GetRegionLabel(crossingRegion, constant.RegionLabelKey))
	re.Empty(regionLabeler.GetRegionLabel(crossingRegion, "generic"))

	// Changing the deterministic rule only touches its old and new slots.
	updated := makeKeyspaceRuleForTest(42, codec.TxnKeyspaceModePrefix)
	re.NoError(regionLabeler.SetLabelRule(updated))
	re.False(regionLabeler.ruleIndex.rangesDirty)
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix), constant.RegionLabelKey))
	re.Equal("42", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix), constant.RegionLabelKey))

	// Moving a rule between the canonical and generic indexes keeps the two
	// derived views in sync with the authoritative map.
	genericReplacement := makeKeyspaceRuleForTest(42, codec.TxnKeyspaceModePrefix)
	genericReplacement.Labels[0].Key = "generic-replacement"
	re.NoError(regionLabeler.SetLabelRule(genericReplacement))
	re.False(regionLabeler.ruleIndex.keyspaces.Contains(updated))
	re.Contains(regionLabeler.ruleIndex.generic, genericReplacement.ID)
	re.Equal("42", regionLabeler.GetRegionLabel(
		makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix), "generic-replacement"))

	restored := makeKeyspaceRuleForTest(42, codec.TxnKeyspaceModePrefix)
	re.NoError(regionLabeler.SetLabelRule(restored))
	re.True(regionLabeler.ruleIndex.keyspaces.Contains(restored))
	re.NotContains(regionLabeler.ruleIndex.generic, restored.ID)

	re.NoError(regionLabeler.DeleteLabelRule(restored.ID))
	re.False(regionLabeler.ruleIndex.rangesDirty)
	re.False(regionLabeler.ruleIndex.keyspaces.Contains(restored))
	re.Equal("yes", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix), "generic"))
}

func TestRegionLabelerUpdatesMutatedKeyspaceRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionLabeler, err := NewRegionLabeler(ctx, store, time.Hour)
	re.NoError(err)

	rule := makeKeyspaceRuleForTest(42, codec.TxnKeyspaceModePrefix)
	re.NoError(regionLabeler.SetLabelRule(rule))

	fetched := regionLabeler.GetLabelRule(rule.ID)
	fetched.Data = makeKeyspaceRuleForTest(42, codec.RawKeyspaceModePrefix).Data
	re.NoError(regionLabeler.SetLabelRule(fetched))
	re.Equal("42", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix), constant.RegionLabelKey))
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix), constant.RegionLabelKey))

	fetched.Labels[0].Value = "mutated"
	re.NoError(regionLabeler.DeleteLabelRule(fetched.ID))
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix), constant.RegionLabelKey))
}

func TestNewLabelRuleIndexBuildsAuthoritativeSnapshot(t *testing.T) {
	re := require.New(t)
	rule := makeKeyspaceRuleForTest(42, codec.TxnKeyspaceModePrefix)
	re.NoError(rule.CheckAndAdjust())
	stale := makeKeyspaceRuleForTest(41, codec.TxnKeyspaceModePrefix)
	re.NoError(stale.CheckAndAdjust())

	index := newLabelRuleIndex(map[string]*LabelRule{stale.ID: stale})
	re.True(index.keyspaces.Contains(stale))
	index = newLabelRuleIndex(map[string]*LabelRule{rule.ID: rule})
	re.False(index.keyspaces.Contains(stale))
	re.True(index.keyspaces.Contains(rule))
	re.Empty(index.generic)

	regionLabeler := &RegionLabeler{ruleIndex: index}
	regionLabeler.Lock()
	regionLabeler.ruleIndex.delete(rule.ID)
	regionLabeler.Unlock()
	region := makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix)
	re.Empty(regionLabeler.GetRegionLabel(region, constant.RegionLabelKey))
	re.Empty(regionLabeler.GetSplitKeys(region.GetStartKey(), region.GetEndKey()))
}

func BenchmarkKeyspaceRuleIndexSparse(b *testing.B) {
	rule := makeKeyspaceRuleForTest(0, codec.TxnKeyspaceModePrefix)
	require.NoError(b, rule.CheckAndAdjust())

	b.Run("add-one", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var index keyspaceRuleIndex
			if !index.Add(rule) {
				b.Fatal("failed to add keyspace rule")
			}
		}
	})

	b.Run("full-range-split", func(b *testing.B) {
		var index keyspaceRuleIndex
		require.True(b, index.Add(rule))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if keys := index.GetSplitKeys(nil, nil); len(keys) != 2 {
				b.Fatalf("expected 2 split keys, got %d", len(keys))
			}
		}
	})

	b.Run("high-id-negative-split", func(b *testing.B) {
		highRule := makeKeyspaceRuleForTest(constant.MaxValidKeyspaceID, codec.TxnKeyspaceModePrefix)
		require.NoError(b, highRule.CheckAndAdjust())
		var index keyspaceRuleIndex
		require.True(b, index.Add(highRule))
		start := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 0)
		end := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if keys := index.GetSplitKeys(start, end); len(keys) != 0 {
				b.Fatalf("expected no split keys, got %d", len(keys))
			}
		}
	})
}

func BenchmarkRegionLabelerNegativeLookup(b *testing.B) {
	keyspaceRegion := makeRegionForKeyspace(1, codec.TxnKeyspaceModePrefix)
	classicRegion := core.NewTestRegionInfo(1, 1, []byte{'a'}, []byte{'b'})
	for _, testCase := range []struct {
		name         string
		withHighRule bool
		region       *core.RegionInfo
	}{
		{name: "empty-keyspace", region: keyspaceRegion},
		{name: "empty-classic", region: classicRegion},
		{name: "sparse-high-keyspace-gap", withHighRule: true, region: keyspaceRegion},
		{name: "sparse-high-classic-gap", withHighRule: true, region: classicRegion},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			regionLabeler, err := NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
			require.NoError(b, err)
			if testCase.withHighRule {
				rule := makeKeyspaceRuleForTest(constant.MaxValidKeyspaceID, codec.TxnKeyspaceModePrefix)
				require.NoError(b, regionLabeler.SetLabelRule(rule))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if value := regionLabeler.GetRegionLabel(testCase.region, constant.RegionLabelKey); value != "" {
					b.Fatalf("unexpected label %q", value)
				}
			}
		})
	}
}

// The startup/1000000 benchmark has a 3-second readiness target. It includes
// in-memory rule construction and index building, but excludes storage paging,
// etcd latency, and rule delivery.
func BenchmarkRegionLabelerKeyspaceIndex(b *testing.B) {
	for _, count := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("startup/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				rules := make(map[string]*LabelRule, count)
				for id := range count {
					rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
					require.NoError(b, rule.CheckAndAdjust())
					rules[rule.ID] = rule
				}
				_ = newLabelRuleIndex(rules)
			}
		})

		b.Run(fmt.Sprintf("single-rule-update/%d", count), func(b *testing.B) {
			rulesByID := make(map[string]*LabelRule, count)
			rules := make([]*LabelRule, 0, count)
			for id := range count {
				rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
				require.NoError(b, rule.CheckAndAdjust())
				rulesByID[rule.ID] = rule
				rules = append(rules, rule)
			}
			regionLabeler := &RegionLabeler{ruleIndex: newLabelRuleIndex(rulesByID)}
			target := rules[len(rules)/2]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				regionLabeler.Lock()
				regionLabeler.ruleIndex.set(target)
				regionLabeler.Unlock()
			}
		})

		b.Run(fmt.Sprintf("lookup/%d", count), func(b *testing.B) {
			rules := make(map[string]*LabelRule, count)
			for id := range count {
				rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
				require.NoError(b, rule.CheckAndAdjust())
				rules[rule.ID] = rule
			}
			regionLabeler := &RegionLabeler{ruleIndex: newLabelRuleIndex(rules)}
			region := makeRegionForKeyspace(uint32(count/2), codec.TxnKeyspaceModePrefix)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if value := regionLabeler.GetRegionLabel(region, constant.RegionLabelKey); value == "" {
					b.Fatal("keyspace label not found")
				}
			}
		})
	}
}
