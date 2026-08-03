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
	re.NoError(rule.checkAndAdjust())

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
	re.NoError(txnOwner.checkAndAdjust())
	re.True(index.Add(txnOwner))
	collision := makeKeyspaceRuleForTest(43, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
	re.NoError(collision.checkAndAdjust())
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

func TestKeyspaceRuleIndexBoundaries(t *testing.T) {
	re := require.New(t)
	ids := []uint32{0, 63, 64, 1023, 1024, constant.MaxValidKeyspaceID}
	var index keyspaceRuleIndex
	expectedByKey := make(map[string][]byte)
	for _, id := range ids {
		rule := makeKeyspaceRuleForTest(id, codec.TxnKeyspaceModePrefix)
		re.NoError(rule.checkAndAdjust())
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

func TestKeyspaceRuleIndexSparseGap(t *testing.T) {
	re := require.New(t)
	rule := makeKeyspaceRuleForTest(constant.MaxValidKeyspaceID, codec.TxnKeyspaceModePrefix)
	re.NoError(rule.checkAndAdjust())

	var index keyspaceRuleIndex
	re.True(index.Add(rule))
	start := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, 0)
	end := keyspaceBoundaryBytes(codec.TxnKeyspaceModePrefix, constant.MaxValidKeyspaceID)
	re.Empty(index.GetSplitKeys(start, end))
	re.False(index.HasSplitKey(start, end))
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
			re.NoError(first.checkAndAdjust())
			re.NoError(second.checkAndAdjust())

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
	for _, id := range []uint32{0, 1, 63, 64, 1023, 1024, constant.MaxValidKeyspaceID} {
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
		for _, mode := range []byte{codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix} {
			regions = append(regions, makeRegionForKeyspace(id, mode))
		}
	}
	for _, id := range []uint32{0, 63, 1023} {
		for _, mode := range []byte{codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix} {
			left, right := makeRegionForKeyspace(id, mode), makeRegionForKeyspace(id+1, mode)
			regions = append(regions, core.NewTestRegionInfo(2, 1, left.GetStartKey(), right.GetEndKey()))
		}
	}
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
	re.Len(regionLabeler.genericRules, 1)

	rule := makeKeyspaceRuleForTest(42, codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix)
	re.NoError(regionLabeler.SetLabelRule(rule))
	re.False(regionLabeler.rangeListDirty)
	re.Len(regionLabeler.genericRules, 1)
	re.True(regionLabeler.keyspaceRules.Contains(rule))

	for _, mode := range []byte{codec.RawKeyspaceModePrefix, codec.TxnKeyspaceModePrefix} {
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
	re.False(regionLabeler.rangeListDirty)
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.RawKeyspaceModePrefix), constant.RegionLabelKey))
	re.Equal("42", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, codec.TxnKeyspaceModePrefix), constant.RegionLabelKey))

	re.NoError(regionLabeler.DeleteLabelRule(updated.ID))
	re.False(regionLabeler.rangeListDirty)
	re.False(regionLabeler.keyspaceRules.Contains(updated))
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

func BenchmarkKeyspaceRuleIndexSparse(b *testing.B) {
	rule := makeKeyspaceRuleForTest(0, codec.TxnKeyspaceModePrefix)
	require.NoError(b, rule.checkAndAdjust())

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
		require.NoError(b, highRule.checkAndAdjust())
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

// The startup/1000000 benchmark has a 3-second readiness target. It includes
// in-memory rule construction and index building, but excludes storage paging,
// etcd latency, and rule delivery.
func BenchmarkRegionLabelerKeyspaceIndex(b *testing.B) {
	for _, count := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("startup/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				regionLabeler := &RegionLabeler{
					labelRules:     make(map[string]*LabelRule, count),
					genericRules:   make(map[string]*LabelRule),
					rangeListDirty: true,
				}
				for id := range count {
					rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
					require.NoError(b, rule.checkAndAdjust())
					regionLabeler.labelRules[rule.ID] = rule
				}
				regionLabeler.BuildRangeListLocked()
			}
		})

		b.Run(fmt.Sprintf("single-rule-update/%d", count), func(b *testing.B) {
			regionLabeler := &RegionLabeler{
				labelRules:     make(map[string]*LabelRule, count),
				genericRules:   make(map[string]*LabelRule),
				rangeListDirty: true,
			}
			rules := make([]*LabelRule, 0, count)
			for id := range count {
				rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
				require.NoError(b, rule.checkAndAdjust())
				regionLabeler.labelRules[rule.ID] = rule
				rules = append(rules, rule)
			}
			regionLabeler.BuildRangeListLocked()
			target := rules[len(rules)/2]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				regionLabeler.Lock()
				regionLabeler.setLabelRuleInMemoryLocked(target)
				regionLabeler.BuildRangeListLocked()
				regionLabeler.Unlock()
			}
		})

		b.Run(fmt.Sprintf("lookup/%d", count), func(b *testing.B) {
			regionLabeler := &RegionLabeler{
				labelRules:     make(map[string]*LabelRule, count),
				genericRules:   make(map[string]*LabelRule),
				rangeListDirty: true,
			}
			for id := range count {
				rule := makeKeyspaceRuleForTest(uint32(id), codec.TxnKeyspaceModePrefix)
				require.NoError(b, rule.checkAndAdjust())
				regionLabeler.labelRules[rule.ID] = rule
			}
			regionLabeler.BuildRangeListLocked()
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
