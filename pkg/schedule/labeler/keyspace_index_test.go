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
		ID:       fmt.Sprintf("keyspaces/%d", id),
		Labels:   []RegionLabel{{Key: "id", Value: strconv.FormatUint(uint64(id), 10)}},
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
	rule := makeKeyspaceRuleForTest(42, keyspaceRawMode, keyspaceTxnMode)
	re.NoError(rule.checkAndAdjust())

	var index keyspaceRuleIndex
	re.True(index.Add(rule))
	re.True(index.Contains(rule))
	re.Same(rule, index.GetRule(
		makeRegionForKeyspace(42, keyspaceRawMode).GetStartKey(),
		makeRegionForKeyspace(42, keyspaceRawMode).GetEndKey(),
	))
	re.Same(rule, index.GetRule(
		makeRegionForKeyspace(42, keyspaceTxnMode).GetStartKey(),
		makeRegionForKeyspace(42, keyspaceTxnMode).GetEndKey(),
	))

	rawStart := keyspaceBoundary(keyspaceRawMode, 41)
	rawEnd := keyspaceBoundary(keyspaceRawMode, 44)
	splitKeys := index.GetSplitKeys(rawStart[:], rawEnd[:])
	re.Equal([][]byte{
		keyspaceBoundaryBytes(keyspaceRawMode, 42),
		keyspaceBoundaryBytes(keyspaceRawMode, 43),
	}, splitKeys)
	re.True(index.HasSplitKey(rawStart[:], rawEnd[:]))

	// A rejected multi-range add must not populate its free slot before it
	// discovers a collision in another slot.
	txnOwner := makeKeyspaceRuleForTest(43, keyspaceTxnMode)
	re.NoError(txnOwner.checkAndAdjust())
	re.True(index.Add(txnOwner))
	collision := makeKeyspaceRuleForTest(43, keyspaceRawMode, keyspaceTxnMode)
	re.NoError(collision.checkAndAdjust())
	re.False(index.Add(collision))
	re.True(index.Contains(txnOwner))
	re.False(index.Contains(collision))
	re.Nil(index.GetRule(
		makeRegionForKeyspace(43, keyspaceRawMode).GetStartKey(),
		makeRegionForKeyspace(43, keyspaceRawMode).GetEndKey(),
	))
	re.Same(txnOwner, index.GetRule(
		makeRegionForKeyspace(43, keyspaceTxnMode).GetStartKey(),
		makeRegionForKeyspace(43, keyspaceTxnMode).GetEndKey(),
	))
	re.True(index.Remove(txnOwner.ID, txnOwner))

	re.True(index.Remove(rule.ID, rule))
	re.False(index.Contains(rule))
	re.Empty(index.GetSplitKeys(rawStart[:], rawEnd[:]))
}

func TestKeyspaceRuleIndexBoundaries(t *testing.T) {
	re := require.New(t)
	ids := []uint32{0, 63, 64, 1023, 1024, keyspaceMaxID}
	var index keyspaceRuleIndex
	expectedByKey := make(map[string][]byte)
	for _, id := range ids {
		rule := makeKeyspaceRuleForTest(id, keyspaceTxnMode)
		re.NoError(rule.checkAndAdjust())
		re.True(index.Add(rule))
		for _, boundaryID := range []uint32{id, id + 1} {
			key := keyspaceBoundaryBytes(keyspaceTxnMode, boundaryID)
			expectedByKey[string(key)] = key
		}

		left := keyspaceBoundary(keyspaceTxnMode, id)
		right := keyspaceBoundary(keyspaceTxnMode, id+1)
		re.Same(rule, index.GetRule(left[:], right[:]))
		region := makeRegionForKeyspace(id, keyspaceTxnMode)
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

	rule := makeKeyspaceRuleForTest(42, keyspaceRawMode, keyspaceTxnMode)
	re.NoError(regionLabeler.SetLabelRule(rule))
	re.False(regionLabeler.rangeListDirty)
	re.Len(regionLabeler.genericRules, 1)
	re.True(regionLabeler.keyspaceRules.Contains(rule))

	for _, mode := range []byte{keyspaceRawMode, keyspaceTxnMode} {
		region := makeRegionForKeyspace(42, mode)
		re.Equal("42", regionLabeler.GetRegionLabel(region, "id"))
		re.Equal("yes", regionLabeler.GetRegionLabel(region, "generic"))
	}
	startRegion := makeRegionForKeyspace(42, keyspaceTxnMode)
	endRegion := makeRegionForKeyspace(43, keyspaceTxnMode)
	crossingRegion := core.NewTestRegionInfo(2, 1, startRegion.GetStartKey(), endRegion.GetEndKey())
	re.Empty(regionLabeler.GetRegionLabel(crossingRegion, "id"))
	re.Empty(regionLabeler.GetRegionLabel(crossingRegion, "generic"))

	// Changing the deterministic rule only touches its old and new slots.
	updated := makeKeyspaceRuleForTest(42, keyspaceTxnMode)
	re.NoError(regionLabeler.SetLabelRule(updated))
	re.False(regionLabeler.rangeListDirty)
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceRawMode), "id"))
	re.Equal("42", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceTxnMode), "id"))

	re.NoError(regionLabeler.DeleteLabelRule(updated.ID))
	re.False(regionLabeler.rangeListDirty)
	re.False(regionLabeler.keyspaceRules.Contains(updated))
	re.Equal("yes", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceTxnMode), "generic"))
}

func TestRegionLabelerUpdatesMutatedKeyspaceRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionLabeler, err := NewRegionLabeler(ctx, store, time.Hour)
	re.NoError(err)

	rule := makeKeyspaceRuleForTest(42, keyspaceTxnMode)
	re.NoError(regionLabeler.SetLabelRule(rule))

	fetched := regionLabeler.GetLabelRule(rule.ID)
	fetched.Data = makeKeyspaceRuleForTest(42, keyspaceRawMode).Data
	re.NoError(regionLabeler.SetLabelRule(fetched))
	re.Equal("42", regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceRawMode), "id"))
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceTxnMode), "id"))

	fetched.Labels[0].Value = "mutated"
	re.NoError(regionLabeler.DeleteLabelRule(fetched.ID))
	re.Empty(regionLabeler.GetRegionLabel(makeRegionForKeyspace(42, keyspaceRawMode), "id"))
}

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
					rule := makeKeyspaceRuleForTest(uint32(id), keyspaceTxnMode)
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
				rule := makeKeyspaceRuleForTest(uint32(id), keyspaceTxnMode)
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
				rule := makeKeyspaceRuleForTest(uint32(id), keyspaceTxnMode)
				require.NoError(b, rule.checkAndAdjust())
				regionLabeler.labelRules[rule.ID] = rule
			}
			regionLabeler.BuildRangeListLocked()
			region := makeRegionForKeyspace(uint32(count/2), keyspaceTxnMode)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if value := regionLabeler.GetRegionLabel(region, keyspaceIDLabelKey); value == "" {
					b.Fatal("keyspace label not found")
				}
			}
		})
	}
}
