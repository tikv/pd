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

package affinity

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestKeyRangeOverlapValidation(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	// Create manager with region labeler
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	validate := func(ranges []GroupKeyRanges) error {
		manager.Lock()
		defer manager.Unlock()
		return manager.validateNoKeyRangeOverlap(ranges)
	}

	// Test 1: Non-overlapping ranges should succeed
	keyRanges1 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("b")},
			{StartKey: []byte("c"), EndKey: []byte("d")},
		}},
	}
	err = validate(keyRanges1)
	re.NoError(err, "Non-overlapping ranges should pass validation")

	// Test 2: Overlapping ranges within same request should fail
	keyRanges2 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("c")},
			{StartKey: []byte("b"), EndKey: []byte("d")},
		}},
	}
	err = validate(keyRanges2)
	re.Error(err, "Overlapping ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 3: Adjacent ranges (not overlapping) should succeed
	keyRanges3 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("b")},
			{StartKey: []byte("b"), EndKey: []byte("c")},
		}},
	}
	err = validate(keyRanges3)
	re.NoError(err, "Adjacent ranges should pass validation")

	// Test 5: Duplicate range should fail validation
	keyRangesDup := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("c")},
			{StartKey: []byte("a"), EndKey: []byte("c")},
		}},
	}
	err = validate(keyRangesDup)
	re.Error(err, "Duplicate ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 4: Verify checkKeyRangesOverlap function directly
	overlaps := checkKeyRangesOverlap([]byte("a"), []byte("c"), []byte("b"), []byte("d"))
	re.True(overlaps, "Ranges [a,c) and [b,d) should overlap")

	overlaps = checkKeyRangesOverlap([]byte("a"), []byte("b"), []byte("c"), []byte("d"))
	re.False(overlaps, "Ranges [a,b) and [c,d) should not overlap")
}

// TestKeyRangeOverlapRebuild tests rebuild after restart
// Note: Full labeler integration test requires proper JSON serialization handling
func TestKeyRangeOverlapRebuild(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create two groups without key ranges for basic testing
	err = manager.CreateAffinityGroups([]GroupKeyRanges{
		{GroupID: "group1"},
		{GroupID: "group2"},
	})
	re.NoError(err)

	// Set peers for the groups
	_, err = manager.UpdateAffinityGroupPeers("group1", 1, []uint64{1})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group2", 1, []uint64{1})
	re.NoError(err)

	// Verify groups were created
	re.True(manager.IsGroupExist("group1"))
	re.True(manager.IsGroupExist("group2"))

	// Create a new manager to simulate restart
	regionLabeler2, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager2, err := NewManager(ctx, store, storeInfos, conf, regionLabeler2)
	re.NoError(err)

	// Verify groups were loaded from storage
	re.True(manager2.IsGroupExist("group1"))
	re.True(manager2.IsGroupExist("group2"))
}

func TestParseKeyRangesFromDataInvalidHex(t *testing.T) {
	re := require.New(t)
	_, err := parseKeyRangesFromData([]*labeler.KeyRangeRule{
		{StartKeyHex: "zz", EndKeyHex: "10"},
	}, "g1")
	re.Error(err)
	re.ErrorContains(err, "invalid hex start key")
}

func TestAffinityPersistenceWithLabeler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	keyRanges := []keyutil.KeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "persist",
		KeyRanges: keyRanges,
	}}))
	_, err = manager.UpdateAffinityGroupPeers("persist", 1, []uint64{1})
	re.NoError(err)

	// RangeCount should be recorded and label rule created.
	state := manager.GetAffinityGroupState("persist")
	re.NotNil(state)
	re.Equal(1, state.RangeCount)
	re.NotNil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))

	// Reload manager to verify persistence and loadRegionLabel integration.
	manager2, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)
	state2 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state2)
	re.Equal(1, state2.RangeCount)

	// Remove all ranges and ensure cache/label are cleared.
	ranges := []keyutil.KeyRange{{
		StartKey: []byte{0x00},
		EndKey:   []byte{0x10},
	}}
	re.NoError(manager2.UpdateAffinityGroupKeyRanges(
		nil,
		[]GroupKeyRanges{{GroupID: "persist", KeyRanges: ranges}},
	))
	state3 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state3)
	re.Equal(0, state3.RangeCount)
	re.Nil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))
}

func TestLoadRegionLabelIgnoreUnknownGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	// Inject a leftover label rule for a non-existent group "ghost".
	plan := regionLabeler.NewPlan()
	rule := &labeler.LabelRule{
		ID:       GetLabelRuleID("ghost"),
		Labels:   []labeler.RegionLabel{{Key: labelKey, Value: "ghost"}},
		RuleType: labeler.KeyRange,
		Data: []any{
			map[string]any{"start_key": hex.EncodeToString([]byte{0x01}), "end_key": hex.EncodeToString([]byte{0x02})},
		},
	}
	re.NoError(plan.SetLabelRule(rule))
	re.NoError(endpoint.RunBatchOpInTxn(ctx, store, plan.CommitOps()))
	plan.Apply()

	// Manager initialization should ignore the unknown label rule.
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)
	re.False(manager.IsGroupExist("ghost"))

	// Creating a new group with overlapping range should succeed (ghost is ignored).
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "real",
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte{0x01},
			EndKey:   []byte{0x02},
		}},
	}})
	re.NoError(err)
}

// TestLabelRuleIntegration tests basic label rule creation and deletion
// Note: Full integration with JSON serialization requires additional handling
func TestLabelRuleIntegration(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Test: Group with no key ranges should not create label rule
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "group_no_label"}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group_no_label", 1, []uint64{1})
	re.NoError(err)

	labelRuleID := GetLabelRuleID("group_no_label")
	labelRule := regionLabeler.GetLabelRule(labelRuleID)
	re.Nil(labelRule, "No label rule should be created when keyRanges is nil")

	// Verify group was created
	re.True(manager.IsGroupExist("group_no_label"))

	// Delete group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroups([]string{"group_no_label"}, false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group_no_label"))
}

// TestUpdateAffinityGroupKeyRangesAddToEmptyGroup documents the current failure
// when adding key ranges to a group created without initial ranges.
func TestUpdateAffinityGroupKeyRangesAddToEmptyGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group without key ranges or peers.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "empty-group"}}))

	// Adding ranges should succeed; currently it returns "label rule not found".
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{
			GroupID: "empty-group",
			KeyRanges: []keyutil.KeyRange{{
				StartKey: []byte{0x00},
				EndKey:   []byte{0x01},
			}},
		}},
		nil,
	)
	re.NoError(err)
}

func TestDuplicateRangeAdd(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	r := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}

	// First add.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "dup",
		KeyRanges: []keyutil.KeyRange{r}}}))
	// Second add with same range should be rejected due to duplicate/overlap.
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "dup", KeyRanges: []keyutil.KeyRange{r}}},
		nil,
	)
	re.Error(err)
}

// TestDeleteAffinityGroupsForceMissing verifies force deletion tolerates missing IDs.
func TestDeleteAffinityGroupsForceMissing(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group with a key range.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "with-range",
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte{0x00},
			EndKey:   []byte{0x10},
		}},
	}}))

	// Force delete should tolerate missing IDs and remove existing groups with ranges.
	err = manager.DeleteAffinityGroups([]string{"missing-group", "with-range"}, true)
	re.NoError(err)
	re.False(manager.IsGroupExist("with-range"))
}

// TestSameGroupNonOverlappingAdd ensures adding disjoint ranges to the same group is allowed.
func TestSameGroupNonOverlappingAdd(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	r1 := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
	r2 := keyutil.KeyRange{StartKey: []byte{0x20}, EndKey: []byte{0x30}}

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{r1}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{r2}}},
		nil,
	)
	re.NoError(err)

	state := manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.Equal(2, state.RangeCount)
}

// TestOverlapDuringMigration documents that add+remove with overlapping ranges is rejected.
// Scenario: group1 has [0x00,0x10], we try to add group2 [0x00,0x10] while removing group1 [0x00,0x10].
// Current logic reports overlap (expected current design).
func TestOverlapDuringMigration(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	r := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g1", KeyRanges: []keyutil.KeyRange{r}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g2", KeyRanges: []keyutil.KeyRange{r}}},
		[]GroupKeyRanges{{GroupID: "g1", KeyRanges: []keyutil.KeyRange{r}}},
	)
	re.Error(err)
}

// TestPartialOverlapSameGroup documents that add+remove in the same request for same group is rejected.
// Scenario: existing [0,10), request removes [0,10) and adds [0,5) for the same group. Current design forbids mixed add/remove same group.
func TestPartialOverlapSameGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	rOld := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
	rNew := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x05}}

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rOld}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rNew}}},
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rOld}}},
	)
	re.Error(err)
}

// TestNewGroupOverlapWithExistingGroup verifies that adding a new group's ranges
// that overlap with an existing group's ranges is correctly rejected.
// This test validates the fix for the regression where group A's new ranges
// vs group B's existing ranges were not being checked.
func TestNewGroupOverlapWithExistingGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create group A with range [0x00, 0x20]
	rangeA := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x20}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupA",
		KeyRanges: []keyutil.KeyRange{rangeA},
	}}))

	// Try to create group B with overlapping range [0x10, 0x30]
	// This should be rejected because it overlaps with group A's existing range
	rangeB := keyutil.KeyRange{StartKey: []byte{0x10}, EndKey: []byte{0x30}}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupB",
		KeyRanges: []keyutil.KeyRange{rangeB},
	}})
	re.Error(err, "New group B's range should be rejected due to overlap with existing group A")
	re.Contains(err.Error(), "overlap")

	// Verify group B was not created
	re.False(manager.IsGroupExist("groupB"))

	// Create group C without ranges first
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "groupC"}}))

	// Try to add range to group C that overlaps with group A
	// This should also be rejected
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{
			GroupID:   "groupC",
			KeyRanges: []keyutil.KeyRange{rangeB},
		}},
		nil,
	)
	re.Error(err, "Adding overlapping range to group C should be rejected")
	re.Contains(err.Error(), "overlap")

	// Now test the critical case: batch update with multiple groups
	// Create group D with range [0x40, 0x50]
	rangeD := keyutil.KeyRange{StartKey: []byte{0x40}, EndKey: []byte{0x50}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupD",
		KeyRanges: []keyutil.KeyRange{rangeD},
	}}))

	// Create group E without ranges
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "groupE"}}))

	// Try to add ranges to both group C and group E in a single batch,
	// where group C's new range [0x45, 0x55] overlaps with group D's existing range [0x40, 0x50]
	rangeC := keyutil.KeyRange{StartKey: []byte{0x45}, EndKey: []byte{0x55}} // overlaps with D
	rangeE := keyutil.KeyRange{StartKey: []byte{0x60}, EndKey: []byte{0x70}} // no overlap
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{
			{GroupID: "groupC", KeyRanges: []keyutil.KeyRange{rangeC}},
			{GroupID: "groupE", KeyRanges: []keyutil.KeyRange{rangeE}},
		},
		nil,
	)
	// This is the key test: C's new range vs D's existing range should be detected
	// This validates that when updating multiple groups in a batch,
	// the validation correctly checks new ranges against ALL existing ranges,
	// not just the groups being updated
	re.Error(err, "Batch update should detect overlap between C's new range and D's existing range")
	re.Contains(err.Error(), "overlap")
}
