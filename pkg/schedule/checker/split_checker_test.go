// Copyright 2021 TiKV Project Authors.
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

package checker

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/codec"
	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestSplit(t *testing.T) {
	re := require.New(t)
	cfg := mockconfig.NewTestOptions()
	cfg.GetReplicationConfig().EnablePlacementRules = true
	ctx := t.Context()
	cluster := mockcluster.NewCluster(ctx, cfg)
	ruleManager := cluster.RuleManager
	regionLabeler := cluster.RegionLabeler
	sc := NewSplitChecker(cluster, ruleManager, regionLabeler)
	cluster.AddLeaderStore(1, 1)
	err := ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "aa",
		EndKeyHex:   "cc",
		Role:        placement.Voter,
		Count:       1,
	})
	re.NoError(err)
	cluster.AddLeaderRegionWithRange(1, "", "", 1)
	op := sc.Check(cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal(1, op.Len())
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	re.Equal("aa", hex.EncodeToString(splitKeys[0]))
	re.Equal("cc", hex.EncodeToString(splitKeys[1]))

	// region label has higher priority.
	err = regionLabeler.SetLabelRule(&labeler.LabelRule{
		ID:       "test",
		Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
		RuleType: labeler.KeyRange,
		Data:     keyutil.BuildKeyRangeMaps("bb", "dd"),
	})
	re.NoError(err)
	op = sc.Check(cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal(1, op.Len())
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	re.Equal("bb", hex.EncodeToString(splitKeys[0]))
	re.Equal("dd", hex.EncodeToString(splitKeys[1]))
}

func TestKeyspaceSplit(t *testing.T) {
	re := require.New(t)
	cfg := mockconfig.NewTestOptions()
	ctx := t.Context()
	cluster := mockcluster.NewCluster(ctx, cfg)
	ruleManager := cluster.RuleManager
	regionLabeler := cluster.RegionLabeler
	sc := NewSplitChecker(cluster, ruleManager, regionLabeler)
	cluster.AddLeaderStore(1, 1)
	// Keyspaces 101-105 must exist for GetKeyspaceSplitKeys to generate a
	// split at their boundaries; mockcluster reports no keyspace by default.
	cluster.AddKeyspace(101, 102, 103, 104, 105)

	// Test 1: Region within one keyspace - should not split
	bound100 := keyspace.MakeRegionBound(100)
	cluster.AddLeaderRegionWithRange(2, string(bound100.TxnLeftBound), string(bound100.TxnRightBound), 1)
	op := sc.Check(cluster.GetRegion(2))
	re.Nil(op, "region within one keyspace should not be split")

	// Test 2: Region spanning two keyspaces - should split
	bound101 := keyspace.MakeRegionBound(101)
	cluster.AddLeaderRegionWithRange(3, string(bound100.TxnLeftBound), string(bound101.TxnRightBound), 1)
	op = sc.Check(cluster.GetRegion(3))
	re.NotNil(op, "region spanning multiple keyspaces should be split")
	re.Equal(1, op.Len())
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	// 101 txn left bound should be the only split key
	re.Len(splitKeys, 1)
	re.Equal(hex.EncodeToString(bound100.TxnRightBound), hex.EncodeToString(splitKeys[0]))

	// Test 3: Region spanning multiple keyspaces - should split at all boundaries
	bound105 := keyspace.MakeRegionBound(105)
	cluster.AddLeaderRegionWithRange(4, string(bound100.TxnLeftBound), string(bound105.TxnRightBound), 1)
	op = sc.Check(cluster.GetRegion(4))
	re.NotNil(op, "region spanning multiple keyspaces should be split")
	re.Equal(1, op.Len())
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	// (105-100)=5 split keys expected, but we only generate 10 keys at most to avoid too many operators.
	re.Len(splitKeys, 5)
	re.Equal(hex.EncodeToString(bound100.TxnRightBound), hex.EncodeToString(splitKeys[0]))
}

func TestKeyspaceSplitMixRange(t *testing.T) {
	re := require.New(t)

	starthexKey := "7200019200000000FB"
	endHexKey := "7800019100000000FB"
	startkey, err := hex.DecodeString(starthexKey)
	re.NoError(err)
	endKey, err := hex.DecodeString(endHexKey)
	re.NoError(err)

	cfg := mockconfig.NewTestOptions()
	ctx := t.Context()
	cluster := mockcluster.NewCluster(ctx, cfg)
	ruleManager := cluster.RuleManager
	regionLabeler := cluster.RegionLabeler
	sc := NewSplitChecker(cluster, ruleManager, regionLabeler)
	// startkey decodes to raw keyspace 402; it must exist for
	// GetKeyspaceSplitKeys to find a split boundary in [402, MaxValidKeyspaceID).
	cluster.AddKeyspace(402)

	spans, err := keyspace.RegionSpansMultipleKeyspaces(startkey, endKey, sc.cluster)
	re.NoError(err)
	re.True(spans)
	keys, err := keyspace.GetKeyspaceSplitKeys(startkey, endKey, coreconstant.Raw, sc.cluster)
	re.NoError(err)
	re.NotEmpty(keys)
}

func TestKeyspaceSplitExtremeBounds(t *testing.T) {
	for _, keyType := range []coreconstant.KeyType{coreconstant.Raw, coreconstant.Txn} {
		t.Run(keyType.String(), func(t *testing.T) {
			prefix := codec.RawKeyspaceModePrefix
			if keyType == coreconstant.Txn {
				prefix = codec.TxnKeyspaceModePrefix
			}
			left := func(id uint32) []byte {
				key := codec.EncodeKeyspaceBoundary(prefix, id)
				return key[:]
			}
			inside := func(id uint32) []byte {
				return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(prefix, id), 'k'))
			}
			testCases := []struct {
				name     string
				id       uint32
				start    []byte
				end      []byte
				expected [][]byte
			}{
				{
					name:     "non-keyspace endpoints enclosing a live keyspace",
					id:       101,
					start:    codec.EncodeBytes([]byte("a")),
					end:      codec.EncodeBytes([]byte("z")),
					expected: [][]byte{left(101), left(102)},
				},
				{
					name:     "negative infinity to inside the first keyspace",
					id:       0,
					end:      inside(0),
					expected: [][]byte{left(0)},
				},
				{
					name:     "inside the last keyspace to positive infinity",
					id:       constant.MaxValidKeyspaceID,
					start:    inside(constant.MaxValidKeyspaceID),
					expected: [][]byte{left(constant.MaxValidKeyspaceID + 1)},
				},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					re := require.New(t)
					cfg := mockconfig.NewTestOptions()
					cfg.GetPDServerConfig().KeyType = keyType.String()
					cluster := mockcluster.NewCluster(t.Context(), cfg)
					cluster.AddLeaderStore(1, 1)
					sc := NewSplitChecker(cluster, cluster.RuleManager, cluster.RegionLabeler)
					cluster.AddLeaderRegionWithRange(1, string(tc.start), string(tc.end), 1)
					region := cluster.GetRegion(1)
					// With no metadata or compatibility labels, no split is needed.
					re.Empty(cluster.GetAllLabelRules())
					re.Nil(sc.Check(region))
					cluster.AddKeyspace(tc.id)
					spans, err := keyspace.RegionSpansMultipleKeyspaces(tc.start, tc.end, cluster)
					re.NoError(err)
					re.True(spans)
					op := sc.Check(region)
					re.NotNil(op)
					re.Equal("keyspace-split-region", op.Desc())
					re.Equal(tc.expected, op.Step(0).(operator.SplitRegion).SplitKeys)
					// A later patrol must not depend on a compatibility label either.
					re.NotNil(sc.Check(region))
					// Once split at these boundaries, none of the resulting regions
					// should be split again in the configured mode.
					start := tc.start
					for _, end := range append(tc.expected, tc.end) {
						cluster.AddLeaderRegionWithRange(2, string(start), string(end), 1)
						re.Nil(sc.Check(cluster.GetRegion(2)))
						start = end
					}
				})
			}
		})
	}
}

type keyspaceLookupCluster struct {
	*mockcluster.Cluster
	manager *keyspace.Manager
}

func (c *keyspaceLookupCluster) GetKeyspaceIDInRange(start, end uint32, limit int) ([]uint32, error) {
	return c.manager.GetKeyspaceIDInRange(start, end, limit)
}

func TestKeyspaceSchedulingRejectsIncompleteBackfill(t *testing.T) {
	for _, keyType := range []coreconstant.KeyType{coreconstant.Raw, coreconstant.Txn} {
		t.Run(keyType.String(), func(t *testing.T) {
			re := require.New(t)
			store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error {
				return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
					Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 101},
					Name:     "ks101",
					State:    keyspacepb.KeyspaceState_ENABLED,
				})
			}))
			manager := keyspace.NewKeyspaceManager(t.Context(), store, nil, nil, nil, nil, nil)
			cfg := mockconfig.NewTestOptions()
			cfg.GetPDServerConfig().KeyType = keyType.String()
			cluster := &keyspaceLookupCluster{Cluster: mockcluster.NewCluster(t.Context(), cfg), manager: manager}
			cluster.AddLeaderStore(1, 1)
			re.Empty(cluster.GetAllLabelRules())
			start, middle, end := keyspace.MakeRegionBound(100).RawLeftBound, keyspace.MakeRegionBound(101).RawLeftBound, keyspace.MakeRegionBound(103).RawLeftBound
			expected := [][]byte{keyspace.MakeRegionBound(101).RawLeftBound, keyspace.MakeRegionBound(101).RawRightBound}
			if keyType == coreconstant.Txn {
				start, middle, end = keyspace.MakeRegionBound(100).TxnLeftBound, keyspace.MakeRegionBound(101).TxnLeftBound, keyspace.MakeRegionBound(103).TxnLeftBound
				expected = [][]byte{keyspace.MakeRegionBound(101).TxnLeftBound, keyspace.MakeRegionBound(101).TxnRightBound}
			}
			cluster.AddLeaderRegionWithRange(1, string(start), string(middle), 1)
			cluster.AddLeaderRegionWithRange(2, string(middle), string(end), 1)
			first, second := cluster.GetRegion(1), cluster.GetRegion(2)
			sc := NewSplitChecker(cluster, cluster.RuleManager, cluster.RegionLabeler)
			cluster.AddLeaderRegionWithRange(3, string(start), string(end), 1)
			region := cluster.GetRegion(3)
			fp := "github.com/tikv/pd/pkg/keyspace/backfillKeyspaceIDRangeBeforeApply"
			re.NoError(failpoint.EnableCall(fp, func() {
				// An unrelated publication invalidates every attempted scan.
				// Keyspace 101 remains uncached while backfill keeps failing.
				manager.UpdateKeyspaceMetaToCache(&keyspacepb.KeyspaceMeta{
					Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 999},
					Name:     "unrelated",
					State:    keyspacepb.KeyspaceState_ENABLED,
				})
			}))
			enabled := true
			t.Cleanup(func() {
				if enabled {
					re.NoError(failpoint.Disable(fp))
				}
			})
			for range 3 {
				re.False(AllowMerge(cluster, first, second), "unknown metadata must not approve a merge")
				re.Nil(sc.Check(region), "unknown metadata must not generate split keys either")
			}
			re.NoError(failpoint.Disable(fp))
			enabled = false
			re.False(AllowMerge(cluster, first, second), "the recovered cache must expose the real boundary")
			op := sc.Check(region)
			re.NotNil(op)
			re.Equal(expected, op.Step(0).(operator.SplitRegion).SplitKeys)
		})
	}
}
