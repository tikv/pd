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
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestSplit(t *testing.T) {
	re := require.New(t)
	cfg := mockconfig.NewTestOptions()
	cfg.GetReplicationConfig().EnablePlacementRules = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		Data:     makeKeyRanges("bb", "dd"),
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, cfg)
	ruleManager := cluster.RuleManager
	regionLabeler := cluster.RegionLabeler
	sc := NewSplitChecker(cluster, ruleManager, regionLabeler)
	cluster.AddLeaderStore(1, 1)

	// Test 1: Region within one keyspace - should not split
	bound100 := keyspace.MakeRegionBound(100)
	cluster.AddLeaderRegionWithRange(2, string(bound100.RawLeftBound), string(bound100.RawRightBound), 1)
	op := sc.Check(cluster.GetRegion(2))
	re.Nil(op, "region within one keyspace should not be split")

	// Test 2: Region spanning two keyspaces - should split
	bound101 := keyspace.MakeRegionBound(101)
	cluster.AddLeaderRegionWithRange(3, string(bound100.RawLeftBound), string(bound101.RawRightBound), 1)
	op = sc.Check(cluster.GetRegion(3))
	re.NotNil(op, "region spanning multiple keyspaces should be split")
	re.Equal(1, op.Len())
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	// Should have one split key at the boundary between keyspace 100 and 101
	re.Equal(1, len(splitKeys))
	re.Equal(hex.EncodeToString(bound100.RawRightBound), hex.EncodeToString(splitKeys[0]))

	// Test 3: Region spanning multiple keyspaces - should split at all boundaries
	bound105 := keyspace.MakeRegionBound(105)
	cluster.AddLeaderRegionWithRange(4, string(bound100.RawLeftBound), string(bound105.RawRightBound), 1)
	op = sc.Check(cluster.GetRegion(4))
	re.NotNil(op, "region spanning multiple keyspaces should be split")
	re.Equal(1, op.Len())
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	// Should have 5 split keys (boundaries for keyspaces 100, 101, 102, 103, 104)
	re.Equal(5, len(splitKeys))

	// Test 4: Txn mode region spanning two keyspaces
	cluster.AddLeaderRegionWithRange(5, string(bound100.TxnLeftBound), string(bound101.TxnRightBound), 1)
	op = sc.Check(cluster.GetRegion(5))
	re.NotNil(op, "txn mode region spanning multiple keyspaces should be split")
	re.Equal(1, op.Len())
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	re.Equal(1, len(splitKeys))
	re.Equal(hex.EncodeToString(bound100.TxnRightBound), hex.EncodeToString(splitKeys[0]))
}
