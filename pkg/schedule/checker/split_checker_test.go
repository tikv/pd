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
	ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "aa",
		EndKeyHex:   "cc",
		Role:        placement.Voter,
		Count:       1,
	})
	cluster.AddLeaderRegionWithRange(1, "", "", 1)
	op := sc.Check(cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal(1, op.Len())
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	re.Equal("aa", hex.EncodeToString(splitKeys[0]))
	re.Equal("cc", hex.EncodeToString(splitKeys[1]))

	// region label has higher priority.
	regionLabeler.SetLabelRule(&labeler.LabelRule{
		ID:       "test",
		Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
		RuleType: labeler.KeyRange,
		Data:     makeKeyRanges("bb", "dd"),
	})
	op = sc.Check(cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal(1, op.Len())
	splitKeys = op.Step(0).(operator.SplitRegion).SplitKeys
	re.Equal("bb", hex.EncodeToString(splitKeys[0]))
	re.Equal("dd", hex.EncodeToString(splitKeys[1]))
}
