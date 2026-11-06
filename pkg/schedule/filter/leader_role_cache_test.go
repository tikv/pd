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

package filter

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestLeaderRoleCheckerMetadataAndMembership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for id := uint64(1); id <= 4; id++ {
		tc.AddLabelsStore(id, 0, map[string]string{"zone": "A"})
	}
	rule := tc.GetRuleManager().GetRule("pd", "default").Clone()
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	guard := newRuleLeaderFitFilter("test", tc.GetBasicCluster(), tc.GetRuleManager(), region, nil, 1, true).(*ruleLeaderFitFilter)
	target := func(id uint64) bool { return guard.Target(tc.GetSharedConfig(), tc.GetStore(id)).IsOK() }
	require.True(t, target(2))
	prepared := guard.roleChecker.Load()
	require.True(t, target(3))
	require.Same(t, prepared, guard.roleChecker.Load())
	// A different membership must not reuse or overwrite the original matcher.
	require.True(t, target(4))
	require.Same(t, prepared, guard.roleChecker.Load())
	require.True(t, target(2))
	require.Nil(t, region.GetStorePeer(4))
	require.Equal(t, uint64(1), region.GetLeader().GetStoreId())
	// A non-candidate store's label change must invalidate the full-layout check.
	tc.PutStoreWithLabels(3, "zone", "B")
	require.False(t, target(2))
	tc.PutStoreWithLabels(3, "zone", "A")
	require.True(t, target(2))
	tc.PutStoreWithLabels(4, "zone", "B")
	rule = rule.Clone()
	rule.LabelConstraints[0].Values = []string{"B"}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	require.False(t, target(2))
	rule = rule.Clone()
	rule.LabelConstraints[0].Values = []string{"A"}
	require.NoError(t, tc.GetRuleManager().SetRule(rule))
	// Concurrent cold calls may build separate snapshots, but must stay safe.
	guard.roleChecker.Store(nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if !target(2) {
					t.Error("legal target rejected")
				}
			}
		})
	}
	wg.Wait()
}
