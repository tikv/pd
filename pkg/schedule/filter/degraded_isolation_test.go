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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestPeerMoveDoesNotWorsenDegradedIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	tc.SetEnablePlacementRules(true)
	for i, path := range [][]string{{"A", "a"}, {"A", "a"}, {"A", "b"}, {"A", "c"}, {"B", "d"}, {"B", "d"}, {"A", "e"}} {
		tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"zone": path[0], "host": path[1]})
	}
	rm := tc.GetRuleManager()
	rule := rm.GetRule("pd", "default").Clone()
	rule.Count = 5
	rule.LocationLabels = []string{"zone", "host"}
	rule.IsolationLevel = "host"
	require.NoError(t, rm.SetRule(rule))
	region := tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)
	before := rm.FitRegionWithoutCache(tc, region)
	after := rm.FitRegionWithoutCache(tc, region.Clone(core.WithReplacePeerStore(4, 6)))
	require.True(t, before.IsSatisfied())
	require.True(t, after.IsSatisfied())
	require.False(t, before.RuleFits[0].IsIsolationSatisfied())
	require.False(t, after.RuleFits[0].IsIsolationSatisfied())
	require.Equal(t, float64(405), before.RuleFits[0].IsolationScore)
	require.Equal(t, float64(602), after.RuleFits[0].IsolationScore)
	guard := NewPlacementSafeguard("review", tc.GetSharedConfig(), tc.GetBasicCluster(), rm, region, tc.GetStore(4), before)
	require.True(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(7)).IsOK(), "same-quality alternative remains legal")
	require.False(t, guard.Target(tc.GetSharedConfig(), tc.GetStore(6)).IsOK(), "must reject increasing same-host peer pairs from one to two")
}
