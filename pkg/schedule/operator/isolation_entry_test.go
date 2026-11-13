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

package operator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestMoveRegionChecksLeaderOnlyIsolation(t *testing.T) {
	tc, region := newLeaderVoterIsolationCluster(t, "host")
	rm := tc.GetRuleManager()
	before := rm.FitRegionWithoutCache(tc, region)
	require.True(t, before.IsSatisfied())
	require.True(t, before.RuleFits[1].IsIsolationSatisfied())
	_, err := CreateTransferLeaderOperator("ordinary-transfer", tc, region, 2, nil, OpAdmin)
	require.Error(t, err)

	roles := map[uint64]placement.PeerRoleType{1: placement.Voter, 2: placement.Leader, 3: placement.Voter}
	op, err := CreateMoveRegionOperator("admin-transfer-region", tc, region, OpAdmin, roles)
	if err == nil && op != nil {
		for range op.Len() {
			region = ApplyOperatorStep(region, op)
		}
		require.Equal(t, uint64(2), region.GetLeader().GetStoreId())
		after := rm.FitRegionWithoutCache(tc, region)
		require.True(t, after.IsSatisfied())
		require.True(t, after.RuleFits[1].IsIsolationSatisfied(), "leader-only transfer-region must preserve voter-rule host isolation")
	}
	require.Error(t, err, "the requested leader violates the voter rule's host isolation")
	require.Nil(t, op)
	roles[2], roles[3] = placement.Voter, placement.Leader
	op, err = CreateMoveRegionOperator("legal-transfer-region", tc, region, OpAdmin, roles)
	require.NoError(t, err)
	require.Equal(t, uint64(3), finalLeader(op))
}

func TestLeaderOnlyBuilderKeepsIsolationExemptions(t *testing.T) {
	for _, mode := range []string{"force", "skip", "no-level", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			tc, region := newLeaderVoterIsolationCluster(t, "host")
			var opts []BuilderOption
			switch mode {
			case "skip":
				opts = append(opts, SkipPlacementRulesCheck)
			case "no-level", "incomplete":
				rule := tc.GetRuleManager().GetRule("pd", "voters").Clone()
				if mode == "no-level" {
					rule.IsolationLevel = ""
				} else {
					rule.Count++
				}
				require.NoError(t, tc.GetRuleManager().SetRule(rule))
			}
			builder := NewBuilder("test", tc, region, opts...).SetLeader(2)
			if mode == "force" {
				builder.EnableForceTargetLeader()
			}
			op, err := builder.Build(OpLeader)
			require.NoError(t, err)
			require.Equal(t, uint64(2), finalLeader(op))
		})
	}
}
