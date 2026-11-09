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

package checker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestAffinityPlannerSameRoleIsolation(t *testing.T) {
	re := require.New(t)
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, zone := range []string{"a", "a", "b"} {
		id := uint64(i + 1)
		tc.AddRegionStore(id, 10)
		tc.SetStoreLabel(id, map[string]string{"zone": zone, "id": fmt.Sprint(id)})
	}
	for i, stores := range [][]string{{"1"}, {"2", "3"}} {
		re.NoError(c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: fmt.Sprint(i), Index: i + 1,
			Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: stores}}}))
	}
	tc.AddLeaderRegion(1, 1, 3)
	region := tc.GetRegion(1)
	re.True(c.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
	virtual := cloneRegionWithReplacePeerStores(region, 1, 1, 2)
	re.True(c.ruleManager.FitRegionWithoutCache(tc, virtual).IsSatisfied())
	re.False(c.isRegionPlacementRuleSatisfiedWithBestLocation(virtual, false))
	loc := [][]int32{{0}, {0}, {1}}
	rules := []*balanceplan.Rule{
		{Role: balanceplan.RoleVoter, Count: 1, Stores: []int{0}, Location: loc},
		{Role: balanceplan.RoleVoter, Count: 1, Stores: []int{1, 2}, Location: loc},
	}
	in := &balanceplan.Input{Stores: 3}
	for range 2 {
		in.Groups = append(in.Groups, balanceplan.Group{Rules: rules, Voters: []int{0, 2}, RuleOf: []int{0, 1},
			Leader: 0, CanLead: []bool{true, true, true}})
	}
	raw := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	re.Equal(balanceplan.Score{Voter: 8, Leader: 4}, raw.Baseline)
	re.Equal(balanceplan.Score{Voter: 6, Leader: 2}, raw.Score)
	for i := range in.Groups {
		in.Groups[i].CheckTarget = func(voters []int, leader int) bool {
			stores := make([]uint64, len(voters))
			for k, s := range voters {
				stores[k] = uint64(s + 1)
			}
			return c.isRegionPlacementRuleSatisfiedWithBestLocation(cloneRegionWithReplacePeerStores(region, uint64(leader+1), stores...), false)
		}
	}
	checked := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	re.Equal(balanceplan.Score{Voter: 8, Leader: 2}, checked.Score)
	for i, voters := range checked.Voters {
		re.ElementsMatch([]int{0, 2}, voters)
		re.True(in.Groups[i].CheckTarget(voters, checked.Leaders[i]))
	}
}
