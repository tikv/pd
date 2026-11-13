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

package scatter

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestScatterLeaderOnlyIsolation(t *testing.T) {
	for _, level := range []string{"", "host"} {
		t.Run("level="+level, func(t *testing.T) {
			for _, internal := range []bool{false, true} {
				t.Run(map[bool]string{false: "admin", true: "internal"}[internal], func(t *testing.T) {
					tc, scatterer := isolationTestScatter(t, []string{"a", "b", "c"})
					tc.SetEnablePlacementRules(true)
					for i, zone := range []string{"z1", "z1", "z2"} {
						tc.PutStoreWithLabels(uint64(i+1), "zone", zone, "host", strconv.Itoa(i+1))
					}
					rm := tc.GetRuleManager()
					leader := rm.GetRule("pd", "default").Clone()
					leader.Role, leader.Count = placement.Leader, 1
					require.NoError(t, rm.SetRule(leader))
					voters := leader.Clone()
					voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
					voters.LocationLabels = []string{"zone", "host"}
					voters.IsolationLevel = level
					require.NoError(t, rm.SetRule(voters))
					region := tc.AddLeaderRegion(1, 1, 2, 3)
					if internal {
						for i := range 15 {
							leaderID, followers := uint64(1), []uint64{2, 3}
							if i >= 10 {
								leaderID, followers = 2, []uint64{1, 3}
							}
							tc.AddLeaderRegionWithRange(uint64(i+1), fmt.Sprintf("a%02d", i), fmt.Sprintf("a%02d", i+1), leaderID, followers...)
						}
						region = tc.GetRegion(1)
					}
					require.True(t, rm.FitRegionWithoutCache(tc, region).IsSatisfied())
					_, err := operator.CreateTransferLeaderOperator("ordinary-transfer", tc, region, 3, nil, operator.OpLeader)
					require.NoError(t, err)

					group := "review-leader-only"
					for id, count := range map[uint64]int{1: 10, 2: 5} {
						for range count {
							scatterer.ordinaryEngine.selectedLeader.Put(id, group)
						}
					}
					var op *operator.Operator
					if internal {
						op, err = scatterer.ScatterInternal(region, group, []byte("a00"), []byte("a15"))
					} else {
						op, err = scatterer.Scatter(region, group, false)
					}
					require.NoError(t, err)
					require.NotNil(t, op)
					peers, targetLeader := finalPlacementAfterOperator(region, op)
					require.Len(t, peers, len(region.GetPeers()))
					for _, peer := range region.GetPeers() {
						require.Contains(t, peers, peer.GetStoreId())
					}
					require.Equal(t, uint64(3), targetLeader, "scatter should accept the legal preferred leader when no peers move and configured isolation is preserved")
				})
			}
		})
	}
}
