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

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func BenchmarkIsolationOperator(b *testing.B) {
	for _, name := range []string{"move-peer/no-level", "move-peer/host", "transfer-leader/host"} {
		b.Run(name, func(b *testing.B) {
			tc := mockcluster.NewCluster(b.Context(), mockconfig.NewTestOptions())
			tc.SetEnablePlacementRules(true)
			for i, host := range []string{"a", "b", "a", "a"} {
				tc.AddLabelsStore(uint64(i+1), 0, map[string]string{"host": host})
			}
			rm := tc.GetRuleManager()
			leader := rm.GetRule("pd", "default").Clone()
			leader.Role, leader.Count = placement.Leader, 1
			require.NoError(b, rm.SetRule(leader))
			voters := leader.Clone()
			voters.ID, voters.Role, voters.Count = "voters", placement.Voter, 2
			voters.LocationLabels = []string{"host"}
			if name != "move-peer/no-level" {
				voters.IsolationLevel = "host"
			}
			require.NoError(b, rm.SetRule(voters))
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			build := func() (*Operator, error) {
				if name == "transfer-leader/host" {
					return CreateTransferLeaderOperator("bench", tc, region, 3, nil, OpLeader)
				}
				return CreateMovePeerOperator("bench", tc, region, OpRegion, 1, &metapb.Peer{StoreId: 4})
			}
			op, err := build()
			require.NoError(b, err)
			require.NotNil(b, op)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := build(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
