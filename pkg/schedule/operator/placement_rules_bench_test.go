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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func BenchmarkTargetLeaderRoles(b *testing.B) {
	for _, n := range []int{3, 5, 9} {
		for _, full := range []bool{false, true} {
			mode := "local"
			if full {
				mode = "complete"
			}
			b.Run(fmt.Sprintf("peers%d/%s", n, mode), func(b *testing.B) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tc := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
				tc.SetEnablePlacementRules(true)
				followers := make([]uint64, 0, n-1)
				for i := 1; i <= n; i++ {
					tc.AddLabelsStore(uint64(i), 0, map[string]string{"zone": strconv.Itoa(i % 3), "host": strconv.Itoa(i)})
					if i > 1 {
						followers = append(followers, uint64(i))
					}
				}
				rm := tc.GetRuleManager()
				rule := rm.GetRule("pd", "default").Clone()
				rule.Role = placement.Leader
				rule.Count = 1
				rule.LocationLabels = []string{"zone", "host"}
				if err := rm.SetRule(rule); err != nil {
					b.Fatal(err)
				}
				for _, id := range []string{"voters1", "voters2"} {
					r := rule.Clone()
					r.ID = id
					r.Role = placement.Voter
					r.Count = (n - 1) / 2
					if err := rm.SetRule(r); err != nil {
						b.Fatal(err)
					}
				}
				region := tc.AddLeaderRegion(1, 1, followers...)
				builder := NewBuilder("review", tc, region)
				builder.currentPeers = builder.originPeers.copy()
				builder.currentLeaderStoreID = builder.originLeaderStoreID
				candidate := region.GetStorePeer(2)
				if !builder.checkTargetRules || !builder.allowTargetLeader(candidate) {
					b.Fatal("invalid benchmark fixture")
				}
				var check func(*metapb.Peer) bool
				if full {
					check = builder.allowTargetLeader
				} else {
					check = func(p *metapb.Peer) bool { return builder.allowLeader(p, false) }
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if !check(candidate) {
						b.Fatal("candidate rejected")
					}
				}
			})
		}
	}
}
