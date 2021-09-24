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
package schedulers

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/placement"
)

var (
	zones = []string{"az1", "az2", "az3"}
	racks = []string{"rack1", "rack2", "rack3"}
	hosts = []string{"host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8", "host9"}

	regionCount  = 100
	storeCount   = 81
	tiflashCount = 9
)

// newBenchCluster store region count is same with storeID and
// the tolerate define storeCount that store can elect candidate but not should balance
// so the case  bench the worst scene
func newBenchCluster(ctx context.Context, ruleEnable, labelEnable bool, tombstoneEnable bool) *mockcluster.Cluster {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	opt.GetScheduleConfig().TolerantSizeRatio = float64(storeCount)
	opt.SetPlacementRuleEnabled(ruleEnable)

	if labelEnable {
		config := opt.GetReplicationConfig()
		config.LocationLabels = []string{"az", "rack", "host"}
		config.IsolationLevel = "az"
	}

	if ruleEnable {
		addTiflash(tc)
	}
	storeID, regionID := uint64(0), uint64(0)
	for _, host := range hosts {
		for _, rack := range racks {
			for _, az := range zones {
				label := make(map[string]string, 3)
				label["az"] = az
				label["rack"] = rack
				label["host"] = host
				tc.AddLabelsStore(storeID, int(storeID), label)
				storeID++
			}
			for j := 0; j < regionCount; j++ {
				if ruleEnable {
					learnID := regionID%uint64(tiflashCount) + uint64(storeCount)
					tc.AddRegionWithLearner(regionID, storeID-1, []uint64{storeID - 2, storeID - 3}, []uint64{learnID})
				} else {
					tc.AddRegionWithLearner(regionID, storeID-1, []uint64{storeID - 2, storeID - 3}, nil)
				}
				regionID++
			}
		}
	}
	if tombstoneEnable {
		for i := uint64(0); i < uint64(storeCount*2/3); i++ {
			s := tc.GetStore(i)
			s.GetMeta().State = metapb.StoreState_Tombstone
		}
	}
	return tc
}

func addTiflash(tc *mockcluster.Cluster) {
	tc.SetPlacementRuleEnabled(true)
	for i := 0; i < tiflashCount; i++ {
		label := make(map[string]string, 3)
		label["engine"] = "tiflash"
		tc.AddLabelsStore(uint64(storeCount+i), regionCount, label)
	}
	rule := &placement.Rule{
		GroupID: "tiflash-override",
		ID:      "learner-replica-table-ttt",
		Role:    "learner",
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "engine", Op: "in", Values: []string{"tiflash"}},
		},
		LocationLabels: []string{"host"},
	}
	tc.SetRule(rule)
}

func BenchmarkPlacementRule(b *testing.B) {
	ctx := context.Background()
	tc := newBenchCluster(ctx, true, true, false)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}
}

func BenchmarkLabel(b *testing.B) {
	ctx := context.Background()
	tc := newBenchCluster(ctx, false, true, false)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}
}

func BenchmarkNoLabel(b *testing.B) {
	ctx := context.Background()
	tc := newBenchCluster(ctx, false, false, false)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}
}

func BenchmarkTombStore(b *testing.B) {
	ctx := context.Background()
	tc := newBenchCluster(ctx, false, false, true)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}
}
