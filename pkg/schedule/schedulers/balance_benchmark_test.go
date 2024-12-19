// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package schedulers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
)

var (
	zones = []string{"zone1", "zone2", "zone3"}
	racks = []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}
	hosts = []string{"host1", "host2", "host3", "host4", "host5", "host6",
		"host7", "host8", "host9"}

	regionCount  = 2000
	storeCount   = len(zones) * len(racks) * len(hosts)
	tiflashCount = 30
)

// newBenchCluster store region count is same with storeID and
// the tolerate define storeCount that store can elect candidate but not should balance
// so the case  bench the worst scene
func newBenchCluster(ruleEnable, labelEnable bool, tombstoneEnable bool) (context.CancelFunc, *mockcluster.Cluster, *operator.Controller) {
	Register()
	ctx, cancel := context.WithCancel(context.Background())
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
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
	storeID, regionID := uint64(1), uint64(1)
	for _, host := range hosts {
		for _, rack := range racks {
			for _, az := range zones {
				label := make(map[string]string, 3)
				label["az"] = az
				label["rack"] = rack
				label["host"] = host
				tc.AddLabelsStore(storeID, regionCount-int(storeID), label)
				storeID++
			}
			for range regionCount {
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
		for i := range uint64(storeCount * 2 / 3) {
			s := tc.GetStore(i)
			s.GetMeta().State = metapb.StoreState_Tombstone
		}
	}
	return cancel, tc, oc
}

func newBenchBigCluster(storeNumInOneRack, regionNum int) (context.CancelFunc, *mockcluster.Cluster, *operator.Controller) {
	Register()
	ctx, cancel := context.WithCancel(context.Background())
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
	opt.GetScheduleConfig().TolerantSizeRatio = float64(storeCount)
	opt.SetPlacementRuleEnabled(true)

	config := opt.GetReplicationConfig()
	config.LocationLabels = []string{"az", "rack", "host"}
	config.IsolationLevel = "az"

	storeID, regionID := uint64(0), uint64(0)
	hosts := make([]string, 0)
	for i := range storeNumInOneRack {
		hosts = append(hosts, fmt.Sprintf("host%d", i+1))
	}
	for _, host := range hosts {
		for _, rack := range racks {
			for _, az := range zones {
				label := make(map[string]string, 3)
				label["az"] = az
				label["rack"] = rack
				label["host"] = host
				storeID++
				tc.AddLabelsStore(storeID, regionNum, label)
			}
			for range regionCount {
				tc.AddRegionWithLearner(regionID, storeID, []uint64{storeID - 1, storeID - 2}, nil)
				regionID++
			}
		}
	}
	return cancel, tc, oc
}

func addTiflash(tc *mockcluster.Cluster) {
	tc.SetPlacementRuleEnabled(true)
	for i := range tiflashCount {
		label := make(map[string]string, 3)
		label["engine"] = "tiflash"
		if i == tiflashCount-1 {
			tc.AddLabelsStore(uint64(storeCount+i), 1, label)
		} else {
			tc.AddLabelsStore(uint64(storeCount+i), regionCount-storeCount-i, label)
		}
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
	re := assert.New(b)
	cancel, tc, oc := newBenchCluster(true, true, false)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	var ops []*operator.Operator
	var plans []plan.Plan
	for i := 0; i < b.N; i++ {
		ops, plans = sc.Schedule(tc, false)
	}
	b.StopTimer()
	re.Empty(plans)
	re.Len(ops, 1)
	re.Contains(ops[0].String(), "to [191]")
}

func BenchmarkLabel(b *testing.B) {
	cancel, tc, oc := newBenchCluster(false, true, false)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, false)
	}
}

func BenchmarkNoLabel(b *testing.B) {
	cancel, tc, oc := newBenchCluster(false, false, false)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, false)
	}
}

func BenchmarkDiagnosticNoLabel1(b *testing.B) {
	cancel, tc, oc := newBenchCluster(false, false, false)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, true)
	}
}

func BenchmarkDiagnosticNoLabel2(b *testing.B) {
	cancel, tc, oc := newBenchBigCluster(100, 100)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, true)
	}
}

func BenchmarkNoLabel2(b *testing.B) {
	cancel, tc, oc := newBenchBigCluster(100, 100)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, false)
	}
}

func BenchmarkTombStore(b *testing.B) {
	cancel, tc, oc := newBenchCluster(false, false, true)
	defer cancel()
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc, false)
	}
}
