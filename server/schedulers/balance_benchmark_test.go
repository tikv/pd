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

	"github.com/tikv/pd/server/schedule"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
)

var (
	zones = []string{"az1", "az2", "az3"}
	racks = []string{"rack1", "rack2", "rack3"}
	hosts = []string{"host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8", "host9"}

	regionCount = 100
	storeCount  = 100
)

func newStoreRegion(ctx context.Context) *mockcluster.Cluster {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(0, 0)
	tc.AddRegionStore(1, 0)
	for i := uint64(2); i < uint64(storeCount); i++ {
		tc.AddRegionStore(i, int(i))
		for j := 0; j < regionCount; j++ {
			tc.AddRegionWithLearner(uint64(j)+i*uint64(regionCount), i, []uint64{i - 1, i - 2}, nil)
		}
	}
	return tc
}

func newStoreWithLabel(ctx context.Context) *mockcluster.Cluster {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	opt.SetPlacementRuleEnabled(false)
	config := opt.GetReplicationConfig()
	config.LocationLabels = []string{"az", "rack", "host"}
	config.IsolationLevel = "az"
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
				tc.AddRegionWithLearner(regionID, storeID-1, []uint64{storeID - 2, storeID - 3}, nil)
				regionID++
			}
		}
	}
	return tc
}

func BenchmarkLabel(b *testing.B) {
	ctx := context.Background()
	tc := newStoreWithLabel(ctx)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}
}

func BenchmarkNoScheduler(b *testing.B) {
	ctx := context.Background()
	tc := newStoreRegion(ctx)
	oc := schedule.NewOperatorController(ctx, nil, nil)
	sc := newBalanceRegionScheduler(oc, &balanceRegionSchedulerConfig{}, []BalanceRegionCreateOption{WithBalanceRegionName(BalanceRegionType)}...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.Schedule(tc)
	}

}
