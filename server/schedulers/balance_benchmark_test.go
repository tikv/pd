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

func newStoreRegion(ctx context.Context) *mockcluster.Cluster {
	opt := config.NewTestOptions()
	storeCount := 100
	regionCount := 100
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(0, 0)
	tc.AddRegionStore(1, 0)
	for i := uint64(2); i < uint64(storeCount); i++ {
		tc.AddRegionStore(i, 0)
		for j := 0; j < regionCount; j++ {
			tc.AddRegionWithLearner(uint64(j)+i*uint64(regionCount), i, []uint64{i - 1, i - 2}, nil)
		}
	}
	return tc
}

func newStoreWithLabel(ctx context.Context) *mockcluster.Cluster {
	storeCount := 100
	regionCount := 100
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	opt.SetPlacementRuleEnabled(false)
	config := opt.GetReplicationConfig()
	config.LocationLabels = []string{"zone,rack,host"}
	tc.AddRegionStore(0, 0)
	tc.AddRegionStore(1, 0)
	for i := uint64(2); i < uint64(storeCount); i++ {
		tc.AddRegionStore(i, 0)
		for j := 0; j < regionCount; j++ {
			tc.AddRegionWithLearner(uint64(j)+i*uint64(regionCount), i, []uint64{i - 1, i - 2}, nil)
		}
	}
	return tc
}

func BenchmarkLabel(b *testing.B) {
	ctx := context.Background()
	tc := newStoreRegion(ctx)
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
