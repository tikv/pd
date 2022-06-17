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

package checker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
)

func TestCheckPriorityRegions(t *testing.T) {
	re := require.New(t)
	opt := config.NewTestOptions()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddLeaderRegion(1, 2, 1, 3)
	tc.AddLeaderRegion(2, 2, 3)
	tc.AddLeaderRegion(3, 2)

	pc := NewPriorityInspector(tc)
	checkPriorityRegionTest(re, pc, tc)
	opt.SetPlacementRuleEnabled(true)
	re.True(opt.IsPlacementRulesEnabled())
	checkPriorityRegionTest(re, pc, tc)
}

func checkPriorityRegionTest(re *require.Assertions, pc *PriorityInspector, tc *mockcluster.Cluster) {
	// case1: inspect region 1, it doesn't lack replica
	region := tc.GetRegion(1)
	opt := tc.GetOpts()
	pc.Inspect(region)
	re.Equal(pc.queue.Len(), 0)

	// case2: inspect region 2, it lacks one replica
	region = tc.GetRegion(2)
	pc.Inspect(region)
	re.Equal(pc.queue.Len(), 1)
	// the region will not rerun after it checks
	re.Equal(len(pc.GetPriorityRegions()), 0)

	// case3: inspect region 3, it will has high priority
	region = tc.GetRegion(3)
	pc.Inspect(region)
	re.Equal(pc.queue.Len(), 2)
	time.Sleep(opt.GetPatrolRegionInterval() * 10)
	// region 3 has higher priority
	ids := pc.GetPriorityRegions()
	re.Equal(len(ids), 2)
	re.Equal(ids[0], uint64(3))
	re.Equal(ids[1], uint64(2))

	// case4: inspect region 2 again after it fixup replicas
	tc.AddLeaderRegion(2, 2, 3, 1)
	region = tc.GetRegion(2)
	pc.Inspect(region)
	re.Equal(pc.queue.Len(), 1)

	// recover
	tc.AddLeaderRegion(2, 2, 3)
	pc.RemovePriorityRegion(uint64(3))
}
