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
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestCheckPriorityRegions(t *testing.T) {
	re := require.New(t)
	opt := mockconfig.NewTestOptions()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddLeaderRegion(1, 2, 1, 3)
	tc.AddLeaderRegion(2, 2, 3)
	tc.AddLeaderRegion(3, 2)

	pc := NewPriorityInspector(tc, tc.GetCheckerConfig())
	checkPriorityRegionTest(re, pc, tc)
	opt.SetPlacementRuleEnabled(true)
	re.True(opt.IsPlacementRulesEnabled())
	checkPriorityRegionTest(re, pc, tc)
}

func checkPriorityRegionTest(re *require.Assertions, pc *PriorityInspector, tc *mockcluster.Cluster) {
	// case1: inspect region 1, it doesn't lack replica
	region := tc.GetRegion(1)
	opt := tc.GetCheckerConfig()
	pc.Inspect(region)
	re.Equal(0, pc.getQueueLen())

	// case2: inspect region 2, it lacks one replica
	region = tc.GetRegion(2)
	pc.Inspect(region)
	re.Equal(1, pc.getQueueLen())
	// the region will not rerun after it checks
	re.Empty(pc.GetPriorityRegions())

	// case3: inspect region 3, it will has high priority
	region = tc.GetRegion(3)
	pc.Inspect(region)
	re.Equal(2, pc.getQueueLen())
	time.Sleep(opt.GetPatrolRegionInterval() * 10)
	// region 3 has higher priority
	ids := pc.GetPriorityRegions()
	re.Len(ids, 2)
	re.Equal(uint64(3), ids[0])
	re.Equal(uint64(2), ids[1])

	// case4: inspect region 2 again after it fixup replicas
	tc.AddLeaderRegion(2, 2, 3, 1)
	region = tc.GetRegion(2)
	pc.Inspect(region)
	re.Equal(1, pc.getQueueLen())

	// case5: inspect region 3 again
	region = tc.GetRegion(3)
	pc.Inspect(region)
	time.Sleep(opt.GetPatrolRegionInterval() * 10)
	ids = pc.GetPriorityRegions()
	re.Empty(ids)

	// recover
	tc.AddLeaderRegion(2, 2, 3)
	pc.RemovePriorityRegion(uint64(3))
}
