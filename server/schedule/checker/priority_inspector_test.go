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
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testPriorityInspectorSuite{})

type testPriorityInspectorSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testPriorityInspectorSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testPriorityInspectorSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testPriorityInspectorSuite) TestCheckPriorityRegions(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, opt)
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddLeaderRegion(1, 2, 1, 3)
	tc.AddLeaderRegion(2, 2, 3)
	tc.AddLeaderRegion(3, 2)

	pc := NewPriorityInspector(tc)
	checkPriorityRegionTest(pc, tc, c)
	opt.SetPlacementRuleEnabled(true)
	c.Assert(opt.IsPlacementRulesEnabled(), IsTrue)
	checkPriorityRegionTest(pc, tc, c)
}

func checkPriorityRegionTest(pc *PriorityInspector, tc *mockcluster.Cluster, c *C) {
	// case1: inspect region 1, it doesn't lack replica
	region := tc.GetRegion(1)
	opt := tc.GetOpts()
	pc.Inspect(region)
	c.Assert(0, Equals, pc.queue.Len())

	// case2: inspect region 2, it lacks one replica
	region = tc.GetRegion(2)
	pc.Inspect(region)
	c.Assert(1, Equals, pc.queue.Len())
	// the region will not rerun after it checks
	c.Assert(0, Equals, len(pc.GetPriorityRegions()))

	// case3: inspect region 3, it will has high priority
	region = tc.GetRegion(3)
	pc.Inspect(region)
	c.Assert(2, Equals, pc.queue.Len())
	time.Sleep(opt.GetPatrolRegionInterval() * 10)
	// region 3 has higher priority
	ids := pc.GetPriorityRegions()
	c.Assert(2, Equals, len(ids))
	c.Assert(uint64(3), Equals, ids[0])
	c.Assert(uint64(2), Equals, ids[1])

	// case4: inspect region 2 again after it fixup replicas
	tc.AddLeaderRegion(2, 2, 3, 1)
	region = tc.GetRegion(2)
	pc.Inspect(region)
	c.Assert(1, Equals, pc.queue.Len())

	// recover
	tc.AddLeaderRegion(2, 2, 3)
	pc.RemovePriorityRegion(uint64(3))
}
