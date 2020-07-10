// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

var _ = Suite(&testBalanceLeaderSchedulerWithRuleEnabledSuite{})

type testBalanceLeaderSchedulerWithRuleEnabledSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *mockoption.ScheduleOptions
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = mockoption.NewScheduleOptions()
	s.opt.EnablePlacementRules = true
	s.tc = mockcluster.NewCluster(s.opt)
	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func (s *testBalanceLeaderSchedulerWithRuleEnabledSuite) TestBalanceLeaderWithConflictRule(c *C) {
	rule := placement.Rule{
		GroupID:  "test",
		ID:       "1",
		Index:    1,
		StartKey: []byte(""),
		EndKey:   []byte(""),
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "role",
				Op:     placement.In,
				Values: []string{"leader"},
			},
		},
		LocationLabels: []string{"host"},
	}
	c.Check(s.tc.SetRule(&rule), IsNil)
	c.Check(s.tc.DeleteRule("pd", "default"), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 0)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	s.tc.AddLabelsStore(1, 1, map[string]string{
		"role": "leader",
	})
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	s.tc.UpdateLeaderCount(1, 16)
	c.Check(s.schedule(), IsNil)
}
