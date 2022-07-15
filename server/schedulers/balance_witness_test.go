// Copyright 2022 TiKV Project Authors.
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

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testBalanceWitnessSchedulerSuite{})

type testBalanceWitnessSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *config.PersistOptions
}

func (s *testBalanceWitnessSchedulerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.ctx, s.opt)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
	lb, err := schedule.CreateScheduler(BalanceWitnessType, s.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceWitnessType, []string{"", ""}))
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceWitnessSchedulerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceWitnessSchedulerSuite) schedule() []*operator.Operator {
	ops, _ := s.lb.Schedule(s.tc, false)
	return ops
}

func (s *testBalanceWitnessSchedulerSuite) TestScheduleWithOpInfluence(c *C) {
	s.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Witnesses:  7    8    9   14
	s.tc.AddWitnessStore(1, 7)
	s.tc.AddWitnessStore(2, 8)
	s.tc.AddWitnessStore(3, 9)
	s.tc.AddWitnessStore(4, 14)
	s.tc.AddLeaderRegionWithWitness(1, 3, []uint64{1, 2, 4}, 4)
	op := s.schedule()[0]
	c.Check(op, NotNil)
	s.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when witness difference is not greater than 5.
	c.Check(s.schedule(), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 8)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 13)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Assert(s.schedule(), HasLen, 0)
}

func (s *testBalanceWitnessSchedulerSuite) TestTransferWitnessOut(c *C) {
	// Stores:     1    2    3    4
	// Witnesses:  7    8    9   12
	s.tc.AddWitnessStore(1, 7)
	s.tc.AddWitnessStore(2, 8)
	s.tc.AddWitnessStore(3, 9)
	s.tc.AddWitnessStore(4, 12)
	s.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		s.tc.AddLeaderRegionWithWitness(i, 3, []uint64{1, 2, 4}, 4)
	}

	// balance leader: 4->1, 4->1, 4->2
	regions := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for i := 0; i < 20; i++ {
		if len(s.schedule()) == 0 {
			continue
		}
		if op := s.schedule()[0]; op != nil {
			if _, ok := regions[op.RegionID()]; !ok {
				s.oc.SetOperator(op)
				regions[op.RegionID()] = struct{}{}
				from := op.Step(0).(operator.RemovePeer).FromStore
				// to := op.Step(1).(operator.AddLearner).ToStore
				// to := op.Step(2).(operator.PromoteLearner).ToStore
				to := op.Step(3).(operator.BecomeWitness).StoreID
				c.Assert(from, Equals, uint64(4))
				targets[to]--
			}
		}
	}
	c.Assert(regions, HasLen, 3)
	for _, count := range targets {
		c.Assert(count, Equals, uint64(0))
	}
}
