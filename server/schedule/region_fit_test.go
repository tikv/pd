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

package schedule

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	opt    *mockoption.ScheduleOptions
}

func (s *testRegionSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(s.opt)
}

func (s *testRegionSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testRegionSuite) fit(region *core.RegionInfo) *placement.RegionFit {
	return s.tc.FitRegion(region)
}

func (s *testRegionSuite) TestRegionFit(c *C) {
	s.opt.EnablePlacementRules = true
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

	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 0)
	s.tc.AddLabelsStore(1, 1, map[string]string{
		"role": "leader",
	})
	region := s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	regionFit := s.tc.FitRegion(region)
	c.Check(len(regionFit.RuleFits), Equals, 2)
	for _, ruleFit := range regionFit.RuleFits {
		if ruleFit.Rule.GroupID == "test" {
			c.Check(len(ruleFit.Peers), Equals, 1)
			c.Check(ruleFit.Peers[0].StoreId, Equals, uint64(1))
		}
		if ruleFit.Rule.GroupID == "pd" {
			c.Check(len(ruleFit.Peers), Equals, 3)
		}
	}
}
