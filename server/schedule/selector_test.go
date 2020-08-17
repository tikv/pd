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
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockoption"
	"github.com/tikv/pd/server/core"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSelectorSuite{})

type testSelectorSuite struct {
	tc *mockcluster.Cluster
}

func (s *testSelectorSuite) SetUpSuite(c *C) {
	opt := mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(opt)
}

func (s *testSelectorSuite) TestScheduleConfig(c *C) {
	filters := make([]Filter, 0)
	s.tc.LeaderSchedulePolicy = core.ByCount.String()
	testScheduleConfig := func(selector *BalanceSelector, stores []*core.StoreInfo, expectSourceID, expectTargetID uint64) {
		c.Assert(selector.SelectSource(s.tc, stores).GetID(), Equals, expectSourceID)
		c.Assert(selector.SelectTarget(s.tc, stores).GetID(), Equals, expectTargetID)
	}

	kinds := []core.ScheduleKind{{
		Resource: core.RegionKind,
		Policy:   core.ByCount,
	}, {
		Resource: core.RegionKind,
		Policy:   core.BySize,
	}}

	for _, kind := range kinds {
		selector := NewBalanceSelector(kind, filters)
		stores := []*core.StoreInfo{
			core.NewStoreInfoWithSizeCount(1, 2, 3, 10, 5),
			core.NewStoreInfoWithSizeCount(2, 2, 3, 4, 5),
			core.NewStoreInfoWithSizeCount(3, 2, 3, 4, 5),
			core.NewStoreInfoWithSizeCount(4, 2, 3, 2, 5),
		}
		testScheduleConfig(selector, stores, 1, 4)
	}

	selector := NewBalanceSelector(core.ScheduleKind{
		Resource: core.LeaderKind,
		Policy:   core.ByCount,
	}, filters)
	stores := []*core.StoreInfo{
		core.NewStoreInfoWithSizeCount(1, 2, 20, 10, 25),
		core.NewStoreInfoWithSizeCount(2, 2, 66, 10, 5),
		core.NewStoreInfoWithSizeCount(3, 2, 6, 10, 5),
		core.NewStoreInfoWithSizeCount(4, 2, 20, 10, 1),
	}
	testScheduleConfig(selector, stores, 2, 3)
	s.tc.LeaderSchedulePolicy = core.BySize.String()
	testScheduleConfig(selector, stores, 1, 4)
}
