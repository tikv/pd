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

package autoscaling

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockoption"
	"github.com/tikv/pd/server/core"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&calculationTestSuite{})

type calculationTestSuite struct{}

func (s *calculationTestSuite) TestGetScaledTiKVGroups(c *C) {
	// case1 indicates the tikv cluster with not any group existed
	case1 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case1.AddLabelsStore(1, 1, map[string]string{})
	case1.AddLabelsStore(2, 1, map[string]string{
		"foo": "bar",
	})
	case1.AddLabelsStore(3, 1, map[string]string{
		"id": "3",
	})

	// case2 indicates the tikv cluster with 1 auto-scaling group existed
	case2 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case2.AddLabelsStore(1, 1, map[string]string{})
	case2.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
	})
	case2.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
	})

	// case3 indicates the tikv cluster with other group existed
	case3 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case3.AddLabelsStore(1, 1, map[string]string{})
	case3.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey: "foo",
	})
	case3.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey: "foo",
	})

	testcases := []struct {
		name             string
		informer         core.StoreSetInformer
		healthyInstances []instance
		expectedPlan     []*Plan
	}{
		{
			name:     "no scaled tikv group",
			informer: case1,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: nil,
		},
		{
			name:     "exist 1 scaled tikv group",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: []*Plan{
				{
					Component: TiKV.String(),
					Count:     2,
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
					},
				},
			},
		},
		{
			name:     "exist 1 tikv scaled group with inconsistency healthy instances",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      4,
					address: "4",
				},
			},
			expectedPlan: nil,
		},
		{
			name:     "exist 1 tikv scaled group with less healthy instances",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
			},
			expectedPlan: []*Plan{
				{
					Component: TiKV.String(),
					Count:     1,
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
					},
				},
			},
		},
		{
			name:     "existed other tikv group",
			informer: case3,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: nil,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		plans := getScaledTiKVGroups(testcase.informer, testcase.healthyInstances)
		if testcase.expectedPlan == nil {
			c.Assert(plans, IsNil)
		} else {
			c.Assert(plans, DeepEquals, testcase.expectedPlan)
		}
	}
}

func (s *calculationTestSuite) TestGetScaledTiDBGroups(c *C) {
	case1 := newMockTiDBInformer()
	case1.SetTiDBs([]*TiDBInfo{
		{
			Address: "tidb-0",
			Labels: map[string]string{
				groupLabelKey: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
			},
		},
		{
			Address: "tidb-1",
			Labels: map[string]string{
				groupLabelKey: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
			},
		},
		{
			Address: "tidb-2",
			Labels:  map[string]string{},
		},
	})
	testcases := []struct {
		name             string
		informer         tidbInformer
		healthyInstances []instance
		expectedPlan     []*Plan
	}{
		{
			name:     "exist 1 scaled tidb group",
			informer: case1,
			healthyInstances: []instance{
				{
					id:      0,
					address: "tidb-0",
				},
				{
					id:      1,
					address: "tidb-1",
				},
				{
					id:      2,
					address: "tidb-2",
				},
			},
			expectedPlan: []*Plan{
				{
					Component: TiDB.String(),
					Count:     2,
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
					},
				},
			},
		},
		{
			name:     "exist 1 tidb scaled group with less healthy instances",
			informer: case1,
			healthyInstances: []instance{
				{
					id:      1,
					address: "tidb-1",
				},
				{
					id:      2,
					address: "tidb-2",
				},
			},
			expectedPlan: []*Plan{
				{
					Component: TiDB.String(),
					Count:     1,
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
					},
				},
			},
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		plans := getScaledTiDBGroups(testcase.informer, testcase.healthyInstances)
		if testcase.expectedPlan == nil {
			c.Assert(plans, IsNil)
		} else {
			c.Assert(plans, DeepEquals, testcase.expectedPlan)
		}
	}
}

type mockTidbInformer struct {
	tidbs map[string]*TiDBInfo
}

func newMockTiDBInformer() *mockTidbInformer {
	return &mockTidbInformer{
		tidbs: map[string]*TiDBInfo{},
	}
}

// GetTiDB implements TiDBInformer.GetTiDB
func (mc *mockTidbInformer) GetTiDB(address string) *TiDBInfo {
	return mc.tidbs[address]
}

// SetTiDBs set multiple TiDB
func (mc *mockTidbInformer) SetTiDBs(tidbs []*TiDBInfo) {
	for _, tidb := range tidbs {
		mc.tidbs[tidb.Address] = tidb
	}
}
