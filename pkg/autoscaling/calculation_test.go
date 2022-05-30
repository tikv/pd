// Copyright 2020 TiKV Project Authors.
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

package autoscaling

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

func TestGetScaledTiKVGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// case1 indicates the tikv cluster with not any group existed
	case1 := mockcluster.NewCluster(ctx, config.NewTestOptions())
	case1.AddLabelsStore(1, 1, map[string]string{})
	case1.AddLabelsStore(2, 1, map[string]string{
		"foo": "bar",
	})
	case1.AddLabelsStore(3, 1, map[string]string{
		"id": "3",
	})

	// case2 indicates the tikv cluster with 1 auto-scaling group existed
	case2 := mockcluster.NewCluster(ctx, config.NewTestOptions())
	case2.AddLabelsStore(1, 1, map[string]string{})
	case2.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
		resourceTypeLabelKey: "a",
	})
	case2.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
		resourceTypeLabelKey: "a",
	})

	// case3 indicates the tikv cluster with other group existed
	case3 := mockcluster.NewCluster(ctx, config.NewTestOptions())
	case3.AddLabelsStore(1, 1, map[string]string{})
	case3.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey: "foo",
	})
	case3.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey: "foo",
	})

	testCases := []struct {
		name             string
		informer         core.StoreSetInformer
		healthyInstances []instance
		expectedPlan     []*Plan
		noError          bool
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
			noError:      true,
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
					Component:    TiKV.String(),
					Count:        2,
					ResourceType: "a",
					Labels: map[string]string{
						groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
						resourceTypeLabelKey: "a",
					},
				},
			},
			noError: true,
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
			noError:      false,
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
					Component:    TiKV.String(),
					Count:        1,
					ResourceType: "a",
					Labels: map[string]string{
						groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
						resourceTypeLabelKey: "a",
					},
				},
			},
			noError: true,
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
			noError:      true,
		},
	}

	for _, testCase := range testCases {
		t.Log(testCase.name)
		plans, err := getScaledTiKVGroups(testCase.informer, testCase.healthyInstances)
		if testCase.expectedPlan == nil {
			require.Len(t, plans, 0)
			require.Equal(t, testCase.noError, err == nil)
		} else {
			require.True(t, reflect.DeepEqual(testCase.expectedPlan, plans))
		}
	}
}

type mockQuerier struct{}

func (q *mockQuerier) Query(options *QueryOptions) (QueryResult, error) {
	result := make(QueryResult)
	for _, addr := range options.addresses {
		result[addr] = mockResultValue
	}

	return result, nil
}

func TestGetTotalCPUUseTime(t *testing.T) {
	querier := &mockQuerier{}
	instances := []instance{
		{
			address: "1",
			id:      1,
		},
		{
			address: "2",
			id:      2,
		},
		{
			address: "3",
			id:      3,
		},
	}
	totalCPUUseTime, _ := getTotalCPUUseTime(querier, TiDB, instances, time.Now(), 0)
	expected := mockResultValue * float64(len(instances))
	require.True(t, math.Abs(expected-totalCPUUseTime) < 1e-6)
}

func TestGetTotalCPUQuota(t *testing.T) {
	querier := &mockQuerier{}
	instances := []instance{
		{
			address: "1",
			id:      1,
		},
		{
			address: "2",
			id:      2,
		},
		{
			address: "3",
			id:      3,
		},
	}
	totalCPUQuota, _ := getTotalCPUQuota(querier, TiDB, instances, time.Now())
	expected := uint64(mockResultValue * float64(len(instances)*milliCores))
	require.Equal(t, expected, totalCPUQuota)
}

func TestScaleOutGroupLabel(t *testing.T) {
	var jsonStr = []byte(`
{
    "rules":[
        {
            "component":"tikv",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "resource_types":["resource_a"]
            }
        },
        {
            "component":"tidb",
            "cpu_rule":{
                "max_threshold":0.8,
                "min_threshold":0.2,
                "max_count":2,
                "resource_types":["resource_a"]
            }
        }
    ],
    "resources":[
        {
            "resource_type":"resource_a",
            "cpu":1,
            "memory":8,
            "storage":1000,
            "count": 2
        }
    ]
}`)
	strategy := &Strategy{}
	err := json.Unmarshal(jsonStr, strategy)
	require.NoError(t, err)
	plan := findBestGroupToScaleOut(strategy, nil, TiKV)
	require.Equal(t, "hotRegion", plan.Labels["specialUse"])
	plan = findBestGroupToScaleOut(strategy, nil, TiDB)
	require.Equal(t, "", plan.Labels["specialUse"])
}

func TestStrategyChangeCount(t *testing.T) {
	var count uint64 = 2
	strategy := &Strategy{
		Rules: []*Rule{
			{
				Component: "tikv",
				CPURule: &CPURule{
					MaxThreshold:  0.8,
					MinThreshold:  0.2,
					ResourceTypes: []string{"resource_a"},
				},
			},
		},
		Resources: []*Resource{
			{
				ResourceType: "resource_a",
				CPU:          1,
				Memory:       8,
				Storage:      1000,
				Count:        &count,
			},
		},
	}

	// tikv cluster with 1 auto-scaling group existed
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, config.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{})
	cluster.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
		resourceTypeLabelKey: "resource_a",
	})
	cluster.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-%s-0", autoScalingGroupLabelKeyPrefix, TiKV.String()),
		resourceTypeLabelKey: "resource_a",
	})

	instances := []instance{{id: 1, address: "1"}, {id: 2, address: "2"}, {id: 3, address: "3"}}

	// under high load
	maxThreshold, _ := getCPUThresholdByComponent(strategy, TiKV)
	totalCPUUseTime := 90.0
	totalCPUTime := 100.0
	scaleOutQuota := (totalCPUUseTime - totalCPUTime*maxThreshold) / 5

	// exist two scaled TiKVs and plan does not change due to the limit of resource count
	groups, err := getScaledTiKVGroups(cluster, instances)
	require.NoError(t, err)
	plans := calculateScaleOutPlan(strategy, TiKV, scaleOutQuota, groups)
	require.Equal(t, uint64(2), plans[0].Count)

	// change the resource count to 3 and plan increates one more tikv
	groups, err = getScaledTiKVGroups(cluster, instances)
	require.NoError(t, err)
	*strategy.Resources[0].Count = 3
	plans = calculateScaleOutPlan(strategy, TiKV, scaleOutQuota, groups)
	require.Equal(t, uint64(3), plans[0].Count)

	// change the resource count to 1 and plan decreases to 1 tikv due to the limit of resource count
	groups, err = getScaledTiKVGroups(cluster, instances)
	require.NoError(t, err)
	*strategy.Resources[0].Count = 1
	plans = calculateScaleOutPlan(strategy, TiKV, scaleOutQuota, groups)
	require.Equal(t, uint64(1), plans[0].Count)
}
