// Copyright 2026 TiKV Project Authors.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func TestAffinityCheckerLimitMetricSkippedWithoutAffinityGroups(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	cfg := opt.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 0
	opt.SetScheduleConfig(cfg)
	opt.SetPlacementRuleEnabled(false)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false /* no need to run */)
	opController := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	controller := NewController(ctx, tc, tc.GetCheckerConfig(), opController)
	region := tc.GetRegion(1)
	metricType := controller.affinityChecker.GetType().String()
	metricName := operator.OpAffinity.String()

	before := getOperatorLimitCounter(re, metricType, metricName)
	re.Empty(controller.CheckRegion(region))
	re.Equal(before, getOperatorLimitCounter(re, metricType, metricName))

	re.NoError(createAffinityGroupForTest(tc.GetAffinityManager(), &affinity.Group{ID: "test-group"}, nil, nil))
	before = getOperatorLimitCounter(re, metricType, metricName)
	re.Empty(controller.CheckRegion(region))
	re.Equal(before+1, getOperatorLimitCounter(re, metricType, metricName))
}

func getOperatorLimitCounter(re *require.Assertions, typ, name string) float64 {
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() != "pd_schedule_operator_limit" {
			continue
		}
		for _, metric := range metricFamily.GetMetric() {
			matchesType, matchesName := false, false
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "type":
					matchesType = label.GetValue() == typ
				case "name":
					matchesName = label.GetValue() == name
				}
			}
			if matchesType && matchesName {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}
