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

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/pkg/v3/report"
)

func TestCollectUsesIndependentWorkloadRoundCount(t *testing.T) {
	avgRegionStats = report.Stats{}
	avgStoreTime = 0
	workloadStatsRounds = 0

	collect(report.Stats{Total: time.Second, Average: 1, RPS: 100}, 10)
	collect(report.Stats{Total: 3 * time.Second, Average: 3, RPS: 300}, 30)

	require.Equal(t, 2*time.Second, avgRegionStats.Total)
	require.Equal(t, 2.0, avgRegionStats.Average)
	require.Equal(t, 200.0, avgRegionStats.RPS)
	require.Equal(t, 20.0, avgStoreTime)
	require.Equal(t, 2, workloadStatsRounds)
}

func TestPrometheusLatencyQueriesReturnMilliseconds(t *testing.T) {
	require.Contains(t, hbLatency99Metric, "* 1000")
	require.Contains(t, hbLatencyAvgMetric, "* 1000")
	breakdown := hbBreakdownMetricByName("RegionGuide")
	require.Contains(t, breakdown, "_count")
	require.Contains(t, breakdown, "* 1000")
}
