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

package server_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/tests"
)

func TestMemberLeaderPriorityMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leaderServer := cluster.GetServer(leaderName)
	re.NoError(leaderServer.BootstrapCluster())

	s := leaderServer.GetServer()
	instance := s.GetMemberInfo().GetName()

	s.GetMember().CheckPriority(ctx)
	val, ok := getGaugeValue(re, "pd_server_member_leader_priority", map[string]string{
		"instance": instance,
	})
	re.True(ok)
	re.Equal(0.0, val)

	re.NoError(s.GetMember().SetMemberLeaderPriority(s.GetMemberInfo().GetMemberId(), 42))
	s.GetMember().CheckPriority(ctx)

	val, ok = getGaugeValue(re, "pd_server_member_leader_priority", map[string]string{
		"instance": instance,
	})
	re.True(ok)
	re.Equal(42.0, val)
}

func getGaugeValue(re *require.Assertions, metricName string, labels map[string]string) (float64, bool) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	re.NoError(err)
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for k, v := range labels {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if m.GetGauge() == nil {
				return 0, false
			}
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

