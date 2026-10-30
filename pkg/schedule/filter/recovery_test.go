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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/plan"
)

func TestRecoveryFreezesDiscretionaryRegionSelection(t *testing.T) {
	region := core.NewRegionInfo(&metapb.Region{Id: 1}, nil)
	for _, status := range []*pdpb.RegionRecoveryStatus{
		{State: pdpb.RegionRecoveryState_RECOVERING_LEADER},
		{DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_UNKNOWN},
		{DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL},
	} {
		blocked := region.Clone(core.WithRecoveryStatus(status))
		require.False(t, IsRegionHealthy(blocked))
		require.False(t, IsRegionHealthyAllowPending(blocked))
		require.Equal(t, plan.StatusCode(plan.StatusRegionRecovering), NewRegionDownFilter().Select(blocked).StatusCode)
	}
	require.True(t, IsRegionHealthy(region))
	require.True(t, NewRegionDownFilter().Select(region).IsOK())
}

func TestReaderProtocolCandidateFilterPreservesLegacyAndRetirement(t *testing.T) {
	legacy := core.NewStoreInfo(&metapb.Store{Id: 1})
	modern := legacy.Clone(core.SetStoreLogOnlyProtocolVersion(2))
	required := NewLogOnlyProtocolFilter("reader-floor", 2, false)
	require.False(t, required.Target(nil, legacy).IsOK())
	require.True(t, required.Target(nil, modern).IsOK())
	require.True(t, required.Source(nil, legacy).IsOK())
	require.True(t, NewLogOnlyProtocolFilter("legacy", 0, false).Target(nil, legacy).IsOK())
	dedicated := core.NewStoreInfoWithLabel(2, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	require.False(t, NewLogOnlyProtocolFilter("new-witness", 0, true).Target(nil, dedicated).IsOK())
}

func TestPlacementCandidatesHonorReaderFloorWithoutWitness(t *testing.T) {
	cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	for id := uint64(1); id <= 4; id++ {
		cluster.AddLeaderStore(id, 1)
	}
	cluster.AddLeaderRegion(1, 1, 2, 3)
	region := cluster.GetRegion(1).Clone(core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2}))
	for _, f := range []Filter{
		newRuleFitFilter("reader", cluster.GetBasicCluster(), cluster.GetRuleManager(), region, nil, 1),
		newRuleLeaderFitFilter("reader", cluster.GetBasicCluster(), cluster.GetRuleManager(), region, nil, 1, true),
		newRuleWitnessFitFilter("reader", cluster.GetBasicCluster(), cluster.GetRuleManager(), region, nil, 1),
	} {
		require.False(t, f.Target(cluster.GetSharedConfig(), cluster.GetStore(2)).IsOK())
	}
	region = region.Clone(core.WithRecoveryStatus(nil))
	f := newRuleLeaderFitFilter("legacy", cluster.GetBasicCluster(), cluster.GetRuleManager(), region, nil, 1, true)
	require.True(t, f.Target(cluster.GetSharedConfig(), cluster.GetStore(2)).IsOK())
}
